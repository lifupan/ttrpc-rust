// Copyright (c) 2019 Ant Financial
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use nix::fcntl::OFlag;
use nix::sys::select::{select, FdSet};
use nix::sys::socket::{self, *};
use nix::unistd::close;
use nix::unistd::pipe2;
use protobuf::{CodedInputStream, Message};
use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

use crate::common::{self, MESSAGE_TYPE_REQUEST};
use crate::error::{get_status, Error, Result};
use crate::sync::channel::{read_message, write_message};
use crate::ttrpc::{Code, Request, Response};
use crate::MessageHeader;
use crate::{response_to_channel, MethodHandler, TtrpcContext};

// poll_queue will create WAIT_THREAD_COUNT_DEFAULT threads in begin.
// If wait thread count < WAIT_THREAD_COUNT_MIN, create number to WAIT_THREAD_COUNT_DEFAULT.
// If wait thread count > WAIT_THREAD_COUNT_MAX, wait thread will quit to WAIT_THREAD_COUNT_DEFAULT.
const DEFAULT_WAIT_THREAD_COUNT_DEFAULT: usize = 3;
const DEFAULT_WAIT_THREAD_COUNT_MIN: usize = 1;
const DEFAULT_WAIT_THREAD_COUNT_MAX: usize = 5;

type MessageSender = Sender<(MessageHeader, Vec<u8>)>;
type MessageReceiver = Receiver<(MessageHeader, Vec<u8>)>;

pub struct Server {
    listeners: Vec<RawFd>,
    monitor_fd: (RawFd, RawFd),
    quit: Arc<AtomicBool>,
    connections: Arc<Mutex<HashMap<RawFd, Connection>>>,
    methods: Arc<HashMap<String, Box<dyn MethodHandler + Send + Sync>>>,
    handler: Option<JoinHandle<()>>,
    thread_count_default: usize,
    thread_count_min: usize,
    thread_count_max: usize,
}

struct Connection {
    fd: RawFd,
    quit: Arc<AtomicBool>,
    handler: Option<JoinHandle<()>>,
}

impl Connection {
    fn close(&self) {
        self.quit.store(true, Ordering::SeqCst);
        // in case the connection had closed
        socket::shutdown(self.fd, Shutdown::Read).unwrap_or(());
    }
}

struct ThreadS<'a> {
    fd: RawFd,
    fdlock: &'a Arc<Mutex<()>>,
    wtc: &'a Arc<AtomicUsize>,
    quit: &'a Arc<AtomicBool>,
    methods: &'a Arc<HashMap<String, Box<dyn MethodHandler + Send + Sync>>>,
    res_tx: &'a MessageSender,
    control_tx: &'a SyncSender<()>,
    default: usize,
    min: usize,
    max: usize,
}

fn start_method_handler_thread(
    fd: RawFd,
    fdlock: Arc<Mutex<()>>,
    wtc: Arc<AtomicUsize>,
    quit: Arc<AtomicBool>,
    methods: Arc<HashMap<String, Box<dyn MethodHandler + Send + Sync>>>,
    res_tx: MessageSender,
    control_tx: SyncSender<()>,
    min: usize,
    max: usize,
) {
    thread::spawn(move || {
        while !quit.load(Ordering::SeqCst) {
            let c = wtc.fetch_add(1, Ordering::SeqCst) + 1;
            if c > max {
                wtc.fetch_sub(1, Ordering::SeqCst);
                break;
            }

            let result;
            {
                let _guard = fdlock.lock().unwrap();
                if quit.load(Ordering::SeqCst) {
                    // notify the connection dealing main thread to stop.
                    control_tx
                        .try_send(())
                        .unwrap_or_else(|err| warn!("Failed to try send {:?}", err));
                    break;
                }
                result = read_message(fd);
            }

            if quit.load(Ordering::SeqCst) {
                // notify the connection dealing main thread to stop.
                control_tx
                    .try_send(())
                    .unwrap_or_else(|err| warn!("Failed to try send {:?}", err));
                break;
            }

            let c = wtc.fetch_sub(1, Ordering::SeqCst) - 1;
            if c < min {
                control_tx
                    .try_send(())
                    .unwrap_or_else(|err| warn!("Failed to try send {:?}", err));
            }

            let mh;
            let buf;
            match result {
                Ok((x, y)) => {
                    mh = x;
                    buf = y;
                }
                Err(x) => match x {
                    Error::Socket(y) => {
                        trace!("Socket error {}", y);
                        quit.store(true, Ordering::SeqCst);
                        // the client connection would be closed and
                        // the connection dealing main thread would
                        // have exited.
                        control_tx
                            .try_send(())
                            .unwrap_or_else(|err| warn!("Failed to try send {:?}", err));
                        break;
                    }
                    _ => {
                        trace!("Others error {:?}", x);
                        continue;
                    }
                },
            }

            if mh.type_ != MESSAGE_TYPE_REQUEST {
                continue;
            }
            let mut s = CodedInputStream::from_bytes(&buf);
            let mut req = Request::new();
            if let Err(x) = req.merge_from(&mut s) {
                let status = get_status(Code::INVALID_ARGUMENT, x.to_string());
                let mut res = Response::new();
                res.set_status(status);
                if let Err(x) = response_to_channel(mh.stream_id, res, res_tx.clone()) {
                    debug!("response_to_channel get error {:?}", x);
                    quit.store(true, Ordering::SeqCst);
                    // the client connection would be closed and
                    // the connection dealing main thread would have
                    // exited.
                    control_tx
                        .try_send(())
                        .unwrap_or_else(|err| warn!("Failed to try send {:?}", err));
                    break;
                }
                continue;
            }
            trace!("Got Message request {:?}", req);

            let path = format!("/{}/{}", req.service, req.method);
            let method;
            if let Some(x) = methods.get(&path) {
                method = x;
            } else {
                let status = get_status(Code::INVALID_ARGUMENT, format!("{} does not exist", path));
                let mut res = Response::new();
                res.set_status(status);
                if let Err(x) = response_to_channel(mh.stream_id, res, res_tx.clone()) {
                    info!("response_to_channel get error {:?}", x);
                    quit.store(true, Ordering::SeqCst);
                    // the client connection would be closed and
                    // the connection dealing main thread would have
                    // exited.
                    control_tx
                        .try_send(())
                        .unwrap_or_else(|err| warn!("Failed to try send {:?}", err));
                    break;
                }
                continue;
            }
            let ctx = TtrpcContext {
                fd,
                mh,
                res_tx: res_tx.clone(),
            };
            if let Err(x) = method.handler(ctx, req) {
                debug!("method handle {} get error {:?}", path, x);
                quit.store(true, Ordering::SeqCst);
                // the client connection would be closed and
                // the connection dealing main thread would have
                // exited.
                control_tx
                    .try_send(())
                    .unwrap_or_else(|err| warn!("Failed to try send {:?}", err));
                break;
            }
        }
    });
}

fn start_method_handler_threads(num: usize, ts: &ThreadS) {
    for _ in 0..num {
        if ts.quit.load(Ordering::SeqCst) {
            break;
        }
        start_method_handler_thread(
            ts.fd,
            ts.fdlock.clone(),
            ts.wtc.clone(),
            ts.quit.clone(),
            ts.methods.clone(),
            ts.res_tx.clone(),
            ts.control_tx.clone(),
            ts.min,
            ts.max,
        );
    }
}

fn check_method_handler_threads(ts: &ThreadS) {
    let c = ts.wtc.load(Ordering::SeqCst);
    if c < ts.min {
        start_method_handler_threads(ts.default - c, &ts);
    }
}

impl Default for Server {
    fn default() -> Self {
        let (rfd, wfd) = pipe2(OFlag::O_CLOEXEC).unwrap();
        Server {
            listeners: Vec::with_capacity(1),
            monitor_fd: (rfd, wfd),
            quit: Arc::new(AtomicBool::new(false)),
            connections: Arc::new(Mutex::new(HashMap::new())),
            methods: Arc::new(HashMap::new()),
            handler: None,
            thread_count_default: DEFAULT_WAIT_THREAD_COUNT_DEFAULT,
            thread_count_min: DEFAULT_WAIT_THREAD_COUNT_MIN,
            thread_count_max: DEFAULT_WAIT_THREAD_COUNT_MAX,
        }
    }
}

impl Server {
    pub fn new() -> Server {
        Server::default()
    }

    pub fn bind(mut self, host: &str) -> Result<Server> {
        if !self.listeners.is_empty() {
            return Err(Error::Others(
                "ttrpc-rust just support 1 host now".to_string(),
            ));
        }

        let (fd, _) = common::do_bind(host)?;
        self.listeners.push(fd);

        Ok(self)
    }

    pub fn add_listener(mut self, fd: RawFd) -> Result<Server> {
        self.listeners.push(fd);

        Ok(self)
    }

    pub fn register_service(
        mut self,
        methods: HashMap<String, Box<dyn MethodHandler + Send + Sync>>,
    ) -> Server {
        let mut_methods = Arc::get_mut(&mut self.methods).unwrap();
        mut_methods.extend(methods);
        self
    }

    pub fn set_thread_count_default(mut self, count: usize) -> Server {
        self.thread_count_default = count;
        self
    }

    pub fn set_thread_count_min(mut self, count: usize) -> Server {
        self.thread_count_min = count;
        self
    }

    pub fn set_thread_count_max(mut self, count: usize) -> Server {
        self.thread_count_max = count;
        self
    }

    pub fn start(&mut self) -> Result<()> {
        if self.thread_count_default >= self.thread_count_max {
            return Err(Error::Others(
                "thread_count_default should smaller than thread_count_max".to_string(),
            ));
        }
        if self.thread_count_default <= self.thread_count_min {
            return Err(Error::Others(
                "thread_count_default should biger than thread_count_min".to_string(),
            ));
        }

        let connections = self.connections.clone();

        if self.listeners.is_empty() {
            return Err(Error::Others("ttrpc-rust not bind".to_string()));
        }

        let listener = self.listeners[0];

        let methods = self.methods.clone();
        let default = self.thread_count_default;
        let min = self.thread_count_min;
        let max = self.thread_count_max;
        let service_quit = self.quit.clone();
        let monitor_fd = self.monitor_fd.0;

        common::do_listen(listener)?;

        let handler = thread::Builder::new()
            .name("listener_loop".into())
            .spawn(move || {
                let (reaper_tx, reaper_rx) = channel();
                let reaper_connections = connections.clone();

                let reaper = thread::Builder::new()
                    .name("reaper".into())
                    .spawn(move || {
                        for fd in reaper_rx.iter() {
                            reaper_connections
                                .lock()
                                .unwrap()
                                .remove(&fd)
                                .map(|mut cn| {
                                    cn.handler.take().map(|handler| handler.join().unwrap())
                                });
                        }
                    })
                    .unwrap();

                loop {
                    if service_quit.load(Ordering::SeqCst) {
                        break;
                    }

                    let mut fd_set = FdSet::new();
                    fd_set.insert(listener);
                    fd_set.insert(monitor_fd);

                    match select(
                        Some(fd_set.highest().unwrap() + 1),
                        &mut fd_set,
                        None,
                        None,
                        None,
                    ) {
                        Ok(_) => (),
                        Err(e) => {
                            if e == nix::Error::from(nix::errno::Errno::EINTR) {
                                continue;
                            } else {
                                break;
                            }
                        }
                    }

                    if fd_set.contains(monitor_fd) || !fd_set.contains(listener) {
                        continue;
                    }

                    if service_quit.load(Ordering::SeqCst) {
                        break;
                    }

                    let fd = match accept4(listener, SockFlag::SOCK_CLOEXEC) {
                        Ok(fd) => fd,
                        Err(_e) => break,
                    };

                    let methods = methods.clone();
                    let quit = Arc::new(AtomicBool::new(false));
                    let child_quit = quit.clone();
                    let reaper_tx_child = reaper_tx.clone();

                    let handler = thread::Builder::new()
                        .name("client_handler".into())
                        .spawn(move || {
                            debug!("Got new client");
                            // Start response thread
                            let quit_res = child_quit.clone();
                            let (res_tx, res_rx): (MessageSender, MessageReceiver) = channel();
                            let handler = thread::spawn(move || {
                                for r in res_rx.iter() {
                                    info!("response thread get {:?}", r);
                                    if let Err(e) = write_message(fd, r.0, r.1) {
                                        info!("write_message got {:?}", e);
                                        quit_res.store(true, Ordering::SeqCst);
                                        break;
                                    }
                                }

                                trace!("response thread quit");
                            });

                            let (control_tx, control_rx): (SyncSender<()>, Receiver<()>) =
                                sync_channel(0);
                            let ts = ThreadS {
                                fd,
                                fdlock: &Arc::new(Mutex::new(())),
                                wtc: &Arc::new(AtomicUsize::new(0)),
                                methods: &methods,
                                res_tx: &res_tx,
                                control_tx: &control_tx,
                                quit: &child_quit,
                                default,
                                min,
                                max,
                            };
                            start_method_handler_threads(ts.default, &ts);

                            while !child_quit.load(Ordering::SeqCst) {
                                check_method_handler_threads(&ts);
                                if control_rx.recv().is_err() {
                                    break;
                                }
                            }

                            // drop the res_tx, thus the res_rx would get terminated notification.
                            drop(res_tx);
                            handler.join().unwrap_or(());
                            close(fd).unwrap_or(());
                            reaper_tx_child.send(fd).unwrap();

                            info!("client thread quit");
                        })
                        .unwrap();

                    let mut cns = connections.lock().unwrap();
                    cns.insert(
                        fd,
                        Connection {
                            fd,
                            handler: Some(handler),
                            quit: quit.clone(),
                        },
                    );
                } // end loop

                // notify reaper thread to exit.
                drop(reaper_tx);
                reaper.join().unwrap();
                info!("ttrpc server stopped");
            })
            .unwrap();

        self.handler = Some(handler);

        Ok(())
    }

    pub fn shutdown(mut self) {
        let connections = self.connections.lock().unwrap();

        self.quit.store(true, Ordering::SeqCst);
        close(self.monitor_fd.1).unwrap_or_else(|e| {
            warn!(
                "failed to close notify fd: {} with error: {}",
                self.monitor_fd.1, e
            )
        });

        for (_fd, c) in connections.iter() {
            c.close();
        }

        // release connections's lock, since the following handler.join()
        // would wait on the other thread's exit in which would take the lock.
        drop(connections);

        if let Some(handler) = self.handler.take() {
            handler.join().unwrap();
        }
    }
}
