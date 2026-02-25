#[cfg(not(feature = "netstack-backend"))]
mod imp {
    use std::io::{self, IoSlice, Read as _, Write as _};
    use std::net::{Shutdown, SocketAddr};
    #[cfg(unix)]
    use std::os::unix::io::{AsFd, AsRawFd, BorrowedFd, RawFd};
    #[cfg(windows)]
    use std::os::windows::io::{AsRawSocket, AsSocket, BorrowedSocket, RawSocket};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use async_io::Async;
    use futures_lite::{prelude::*, ready};

    pub(crate) type ReadableOwned = async_io::ReadableOwned<std::net::TcpStream>;
    pub(crate) type WritableOwned = async_io::WritableOwned<std::net::TcpStream>;

    #[derive(Clone, Debug)]
    pub(crate) struct TcpListener {
        inner: Arc<Async<std::net::TcpListener>>,
    }

    impl TcpListener {
        pub(crate) fn bind(addr: SocketAddr) -> io::Result<Self> {
            Ok(Self {
                inner: Arc::new(Async::<std::net::TcpListener>::bind(addr)?),
            })
        }

        pub(crate) fn from_async(listener: Async<std::net::TcpListener>) -> Self {
            Self {
                inner: Arc::new(listener),
            }
        }

        pub(crate) fn from_std(listener: std::net::TcpListener) -> io::Result<Self> {
            Ok(Self::from_async(Async::new(listener)?))
        }

        pub(crate) fn into_async_arc(self) -> Arc<Async<std::net::TcpListener>> {
            self.inner
        }

        pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
            self.inner.get_ref().local_addr()
        }

        pub(crate) async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
            let (stream, addr) = self.inner.accept().await?;
            Ok((TcpStream::from_async(stream), addr))
        }

        pub(crate) fn incoming(&self) -> Pin<Box<dyn Stream<Item = io::Result<TcpStream>> + '_>> {
            Box::pin(
                self.inner
                    .incoming()
                    .map(|res| res.map(TcpStream::from_async)),
            )
        }

        pub(crate) fn ttl(&self) -> io::Result<u32> {
            self.inner.get_ref().ttl()
        }

        pub(crate) fn set_ttl(&self, ttl: u32) -> io::Result<()> {
            self.inner.get_ref().set_ttl(ttl)
        }

        #[cfg(unix)]
        pub(crate) fn as_raw_fd(&self) -> RawFd {
            self.inner.as_raw_fd()
        }

        #[cfg(unix)]
        pub(crate) fn as_fd(&self) -> BorrowedFd<'_> {
            self.inner.get_ref().as_fd()
        }

        #[cfg(windows)]
        pub(crate) fn as_raw_socket(&self) -> RawSocket {
            self.inner.as_raw_socket()
        }

        #[cfg(windows)]
        pub(crate) fn as_socket(&self) -> BorrowedSocket<'_> {
            self.inner.get_ref().as_socket()
        }
    }

    #[derive(Debug)]
    pub(crate) struct TcpStream {
        inner: Arc<Async<std::net::TcpStream>>,
    }

    impl Clone for TcpStream {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }

    impl TcpStream {
        pub(crate) fn from_async(stream: Async<std::net::TcpStream>) -> Self {
            Self {
                inner: Arc::new(stream),
            }
        }

        pub(crate) fn from_std(stream: std::net::TcpStream) -> io::Result<Self> {
            Ok(Self::from_async(Async::new(stream)?))
        }

        pub(crate) fn into_async_arc(self) -> Arc<Async<std::net::TcpStream>> {
            self.inner
        }

        pub(crate) async fn connect(addr: SocketAddr) -> io::Result<Self> {
            let stream = Async::<std::net::TcpStream>::connect(addr).await?;
            Ok(Self::from_async(stream))
        }

        pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
            self.inner.get_ref().local_addr()
        }

        pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
            self.inner.get_ref().peer_addr()
        }

        pub(crate) fn shutdown(&self, how: Shutdown) -> io::Result<()> {
            self.inner.get_ref().shutdown(how)
        }

        pub(crate) async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
            self.inner.peek(buf).await
        }

        pub(crate) fn nodelay(&self) -> io::Result<bool> {
            self.inner.get_ref().nodelay()
        }

        pub(crate) fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
            self.inner.get_ref().set_nodelay(nodelay)
        }

        pub(crate) fn ttl(&self) -> io::Result<u32> {
            self.inner.get_ref().ttl()
        }

        pub(crate) fn set_ttl(&self, ttl: u32) -> io::Result<()> {
            self.inner.get_ref().set_ttl(ttl)
        }

        #[cfg(unix)]
        pub(crate) fn as_raw_fd(&self) -> RawFd {
            self.inner.as_raw_fd()
        }

        #[cfg(unix)]
        pub(crate) fn as_fd(&self) -> BorrowedFd<'_> {
            self.inner.get_ref().as_fd()
        }

        #[cfg(windows)]
        pub(crate) fn as_raw_socket(&self) -> RawSocket {
            self.inner.as_raw_socket()
        }

        #[cfg(windows)]
        pub(crate) fn as_socket(&self) -> BorrowedSocket<'_> {
            self.inner.get_ref().as_socket()
        }

        pub(crate) fn poll_read(
            &self,
            cx: &mut Context<'_>,
            readable: &mut Option<ReadableOwned>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            loop {
                match self.inner.get_ref().read(buf) {
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                    res => {
                        *readable = None;
                        return Poll::Ready(res);
                    }
                }

                if readable.is_none() {
                    *readable = Some(self.inner.clone().readable_owned());
                }

                if let Some(waiter) = readable {
                    let res = ready!(Pin::new(waiter).poll(cx));
                    *readable = None;
                    res?;
                }
            }
        }

        pub(crate) fn poll_write(
            &self,
            cx: &mut Context<'_>,
            writable: &mut Option<WritableOwned>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            loop {
                match self.inner.get_ref().write(buf) {
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                    res => {
                        *writable = None;
                        return Poll::Ready(res);
                    }
                }

                if writable.is_none() {
                    *writable = Some(self.inner.clone().writable_owned());
                }

                if let Some(waiter) = writable {
                    let res = ready!(Pin::new(waiter).poll(cx));
                    *writable = None;
                    res?;
                }
            }
        }

        pub(crate) fn poll_flush(
            &self,
            cx: &mut Context<'_>,
            writable: &mut Option<WritableOwned>,
        ) -> Poll<io::Result<()>> {
            loop {
                match self.inner.get_ref().flush() {
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                    res => {
                        *writable = None;
                        return Poll::Ready(res);
                    }
                }

                if writable.is_none() {
                    *writable = Some(self.inner.clone().writable_owned());
                }

                if let Some(waiter) = writable {
                    let res = ready!(Pin::new(waiter).poll(cx));
                    *writable = None;
                    res?;
                }
            }
        }

        pub(crate) fn poll_write_vectored(
            &self,
            cx: &mut Context<'_>,
            writable: &mut Option<WritableOwned>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            loop {
                match self.inner.get_ref().write_vectored(bufs) {
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                    res => {
                        *writable = None;
                        return Poll::Ready(res);
                    }
                }

                if writable.is_none() {
                    *writable = Some(self.inner.clone().writable_owned());
                }

                if let Some(waiter) = writable {
                    let res = ready!(Pin::new(waiter).poll(cx));
                    *writable = None;
                    res?;
                }
            }
        }
    }
}

#[cfg(feature = "netstack-backend")]
mod imp {
    use std::cell::RefCell;
    use std::env;
    use std::fmt;
    use std::io::{self, IoSlice};
    use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr};
    use std::sync::{Arc, Mutex};
    use std::{pin::Pin, task::Context, task::Poll};

    use futures_lite::prelude::*;
    use netstack::{
        init as netstack_init, AsyncTcpListener as NetTcpListener, AsyncTcpStream as NetTcpStream,
        NetStack,
    };

    pub(crate) type ReadableOwned = ();
    pub(crate) type WritableOwned = ();

    #[derive(Clone)]
    struct NetstackRuntime {
        stack: Arc<NetStack>,
        stack_ip: Ipv4Addr,
    }

    thread_local! {
        static RUNTIME: RefCell<Option<NetstackRuntime>> = const { RefCell::new(None) };
    }
    static RUNTIME_INIT_LOCK: Mutex<()> = Mutex::new(());

    fn parse_env_ipv4(key: &str, default: &str) -> io::Result<Ipv4Addr> {
        let value = env::var(key).unwrap_or_else(|_| default.to_string());
        value.parse::<Ipv4Addr>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid IPv4 in {key}: {value}"),
            )
        })
    }

    fn parse_env_i32(key: &str, default: i32) -> io::Result<i32> {
        let value = env::var(key).unwrap_or_else(|_| default.to_string());
        value.parse::<i32>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid integer in {key}: {value}"),
            )
        })
    }

    fn runtime() -> io::Result<NetstackRuntime> {
        if let Some(runtime) = RUNTIME.with(|rt| rt.borrow().clone()) {
            return Ok(runtime);
        }

        let _guard = RUNTIME_INIT_LOCK
            .lock()
            .map_err(|_| io::Error::other("netstack runtime init lock poisoned"))?;

        if let Some(runtime) = RUNTIME.with(|rt| rt.borrow().clone()) {
            return Ok(runtime);
        }

        let unit = parse_env_i32("ASYNC_NET_NETSTACK_UNIT", 10)?;
        let host_ip = parse_env_ipv4("ASYNC_NET_NETSTACK_HOST_IP", "10.200.0.1")?;
        let stack_ip = parse_env_ipv4("ASYNC_NET_NETSTACK_STACK_IP", "10.200.0.2")?;
        let netmask = parse_env_ipv4("ASYNC_NET_NETSTACK_NETMASK", "255.255.255.0")?;

        let runtime = NetstackRuntime {
            // Ensure core netstack globals are initialized before socket APIs.
            // NetStack::new() configures TUN/interface but does not call netstack_init().
            stack: {
                netstack_init().map_err(io::Error::other)?;
                Arc::new(NetStack::new(
                    unit,
                    IpAddr::V4(host_ip),
                    IpAddr::V4(stack_ip),
                    IpAddr::V4(netmask),
                )?)
            },
            stack_ip,
        };

        RUNTIME.with(|rt| {
            *rt.borrow_mut() = Some(runtime.clone());
        });

        Ok(runtime)
    }

    fn unsupported(msg: &'static str) -> io::Error {
        io::Error::new(io::ErrorKind::Unsupported, msg)
    }

    fn poll_once() {
        if let Some(runtime) = RUNTIME.with(|rt| rt.borrow().clone()) {
            let _ = runtime.stack.poll_once();
        }
    }

    #[derive(Clone)]
    pub(crate) struct TcpListener {
        inner: Arc<NetTcpListener>,
        local_addr: SocketAddr,
    }

    impl fmt::Debug for TcpListener {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("TcpListener")
                .field("local_addr", &self.local_addr)
                .finish_non_exhaustive()
        }
    }

    impl TcpListener {
        pub(crate) fn bind(addr: SocketAddr) -> io::Result<Self> {
            let _rt = runtime()?;
            let listener = NetTcpListener::bind(addr)?;
            Ok(Self {
                inner: Arc::new(listener),
                local_addr: addr,
            })
        }

        pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(self.local_addr)
        }

        pub(crate) async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
            let stream = futures_lite::future::poll_fn(|cx| match self.inner.poll_accept(cx) {
                Poll::Ready(res) => Poll::Ready(res),
                Poll::Pending => {
                    poll_once();
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await?;

            let peer = SocketAddr::from(([0, 0, 0, 0], 0));
            Ok((
                TcpStream {
                    inner: Arc::new(stream),
                    local_addr: self.local_addr,
                    peer_addr: peer,
                },
                peer,
            ))
        }

        pub(crate) fn incoming(&self) -> Pin<Box<dyn Stream<Item = io::Result<TcpStream>> + '_>> {
            let local_addr = self.local_addr;
            let listener = self.inner.clone();
            Box::pin(futures_lite::stream::poll_fn(move |cx| {
                match listener.poll_accept(cx) {
                    Poll::Ready(Ok(stream)) => {
                        let peer = SocketAddr::from(([0, 0, 0, 0], 0));
                        Poll::Ready(Some(Ok(TcpStream {
                            inner: Arc::new(stream),
                            local_addr,
                            peer_addr: peer,
                        })))
                    }
                    Poll::Pending => {
                        poll_once();
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                }
            }))
        }

        pub(crate) fn ttl(&self) -> io::Result<u32> {
            Err(unsupported(
                "IP_TTL is not supported by netstack backend yet",
            ))
        }

        pub(crate) fn set_ttl(&self, _ttl: u32) -> io::Result<()> {
            Err(unsupported(
                "IP_TTL is not supported by netstack backend yet",
            ))
        }
    }

    #[derive(Clone)]
    pub(crate) struct TcpStream {
        inner: Arc<NetTcpStream>,
        local_addr: SocketAddr,
        peer_addr: SocketAddr,
    }

    impl fmt::Debug for TcpStream {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("TcpStream")
                .field("local_addr", &self.local_addr)
                .field("peer_addr", &self.peer_addr)
                .finish_non_exhaustive()
        }
    }

    impl TcpStream {
        pub(crate) async fn connect(addr: SocketAddr) -> io::Result<Self> {
            let rt = runtime()?;
            let stream = NetTcpStream::connect(addr)?;
            futures_lite::future::poll_fn(|cx| match stream.poll_wait_connected(cx) {
                Poll::Ready(res) => Poll::Ready(res),
                Poll::Pending => {
                    poll_once();
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await?;

            Ok(Self {
                inner: Arc::new(stream),
                local_addr: SocketAddr::from((rt.stack_ip, 0)),
                peer_addr: addr,
            })
        }

        pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(self.local_addr)
        }

        pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
            Ok(self.peer_addr)
        }

        pub(crate) fn shutdown(&self, how: Shutdown) -> io::Result<()> {
            match how {
                Shutdown::Write | Shutdown::Both => {
                    self.inner.shutdown_write();
                    Ok(())
                }
                Shutdown::Read => Err(unsupported(
                    "read-half shutdown is not supported by netstack backend yet",
                )),
            }
        }

        pub(crate) async fn peek(&self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(unsupported("peek is not supported by netstack backend yet"))
        }

        pub(crate) fn nodelay(&self) -> io::Result<bool> {
            self.inner.nodelay()
        }

        pub(crate) fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
            self.inner.set_nodelay(nodelay)
        }

        pub(crate) fn ttl(&self) -> io::Result<u32> {
            Err(unsupported(
                "IP_TTL is not supported by netstack backend yet",
            ))
        }

        pub(crate) fn set_ttl(&self, _ttl: u32) -> io::Result<()> {
            Err(unsupported(
                "IP_TTL is not supported by netstack backend yet",
            ))
        }

        pub(crate) fn poll_read(
            &self,
            cx: &mut Context<'_>,
            _readable: &mut Option<ReadableOwned>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            match self.inner.poll_recv(cx, buf) {
                Poll::Pending => {
                    poll_once();
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Ready(res) => Poll::Ready(res),
            }
        }

        pub(crate) fn poll_write(
            &self,
            cx: &mut Context<'_>,
            _writable: &mut Option<WritableOwned>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            match self.inner.poll_send(cx, buf) {
                Poll::Pending => {
                    poll_once();
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Poll::Ready(res) => Poll::Ready(res),
            }
        }

        pub(crate) fn poll_flush(
            &self,
            _cx: &mut Context<'_>,
            _writable: &mut Option<WritableOwned>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        pub(crate) fn poll_write_vectored(
            &self,
            cx: &mut Context<'_>,
            writable: &mut Option<WritableOwned>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            if let Some(buf) = bufs.iter().find(|b| !b.is_empty()) {
                self.poll_write(cx, writable, buf)
            } else {
                Poll::Ready(Ok(0))
            }
        }
    }
}

pub(crate) use imp::*;
