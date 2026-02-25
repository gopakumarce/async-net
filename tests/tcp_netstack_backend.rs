use async_io::Timer;
use async_net::{TcpListener, TcpStream};
use futures_lite::future;
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::mpsc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn parse_env_ipv4(key: &str, default: &str) -> io::Result<Ipv4Addr> {
    let value = std::env::var(key).unwrap_or_else(|_| default.into());
    value.parse::<Ipv4Addr>().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {key}: {value}"),
        )
    })
}

fn stack_ip() -> io::Result<Ipv4Addr> {
    parse_env_ipv4("ASYNC_NET_NETSTACK_STACK_IP", "10.200.0.2")
}

fn host_ip() -> io::Result<Ipv4Addr> {
    parse_env_ipv4("ASYNC_NET_NETSTACK_HOST_IP", "10.200.0.1")
}

async fn with_timeout<T, F>(dur: Duration, fut: F, label: &'static str) -> io::Result<T>
where
    F: std::future::Future<Output = io::Result<T>>,
{
    future::or(fut, async move {
        Timer::after(dur).await;
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("timeout waiting for {label}"),
        ))
    })
    .await
}

#[test]
fn netstack_backend_roundtrip() -> io::Result<()> {
    let _guard = TEST_LOCK
        .lock()
        .map_err(|_| io::Error::other("test lock poisoned"))?;

    future::block_on(async {
        let (listener, addr) = if cfg!(feature = "netstack-backend") {
            let ip = stack_ip()?;
            let addr = SocketAddr::from((ip, 18080));
            (TcpListener::bind(addr).await?, addr)
        } else {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            (listener, addr)
        };

        let (tx, rx) = mpsc::channel::<io::Result<()>>();
        thread::spawn({
            move || {
                let result = (|| -> io::Result<()> {
                    std::thread::sleep(Duration::from_millis(150));

                    let mut last_err: Option<io::Error> = None;
                    let mut sock_opt = None;
                    for _ in 0..30 {
                        match std::net::TcpStream::connect_timeout(
                            &addr,
                            Duration::from_millis(200),
                        ) {
                            Ok(sock) => {
                                sock_opt = Some(sock);
                                break;
                            }
                            Err(e) => {
                                last_err = Some(e);
                                std::thread::sleep(Duration::from_millis(50));
                            }
                        }
                    }

                    let mut socket = if let Some(sock) = sock_opt {
                        sock
                    } else {
                        return Err(last_err.unwrap_or_else(|| {
                            io::Error::new(io::ErrorKind::TimedOut, "client connect timeout")
                        }));
                    };

                    socket.set_read_timeout(Some(Duration::from_secs(3)))?;
                    socket.set_write_timeout(Some(Duration::from_secs(3)))?;

                    use std::io::{Read, Write};
                    socket.write_all(b"ping")?;
                    let mut buf = [0_u8; 4];
                    socket.read_exact(&mut buf)?;
                    if &buf != b"ping" {
                        return Err(io::Error::other("response payload mismatch"));
                    }
                    Ok(())
                })();

                let _ = tx.send(result);
            }
        });

        let (mut server, _) =
            with_timeout(Duration::from_secs(6), listener.accept(), "accept()").await?;
        let mut buf = [0_u8; 4];
        with_timeout(
            Duration::from_secs(6),
            server.read_exact(&mut buf),
            "server read",
        )
        .await?;
        with_timeout(
            Duration::from_secs(6),
            server.write_all(&buf),
            "server write",
        )
        .await?;

        match rx.recv_timeout(Duration::from_secs(6)) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timeout waiting for host client completion",
                ))
            }
        }

        Ok(())
    })
}

#[test]
fn netstack_backend_unsupported_ops() -> io::Result<()> {
    let _guard = TEST_LOCK
        .lock()
        .map_err(|_| io::Error::other("test lock poisoned"))?;

    future::block_on(async {
        let listener = if cfg!(feature = "netstack-backend") {
            let ip = stack_ip()?;
            TcpListener::bind(SocketAddr::from((ip, 18081))).await?
        } else {
            TcpListener::bind("127.0.0.1:0").await?
        };

        if cfg!(feature = "netstack-backend") {
            let ttl_err = listener
                .ttl()
                .expect_err("netstack mode should not support ttl yet");
            assert_eq!(ttl_err.kind(), io::ErrorKind::Unsupported);
        } else {
            let ttl = listener.ttl()?;
            listener.set_ttl(ttl)?;
        }

        Ok(())
    })
}

#[test]
fn netstack_backend_client_roundtrip() -> io::Result<()> {
    let _guard = TEST_LOCK
        .lock()
        .map_err(|_| io::Error::other("test lock poisoned"))?;

    let listen_addr = if cfg!(feature = "netstack-backend") {
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, 18082))
    } else {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 18082))
    };

    let target = if cfg!(feature = "netstack-backend") {
        SocketAddr::from((host_ip()?, 18082))
    } else {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 18082))
    };

    let (tx, rx) = mpsc::channel::<io::Result<()>>();
    thread::spawn(move || {
        let result = (|| -> io::Result<()> {
            let listener = std::net::TcpListener::bind(listen_addr)?;
            listener.set_nonblocking(false)?;
            let (mut conn, _) = listener.accept()?;
            conn.set_read_timeout(Some(Duration::from_secs(6)))?;
            conn.set_write_timeout(Some(Duration::from_secs(6)))?;

            use std::io::{Read, Write};
            let mut buf = [0_u8; 4];
            conn.read_exact(&mut buf)?;
            if &buf != b"pong" {
                return Err(io::Error::other("host server payload mismatch"));
            }
            conn.write_all(b"pong")?;
            Ok(())
        })();
        let _ = tx.send(result);
    });

    future::block_on(async {
        let mut client = with_timeout(
            Duration::from_secs(8),
            TcpStream::connect(target),
            "connect()",
        )
        .await?;
        with_timeout(
            Duration::from_secs(6),
            client.write_all(b"pong"),
            "client write",
        )
        .await?;

        let mut buf = [0_u8; 4];
        with_timeout(
            Duration::from_secs(6),
            client.read_exact(&mut buf),
            "client read",
        )
        .await?;

        if &buf != b"pong" {
            return Err(io::Error::other("client payload mismatch"));
        }

        Ok::<_, io::Error>(())
    })?;

    match rx.recv_timeout(Duration::from_secs(8)) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "timeout waiting for host server completion",
        )),
    }
}
