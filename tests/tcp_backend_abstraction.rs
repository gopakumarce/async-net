use async_net::{TcpListener, TcpStream};
use futures_lite::future;
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use futures_lite::stream::StreamExt;
use std::io;

#[test]
fn tcp_roundtrip_still_works() -> io::Result<()> {
    future::block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let server = async {
            let (mut stream, _) = listener.accept().await?;
            let mut buf = [0_u8; 4];
            stream.read_exact(&mut buf).await?;
            stream.write_all(&buf).await?;
            io::Result::Ok(())
        };

        let client = async {
            let mut stream = TcpStream::connect(addr).await?;
            stream.write_all(b"ping").await?;
            let mut buf = [0_u8; 4];
            stream.read_exact(&mut buf).await?;
            assert_eq!(&buf, b"ping");
            io::Result::Ok(())
        };

        let (server_res, client_res) = future::zip(server, client).await;
        server_res?;
        client_res?;
        Ok(())
    })
}

#[test]
fn tcp_incoming_stream_accepts_connections() -> io::Result<()> {
    future::block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let mut incoming = listener.incoming();

        let server = async {
            let mut stream = incoming
                .next()
                .await
                .expect("incoming stream should not end")?;
            let mut buf = [0_u8; 2];
            stream.read_exact(&mut buf).await?;
            stream.write_all(b"ok").await?;
            io::Result::Ok(())
        };

        let client = async {
            let mut stream = TcpStream::connect(addr).await?;
            stream.write_all(b"go").await?;
            let mut buf = [0_u8; 2];
            stream.read_exact(&mut buf).await?;
            assert_eq!(&buf, b"ok");
            io::Result::Ok(())
        };

        let (server_res, client_res) = future::zip(server, client).await;
        server_res?;
        client_res?;
        Ok(())
    })
}
