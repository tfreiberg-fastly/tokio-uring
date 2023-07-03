use std::{io::Write, os::fd::AsRawFd, path::Path};

use futures_util::future::join;
use tempfile::{tempdir, NamedTempFile};
use tokio_uring::{
    fs::File,
    net::{UnixListener, UnixStream},
};

const HELLO: &[u8] = b"hello world...";

#[test]
fn splice() {
    tokio_uring::start(async {
        let mut tempfile = NamedTempFile::new().unwrap();
        tempfile.write_all(HELLO).unwrap();
        let path = tempfile.path();
        let file = File::open(path).await.unwrap();

        let dir = tempdir().unwrap();
        let (send, recv) = pair(&dir.path().join("stream.sock")).await;

        join(
            async {
                file.splice(send.as_raw_fd(), 0, HELLO.len() as u32)
                    .await
                    .unwrap()
            },
            async {
                let buf = vec![0; HELLO.len()];
                let (r, buf) = recv.read(buf).await;
                r.unwrap();
                assert_eq!(buf, HELLO);
            },
        )
        .await;
    })
}

async fn pair(path: &Path) -> (UnixStream, UnixStream) {
    let listener = UnixListener::bind(path).unwrap();
    let (accepted, connected) = join(listener.accept(), UnixStream::connect(path)).await;
    (accepted.unwrap(), connected.unwrap())
}
