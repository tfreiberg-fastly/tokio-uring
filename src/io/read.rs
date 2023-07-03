use crate::buf::BoundedBufMut;
use crate::io::SharedFd;
use crate::BufResult;

use crate::runtime::driver::op::{Completable, CqeResult, Op};
use crate::runtime::CONTEXT;
use std::io;

pub(crate) struct Splice {
    /// Holds a strong ref to the input FD, preventing the file from being closed
    /// while the operation is in-flight.
    #[allow(dead_code)]
    fd_in: SharedFd,

    /// Holds a strong ref to the output FD, preventing the file from being closed
    /// while the operation is in-flight.
    #[allow(dead_code)]
    fd_out: SharedFd,
}

impl Op<Splice> {
    pub(crate) fn splice(
        fd_in: &SharedFd,
        off_in: i64,
        fd_out: &SharedFd,
        off_out: i64,
        len: u32,
    ) -> io::Result<Op<Splice>> {
        use io_uring::{opcode, types};

        CONTEXT.with(|x| {
            x.handle().expect("Not in a runtime context").submit_op(
                Splice {
                    fd_in: fd_in.clone(),
                    fd_out: fd_out.clone(),
                },
                |_| {
                    opcode::Splice::new(
                        types::Fd(fd_in.raw_fd()),
                        off_in,
                        types::Fd(fd_out.raw_fd()),
                        off_out,
                        len,
                    )
                    .build()
                },
            )
        })
    }
}

impl Completable for Splice {
    type Output = io::Result<u32>;

    fn complete(self, cqe: CqeResult) -> Self::Output {
        cqe.result
    }
}

pub(crate) struct Read<T> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    #[allow(dead_code)]
    fd: SharedFd,

    /// Reference to the in-flight buffer.
    pub(crate) buf: T,
}

impl<T: BoundedBufMut> Op<Read<T>> {
    pub(crate) fn read_at(fd: &SharedFd, buf: T, offset: u64) -> io::Result<Op<Read<T>>> {
        use io_uring::{opcode, types};

        CONTEXT.with(|x| {
            x.handle().expect("Not in a runtime context").submit_op(
                Read {
                    fd: fd.clone(),
                    buf,
                },
                |read| {
                    // Get raw buffer info
                    let ptr = read.buf.stable_mut_ptr();
                    let len = read.buf.bytes_total();
                    opcode::Read::new(types::Fd(fd.raw_fd()), ptr, len as _)
                        .offset(offset as _)
                        .build()
                },
            )
        })
    }
}

impl<T> Completable for Read<T>
where
    T: BoundedBufMut,
{
    type Output = BufResult<usize, T>;

    fn complete(self, cqe: CqeResult) -> Self::Output {
        // Convert the operation result to `usize`
        let res = cqe.result.map(|v| v as usize);
        // Recover the buffer
        let mut buf = self.buf;

        // If the operation was successful, advance the initialized cursor.
        if let Ok(n) = res {
            // Safety: the kernel wrote `n` bytes to the buffer.
            unsafe {
                buf.set_init(n);
            }
        }

        (res, buf)
    }
}
