//! Files that can be read concurrently.
//!
//! [`std::fs::File`] is `Sync` but reading concurrently from it results in race
//! conditions, because the OS has a single cursor which is advanced and used
//! by several threads.
//!
//! [`SyncFile`] solves this problem by using platform-specific extensions to
//! do positional I/O, so the cursor of the file is not shared. Note that
//! writing concurrently at the same position in a file can still result in race
//! conditions, but only on the content, not the position.
//!
//! This library also exposes platform-independant fonctions for positional I/O.
//!
//! # Example
//!
//! ```
//! use std::io::Read;
//! use sync_file::SyncFile;
//! # use std::io::Write;
//! # let mut f = SyncFile::create("hello.txt")?;
//! # f.write_all(b"Hello World!\n")?;
//! # drop(f);
//!
//! /// Reads a file byte by byte.
//! /// Don't do this in real code !
//! fn read_all<R: Read>(mut file: R) -> std::io::Result<Vec<u8>> {
//!     let mut result = Vec::new();
//!     let mut buf = [0];
//!
//!     while file.read(&mut buf)? != 0 {
//!         result.extend(&buf);
//!     }
//!
//!     Ok(result)
//! }
//!
//! // Open a file
//! let f = SyncFile::open("hello.txt")?;
//! let f_clone = f.clone();
//!
//! // Read it concurrently
//! let thread = std::thread::spawn(move || read_all(f_clone));
//! let res1 = read_all(f)?;
//! let res2 = thread.join().unwrap()?;
//!
//! // Both clones read the whole content
//! // This would not work with `std::fs::File`
//! assert_eq!(res1, b"Hello World!\n");
//! assert_eq!(res2, b"Hello World!\n");
//!
//! # std::fs::remove_file("hello.txt")?;
//! # Ok::<_, std::io::Error>(())
//! ```
//!
//! # OS support
//!
//! Windows, Unix and `wasip1` targets provide extensions for positional I/O,
//! so on these targets `SyncFile` is zero-cost.
//!
//! If platform-specific extensions are not available, `SyncFile` fallbacks to a
//! mutex.

#![warn(missing_docs)]

mod adapter;
mod file;

pub use adapter::Adapter;
pub use file::{RandomAccessFile, SyncFile};

use std::{cmp::min, convert::TryInto, io};

/// The `ReadAt` trait allows for reading bytes from a source at a given offset.
///
/// Additionally, the methods of this trait only require a shared reference,
/// which makes it ideal for parallel use.
pub trait ReadAt {
    /// Reads a number of bytes starting from a given offset.
    ///
    /// Returns the number of bytes read.
    ///
    /// Note that similar to [`io::Read::read`], it is not an error to return with
    /// a short read.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    /// Reads the exact number of byte required to fill buf from the given
    /// offset.
    ///
    /// # Errors
    ///
    /// If this function encounters an error of the kind
    /// [`io::ErrorKind::Interrupted`] then the error is ignored and the
    /// operation will continue.
    ///
    /// If this function encounters an “end of file” before completely filling
    /// the buffer, it returns an error of the kind
    /// [`io::ErrorKind::UnexpectedEof`]. The contents of buf are unspecified
    /// in this case.
    ///
    /// If any other read error is encountered then this function immediately
    /// returns. The contents of buf are unspecified in this case.
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match self.read_at(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    buf = &mut buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if buf.is_empty() {
            Ok(())
        } else {
            Err(fill_buffer_error())
        }
    }

    /// Like `read_at`, except that it reads into a slice of buffers.
    ///
    /// Data is copied to fill each buffer in order, with the final buffer
    /// written to possibly being only partially filled. This method must behave
    /// equivalently to a single call to read with concatenated buffers.
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        let buf = bufs
            .iter_mut()
            .find(|b| !b.is_empty())
            .map_or(&mut [][..], |b| &mut **b);
        self.read_at(buf, offset)
    }
}

impl ReadAt for [u8] {
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let read = (|| {
            let offset = offset.try_into().ok()?;
            let this = self.get(offset..)?;
            let len = min(this.len(), buf.len());

            buf[..len].copy_from_slice(&this[..len]);
            Some(len)
        })();

        Ok(read.unwrap_or(0))
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (|| {
            let offset = offset.try_into().ok()?;
            let this = self.get(offset..)?;
            let len = buf.len();
            (this.len() >= len).then(|| buf.copy_from_slice(&this[..len]))
        })()
        .ok_or_else(fill_buffer_error)
    }
}

impl<const N: usize> ReadAt for [u8; N] {
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.as_ref().read_at(buf, offset)
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.as_ref().read_exact_at(buf, offset)
    }

    #[inline]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        self.as_ref().read_vectored_at(bufs, offset)
    }
}

impl ReadAt for Vec<u8> {
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        (**self).read_at(buf, offset)
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }

    #[inline]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        (**self).read_vectored_at(bufs, offset)
    }
}

impl ReadAt for std::borrow::Cow<'_, [u8]> {
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        (**self).read_at(buf, offset)
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }

    #[inline]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        (**self).read_vectored_at(bufs, offset)
    }
}

impl<R> ReadAt for &R
where
    R: ReadAt + ?Sized,
{
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        (**self).read_at(buf, offset)
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }

    #[inline]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        (**self).read_vectored_at(bufs, offset)
    }
}

impl<R> ReadAt for Box<R>
where
    R: ReadAt + ?Sized,
{
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        (**self).read_at(buf, offset)
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }

    #[inline]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        (**self).read_vectored_at(bufs, offset)
    }
}

impl<R> ReadAt for std::sync::Arc<R>
where
    R: ReadAt + ?Sized,
{
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        (**self).read_at(buf, offset)
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }

    #[inline]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        (**self).read_vectored_at(bufs, offset)
    }
}

impl<R> ReadAt for std::rc::Rc<R>
where
    R: ReadAt + ?Sized,
{
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        (**self).read_at(buf, offset)
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        (**self).read_exact_at(buf, offset)
    }

    #[inline]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        (**self).read_vectored_at(bufs, offset)
    }
}

impl<T> ReadAt for io::Cursor<T>
where
    T: AsRef<[u8]>,
{
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.get_ref().as_ref().read_at(buf, offset)
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.get_ref().as_ref().read_exact_at(buf, offset)
    }

    #[inline]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        self.get_ref().as_ref().read_vectored_at(bufs, offset)
    }
}

impl ReadAt for io::Empty {
    #[inline]
    fn read_at(&self, _buf: &mut [u8], _offset: u64) -> io::Result<usize> {
        Ok(0)
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], _offset: u64) -> io::Result<()> {
        if buf.is_empty() {
            Ok(())
        } else {
            Err(fill_buffer_error())
        }
    }

    #[inline]
    fn read_vectored_at(&self, _: &mut [io::IoSliceMut<'_>], _: u64) -> io::Result<usize> {
        Ok(0)
    }
}

/// The `WriteAt` trait allows for writing bytes to a source at a given offset.
///
/// Additionally, the methods of this trait only require a shared reference,
/// which makes it ideal for parallel use.
pub trait WriteAt {
    /// Writes a number of bytes starting from a given offset.
    ///
    /// Returns the number of bytes written.
    ///
    /// Note that similar to [`io::Write::write`], it is not an error to return a
    /// short write.
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize>;

    /// Attempts to write an entire buffer starting from a given offset.
    ///
    /// # Errors
    ///
    /// This function will return the first error of
    /// non-[`io::ErrorKind::Interrupted`] kind that `write_at` returns.
    fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match self.write_at(buf, offset) {
                Ok(0) => {
                    return Err(write_buffer_error());
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Like `write_at`, except that it writes from a slice of buffers.
    ///
    /// Data is copied from each buffer in order, with the final buffer read
    /// from possibly being only partially consumed. This method must behave as
    /// a call to `write_at` with the buffers concatenated would.
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        let buf = bufs
            .iter()
            .find(|b| !b.is_empty())
            .map_or(&[][..], |b| &**b);
        self.write_at(buf, offset)
    }

    /// Flush this output stream, ensuring that all intermediately buffered
    /// contents reach their destination.
    ///
    /// # Errors
    ///
    /// It is considered an error if not all bytes could be written due to I/O
    /// errors or EOF being reached.
    #[inline]
    fn flush(&self) -> io::Result<()> {
        Ok(())
    }
}

impl<W> WriteAt for &W
where
    W: WriteAt + ?Sized,
{
    #[inline]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        (**self).write_at(buf, offset)
    }

    #[inline]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        (**self).write_all_at(buf, offset)
    }

    #[inline]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        (**self).write_vectored_at(bufs, offset)
    }

    #[inline]
    fn flush(&self) -> io::Result<()> {
        (**self).flush()
    }
}

impl<W> WriteAt for Box<W>
where
    W: WriteAt + ?Sized,
{
    #[inline]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        (**self).write_at(buf, offset)
    }

    #[inline]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        (**self).write_all_at(buf, offset)
    }

    #[inline]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        (**self).write_vectored_at(bufs, offset)
    }

    #[inline]
    fn flush(&self) -> io::Result<()> {
        (**self).flush()
    }
}

impl<W> WriteAt for std::sync::Arc<W>
where
    W: WriteAt + ?Sized,
{
    #[inline]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        (**self).write_at(buf, offset)
    }

    #[inline]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        (**self).write_all_at(buf, offset)
    }

    #[inline]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        (**self).write_vectored_at(bufs, offset)
    }

    #[inline]
    fn flush(&self) -> io::Result<()> {
        (**self).flush()
    }
}

impl<W> WriteAt for std::rc::Rc<W>
where
    W: WriteAt + ?Sized,
{
    #[inline]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        (**self).write_at(buf, offset)
    }

    #[inline]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        (**self).write_all_at(buf, offset)
    }

    #[inline]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        (**self).write_vectored_at(bufs, offset)
    }

    #[inline]
    fn flush(&self) -> io::Result<()> {
        (**self).flush()
    }
}

impl WriteAt for io::Sink {
    #[inline]
    fn write_at(&self, buf: &[u8], _offset: u64) -> io::Result<usize> {
        Ok(buf.len())
    }

    #[inline]
    fn write_all_at(&self, _buf: &[u8], _offset: u64) -> io::Result<()> {
        Ok(())
    }

    #[inline]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], _offset: u64) -> io::Result<usize> {
        Ok(bufs.iter().map(|b| b.len()).sum())
    }
}

/// The `Size` trait allows for getting the size of a stream.
pub trait Size {
    /// Returns the size of the stream.
    fn size(&self) -> io::Result<u64>;
}

impl Size for [u8] {
    #[inline]
    fn size(&self) -> io::Result<u64> {
        Ok(self.len() as u64)
    }
}

impl<const N: usize> Size for [u8; N] {
    #[inline]
    fn size(&self) -> io::Result<u64> {
        Ok(self.len() as u64)
    }
}

impl Size for Vec<u8> {
    #[inline]
    fn size(&self) -> io::Result<u64> {
        Ok(self.len() as u64)
    }
}

impl Size for std::borrow::Cow<'_, [u8]> {
    #[inline]
    fn size(&self) -> io::Result<u64> {
        Ok(self.len() as u64)
    }
}

impl<T> Size for io::Cursor<T>
where
    T: AsRef<[u8]>,
{
    #[inline]
    fn size(&self) -> io::Result<u64> {
        Ok(self.get_ref().as_ref().len() as u64)
    }
}

impl<T: Size + ?Sized> Size for &'_ T {
    #[inline]
    fn size(&self) -> io::Result<u64> {
        (**self).size()
    }
}

impl<T: Size + ?Sized> Size for Box<T> {
    #[inline]
    fn size(&self) -> io::Result<u64> {
        (**self).size()
    }
}

impl<T: Size + ?Sized> Size for std::sync::Arc<T> {
    #[inline]
    fn size(&self) -> io::Result<u64> {
        (**self).size()
    }
}

impl<T: Size + ?Sized> Size for std::rc::Rc<T> {
    #[inline]
    fn size(&self) -> io::Result<u64> {
        (**self).size()
    }
}

impl Size for io::Empty {
    #[inline]
    fn size(&self) -> io::Result<u64> {
        Ok(0)
    }
}

#[cold]
fn fill_buffer_error() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer")
}

#[cold]
fn write_buffer_error() -> io::Error {
    io::Error::new(io::ErrorKind::WriteZero, "failed to write whole buffer")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::prelude::*;

    #[test]
    fn smoke_test() {
        let mut f = SyncFile::open("LICENSE-APACHE").unwrap();
        let mut buf = [0; 9];
        f.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"Copyright");
        assert_eq!(f.stream_position().unwrap(), 9);
        assert_eq!(f.seek(io::SeekFrom::Current(-2)).unwrap(), 7);
        f.read_exact(&mut buf[..2]).unwrap();
        assert_eq!(&buf[..2], b"ht");
        assert!(f.seek(io::SeekFrom::Current(-10)).is_err());
    }

    #[test]
    fn read_to_string() {
        let mut f = SyncFile::open("LICENSE-APACHE").unwrap();

        f.seek(io::SeekFrom::End(-54)).unwrap();
        let mut buf = String::new();
        f.read_to_string(&mut buf).unwrap();
        assert_eq!(buf.len(), 54);
        assert_eq!(buf.capacity(), 54);
        assert!(buf.contains("limitations under the License."));
    }
}
