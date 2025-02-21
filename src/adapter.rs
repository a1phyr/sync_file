use crate::{ReadAt, WriteAt};
use std::io;

/// An adapter that implement `std::io` traits.
///
/// This type works by maintaining its own cursor.
#[derive(Debug, Clone, Copy)]
pub struct Adapter<T: ?Sized> {
    offset: u64,
    inner: T,
}

impl<T> Adapter<T> {
    /// Creates a new `Adapter`.
    #[inline]
    pub fn new(inner: T) -> Self {
        Self { offset: 0, inner }
    }

    /// Unwraps the inner stream.
    #[inline]
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T: ?Sized> Adapter<T> {
    /// Returns the offset used when reading the stream.
    ///
    /// This is equivalent to [`io::Seek::stream_position`] but does not use a
    /// fallible API nor require a mutable reference.
    #[must_use]
    #[inline]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Gets a reference to the underlying stream.
    #[inline]
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Gets a mutable reference to the underlying stream.
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T> ReadAt for Adapter<T>
where
    T: ReadAt + ?Sized,
{
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.inner.read_exact_at(buf, offset)
    }

    #[inline]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        self.inner.read_vectored_at(bufs, offset)
    }
}

impl<T> WriteAt for Adapter<T>
where
    T: WriteAt + ?Sized,
{
    #[inline]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        self.inner.write_at(buf, offset)
    }

    #[inline]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.inner.write_all_at(buf, offset)
    }

    #[inline]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        self.inner.write_vectored_at(bufs, offset)
    }

    #[inline]
    fn flush(&self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<T> io::Read for Adapter<T>
where
    T: ReadAt + ?Sized,
{
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read_at(buf, self.offset)?;
        self.offset += read as u64;
        Ok(read)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let ret = self.inner.read_exact_at(buf, self.offset);
        if ret.is_ok() {
            self.offset += buf.len() as u64;
        }
        ret
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        let read = self.inner.read_vectored_at(bufs, self.offset)?;
        self.offset += read as u64;
        Ok(read)
    }
}

impl<T> io::Write for Adapter<T>
where
    T: WriteAt + ?Sized,
{
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write_at(buf, self.offset)?;
        self.offset += written as u64;
        Ok(written)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let ret = self.inner.write_all_at(buf, self.offset);
        if ret.is_ok() {
            self.offset += buf.len() as u64;
        }
        ret
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let written = self.inner.write_vectored_at(bufs, self.offset)?;
        self.offset += written as u64;
        Ok(written)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<T> io::Seek for Adapter<T>
where
    T: ?Sized,
{
    /// Note: seeking to an offset relative to the end of a stream is unsupported.
    #[inline]
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.offset = match pos {
            io::SeekFrom::Start(p) => p,
            io::SeekFrom::Current(p) => {
                self.offset.checked_add_signed(p).ok_or_else(invalid_seek)?
            }
            io::SeekFrom::End(_) => return Err(unsupported()),
        };

        Ok(self.offset)
    }

    #[inline]
    fn rewind(&mut self) -> io::Result<()> {
        self.offset = 0;
        Ok(())
    }

    #[inline]
    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.offset)
    }
}

#[cold]
fn invalid_seek() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid seek to a negative or overflowing position",
    )
}

#[cold]
fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "unsupported seek to end of stream",
    )
}
