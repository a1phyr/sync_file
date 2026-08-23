use crate::{ReadAt, Size, WriteAt};
use std::io;

/// An adapter that limits the range of a `ReadAt` or `WriteAt`.
///
/// This can be obtained by calling [`Size::take_range`].
pub struct TakeRange<T: ?Sized> {
    from: u64,
    to: Option<u64>,
    inner: T,
}

impl<T> TakeRange<T> {
    pub(crate) fn new(inner: T, range: impl std::ops::RangeBounds<u64>) -> Self {
        let from = match range.start_bound() {
            std::ops::Bound::Included(from) => *from,
            std::ops::Bound::Excluded(from) => *from + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let to = match range.end_bound() {
            std::ops::Bound::Included(to) => to.checked_add(1),
            std::ops::Bound::Excluded(to) => Some(*to),
            std::ops::Bound::Unbounded => None,
        };

        Self { from, to, inner }
    }
}

impl<T: ?Sized> TakeRange<T> {
    fn range(&self, offset: u64, len: usize) -> (u64, usize) {
        let Some(real_offset) = self.from.checked_add(offset) else {
            return (u64::MAX, 0);
        };

        let real_len = match self.to {
            Some(to) => {
                let max_len = to.saturating_sub(real_offset);
                let max_len = max_len.try_into().unwrap_or(usize::MAX);
                std::cmp::min(len, max_len)
            }
            None => len,
        };

        (real_offset, real_len)
    }

    /// Gets a reference to the underlying stream.
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Gets a mutable reference to the underlying stream.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Consumes the `TakeRange`, returning the wrapped stream.
    pub fn into_inner(self) -> T
    where
        T: Sized,
    {
        self.inner
    }
}

impl<T: Size + ?Sized> Size for TakeRange<T> {
    fn size(&self) -> io::Result<u64> {
        let inner_size = self.inner.size()?;

        let end = match self.to {
            Some(to) => std::cmp::min(to, inner_size),
            None => inner_size,
        };

        Ok(end.saturating_sub(self.from))
    }
}

impl<T: ReadAt + ?Sized> ReadAt for TakeRange<T> {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let (offset, len) = self.range(offset, buf.len());
        if len == 0 {
            return Ok(0);
        }

        self.inner.read_at(&mut buf[..len], offset)
    }
}

impl<T: WriteAt + ?Sized> WriteAt for TakeRange<T> {
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        let (offset, len) = self.range(offset, buf.len());
        if len == 0 {
            return Ok(0);
        }

        self.inner.write_at(&buf[..len], offset)
    }
}
