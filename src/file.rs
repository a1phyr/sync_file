#[cfg(not(any(
    unix,
    target_os = "windows",
    all(target_os = "wasi", target_env = "p1")
)))]
use std::sync::{Mutex, PoisonError};

use std::{
    fmt,
    fs::{self, File},
    io,
    path::Path,
    sync::Arc,
};

#[cfg(unix)]
use std::os::unix::prelude::*;
#[cfg(all(target_os = "wasi", target_env = "p1"))]
use std::os::wasi::prelude::*;
#[cfg(target_os = "windows")]
use std::os::windows::prelude::*;

use crate::Adapter;

use super::{ReadAt, WriteAt};

#[cfg(all(target_os = "wasi", target_env = "p1"))]
trait FileExt {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize>;

    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize>;

    fn write_at(&self, buffer: &[u8], offset: u64) -> io::Result<usize>;

    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize>;
}

#[cfg(all(target_os = "wasi", target_env = "p1"))]
impl FileExt for File {
    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        unsafe {
            let raw = self.as_raw_fd() as wasi::Fd;

            let iovec = [wasi::Iovec {
                buf: buffer.as_mut_ptr(),
                buf_len: buffer.len(),
            }];

            wasi::fd_pread(raw, &iovec, offset)
                .map_err(|err| io::Error::from_raw_os_error(err.raw() as _))
        }
    }

    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        unsafe {
            let raw = self.as_raw_fd() as wasi::Fd;
            let iovec = std::mem::transmute(bufs);

            wasi::fd_pread(raw, iovec, offset)
                .map_err(|err| io::Error::from_raw_os_error(err.raw() as _))
        }
    }

    fn write_at(&self, buffer: &[u8], offset: u64) -> io::Result<usize> {
        unsafe {
            let raw = self.as_raw_fd() as wasi::Fd;

            let iovec = [wasi::Ciovec {
                buf: buffer.as_ptr(),
                buf_len: buffer.len(),
            }];

            wasi::fd_pwrite(raw, &iovec, offset)
                .map_err(|err| io::Error::from_raw_os_error(err.raw() as _))
        }
    }

    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        unsafe {
            let raw = self.as_raw_fd() as wasi::Fd;
            let iovec = std::mem::transmute(bufs);

            wasi::fd_pwrite(raw, iovec, offset)
                .map_err(|err| io::Error::from_raw_os_error(err.raw() as _))
        }
    }
}

#[cfg(any(
    unix,
    target_os = "windows",
    all(target_os = "wasi", target_env = "p1")
))]
type FileRepr = File;

// If no platform-specific extension is available, we use a mutex to make sure
// operations (seek + read) are atomic.
#[cfg(not(any(
    unix,
    target_os = "windows",
    all(target_os = "wasi", target_env = "p1")
)))]
type FileRepr = Mutex<File>;

/// A file with cross-platform positioned I/O.
///
/// Reading from this file or writing to it does not use its internal OS cursor,
/// but it may move it anyway. This can cause surprising behaviour if shared
/// with a [`File`] (this could be done with `try_clone`).
#[derive(Debug)]
pub struct RandomAccessFile(FileRepr);

impl RandomAccessFile {
    /// Attempts to open a file in read-only mode.
    ///
    /// See [`File::open`] for details.
    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<RandomAccessFile> {
        let f = File::open(path.as_ref())?;
        Ok(RandomAccessFile::from(f))
    }

    /// Opens a file in write-only mode.
    ///
    /// See [`File::create`] for details.
    #[inline]
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<RandomAccessFile> {
        let f = File::create(path.as_ref())?;
        Ok(RandomAccessFile::from(f))
    }

    #[inline]
    pub(crate) fn with_file<T>(&self, f: impl FnOnce(&File) -> T) -> T {
        #[cfg(any(
            unix,
            target_os = "windows",
            all(target_os = "wasi", target_env = "p1")
        ))]
        {
            f(&self.0)
        }

        #[cfg(not(any(
            unix,
            target_os = "windows",
            all(target_os = "wasi", target_env = "p1")
        )))]
        {
            f(&self.0.lock().unwrap_or_else(PoisonError::into_inner))
        }
    }

    /// Attempts to sync all OS-internal metadata to disk.
    ///
    /// See [`File::sync_all`] for details.
    #[inline]
    pub fn sync_all(&self) -> io::Result<()> {
        self.with_file(|f| f.sync_all())
    }

    /// This function is similar to `sync_all`, except that it may not
    /// synchronize file metadata to the filesystem.
    ///
    /// See [`File::sync_data`] for details.
    #[inline]
    pub fn sync_data(&self) -> io::Result<()> {
        self.with_file(|f| f.sync_data())
    }

    /// Truncates or extends the underlying file, updating the size of this file
    /// to become `size`.
    ///
    /// See [`File::set_len`] for details.
    #[inline]
    pub fn set_len(&self, size: u64) -> io::Result<()> {
        self.with_file(|f| f.set_len(size))
    }

    /// Queries metadata about the underlying file.
    ///
    /// See [`File::metadata`] for details.
    #[inline]
    pub fn metadata(&self) -> io::Result<fs::Metadata> {
        self.with_file(|f| f.metadata())
    }

    /// Creates a new `File` instance that shares the same underlying file handle
    /// as the existing `File` instance
    ///
    /// See [`File::try_clone`] for details.
    #[inline]
    pub fn try_clone(&self) -> io::Result<RandomAccessFile> {
        let file = self.with_file(|f| f.try_clone())?;
        Ok(RandomAccessFile::from(file))
    }

    /// Changes the permissions on the underlying file.
    ///
    /// See [`File::set_permissions`] for details.
    #[inline]
    pub fn set_permissions(&self, perm: fs::Permissions) -> io::Result<()> {
        self.with_file(|f| f.set_permissions(perm))
    }

    /// Unwraps the inner [`File`].
    ///
    /// The file's cursor position is unspecified.
    #[inline]
    pub fn into_inner(self) -> File {
        #[cfg(any(
            unix,
            target_os = "windows",
            all(target_os = "wasi", target_env = "p1")
        ))]
        {
            self.0
        }

        #[cfg(not(any(
            unix,
            target_os = "windows",
            all(target_os = "wasi", target_env = "p1")
        )))]
        {
            self.0.into_inner().unwrap_or_else(PoisonError::into_inner)
        }
    }
}

impl ReadAt for RandomAccessFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        #[cfg(any(unix, all(target_os = "wasi", target_env = "p1")))]
        {
            self.0.read_at(buf, offset)
        }

        #[cfg(target_os = "windows")]
        {
            self.0.seek_read(buf, offset)
        }

        #[cfg(not(any(
            unix,
            target_os = "windows",
            all(target_os = "wasi", target_env = "p1")
        )))]
        {
            use io::{Read, Seek};

            let file = &mut *self.0.lock().unwrap_or_else(PoisonError::into_inner);
            file.seek(io::SeekFrom::Start(offset))?;
            file.read(buf)
        }
    }

    #[cfg(unix)]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.0.read_exact_at(buf, offset)
    }

    #[cfg(not(any(
        unix,
        target_os = "windows",
        all(target_os = "wasi", target_env = "p1")
    )))]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        use io::{Read, Seek};

        let file = &mut *self.0.lock().unwrap_or_else(PoisonError::into_inner);
        file.seek(io::SeekFrom::Start(offset))?;
        file.read_exact(buf)
    }

    #[cfg(all(target_os = "wasi", target_env = "p1"))]
    #[inline]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        self.0.read_vectored_at(bufs, offset)
    }

    #[cfg(not(any(
        unix,
        target_os = "windows",
        all(target_os = "wasi", target_env = "p1")
    )))]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        use io::{Read, Seek};

        let file = &mut *self.0.lock().unwrap_or_else(PoisonError::into_inner);
        file.seek(io::SeekFrom::Start(offset))?;
        file.read_vectored(bufs)
    }
}

impl WriteAt for RandomAccessFile {
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        #[cfg(any(unix, all(target_os = "wasi", target_env = "p1")))]
        {
            self.0.write_at(buf, offset)
        }

        #[cfg(target_os = "windows")]
        {
            self.0.seek_write(buf, offset)
        }

        #[cfg(not(any(
            unix,
            target_os = "windows",
            all(target_os = "wasi", target_env = "p1")
        )))]
        {
            use io::{Seek, Write};

            let file = &mut *self.0.lock().unwrap_or_else(PoisonError::into_inner);
            file.seek(io::SeekFrom::Start(offset))?;
            file.write(buf)
        }
    }

    #[cfg(unix)]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.0.write_all_at(buf, offset)
    }

    #[cfg(not(any(
        unix,
        target_os = "windows",
        all(target_os = "wasi", target_env = "p1")
    )))]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        use io::{Seek, Write};

        let file = &mut *self.0.lock().unwrap_or_else(PoisonError::into_inner);
        file.seek(io::SeekFrom::Start(offset))?;
        file.write_all(buf)
    }

    #[cfg(all(target_os = "wasi", target_env = "p1"))]
    #[inline]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        self.0.write_vectored_at(bufs, offset)
    }

    #[cfg(not(any(
        unix,
        target_os = "windows",
        all(target_os = "wasi", target_env = "p1")
    )))]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        use io::{Seek, Write};

        let file = &mut *self.0.lock().unwrap_or_else(PoisonError::into_inner);
        file.seek(io::SeekFrom::Start(offset))?;
        file.write_vectored(bufs)
    }

    #[inline]
    fn flush(&self) -> io::Result<()> {
        use std::io::Write;
        self.with_file(|mut f| f.flush())
    }
}

impl From<File> for RandomAccessFile {
    /// Creates a new `RandomAccessFile` from an open [`File`].
    #[inline]
    fn from(file: File) -> RandomAccessFile {
        #[cfg(not(any(
            unix,
            target_os = "windows",
            all(target_os = "wasi", target_env = "p1")
        )))]
        let file = Mutex::new(file);

        RandomAccessFile(file)
    }
}

impl From<RandomAccessFile> for File {
    #[inline]
    fn from(file: RandomAccessFile) -> File {
        file.into_inner()
    }
}

#[cfg(any(unix, all(target_os = "wasi", target_env = "p1")))]
impl AsRawFd for RandomAccessFile {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

#[cfg(target_os = "windows")]
impl AsRawHandle for RandomAccessFile {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        self.0.as_raw_handle()
    }
}

#[cfg(any(unix, all(target_os = "wasi", target_env = "p1")))]
impl AsFd for RandomAccessFile {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

#[cfg(target_os = "windows")]
impl AsHandle for RandomAccessFile {
    #[inline]
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.0.as_handle()
    }
}

#[cfg(any(unix, all(target_os = "wasi", target_env = "p1")))]
impl FromRawFd for RandomAccessFile {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { Self::from(File::from_raw_fd(fd)) }
    }
}

#[cfg(target_os = "windows")]
impl FromRawHandle for RandomAccessFile {
    #[inline]
    unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        unsafe { Self::from(File::from_raw_handle(handle)) }
    }
}

#[cfg(any(unix, all(target_os = "wasi", target_env = "p1")))]
impl From<OwnedFd> for RandomAccessFile {
    #[inline]
    fn from(fd: OwnedFd) -> Self {
        Self::from(File::from(fd))
    }
}

#[cfg(target_os = "windows")]
impl From<OwnedHandle> for RandomAccessFile {
    #[inline]
    fn from(handle: OwnedHandle) -> Self {
        Self::from(File::from(handle))
    }
}

#[cfg(any(unix, all(target_os = "wasi", target_env = "p1")))]
impl IntoRawFd for RandomAccessFile {
    #[inline]
    fn into_raw_fd(self) -> RawFd {
        self.0.into_raw_fd()
    }
}

#[cfg(target_os = "windows")]
impl IntoRawHandle for RandomAccessFile {
    #[inline]
    fn into_raw_handle(self) -> RawHandle {
        self.0.into_raw_handle()
    }
}

#[cfg(any(unix, all(target_os = "wasi", target_env = "p1")))]
impl From<RandomAccessFile> for OwnedFd {
    #[inline]
    fn from(f: RandomAccessFile) -> Self {
        f.0.into()
    }
}

#[cfg(target_os = "windows")]
impl From<RandomAccessFile> for OwnedHandle {
    #[inline]
    fn from(f: RandomAccessFile) -> Self {
        f.0.into()
    }
}

/// A file wrapper that is safe to use concurrently.
///
/// This wrapper exists because [`std::fs::File`] uses a single cursor, so
/// reading from a file concurrently will likely produce race conditions.
///
/// `SyncFile`s are cheap to clone and clones use distinct cursors, so they can
/// be used concurrently without issues.
#[derive(Clone)]
pub struct SyncFile(Adapter<Arc<RandomAccessFile>>);

impl SyncFile {
    /// Attempts to open a file in read-only mode.
    ///
    /// See [`File::open`] for details.
    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<SyncFile> {
        let f = File::open(path.as_ref())?;
        Ok(SyncFile::from(f))
    }

    /// Opens a file in write-only mode.
    ///
    /// See [`File::create`] for details.
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<SyncFile> {
        let f = File::create(path.as_ref())?;
        Ok(SyncFile::from(f))
    }

    /// Returns the offset used when reading the file.
    ///
    /// This is equivalent to [`io::Seek::stream_position`] but does not use a
    /// fallible API nor require a mutable reference.
    #[must_use]
    pub fn offset(&self) -> u64 {
        self.0.offset()
    }
}

impl std::ops::Deref for SyncFile {
    type Target = RandomAccessFile;

    #[inline]
    fn deref(&self) -> &RandomAccessFile {
        self.0.get_ref()
    }
}

impl ReadAt for SyncFile {
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.0.read_at(buf, offset)
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.0.read_exact_at(buf, offset)
    }

    #[inline]
    fn read_vectored_at(&self, bufs: &mut [io::IoSliceMut<'_>], offset: u64) -> io::Result<usize> {
        self.0.read_vectored_at(bufs, offset)
    }
}

impl WriteAt for SyncFile {
    #[inline]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        self.0.write_at(buf, offset)
    }

    #[inline]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.0.write_all_at(buf, offset)
    }

    #[inline]
    fn write_vectored_at(&self, bufs: &[io::IoSlice<'_>], offset: u64) -> io::Result<usize> {
        self.0.write_vectored_at(bufs, offset)
    }

    #[inline]
    fn flush(&self) -> io::Result<()> {
        self.0.flush()
    }
}

impl io::Read for SyncFile {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.0.read_exact(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.0.read_vectored(bufs)
    }
}

impl io::Seek for SyncFile {
    #[inline]
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let pos = match pos {
            // Override `Adapter`'s implementation to support seeking to the end of file.
            io::SeekFrom::End(_) => {
                let offset = self.0.get_ref().with_file(|mut f| f.seek(pos))?;
                io::SeekFrom::Start(offset)
            }
            pos => pos,
        };

        self.0.seek(pos)
    }

    #[inline]
    fn rewind(&mut self) -> io::Result<()> {
        self.0.rewind()
    }

    #[inline]
    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.offset())
    }
}

impl io::Write for SyncFile {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.0.write_all(buf)
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.0.write_vectored(bufs)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl From<File> for SyncFile {
    /// Creates a new `SyncFile` from an open [`File`].
    ///
    /// The cursor starts at the beginning of the file.
    #[inline]
    fn from(file: File) -> SyncFile {
        SyncFile::from(RandomAccessFile::from(file))
    }
}

impl From<RandomAccessFile> for SyncFile {
    /// Creates a new `SyncFile` from an open [`RandomAccessFile`].
    ///
    /// The cursor starts at the beginning of the file.
    #[inline]
    fn from(file: RandomAccessFile) -> SyncFile {
        SyncFile(Adapter::new(Arc::new(file)))
    }
}

#[cfg(any(unix, all(target_os = "wasi", target_env = "p1")))]
impl AsRawFd for SyncFile {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.0.get_ref().as_raw_fd()
    }
}

#[cfg(target_os = "windows")]
impl AsRawHandle for SyncFile {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        self.0.get_ref().as_raw_handle()
    }
}

#[cfg(any(unix, all(target_os = "wasi", target_env = "p1")))]
impl AsFd for SyncFile {
    #[inline]
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.get_ref().as_fd()
    }
}

#[cfg(target_os = "windows")]
impl AsHandle for SyncFile {
    #[inline]
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.0.get_ref().as_handle()
    }
}

#[cfg(any(unix, all(target_os = "wasi", target_env = "p1")))]
impl FromRawFd for SyncFile {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        unsafe { Self::from(File::from_raw_fd(fd)) }
    }
}

#[cfg(target_os = "windows")]
impl FromRawHandle for SyncFile {
    #[inline]
    unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        unsafe { Self::from(File::from_raw_handle(handle)) }
    }
}

#[cfg(any(unix, all(target_os = "wasi", target_env = "p1")))]
impl From<OwnedFd> for SyncFile {
    #[inline]
    fn from(fd: OwnedFd) -> Self {
        Self::from(File::from(fd))
    }
}

#[cfg(target_os = "windows")]
impl From<OwnedHandle> for SyncFile {
    #[inline]
    fn from(handle: OwnedHandle) -> Self {
        Self::from(File::from(handle))
    }
}

impl fmt::Debug for SyncFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncFile")
            .field("file", self.0.get_ref())
            .field("offset", &self.offset())
            .finish()
    }
}
