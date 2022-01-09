#[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
use std::sync::{Mutex, PoisonError};

use std::{
    fs::{self, File},
    io,
    path::Path,
    sync::Arc,
};

#[cfg(unix)]
use std::os::unix::prelude::*;
#[cfg(wasi_ext)]
use std::os::wasi::prelude::*;
#[cfg(target_os = "windows")]
use std::os::windows::prelude::*;

use super::{ReadAt, WriteAt};

#[cfg(any(unix, target_os = "windows", wasi_ext))]
type FileRepr = File;

// If no platform-specific extension is available, we use a mutex to make sure
// operations (seek + read) are atomic.
#[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
type FileRepr = Mutex<File>;

/// A file with cross-platform positioned I/O.
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
        #[cfg(any(unix, target_os = "windows", wasi_ext))]
        {
            f(&self.0)
        }

        #[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
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
        #[cfg(any(unix, target_os = "windows", wasi_ext))]
        {
            self.0
        }

        #[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
        {
            self.0.into_inner().unwrap_or_else(PoisonError::into_inner)
        }
    }
}

impl ReadAt for RandomAccessFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        #[cfg(any(unix, wasi_ext))]
        {
            self.0.read_at(buf, offset)
        }

        #[cfg(target_os = "windows")]
        {
            self.0.seek_read(buf, offset)
        }

        #[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
        {
            use io::{Read, Seek};

            let file = &mut *self.0.lock().unwrap_or_else(PoisonError::into_inner);
            file.seek(io::SeekFrom::Start(offset))?;
            file.read(buf)
        }
    }

    #[cfg(any(unix, wasi_ext))]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.0.read_exact_at(buf, offset)
    }

    #[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        use io::{Read, Seek};

        let file = &mut *self.0.lock().unwrap_or_else(PoisonError::into_inner);
        file.seek(io::SeekFrom::Start(offset))?;
        file.read_exact(buf)
    }
}

impl WriteAt for RandomAccessFile {
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        #[cfg(any(unix, wasi_ext))]
        {
            self.0.write_at(buf, offset)
        }

        #[cfg(target_os = "windows")]
        {
            self.0.seek_write(buf, offset)
        }

        #[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
        {
            use io::{Seek, Write};

            let file = &mut *self.0.lock().unwrap_or_else(PoisonError::into_inner);
            file.seek(io::SeekFrom::Start(offset))?;
            file.write(buf)
        }
    }

    #[cfg(any(unix, wasi_ext))]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.0.write_all_at(buf, offset)
    }

    #[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        use io::{Seek, Write};

        let file = &mut *self.0.lock().unwrap_or_else(PoisonError::into_inner);
        file.seek(io::SeekFrom::Start(offset))?;
        file.write_all(buf)
    }
}

impl From<File> for RandomAccessFile {
    /// Creates a new `RandomAccessFile` from an open [`File`].
    #[inline]
    fn from(file: File) -> RandomAccessFile {
        #[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
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

#[cfg(any(unix, wasi_ext))]
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

#[cfg(any(unix, wasi_ext))]
impl FromRawFd for RandomAccessFile {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self::from(File::from_raw_fd(fd))
    }
}

#[cfg(target_os = "windows")]
impl FromRawHandle for RandomAccessFile {
    #[inline]
    unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        Self::from(File::from_raw_handle(handle))
    }
}

#[cfg(any(unix, wasi_ext))]
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

/// A file wrapper that is safe to use concurrently.
///
/// This wrapper exists because [`std::fs::File`] uses a single cursor, so
/// reading from a file concurrently will likely produce race conditions.
///
/// `SyncFile`s are cheap to clone and clones use distinct cursors, so they can
/// be used concurrently without issues.
#[derive(Debug, Clone)]
pub struct SyncFile {
    file: Arc<RandomAccessFile>,
    offset: u64,
}

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
        self.offset
    }
}

impl std::ops::Deref for SyncFile {
    type Target = RandomAccessFile;

    #[inline]
    fn deref(&self) -> &RandomAccessFile {
        &self.file
    }
}

impl ReadAt for SyncFile {
    #[inline]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.file.read_at(buf, offset)
    }

    #[inline]
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(buf, offset)
    }
}

impl WriteAt for SyncFile {
    #[inline]
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        self.file.write_at(buf, offset)
    }

    #[inline]
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.file.write_all_at(buf, offset)
    }
}

impl io::Read for SyncFile {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.read_at(buf, self.offset)?;
        self.offset += read as u64;
        Ok(read)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let ret = self.read_exact_at(buf, self.offset);
        if ret.is_ok() {
            self.offset += buf.len() as u64;
        }
        ret
    }
}

impl io::Seek for SyncFile {
    #[inline]
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.offset = match pos {
            io::SeekFrom::Start(p) => p,
            io::SeekFrom::Current(p) => {
                let (offset, overflowed) = self.offset.overflowing_add(p as u64);
                if overflowed ^ (p < 0) {
                    return Err(invalid_seek());
                }
                offset
            }
            io::SeekFrom::End(_) => self.file.with_file(|mut f| f.seek(pos))?,
        };

        Ok(self.offset)
    }

    #[inline]
    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.offset)
    }
}

impl io::Write for SyncFile {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.write_at(buf, self.offset)?;
        self.offset += written as u64;
        Ok(written)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let ret = self.write_all_at(buf, self.offset);
        if ret.is_ok() {
            self.offset += buf.len() as u64;
        }
        ret
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.file.with_file(|mut f| f.flush())
    }
}

impl From<File> for SyncFile {
    /// Creates a new `SyncFile` from an open [`File`].
    ///
    /// The cursor starts at the beginning of the file.
    #[inline]
    fn from(file: File) -> SyncFile {
        SyncFile {
            file: Arc::new(RandomAccessFile::from(file)),
            offset: 0,
        }
    }
}

impl From<RandomAccessFile> for SyncFile {
    /// Creates a new `SyncFile` from an open [`RandomAccessFile`].
    ///
    /// The cursor starts at the beginning of the file.
    #[inline]
    fn from(file: RandomAccessFile) -> SyncFile {
        SyncFile {
            file: Arc::new(file),
            offset: 0,
        }
    }
}

#[cfg(any(unix, wasi_ext))]
impl AsRawFd for SyncFile {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

#[cfg(target_os = "windows")]
impl AsRawHandle for SyncFile {
    #[inline]
    fn as_raw_handle(&self) -> RawHandle {
        self.file.as_raw_handle()
    }
}

#[cfg(any(unix, wasi_ext))]
impl FromRawFd for SyncFile {
    #[inline]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self::from(File::from_raw_fd(fd))
    }
}

#[cfg(target_os = "windows")]
impl FromRawHandle for SyncFile {
    #[inline]
    unsafe fn from_raw_handle(handle: RawHandle) -> Self {
        Self::from(File::from_raw_handle(handle))
    }
}

#[cold]
fn invalid_seek() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "invalid seek to a negative or overflowing position",
    )
}
