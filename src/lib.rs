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
//! # Example
//!
//! ```
//! use std::io::Read;
//! # use std::io::Write;
//! use sync_file::SyncFile;
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
//! # let mut f = SyncFile::create("hello.txt")?;
//! # f.write_all(b"Hello World!\n")?;
//! # drop(f);
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
//! Windows and Unix targets provide extensions for positional I/O, so on these
//! targets `SyncFile` is zero-cost. Wasi also provide these but only but only
//! with a nightly compiler.
//!
//! If platform-specific extensions are not available, `SyncFile` fallbacks to a
//! mutex.

#![cfg_attr(wasi_ext, feature(wasi_ext))]
#![warn(missing_docs)]

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

#[cfg(target_os = "windows")]
use std::os::windows::prelude::*;

#[cfg(wasi_ext)]
use std::os::wasi::prelude::*;

#[cfg(any(unix, target_os = "windows", wasi_ext))]
type FileRepr = Arc<File>;

// If no platform-specific extension is available, we use a mutex to make sure
// operations are atomic.
#[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
type FileRepr = Arc<Mutex<File>>;

/// A file wrapper that is safe to use concurrently.
///
/// This wrapper exists because [`std::fs::File`] uses a single cursor, so
/// reading from a file concurrently will likely produce race conditions.
///
/// `SyncFile`s are cheap to clone and clones use distinct cursors, so they can
/// be used concurrently without issues.
#[derive(Clone, Debug)]
pub struct SyncFile {
    file: FileRepr,
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

    #[inline]
    fn with_file<T>(&self, f: impl FnOnce(&File) -> T) -> T {
        #[cfg(any(unix, target_os = "windows", wasi_ext))]
        {
            f(&self.file)
        }

        #[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
        {
            f(&self.file.lock().unwrap_or_else(PoisonError::into_inner))
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
}

impl io::Read for SyncFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        #[cfg(any(unix, all(target_os = "wasi", wasi_ext)))]
        {
            let read = self.file.read_at(buf, self.offset)?;
            self.offset += read as u64;
            Ok(read)
        }

        #[cfg(target_os = "windows")]
        {
            let read = self.file.seek_read(buf, self.offset)?;
            self.offset += read as u64;
            Ok(read)
        }

        #[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
        {
            use io::Seek;

            let file = &mut *self.file.lock().unwrap_or_else(PoisonError::into_inner);
            file.seek(io::SeekFrom::Start(self.offset))?;
            let read = file.read(buf)?;
            self.offset += read as u64;
            Ok(read)
        }
    }
}

impl io::Seek for SyncFile {
    #[inline]
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.offset = match pos {
            io::SeekFrom::Start(p) => p,
            io::SeekFrom::Current(p) => {
                let offset = self.offset as i64 + p;
                if offset >= 0 {
                    offset as u64
                } else {
                    Err(io::ErrorKind::InvalidInput)?
                }
            }
            io::SeekFrom::End(_) => self.with_file(|mut f| f.seek(pos))?,
        };

        Ok(self.offset)
    }

    #[inline]
    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.offset)
    }
}

impl io::Write for SyncFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        #[cfg(any(unix, all(target_os = "wasi", wasi_ext)))]
        {
            let written = self.file.write_at(buf, self.offset)?;
            self.offset += written as u64;
            Ok(written)
        }

        #[cfg(target_os = "windows")]
        {
            let written = self.file.seek_write(buf, self.offset)?;
            self.offset += written as u64;
            Ok(written)
        }

        #[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
        {
            use io::Seek;

            let file = &mut *self.file.lock().unwrap_or_else(PoisonError::into_inner);
            file.seek(io::SeekFrom::Start(self.offset))?;
            let written = file.write(buf)?;
            self.offset += written as u64;
            Ok(written)
        }
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.with_file(|mut f| f.flush())
    }
}

impl From<File> for SyncFile {
    /// Creates a new `SyncFile` from an open [`File`].
    #[inline]
    fn from(file: File) -> SyncFile {
        #[cfg(any(unix, target_os = "windows", wasi_ext))]
        let file = Arc::new(file);

        #[cfg(not(any(unix, target_os = "windows", wasi_ext)))]
        let file = Arc::new(Mutex::new(file));

        SyncFile { file, offset: 0 }
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
