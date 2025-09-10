# Sync File

[![Crates.io](https://img.shields.io/crates/v/sync_file.svg)](https://crates.io/crates/sync_file)
[![Docs.rs](https://docs.rs/sync_file/badge.svg)](https://docs.rs/sync_file/)

Files that can be read concurrently.

`std::fs::File` is `Sync` but reading concurrently from it results in race
conditions, because the OS has a single cursor which is advanced and used
by several threads.

`SyncFile` solves this problem by using platform-specific extensions to do
positional I/O, so the cursor of the file is not shared.

## Example

```rust
use std::io::Read;
use sync_file::SyncFile;

/// Reads a file byte by byte.
/// Don't do this in real code!
fn read_all<R: Read>(mut file: R) -> std::io::Result<Vec<u8>> {
    let mut result = Vec::new();
    let mut buf = [0];

    while file.read(&mut buf)? != 0 {
        result.extend(&buf);
    }

    Ok(result)
}

// Open a file
let f = SyncFile::open("hello.txt")?;
let f_clone = f.clone();

// Read it concurrently
let thread = std::thread::spawn(move || read_all(f_clone));
let res1 = read_all(f)?;
let res2 = thread.join().unwrap()?;

// Both clones read the whole content
// This would not work with `std::fs::File`
assert_eq!(res1, b"Hello World!\n");
assert_eq!(res2, b"Hello World!\n");
```

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
