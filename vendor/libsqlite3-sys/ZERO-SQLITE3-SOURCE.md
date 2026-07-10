# Zero SQLite source

The files `sqlite3/sqlite3.c`, `sqlite3/sqlite3.h`, and
`sqlite3/sqlite3ext.h` are copied without modification from
`@rocicorp/zero-sqlite3@1.1.2`, the version pinned by Zero v1.7.0 commit
`6863de5f00a3c1e7dc09c83ea3263dec4a94ebee`.

- npm integrity: `sha512-bpxeS/JXENp8Wgo68wHsiMJjy41zePI0tCX0va/T4wAlUGfQ+gy4/Fr0TTfWWwHgDkVkAiQn5nb2EdN9xdOQWQ==`
- tarball SHA-512: `6e9c5e4bf25710da7c5a0a3af301ec88c263cb8d7378f234b425f4bdafd3e300255067d0fa0cb8fc5af44d37d65b01e00e4564022427e676f611d37dc5d39059`
- upstream repository: `https://github.com/rocicorp/zero-sqlite3`

The remainder of this directory is `libsqlite3-sys` 0.30.1, patched only in
`build.rs` so the Zero compile definitions are used. `rusqlite` remains the
workspace's high-level Rust API.
