# Changelog

## 2.0.0 (2026-10-01)

### Breaking

- Declare `required_ruby_version >= 3.3`. Ruby 3.2 reached end of life in March
  2026 and is no longer tested. Installing on an older Ruby now fails with a
  clear message rather than breaking at runtime. This is the only breaking
  change, and it is what makes this release a major one.

### Note on the gap since 1.0.0

1.0.0 (April 2024) was the previous published release. There was never a 1.0.1
release and never a v1.0.1 tag — 1.0.1 was only a gemspec version bump, at
`ce8b23e`, never tagged and never published. Everything below had therefore been
unavailable to users until this release, which is the first to carry any of it.
The list is reconstructed from git history.

### Added

- Shared Hexspace mock adapter support, so dependent projects can test without a
  live Spark server.
- `EXTERNAL`, `LOCATION` and `LIKE` support in `CREATE TABLE`.
- Always-on SimpleCov coverage and RuboCop enforcement in the development setup.

### Fixed

- Support thrift 0.24 by restoring `Thrift::Client#handle_exception` and
  `#reply_seqid`, which 0.24.0 removed while hexspace's generated client still
  calls them. Without this every Spark connection failed with a
  `Sequel::DatabaseConnectionError`. thrift is constrained to `>= 0.18, < 0.25`.
- Silence a frozen-string-literal warning from thrift 0.22.0 on Ruby 3.4+.
- `Database#database_type` returns `:spark` rather than `:hexspace`.
- Ignore `CASCADE` in `drop_table_sql`, which Spark SQL does not accept.
- Route prepared `DELETE` statements through the emulated delete path.
- Date and timestamp casting and literalisation, including Ruby 4.0
  compatibility.
