# Changelog

## [2.0.1] - 2026-10-07

### Fixed

- Emulated `DELETE` and `UPDATE` no longer drop the original table. Spark has
  neither statement, so the adapter rebuilds the table under a temporary name and
  swaps it in. The swap used to drop the original and rename the copy into place.
  A process killed between those two steps lost the table, with its rows left
  under a generated name. The swap is now an `INSERT OVERWRITE`, so the original
  table always exists.
- A temporary table left behind by an interrupted delete or update no longer
  breaks every later delete or update on that table with
  `TABLE_OR_VIEW_ALREADY_EXISTS`. The adapter now tolerates it and cleans it up.
- The temporary table for a schema-qualified table (`sch.items`) is now created
  in that schema. It used to be created as a single dotted identifier in the
  default database.
- `Sequel.date_add` / `Sequel.date_sub` now honour the `date_arithmetic`
  extension's cast: the `:cast` option, or the generic timestamp type by
  default. Both were silently ignored.

### Changed

- Require `thrift >= 0.23` (was `>= 0.18`), and delete the `Thrift::Bytes`
  backport that older versions needed. The upper bound stays `< 0.25`.
- Bound `hexspace` to `>= 0.2.1, < 0.4` (was `>= 0.2.1`). hexspace is a 0.x gem,
  so a new minor version may break its API. 0.3.0 is its newest release, so the
  bound excludes nothing that exists.
- Internal simplifications with no change in behaviour.

## [2.0.0] - 2026-10-01

### Breaking changes

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

[2.0.0]: https://github.com/outcomesinsights/sequel-hexspace/compare/v1.0.0...v2.0.0
[2.0.1]: https://github.com/outcomesinsights/sequel-hexspace/compare/v2.0.0...v2.0.1
