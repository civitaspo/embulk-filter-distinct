0.0.4 (2017-08-03)
==================

- Migrate to Embulk v0.8.18
  - https://github.com/civitaspo/embulk-filter-distinct/pull/4
  - https://github.com/civitaspo/embulk-filter-distinct/pull/5
- Add tests
  - https://github.com/civitaspo/embulk-filter-distinct/pull/8

0.0.3 (2016-01-05)
==================

- Cosmetic Change: Rename class name
- Cosmetic Change: Rename variables
- Cosmetic Change: Change indents

0.0.2 (2015-12-09)
==================

- Fix a bug: when the distinct key includes null, this plugin did not guarantee the distinctness.
- Add debug log: the filtered key like `Duplicated key: [value1, value2, value3]`

0.0.1 (2015-12-08)
==================

- first version
