# Distinct filter plugin for Embulk

filter return only distinct (different) records by columns you configured.

## Overview

* **Plugin type**: filter

## Configuration

- **columns**: column name list to distinguish records (array of string, required)

## Example

```yaml
filters:
  - type: distinct
    columns: [c0, c1]
```

## Run Example

```
$ ./gradlew classpath
$ embulk run -I lib example/config.yml
```

## Note

this plugin uses a lot of memory because of having distinct column values.

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
