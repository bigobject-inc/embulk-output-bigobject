# Bigobject output plugin for Embulk

BigObject output plugins for Embulk loads records to BigObject.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Installation

```
embulk gem install embulk-output-bigobject
```

## Configuration

- **host**: database host name (string, default: localhost)
- **restport**: database port number (integer, default: 9090)
- **ncport**: database port number (integer, default: 9091)
- **table**: database table name (string, required)

## Example

```yaml
out:
  type: bigobject
  host: localhost
  table: mytest
  column_options:
    - {name: "col1", type: 'INT64', is_key: true}
    - {name: "col2", type: 'BYTE', is_key: true}
    - {name: "col3", type: 'DATE32'}
    - {name: "col4", type: 'STRING(16)'}
```


## Build

```
$ rake
```
