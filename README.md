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

- **host**: database host name (string, required)
- **port**: database port number (integer, default: 9090)
- **table**: database table name (string, required)

## Example

```yaml
out:
  type: bigobject
  host: localhost
  table: mytest
```


## Build

```
$ rake
```
