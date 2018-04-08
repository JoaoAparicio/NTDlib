# NTDlib
Open NinjaTrader data files with Python

# Usage

- Minute data:
```
filename = "/path/to/file.Last.ntd"
df = NTDLib.read_ntd(filename, kind="min")
```
- Tick data:
```
filename = "/path/to/file.Last.ntd"
df = NTDLib.read_ntd(filename, kind="tick")
```

# TODO
- Nice consistency check: check that the `record_count` variable matches the number of records that can be read before throwing `StopIteration`
