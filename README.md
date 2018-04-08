# NTDlib
Open NinjaTrader data files with Python

# Usage

- Minute data:
```
filename = "/path/to/file.Last.ntd"
df = NTDLib.read_ntd(filename, kind="minute")
```
- Tick data:
```
filename = "/path/to/file.Last.ntd"
df = NTDLib.read_ntd(filename, kind="tick")
```

If `kind` argument is not passed into the function `read_ntd`, this function will try to auto-detect the `kind` from the structer of the filepath. So if the filepath has a structure like `Ninjatrader 7/db/(minute/kind)/symbol`, then `read_ntd` will auto-detect and the `kind` argument is optional.

# TODO
- Nice consistency check: check that the `record_count` variable matches the number of records that can be read before throwing `StopIteration`
