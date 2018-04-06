# NTDlib
Open NinjaTrader data files with Python

# Usage (currently minute data only)

```
reader = NTDFileReader("/path/to/file.Last.ntd")
for i in reader:
    print(i) ## prints timestamp, open, high, low, close, volume
```

# Loading into Pandas DataFrame
```
r = NTDFileReader(s)
list_timestamps = []
list_opens = []
list_highs = []
list_lows = []
list_closes = []
list_volumes = []
for n,i in enumerate(r):
    counter += 1
    #print("counter: {}".format(counter))
    #data.append(i)
    #data[n] = i
    list_timestamps.append(i[0])
    list_opens.append(i[1])
    list_highs.append(i[2])
    list_lows.append(i[3])
    list_closes.append(i[4])
    list_volumes.append(i[5])

df = pd.DataFrame({'timestamp': list_timestamps,
                   'open': list_opens,
                   'high': list_highs,
                   'low': list_lows,
                   'close': list_closes,
                   'volume': list_volumes
                  }).set_index("timestamp")
df = df[['open', 'high', 'low', 'close', 'volume']]
```
