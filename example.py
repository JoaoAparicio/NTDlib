import NTDLib

s = "Ninjatrader 7/db/minute/ZL 10-13/20130821.Last.ntd"
r = NTDLib.NTDFileReader(s)
for i in r:
    print(i)
