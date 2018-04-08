import struct, datetime, re
import pandas as pd

class NTDFileReaderMinute():
    def __init__(self, s):
        with open(s, "rb") as f:
            self.data = f.read()
            
        multiplier_bytes = self.data[0:8]
        record_count_bytes = self.data[12:16]
        open_bytes = self.data[16:24]
        high_bytes = self.data[24:32]
        low_bytes = self.data[32:40]
        close_bytes = self.data[40:48]
        timestamp_bytes = self.data[48:56]
        volume_bytes = self.data[56:64]
    
        self.multiplier = - struct.unpack("<d", multiplier_bytes)[0]
        self.record_count = struct.unpack("I", record_count_bytes)[0]
        self.open = struct.unpack("<d", open_bytes)[0]
        self.high = struct.unpack("<d", high_bytes)[0]
        self.low = struct.unpack("<d", low_bytes)[0]
        self.close = struct.unpack("<d", close_bytes)[0]
        self.volume = struct.unpack("<Q", volume_bytes)[0]
        ticks = struct.unpack("<Q", timestamp_bytes)[0]
        self.timestamp = datetime.datetime(1,1,1,1) + datetime.timedelta(microseconds = ticks/10)
        
        self.cursor = 64
        self.first = True

    def __iter__(self):
        return self
    
    def decode_volume_mask(self, mask_volume):
        """ returns tuple (nr_bytes_volume, multiplier_volume) """
        if mask_volume == 1:
            nr_bytes_volume = 1
            multiplier_volume = 1
        elif mask_volume == 6:
            nr_bytes_volume = 2
            multiplier_volume = 1
        elif mask_volume == 7:
            nr_bytes_volume = 4
            multiplier_volume = 1
        elif mask_volume == 2:
            nr_bytes_volume = 8
            multiplier_volume = 1
        elif mask_volume == 3:
            nr_bytes_volume = 1
            multiplier_volume = 100
        elif mask_volume == 4:
            nr_bytes_volume = 1
            multiplier_volume = 500
        elif mask_volume == 5:
            nr_bytes_volume = 1
            multiplier_volume = 1000
        elif mask_volume == 0: ## not sure about this one
            nr_bytes_volume = 0
            multiplier_volume = 1000
        else:
            raise Exception("unexpected mask_volume: {}".format(mask_volume))
        return (nr_bytes_volume, multiplier_volume)

    def decode_price_mask(self, mask_price):
        if mask_price == 0:
            nr_bytes_price = 0
        elif mask_price == 1:
            nr_bytes_price = 1
        elif mask_price == 2:
            nr_bytes_price = 2
        elif mask_price == 3:
            nr_bytes_price = 4
        else:
            raise Exception("unexpected mask_price: {}".format(mask_price))
        return nr_bytes_price
    
    def decode_timestamp_data(self, timestamp_data):
        if timestamp_data == 0:
            time_delta = 1
        else:
            time_delta = timestamp_data
        return time_delta
    
    def decode_standard_mask(self, msk):
        if msk == 0:
            nr_bytes = 0
        elif msk == 1:
            nr_bytes = 1
        elif msk == 2:
            nr_bytes = 2
        elif msk == 3:
            nr_bytes = 4
        else:
            raise Exception("unexpected mask: {}".format(msk))
        return nr_bytes
        
    def get_n_bytes(self, n):
        data = self.data[self.cursor: self.cursor+n]
        self.cursor += n
        return data
        
    def read_open_delta(self, mask):
        open_mask = (mask & 12) >> 2
        nr_bytes_open = self.decode_standard_mask(open_mask)
        if nr_bytes_open == 0:
            return 0
        
        open_delta = int.from_bytes(self.get_n_bytes(nr_bytes_open), "big")
        if nr_bytes_open == 1:
            open_delta -= 0x80
        elif nr_bytes_open == 2:
            open_delta -= 0x4000
        elif nr_bytes_open == 3:
            open_delta -= 0x40000000
        else:
            raise Exception("lala")
        return open_delta
    
    def read_high_delta(self, mask, mask2):
        high_mask = (mask2 & 48) >> 4
        nr_bytes_high = self.decode_standard_mask(high_mask)
        if nr_bytes_high == 0:
            return 0
        
        high_delta = int.from_bytes(self.get_n_bytes(nr_bytes_high), "big")
        return high_delta

    def read_low_delta(self, mask, mask2):
        low_mask = (mask2 & 192) >> 6
        nr_bytes_low = self.decode_standard_mask(low_mask)
        if nr_bytes_low == 0:
            return 0
        
        low_delta = int.from_bytes(self.get_n_bytes(nr_bytes_low), "big")
        return low_delta

    def read_close_delta(self, mask, mask2):
        close_mask = mask2 & 3
        nr_bytes_close = self.decode_standard_mask(close_mask)
        if nr_bytes_close == 0:
            return 0
        
        close_delta = int.from_bytes(self.get_n_bytes(nr_bytes_close), "big")
        return close_delta

    def read_open(self, mask):
        open_delta = self.read_open_delta(mask)
        self.open += self.multiplier * open_delta
        
    def read_high(self, mask, mask2):
        high_delta = self.read_high_delta(mask, mask2)
        self.high = self.open + self.multiplier * high_delta
        
    def read_low(self, mask, mask2):
        low_delta = self.read_low_delta(mask, mask2)
        self.low = self.open - self.multiplier * low_delta
        
    def read_close(self, mask, mask2):
        close_delta = self.read_close_delta(mask, mask2)
        self.close = self.low + self.multiplier * close_delta

    def read_volume(self, mask):
        mask_volume = (mask & 112) >> 4
        if mask_volume == 0:
            self.volume = 0
            return
        
        (nr_bytes_volume, multiplier) = self.decode_volume_mask(mask_volume)
        
        volume = int.from_bytes(self.get_n_bytes(nr_bytes_volume), "big")
        volume *= multiplier
        self.volume = volume
    
    def read_timestamp(self, mask):
        nr_bytes_time = (mask & 3)
        if nr_bytes_time == 0:
            delta_time = 1
        else:
            delta_time = int.from_bytes(self.get_n_bytes(nr_bytes_time), "big")
        self.timestamp += datetime.timedelta(minutes = delta_time)
        
    def __next__(self):
        if self.first:
            self.first = False
            return (self.timestamp, self.open, self.high, self.low, self.close, self.volume)
        else:
            try:
                mask = self.data[self.cursor]
                mask2 = self.data[self.cursor+1]
                self.cursor += 2
            except IndexError:
                raise StopIteration

            mask_volume = (mask & 112) >> 4 # 112 = 01110000
            mask_open = (mask & 12) >> 2 # 12 = 00001100
            mask_time = mask & 3 # 3 = 00000011

            mask_low = (mask2 & 192) >> 6 # 192 = 11000000
            mask_high = (mask2 & 48) >> 4 # 48 = 00110000
            mask_close = mask2 & 3
            
            nr_bytes_time = mask_time
            (nr_bytes_volume, multiplier_volume) = self.decode_volume_mask(mask_volume)
            nr_bytes_open = self.decode_standard_mask(mask_open)
            
            nr_bytes_low = self.decode_standard_mask(mask_low)
            nr_bytes_high = self.decode_standard_mask(mask_high)
            nr_bytes_close = self.decode_standard_mask(mask_close)

            self.read_timestamp(mask)
            self.read_open(mask)
            self.read_high(mask, mask2)
            self.read_low(mask, mask2)
            self.read_close(mask, mask2)
            self.read_volume(mask)
            
            return (self.timestamp, self.open, self.high, self.low, self.close, self.volume)

class NTDFileReaderTick():
    def __init__(self, s):
        with open(s, "rb") as f:
            self.data = f.read()

        multiplier_bytes = self.data[0:8]
        record_count_bytes = self.data[12:16]
        price_bytes = self.data[16:24]
        _ = self.data[24:32]
        _ = self.data[32:40]
        _ = self.data[40:48]
        timestamp_bytes = self.data[48:56]
        volume_bytes = self.data[56:64]

        self.multiplier = - struct.unpack("<d", multiplier_bytes)[0]
        self.record_count = struct.unpack("I", record_count_bytes)[0]
        self.price = struct.unpack("<d", price_bytes)[0]
        self.volume = struct.unpack("<Q", volume_bytes)[0]
        ticks = struct.unpack("<Q", timestamp_bytes)[0]
        self.timestamp = datetime.datetime(1,1,1,1) + datetime.timedelta(microseconds = ticks/10)

        self.cursor = 64
        self.first = True

    def __iter__(self):
        return self

    def decode_volume_mask(self, mask_volume):
        """ returns tuple (nr_bytes_volume, multiplier_volume) """
        if mask_volume == 1:
            nr_bytes_volume = 1
            multiplier_volume = 1
        elif mask_volume == 6:
            nr_bytes_volume = 2
            multiplier_volume = 1
        elif mask_volume == 7:
            nr_bytes_volume = 4
            multiplier_volume = 1
        elif mask_volume == 2:
            nr_bytes_volume = 8
            multiplier_volume = 1
        elif mask_volume == 3:
            nr_bytes_volume = 1
            multiplier_volume = 100
        elif mask_volume == 4:
            nr_bytes_volume = 1
            multiplier_volume = 500
        elif mask_volume == 5:
            nr_bytes_volume = 1
            multiplier_volume = 1000
        elif mask_volume == 0: ## not sure about this one
            nr_bytes_volume = 0
            multiplier_volume = 1000
        else:
            raise Exception("unexpected mask_volume: {}".format(mask_volume))
        return (nr_bytes_volume, multiplier_volume)

    def decode_price_mask(self, mask_price):
        if mask_price == 0:
            nr_bytes_price = 0
        elif mask_price == 1:
            nr_bytes_price = 1
        elif mask_price == 2:
            nr_bytes_price = 2
        elif mask_price == 3:
            nr_bytes_price = 4
        else:
            raise Exception("unexpected mask_price: {}".format(mask_price))
        return nr_bytes_price

    def decode_timestamp_data(self, timestamp_data):
        if timestamp_data == 0:
            time_delta = 1
        else:
            time_delta = timestamp_data
        return time_delta

    def decode_standard_mask(self, msk):
        if msk == 0:
            nr_bytes = 0
        elif msk == 1:
            nr_bytes = 1
        elif msk == 2:
            nr_bytes = 2
        elif msk == 3:
            nr_bytes = 4
        else:
            raise Exception("unexpected mask: {}".format(msk))
        return nr_bytes

    def get_n_bytes(self, n):
        data = self.data[self.cursor: self.cursor+n]
        self.cursor += n
        return data

    def read_price_delta(self, mask):
        price_mask = (mask & 12) >> 2
        nr_bytes_price = self.decode_standard_mask(price_mask)
        if nr_bytes_price == 0:
            return 0

        price_delta = int.from_bytes(self.get_n_bytes(nr_bytes_price), "big")
        if nr_bytes_price == 1:
            price_delta -= 0x80
        elif nr_bytes_price == 2:
            price_delta -= 0x4000
        elif nr_bytes_price == 3:
            price_delta -= 0x40000000
        else:
            raise Exception("lala")
        return price_delta

    def read_price(self, mask):
        price_delta = self.read_price_delta(mask)
        self.price += self.multiplier * price_delta

    def read_volume(self, mask):
        mask_volume = (mask & 112) >> 4
        if mask_volume == 0:
            self.volume = 0
            return

        (nr_bytes_volume, multiplier) = self.decode_volume_mask(mask_volume)

        volume = int.from_bytes(self.get_n_bytes(nr_bytes_volume), "big")
        volume *= multiplier
        self.volume = volume

    def read_timestamp(self, mask):
        nr_bytes_time = (mask & 3)
        if nr_bytes_time == 0:
            delta_time = 0
        else:
            delta_time = int.from_bytes(self.get_n_bytes(nr_bytes_time), "big")
        self.timestamp += datetime.timedelta(seconds = delta_time)

    def __next__(self):
        if self.first:
            self.first = False
            return (self.timestamp, self.price, self.volume)
        else:
            try:
                mask = self.data[self.cursor]
                self.cursor += 1
            except IndexError:
                raise StopIteration

            mask_volume = (mask & 112) >> 4 # 112 = 01110000
            mask_price = (mask & 12) >> 2 # 12 = 00001100
            mask_time = mask & 3 # 3 = 00000011

            nr_bytes_time = mask_time
            (nr_bytes_volume, multiplier_volume) = self.decode_volume_mask(mask_volume)
            nr_bytes_price = self.decode_standard_mask(mask_price)

            self.read_timestamp(mask)
            self.read_price(mask)
            self.read_volume(mask)

            return (self.timestamp, self.price, self.volume)

def read_ntd(filepath, kind=None):
    FILENAME_REGEX = r"Ninjatrader 7/db/(?P<kind>(minute)|(tick))/(?P<symbol>.*?) (?P<month>\d\d)-(?P<year>\d\d)"
    # if kind is none, try auto-detect from filepath
    if kind is None:
        m = re.search(FILENAME_REGEX, filepath)
        if m:
            kind = m.groupdict()['kind']

    if kind not in ("minute", "tick"):
        raise ValueError("kind argument must be either 'minute' or 'tick', received '{}'".format(kind))

    if kind == "minute":
        reader = NTDFileReaderMinute(filepath)
    elif kind == "tick":
        reader = NTDFileReaderTick(filepath)

    data_temp = [0]*reader.record_count
    for n,i in enumerate(reader):
        data_temp[n] = i

    if kind == "minute":
        df = pd.DataFrame(data_temp, columns=['timestamp','open','high','low','close','volume']).set_index("timestamp")
        return df[['open', 'high', 'low', 'close', 'volume']]
    elif kind == "tick":
        df = pd.DataFrame(data_temp, columns=['timestamp','price','volume']).set_index("timestamp")
        return df[['price', 'volume']]
