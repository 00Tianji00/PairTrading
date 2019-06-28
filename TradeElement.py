import talib

class TrendSignal(object):

    def __init__(self):
        
        self.base_cur_t = None
        self.combo_cur_t = None

        self.base_trend = 'N'
        self.combo_trend = 'N'

        self.crosscandle_trend = 'N'

        self.isLongCross = False
        self.isShortCross = False
        self.longK = None
        self.longC = None
        self.shortK = None
        self.shortC = None

        self.kd_trend = 'N'
        self.kd_deviate = 'N'

        self.bb_flat_trend = 'N'
        self.bb_break_trend = 'N'

    def update_base_signal(self, df, win):
        if df.index[-1] != self.base_cur_t:

            self.base_cur_t = df.index[-1]

            close = df.Close.values[-3:-1]
            ma = df.Close.rolling(win).mean().values[-3:-1]
            
         
            if close[0] < ma[0] and close[1] > ma[1]:
                self.base_trend = 'B'
            elif close[0] > ma[0] and close[1] < ma[1]:
                self.base_trend = 'S'
            else:
                self.base_trend = 'N'

        return self.base_trend

    def update_base_signal_2(self, df, win):
        if df.index[-1] != self.base_cur_t:

            self.base_cur_t = df.index[-1]

            close = df.Close.values[-2]
            ma = df.Close.rolling(win).mean().values[-2]
            
         
            if close > ma:
                self.base_trend = 'B'
            elif close < ma:
                self.base_trend = 'S'
        


    def update_combo_signal(self, df, win):

        if df.index[-1] != self.combo_cur_t:

            self.combo_cur_t = df.index[-1]

            close = df.Close.values[-3:-1]
            ma = df.Close.rolling(win).mean().values[-3:-1]
            
         
            if close[0] < ma[0] and close[1] > ma[1]:
                self.combo_trend = 'B'
            elif close[0] > ma[0] and close[1] < ma[1]:
                self.combo_trend = 'S'
            else:
                self.combo_trend = 'N'

    # Moving average envelope band
    def update_combo_signal_2(self, df, win, dist):

        close = df.Close.values[-1]
        ma = df.Close.rolling(win).mean().values[-1]
        up = ma * (1 + dist)
        down = ma * (1 - dist)

        if close > up:
            self.combo_trend = 'B'
        elif close < down:
            self.combo_trend = 'S'
        else:
            self.combo_trend = 'N'




    # strategyV7_0
    def update_crosscandle_singal(self, df):

        pre_open = df.Open[-2]
        pre_close = df.Close[-2]

        cur_open = df.Open[-1]
        cur_close = df.Close[-1]

        if pre_open > pre_close and cur_close > pre_close > cur_open:
            self.crosscandle_trend = 'B'
        elif pre_open < pre_close and cur_close < pre_close < cur_open:
            self.crosscandle_trend = 'S'

    def update_crosscandle_singal_2(self, df):

       
        pre_open = df.Open[-2]
        pre_close = df.Close[-2]
        

        cur_open = df.Open[-1]
        cur_high = df.High[-1]
        cur_low = df.Low[-1]
        cur_close = df.Close[-1]

        if pre_open > pre_close and cur_high > cur_close > pre_close > cur_open:
            self.crosscandle_trend = 'B'
        elif pre_open < pre_close and cur_low < cur_close < pre_close < cur_open:
            self.crosscandle_trend = 'S'


    def update_slowKD_signal(self, df, fk, sk, sd):

        high = df.High.values
        low = df.Low.values
        close = df.Close.values

        slowk, slowd = talib.STOCH(high, low, close, fastk_period = fk, slowk_period = sk, slowk_matype = 0, slowd_period = sd, slowd_matype = 0)

        preK = slowk[-2]
        preD = slowd[-2]
        k = slowk[-1]
        d = slowd[-1]

       

        if  5 < preK < 25 and 5 < preD < 25 and 5 < k < 25 and 5 < d < 25:

            if preK < preD and k > d:
                self.kd_trend = 'B'
            else:
                self.kd_trend = 'N'

        elif  95 > preK > 75 and 95 > preD > 75 and 95 > k > 75 and 95 > d > 75:
            if preK > preD and k < d:
                self.kd_trend = 'S'
            else:
                self.kd_trend = 'N'
        else:
            self.kd_trend = 'N'
        
    def update_slowKD_signal_2(self, df, fk, sk, sd):

        if df.index[-1] != self.base_cur_t:

            self.base_cur_t = df.index[-1]
        
            high = df.High.values
            low = df.Low.values
            close = df.Close.values

            slowk, slowd = talib.STOCH(high, low, close, fastk_period = fk, slowk_period = sk, slowk_matype = 0, slowd_period = sd, slowd_matype = 0)

            preK = slowk[-2]
            preD = slowd[-2]
            k = slowk[-1]
            d = slowd[-1]

           

            if  preK < 50 and preD < 50 and k < 50 and d < 50:

                if preK < preD and k > d:
                    self.kd_trend = 'B'
                else:
                    self.kd_trend = 'N'

            elif  preK > 50 and preD > 50 and k > 50 and d > 50:
                if preK > preD and k < d:
                    self.kd_trend = 'S'
                else:
                    self.kd_trend = 'N'
            else:
                self.kd_trend = 'N'
            # print('{} - preK : {}, preD : {}, K : {}, D : {}, signal : {}'.format(self.base_cur_t, preK, preD, k, d, self.kd_trend))

    def update_deviateSlowKD_signal(self, df, fk, sk, sd):

        high = df.High.values
        low = df.Low.values
        close = df.Close.values

        slowk, slowd = talib.STOCH(high, low, close, fastk_period = fk, slowk_period = sk, slowk_matype = 0, slowd_period = sd, slowd_matype = 0)

        preK = slowk[-3]
        preD = slowd[-3]
        k = slowk[-2]
        d = slowd[-2]

        preK_ = slowk[-2]
        k_ = slowk[-1]
        preC = close[-2]
        c = close[-1]

        if preK < 20 and preD < 20 and k < 25 and d < 25:

            if preK < preD and k > d:
                self.kd_trend = 'B'
                
                
        elif preK > 80 and preD > 80 and k > 75 and d > 75:
            if preK > preD and k < d:
                self.kd_trend = 'S'

        
        if self.kd_trend == 'B':

            if k_ > preK_ and c < preC and k_ < 30:
                self.kd_deviate = 'B'
            else:
                self.kd_deviate = 'N'
                

        elif self.kd_trend == 'S':
            if k_ < preK_ and c > preC and k_ > 70:
                self.kd_deviate = 'S'
            else:
                self.kd_deviate = 'N'
            
        else:
            self.kd_deviate = 'N'


    def update_deviateSlowKD_signal_2(self, df, fk, sk, sd):

        high = df.High.values
        low = df.Low.values
        close = df.Close.values

        slowk, slowd = talib.STOCH(high, low, close, fastk_period = fk, slowk_period = sk, slowk_matype = 0, slowd_period = sd, slowd_matype = 0)

        preK = slowk[-3]
        preD = slowd[-3]
        k = slowk[-2]
        d = slowd[-2]
        c = close[-2]

        if preK < 25 and preD < 25 and k < 25 and d < 25:
            if self.isLongCross:
                if preK < preD and k > d:
                    k_ = (preK + k) / 2
                    if self.longK < k_ and self.longC > c:
                        self.longK = k_
                        self.longC = c
                        self.kd_deviate = 'B' 

            else:
                if preK < preD and k > d:
                    self.longK = (preK + k) / 2
                    self.longC = c
                    self.isLongCross = True
           


        elif preK > 75 and preD > 75 and k > 75 and d > 75:
            
            if self.isShortCross:
                if preK > preD and k < d:
                    k_ = (preK + k) / 2
                    if self.shortK > k_ and self.shortC < c:
                        self.shortK = k_
                        self.shortC = c
                        self.kd_deviate = 'S'
            else:
                if preK > preD and k < d:
                    self.shortK = (preK + k) / 2
                    self.shortC = c
                    self.isShortCross = True

        else:
            self.kd_deviate = 'N'

    def update_bb_flat_signal(self, df, win, stdup, stddown):
        close = df.Close.values
        o = df.Open.values[-1]
        h = df.High.values[-1]
        l = df.Low.values[-1]
        c = df.Close.values[-1]
        upperband, middleband, lowerband = talib.BBANDS(close * 1000, timeperiod = win , nbdevup = stdup, nbdevdn = stddown, matype = 0)
        upperband = upperband / 1000
        middleband = middleband / 1000
        lowerband = lowerband / 1000

        
        if l < lowerband[-1] < h:
            if o <= c:
                self.bb_flat_trend = 'B'
            else:
                self.bb_flat_trend = 'N'
        elif h > upperband[-1] > l:
            if o >= c:
                self.bb_flat_trend = 'S'
            else:
                self.bb_flat_trend = 'N'
        else:
            self.bb_flat_trend = 'N'

       
    def update_bb_break_signal(self, df, win, stdup, stddown):

        close = df.Close.values
        o = df.Open.values[-1]
        h = df.High.values[-1]
        l = df.Low.values[-1]
        c = df.Close.values[-1]
        upperband, middleband, lowerband = talib.BBANDS(close * 1000, timeperiod = win , nbdevup = stdup, nbdevdn = stddown, matype = 0)
        upperband = upperband / 1000
        middleband = middleband / 1000
        lowerband = lowerband / 1000

        
        if l < upperband[-1] < h:    
            if o <= c:
                self.bb_break_trend = 'B'
            else:
                self.bb_break_trend = 'N'
        elif h > lowerband[-1] > l:
            if o >= c:
                self.bb_break_trend = 'S'
            else:
                self.bb_break_trend = 'N'
        else:
            self.bb_break_trend = 'N'



class NNDSSignal(object):
    def __init__(self):
        self.last_ma = 'N'
        self.first_sig = 'N'
        self.signal = 'N'

    def determine_ma(self, _open, _close, _ma):
        if _open < _ma < _close:
            return 'B'
        elif _open > _ma > _close:
            return 'S'
        else:
            return 'N'

    def find_last_ma(self):
        self.signal = 'N'
        self.last_ma = self.determine_ma(self.df2.Open.iloc[-1], self.df2.Close.iloc[-1], self.df2.Ma.iloc[-1])
        
    def find_signal(self):
        
        window = self.df2.tail(21).head(17) # for one hour
       

        for index, row in window.iterrows():
            _ma_sig = self.determine_ma(row.Open, row.Close, row.Ma)
           

            ATR = abs(row.Open - row.Close)

            if _ma_sig == self.last_ma:
                threshold_1 = max(row.Open, row.Close)
                threshold_2 = min(row.Open, row.Close)
             

                cond1 = threshold_1 >= self.df2.Open.iloc[-1] >= threshold_2
                cond2 = threshold_1 >= self.df2.Close.iloc[-1] >= threshold_2

                if cond1 or cond2:
                    self.signal = self.last_ma
                    
                    break


    def first_update(self, df):
        df = df.copy()
        df.reset_index(inplace=True)
        df['Ma'] = df.Close.rolling(center = False, window = 10).mean()

        tmp_sig = self.determine_ma(df.Open.iloc[-1], df.Close.iloc[-1], df.Ma.iloc[-1])

        if tmp_sig != 'N':
            self.first_sig = tmp_sig

    def second_update(self, df):
        df = df.copy()
        df.reset_index(inplace=True)
        df['Ma'] = df.Close.rolling(center = False, window = 10).mean()

        self.df2 = df
        self.find_last_ma()
        if self.last_ma != 'N':
            self.find_signal()

        return self.signal


class NNDSSignal_2(object):
    def __init__(self):
        self.last_ma = 'N'
        self.first_sig = 'N'
        self.signal = 'N'

    def determine_ma(self, _open, _close, _ma):
        if _open < _ma < _close:
            return 'B'
        elif _open > _ma > _close:
            return 'S'
        else:
            return 'N'

    def find_last_ma(self):
  
        self.last_ma = self.determine_ma(self.df2.Open.iloc[-1], self.df2.Close.iloc[-1], self.df2.Ma.iloc[-1])
        
    def find_signal(self):
        
        window = self.df2.tail(21).head(17) # for one hour
       

        for index, row in window.iterrows():
            _ma_sig = self.determine_ma(row.Open, row.Close, row.Ma)
            

            ATR = abs(row.Open - row.Close)

            if _ma_sig == self.last_ma:
                threshold_1 = max(row.Open, row.Close)
                threshold_2 = min(row.Open, row.Close)
             

                cond1 = threshold_1 >= self.df2.Open.iloc[-1] >= threshold_2
                cond2 = threshold_1 >= self.df2.Close.iloc[-1] >= threshold_2

                if cond1 or cond2:
                    self.signal = self.last_ma
                   
                    break


    def first_update(self, df):
        df = df.copy()
        df.reset_index(inplace=True)
        df['Ma'] = df.Close.rolling(center = False, window = 10).mean()

        tmp_sig = self.determine_ma(df.Open.iloc[-1], df.Close.iloc[-1], df.Ma.iloc[-1])

        if tmp_sig != 'N':
            self.first_sig = tmp_sig

    def second_update(self, df):
        df = df.copy()
        df.reset_index(inplace=True)
        df['Ma'] = df.Close.rolling(center = False, window = 10).mean()

        self.df2 = df
        self.find_last_ma()
        if self.last_ma != 'N':
            self.find_signal()


