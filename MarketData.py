
import pandas as pd
from datetime import timedelta
from datetime import datetime as dt
import re


class MarketData(object):
    
    def __init__(self):
        pass
    
    def set_new_data(self, name, value):
        setattr(self, name, value)

    def update(self, name, price, volume, past_interval, cur_interval):
        if hasattr(self, name):
            x = getattr(self, name)

            if past_interval == cur_interval:
                if price > x.High.tail(1).tolist()[0]:
                    x.iloc[-1, x.columns.get_loc('High')] = price
                if price < x.Low.tail(1).tolist()[0]:
                    x.iloc[-1, x.columns.get_loc('Low')] = price

                x.iloc[-1, x.columns.get_loc('Close')] = price
                x.iloc[-1, x.columns.get_loc('Volume')] += volume
            else:
                tick = pd.DataFrame({'Open':[price], 'High':[price], 'Low':[price], 'Close':[price], 
                                     'Volume':[volume], 'Time':[cur_interval]}).set_index('Time')
                x = x.append(tick).tail(100)
                
            setattr(self, name, x)
        else:
            raise AttributeError('could not find attribute[{}] in MarketData object'.format(name))
        
    def __convert(self, df, interval, unit):
        period = '{i}{u}'.format(i = interval, u = unit)
        df = df.resample(period).agg({'Open':'first', 'High':'max', 'Low':'min','Close':'last','Volume':'sum'}).dropna(axis=0, how='any')
        df.reindex(pd.to_datetime(df.index.strftime('%F %T')))
        return df
        
    def to_period(self, interval = 1, unit = 'MIN', df = None, byname = '', inplace = False, new_name = ''):
        
        byname = str(byname)
        new_name = str(new_name)
        
        if byname != '' and hasattr(self, byname):
            df = getattr(self, byname).copy()
        elif isinstance(df, pd.DataFrame):
            df = df.copy()
        else:
            print('No data to be convert!')
            return None
        
        df = self.__convert(df, interval, unit)

        if inplace:
            if new_name != '':
                self.set_new_data(new_name, df)
                
            if hasattr(self, byname):
                self.set_new_data(byname, df)
        else:
            return df