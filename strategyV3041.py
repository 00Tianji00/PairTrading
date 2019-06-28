
import sys
sys.path.insert(0, '../libs/api/')

from Account import Account

from datetime import datetime as dt
from datetime import timedelta
from DataUpdater import *
from Fixprotocol import Fixprotocol
from AlgoBank import AlgoBank

import pandas as pd
import numpy as np
import time
import json
import threading
import signal
import csv

signal.signal(signal.SIGINT, signal.SIG_DFL)

account = Account(sys.argv[1])

API_PATH = '../libs/api/xxx.so' #fixprotocl api

###############################
#         Price Info        #
###############################

PRICE_USERNAME = account.price_username
PRICE_PASSWORD = account.price_password
PRICE_PATH = account.price_path

###############################
#         Account Info        #
###############################
USERNAME_1 = account.username
PASSWORD_1 = account.password
ACCOUNT_1 = account.account
ORDER_PATH_1 = account.order_path

###############################

PRICE_DECIMAL = 'PRICE_DECIMAL'
VOLUME = 'VOLUME'
FEE = 'FEE'
PUNCH_VOLUME = 'PUNCH_VOLUME'
PUNCH_FEE = 'PUNCH_FEE'
PUNCH_LIMIT = 'PUNCH_LIMIT'
UNIT_STOP_PROFIT = 'UNIT_STOP_PROFIT'
UNIT_TRAILING = 'UNIT_TRAILING'
UNIT_STOP_LOSS = 'UNIT_STOP_LOSS'


EXCHANGE = 'CFH'
MERCHANDISES = [
                'GBPUSD',
                'USDCAD',
                'USDCNH',
                'USDJPY'
                ]

#[mer1, mer2, coeff, mean, std, type, plus coeff, plus max]
PORTFOLIO = [
            ['GBPUSD', 'USDCAD', 0.4701, -0.1529, 0.0062, 't1', 1, 3],
            ['USDCNH', 'USDJPY', 0.0916, 0.0044, 9.4249e-05, 't2', 1, 3]
            ]

fee_coeff = 2
punch_coeff = 100
INFO = {
    
    'GBPUSD' : {PRICE_DECIMAL : 5,
                VOLUME : 100000,
                FEE : 0.04 * fee_coeff,
                PUNCH_VOLUME : 10000 * punch_coeff,
                PUNCH_LIMIT : 5,
                UNIT_STOP_PROFIT : 0.00100,
                UNIT_TRAILING : 0.00100,
                UNIT_STOP_LOSS : 0.00200
                },

    'USDCAD' : {PRICE_DECIMAL : 5,
                VOLUME : 100000,
                FEE : 0.04 * fee_coeff,
                PUNCH_VOLUME : 10000 * punch_coeff,
                PUNCH_LIMIT : 5,
                UNIT_STOP_PROFIT : 0.00100,
                UNIT_TRAILING : 0.00100,
                UNIT_STOP_LOSS : 0.00200
                },

    'USDCNH' : {PRICE_DECIMAL : 5,
                VOLUME : 100000,
                FEE : 0.04 * fee_coeff,
                PUNCH_VOLUME : 10000 * punch_coeff,
                PUNCH_LIMIT : 5,
                UNIT_STOP_PROFIT : 0.00060,
                UNIT_TRAILING : 0.00060,
                UNIT_STOP_LOSS : 0.00120
                },

    'USDJPY' : {PRICE_DECIMAL : 4,
                VOLUME : 100000,
                FEE : 0.04 * fee_coeff,
                PUNCH_VOLUME : 10000 * punch_coeff,
                PUNCH_LIMIT : 5,
                UNIT_STOP_PROFIT : 0.00060,
                UNIT_TRAILING : 0.00060,
                UNIT_STOP_LOSS : 0.00120
                }
}




TRAIL_FLAG_1 = '1'
TRAIL_FLAG_2 = '2'

MARKET = '1'
LIMIT = '2'
STOP = '3'

DAY = '0'
GTC = '1'
IOC = '3'
FOK = '4'
GTD = '6'

BUY = '1'
SELL = '2'
ERROR = '3'

def Logs(msg):

    print('[{}] {}'.format(dt.now(), msg))


class MatchTrade(object):

    def __init__(self, algo, priceFix, fixprotocol_1):



        self.preSignalTime = None

        self.algobank = algo
        self.priceFix = priceFix
        self.fixprotocol_1 = fixprotocol_1
       
    
        self.canStopProfit = False
        self.stop_profit = STOP_PROFIT
        self.stop_loss = STOP_LOSS
        self.canPlus = False

        self.signal = 0

        self.hasPosition = False
        self.hasPunchPosition = False

        self.isMonitor = False


        self.priceThread = threading.Thread()
    
        self.positionThread_1 = threading.Thread()
        self.resetThread_1 = threading.Thread()

        self.positionThread_2 = threading.Thread()
        self.resetThread_2 = threading.Thread()

        self.positionThread = threading.Thread()

        self.balanceRecordThread = threading.Thread()

        self.punchCount = 0

        self.PreBalance_1 = 0
        self.Balance_1 = 0
        self.OpenPosition_1 = 0
   
        self.isTrading = False
        

        self.LOCK_1 = threading.Lock()
                
        self.pNow1 = dt.now()
       
        self.isResetPreBalance = True


        self.init_trade_args()

    def init_trade_args(self):

        for mer in MERCHANDISES:

            args = INFO[mer]
            for arg in args:
                setattr(self, '{}_{}'.format(mer, arg), args[arg])

        for portf in PORTFOLIO:

            setattr(self, '{}_tradeBaseStatus'.format(portf), 0) #default 0
            setattr(self, '{}_canTrade'.format(portf), True) #default True

            setattr(self, '{}_tradeCount'.format(portf), 0)
            setattr(self, '{}_trailFlag'.format(portf), TRAIL_FLAG_1)
            setattr(self, '{}_isUpdateBasePrice'.format(portf), True)


            setattr(self, '{}_signal'.format(portf), 0) #default 0
            setattr(self, '{}_exitSignal'.format(portf), 0) #default 0
            setattr(self, '{}_plusCount'.format(portf), 1) #default 1
            setattr(self, '{}_plusSignal'.format(portf), False) #default False
            setattr(self, '{}_stopSignal'.format(portf), False) #default False
            

        ## GBPUSD - USDCAD
        # setattr(self, '{}_tradeBaseStatus'.format(PORTFOLIO[0]), 1)
        # setattr(self, '{}_canTrade'.format(PORTFOLIO[0]), True)
        # setattr(self, '{}_plusCount'.format((PORTFOLIO[0])), 3)
        # setattr(self, '{}_stopSignal'.format(PORTFOLIO[0]), False)

        ## USDCNH - USDJPY
        # setattr(self, '{}_tradeBaseStatus'.format(PORTFOLIO[1]), 1)
        # setattr(self, '{}_canTrade'.format(PORTFOLIO[1]), True)
        # setattr(self, '{}_plusCount'.format((PORTFOLIO[1])), 3)
        # setattr(self, '{}_stopSignal'.format(PORTFOLIO[1]), False)

        
        




    def __record(self):

        date = dt.now().strftime('%F')

        while self.isTrading:
            time.sleep(1)
            with open('./Balance/{}_BalanceRecord.csv'.format(date), 'a', newline='') as csvfile:
 
                writer = csv.writer(csvfile)
                now = dt.now()
                p = self.Balance_1 - self.PreBalance_1 + self.Balance_2 - self.PreBalance_2 - self.tradeCount * FEE - self.punchCount * PUNCH_FEE
                writer.writerow([now, p])

    def __genBalanceRecordThread(self):

        self.balanceRecordThread = threading.Thread(name = 'balanceRecordThread',
                                                    target = self.__record)
        self.balanceRecordThread.start()

    def __genTradeThread(self, portf):

 
        setattr(self, '{}_tradeThread'.format(portf), threading.Thread(name = '{}_tradeThread'.format(portf),
                                                                    target = self.__trade,
                                                                    args = (portf, 1)))

        
        getattr(self, '{}_tradeThread'.format(portf)).start()

    def openPair(self, lock, fixprotocol, side1, side2, portf):

        mer1 = portf[0]
        mer2 = portf[1]

        bid1 = getattr(self, '{}_bid'.format(mer1))
        bid2 = getattr(self, '{}_bid'.format(mer2))
        ask1 = getattr(self, '{}_ask'.format(mer1))
        ask2 = getattr(self, '{}_ask'.format(mer2))

        price1 = (bid1+ask1) / 2
        price2 = (bid2+ask2) / 2

        vol1 = int(round(getattr(self, '{}_{}'.format(mer1, VOLUME)), 0))

        if portf[5] == 't1':
            vol2 = int(round(getattr(self, '{}_{}'.format(mer1, VOLUME)) / price2 / portf[2], 0))

        elif portf[5] == 't2':
            vol2 = int(round(getattr(self, '{}_{}'.format(mer1, VOLUME)) * price1 / price2 / portf[2], 0))

        elif portf[5] == 't3':
            vol2 = int(round(getattr(self, '{}_{}'.format(mer1, VOLUME)) / portf[2]), 0)

        elif portf[5] == 't4':
            vol2 = int(round(getattr(self, '{}_{}'.format(mer1, VOLUME)) * price1 / portf[2]), 0)


        
        lock.acquire()

        fixprotocol.order(mer1, side1, vol1, MARKET, bid1, 0, '1', '', '', 1)

        time.sleep(0.0001)

        fixprotocol.order(mer2, side2, vol2, MARKET, bid2, 0, '1', '', '', 1)

        lock.release()
        
        print('---------------------------------------')
        Logs('Open pair position')
        Logs('{} - {} @ {}, vol : {}'.format(mer1, side1, round(price1, getattr(self, '{}_{}'.format(mer1, PRICE_DECIMAL))), vol1))
        Logs('{} - {} @ {}, vol : {}'.format(mer2, side2, round(price2, getattr(self, '{}_{}'.format(mer2, PRICE_DECIMAL))), vol2))
        print('---------------------------------------')



    def __trade(self, portf, t):

        Logs('{} - Start trading'.format(portf))

        # self.__genPositionThread()
        # self.__genBalanceRecordThread()
   

        if portf[5] == 't1':
            side_1_1 = SELL
            side_1_2 = SELL
            side_2_1 = BUY
            side_2_2 = BUY

        elif portf[5] == 't2':
            side_1_1 = BUY
            side_1_2 = SELL
            side_2_1 = SELL
            side_2_2 = BUY

        elif portf[5] == 't3':
            side_1_1 = SELL
            side_1_2 = BUY
            side_2_1 = BUY
            side_2_2 = SELL

        elif portf[5] == 't4':
            side_1_1 = BUY
            side_1_2 = BUY
            side_2_1 = SELL
            side_2_2 = SELL



        while self.isTrading:
            
            tradeBaseStatus = getattr(self, '{}_tradeBaseStatus'.format(portf))
            canTrade = getattr(self, '{}_canTrade'.format(portf))
            signal = getattr(self, '{}_signal'.format(portf))
            exitSignal = getattr(self, '{}_exitSignal'.format(portf))
            plusSignal = getattr(self, '{}_plusSignal'.format(portf))
            stopSignal = getattr(self, '{}_stopSignal'.format(portf))

            if canTrade:

                if tradeBaseStatus == 0:
                    if signal == 1:
                        self.openPair(self.LOCK_1, self.fixprotocol_1, side_1_1, side_1_2, portf)
                        setattr(self, '{}_tradeBaseStatus'.format(portf), 1)

                        Logs('{} - Trade status : {}'.format(portf, 1))

                    elif signal == 2:
                        self.openPair(self.LOCK_1, self.fixprotocol_1, side_2_1, side_2_2, portf)
                        setattr(self, '{}_tradeBaseStatus'.format(portf), 2)

                        Logs('{} - Trade status : {}'.format(portf, 2))

                elif tradeBaseStatus == 1:
                    if plusSignal:
                        self.openPair(self.LOCK_1, self.fixprotocol_1, side_1_1, side_1_2, portf)
                        setattr(self, '{}_plusSignal'.format(portf), False)
                        Logs('{} - Plus position - status 1'.format(portf))

                    if exitSignal == 2:
                        self.resetTradeStatus(portf[0])
                        self.resetTradeStatus(portf[1])
                        setattr(self, '{}_tradeBaseStatus'.format(portf), 0)
                        setattr(self, '{}_plusCount'.format(portf), 1)

                        Logs('{} - Reach exit condition : 1 -> exit 2'.format(portf))

                    if stopSignal:
                        self.resetTradeStatus(portf[0])
                        self.resetTradeStatus(portf[1])

                        setattr(self, '{}_canTrade'.format(portf), False)
                        setattr(self, '{}_tradeBaseStatus'.format(portf), 0)
                        setattr(self, '{}_plusCount'.format(portf), 1)

                        Logs('{} - Stop Loss successfully'.format(portf))

                elif tradeBaseStatus == 2:
                    if plusSignal:
                        self.openPair(self.LOCK_1, self.fixprotocol_1, side_2_1, side_2_2, portf)
                        setattr(self, '{}_plusSignal'.format(portf), False)
                        Logs('{} - Plus position - status 2'.format(portf))

                    if exitSignal == 1:
                        self.resetTradeStatus(portf[0])
                        self.resetTradeStatus(portf[1])
                        setattr(self, '{}_tradeBaseStatus'.format(portf), 0)
                        setattr(self, '{}_plusCount'.format(portf), 1)

                        Logs('{} - Reach exit condition : 2 -> exit 1'.format(portf))

                    if stopSignal:
                        self.resetTradeStatus(portf[0])
                        self.resetTradeStatus(portf[1])

                        setattr(self, '{}_canTrade'.format(portf), False)
                        setattr(self, '{}_tradeBaseStatus'.format(portf), 0)
                        setattr(self, '{}_plusCount'.format(portf), 1)

                        Logs('{} - Stop Loss successfully'.format(portf))

                
        Logs('{} - Stop trading'.format(portf))


    def update_signal(self, portf):

     
        bid1 = getattr(self, '{}_bid'.format(portf[0]))
        ask1 = getattr(self, '{}_ask'.format(portf[0]))
        
        bid2 = getattr(self, '{}_bid'.format(portf[1]))
        ask2 = getattr(self, '{}_ask'.format(portf[1]))
        
        if portf[5] == 't1':
            price1 = (bid1 + ask1) / 2
            price2 = 1 / ((bid2 + ask2) / 2)
        
        elif portf[5] == 't2':
            price1 = 1 / ((bid1 + ask1) / 2)
            price2 = 1 / ((bid2 + ask2) / 2)
        
        elif portf[5] == 't3':
            price1 = (bid1 + ask1) / 2
            price2 = (bid2 + ask2) / 2

        elif portf[5] == 't4':
            price1 = 1 / ((bid1 + ask1) / 2)
            price2 = (bid2 + ask2) / 2


        plusCount = getattr(self, '{}_plusCount'.format(portf))
        plus_coeff = portf[6]
        plus_max = portf[7]

        stopSignal = getattr(self, '{}_stopSignal'.format(portf))

        z = portf[2]*price1 - price2

        if z > portf[3] + portf[4]:
            setattr(self, '{}_signal'.format(portf), 1)

            if not stopSignal:

                if z > portf[3] + portf[4]*(1 + plusCount*plus_coeff) and z < portf[3] + portf[4]*(1 + plus_max*plus_coeff):

                    setattr(self, '{}_plusSignal'.format(portf), True)
                    setattr(self, '{}_plusCount'.format(portf), plusCount + 1)

                    Logs('{} - Generate PLUS signal, times : {}'.format(portf, plusCount))

                elif z > portf[3] + portf[4]*(1 + plus_max*plus_coeff):

                    setattr(self, '{}_stopSignal'.format(portf), True)
                    Logs('{} - Have to Stop'.format(portf))


        elif z < portf[3] - portf[4]:
            setattr(self, '{}_signal'.format(portf), 2)

            if not stopSignal:

                if z < portf[3] - portf[4]*(1 + plusCount*plus_coeff) and z > portf[3] - portf[4]*(1 + plus_max*plus_coeff):
                    setattr(self, '{}_plusSignal'.format(portf), True)
                    setattr(self, '{}_plusCount'.format(portf), plusCount + 1)

                    Logs('{} - Generate PLUS signal, times : {}'.format(portf, plusCount))

                elif z < portf[3] - portf[4]*(1 + plus_max*plus_coeff):

                    setattr(self, '{}_stopSignal'.format(portf), True)
                    Logs('{} - Have to Stop'.format(portf))


        else:
            setattr(self, '{}_signal'.format(portf), 0)
            setattr(self, '{}_canTrade'.format(portf), True)
            setattr(self, '{}_stopSignal'.format(portf), False)


        if z > portf[3]:
            setattr(self, '{}_exitSignal'.format(portf), 1)

        elif z < portf[3]:
            setattr(self, '{}_exitSignal'.format(portf), 2)

        # print(portf, round((z - portf[3])/portf[4],2), getattr(self, '{}_signal'.format(portf)))


    def process_tick(self, tick):

        try:
      
            if tick != '':
                tick = json.loads(tick)
                mer = tick['MDEntries'][0]['Symbol']
                
                getattr(self, '{}_updater'.format(mer)).update(tick)
                setattr(self, '{}_curtime'.format(mer), getattr(self, '{}_updater'.format(mer)).processor.tick['cur_time'])
                setattr(self, '{}_bid'.format(mer), getattr(self, '{}_updater'.format(mer)).processor.tick['bid1_price'])
                setattr(self, '{}_ask'.format(mer), getattr(self, '{}_updater'.format(mer)).processor.tick['ask1_price'])
                    
                
        except KeyError:
            return {}

        except json.decoder.JSONDecodeError:
            print(tick)
            return {}

        
    def prepare_data(self):

        def prepare(updater, mer):
            df_min = self.algobank.Get_Market_his_tail(EXCHANGE, mer, 20)
            df_min['Volume'] = (df_min.BidVolume + df_min.AskVolume)/2

            updater.set_default_data(df_min)
            # updater.delete_period(1, 'min')
            updater.delete_period(3, 'min')
            updater.delete_period(5, 'min')
            updater.delete_period(15, 'min')
            updater.delete_period(1, 'hour')
            updater.delete_period(1, 'day')

        
        res = True
        Logs('Start to prepare')
        for mer in MERCHANDISES:
        
            r = True
            if r:
                setattr(self, '{}_updater'.format(mer), select_updater(EXCHANGE))
                setattr(self, '{}_curtime'.format(mer), None)
                setattr(self, '{}_bid'.format(mer), 0)
                setattr(self, '{}_ask'.format(mer), 0)
                prepare(getattr(self, '{}_updater'.format(mer)), mer)

            else:
                Logs('AlgoBank - {} add target failed'.format(mer))
                res = False
                break

        if res:

            self.priceFix.market_data_request(' '.join(MERCHANDISES), 1)
            
            for mer in MERCHANDISES:
                Logs('{} - get price ...'.format(mer))
                while True:
                
                    tick = self.priceFix.market_data()
                    self.process_tick(tick)
                    if None not in getattr(self, '{}_updater'.format(mer)).processor.tick.values():
                        break

            Logs('Data is prepared')


        else:
            Logs('Start to prepare failed')
            self.exit()

    def __genPriceThread(self):

    
        self.priceThread = threading.Thread(name = 'priceThread',
                                            target = self.__updatePrice)
        self.priceThread.start()


    def __updatePrice(self):
        Logs('Start to get price')
        while self.isTrading:

            tick = self.priceFix.market_data()
            self.process_tick(tick)

        Logs('Stop get price')

    def __genSignalThread(self, portf):

    
        setattr(self, '{}_signalThread'.format(portf), threading.Thread(name = '{}_tradeThread'.format(portf),
                                                                    target = self.__genSignal,
                                                                    args = (portf, 1)))

        getattr(self, '{}_signalThread'.format(portf)).start()


    def __genSignal(self, portf, t):
        Logs('{} - Start signal'.format(portf))
        
        while self.isTrading:
            self.update_signal(portf)
        
        Logs('{} - Stop signal'.format(portf))

    
    def __genPositionThread(self):


        Logs('---- Start position monitor ----')

        self.isMonitor = True

        self.tradeCount = 0
        self.punchCount = 0

        self.OpenPosition_1 = 0
       
        self.canStopProfit = False
        self.stop_profit = STOP_PROFIT
        self.stop_loss = STOP_LOSS
        self.canPlus = False


        self.hasPunchPosition = False


        do = True
        while do:
            
            self.LOCK_1.acquire()
            self.fixprotocol_1.request_account_info(ACCOUNT_1)

            do_ = True
            self.pNow1 = dt.now()
            while do_:

                res = self.fixprotocol_1.receive_message()

                if res != '':
                    
                    self.PreBalance_1 = self.Balance_1 = round(float(res['AvailableForMarginTrading']) + float(res['MarginRequirement']), 2) 
                   
                    do = False
                    do_ = False

                n = dt.now()
                if (n - self.pNow1).seconds > 10:
                    Logs('{} - Pre balance time out'.format(ACCOUNT_1))
                    do_ = False

            self.LOCK_1.release()


        Logs('{} balance : {}'.format(ACCOUNT_1, self.PreBalance_1))

       

        self.positionThread_1 = threading.Thread(name = 'positionThread_1',
                                                target = self.__positionMonitor_1)

        self.positionThread_1.start()


        self.positionThread = threading.Thread(name = 'positionThread',
                                                target = self.__positionMonitor)
        self.positionThread.start()

    def __positionMonitor(self):

        time.sleep(5)
        Logs('Start position monitor - master')
        while self.isMonitor:
            
            position = self.OpenPosition_1
           
            
            pl1 = self.Balance_1 - self.PreBalance_1
            PL = pl1 - self.tradeCount * FEE - self.punchCount * PUNCH_FEE


            if not self.canStopProfit:
                if PL > self.stop_profit:
                    self.canStopProfit = True 
                    self.stop_loss = STOP_LOSS_2
                    Logs('Stop loss change to {}'.format(self.stop_loss))
                    self.canPlus = True


            else:
                if self.stop_loss < 0:
                    if PL > self.stop_profit + 2 * TRAILING * self.punchCount:
                        self.stop_loss = 0
                        self.stop_profit = self.stop_profit + TRAILING * self.punchCount
                        Logs('Trailing from {}'.format(self.stop_profit))
                        self.canPlus = True

                else:

                    if PL < self.stop_profit:
                        print('--------------------------------------------')
                        Logs('Stop Profit')
                        Logs('Open PL : {}'.format(PL))
                        Logs('Open position - 1 : {}'.format(self.OpenPosition_1))
                        print('--------------------------------------------')
                        
                        self.resetTradeStatus()
                        self.isMonitor = False
                        while self.resetThread_1.is_alive() or self.resetThread_2.is_alive() or self.positionThread_1.is_alive() or self.positionThread_2.is_alive():
                            pass
                    elif PL > self.stop_profit + 2 * TRAILING * self.punchCount:
                        self.stop_profit = self.stop_profit + TRAILING * self.punchCount
                        Logs('Trailing to {}'.format(self.stop_profit))
                        self.canPlus = True

            if PL < self.stop_loss:
                print('--------------------------------------------')
                Logs('Stop Loss')
                Logs('Open PL : {}'.format(PL))
                Logs('Open position - 1 : {}'.format(self.OpenPosition_1))
                print('--------------------------------------------')
                
                self.resetTradeStatus()
                self.isMonitor = False
                while self.resetThread_1.is_alive() or self.positionThread_1.is_alive():
                    pass





            # now = dt.now()
            # if now.hour == 4 and now.minute == 59 and 55 < now.second < 60:
            
            #     if self.isResetPreBalance:

            #         print('--------------------------------------------')
            #         Logs('Reset Pre Balance')
            #         Logs('Open PL - 1 : {}, 2 : {}'.format(pl1, pl2))
            #         Logs('Open position - 1 : {}, 2 : {}'.format(self.OpenPosition_1, self.OpenPosition_2))
            #         print('--------------------------------------------')
                    
            #         self.resetTradeStatus()
            #         self.isMonitor = False
            #         while self.resetThread_1.is_alive() or self.resetThread_2.is_alive() or self.positionThread_1.is_alive() or self.positionThread_2.is_alive():
            #             pass
                    
            #         self.isResetPreBalance = False
            # else:
            #     self.isResetPreBalance = True



        Logs('Stop position monitor - master')

        if self.isTrading:
            self.__genPositionThread()

    def __positionMonitor_1(self):

        Logs('Start position monitor 1')
        
        while self.isMonitor:
            
            self.LOCK_1.acquire()
            self.fixprotocol_1.request_account_info(ACCOUNT_1)

            do = True
            self.pNow1 = dt.now()
            while do:
              
                res = self.fixprotocol_1.receive_message()
                
                if res != '':
                    
                    self.Balance_1 = round(float(res['AvailableForMarginTrading']) + float(res['MarginRequirement']), 2) 
                    _, self.OpenPosition_1 = self.queryPosition(self.fixprotocol_1, ACCOUNT_1)

                    do = False

                n = dt.now()
                if (n - self.pNow1).seconds > 10:
                    Logs('Position monitor 1 time out')
                    do = False

            self.LOCK_1.release()

            time.sleep(1)

        Logs('Stop position monitor 1')



    def __trailing(self, lock, fixprotocol, side, price, flag, mer):

        tradeCount = 0
        canStopProfit = False


        lock.acquire()

        id_tag = fixprotocol.order(mer, side, getattr(self, '{}_{}'.format(mer, VOLUME)),
                                    MARKET, price, 0,
                                    '1', '', '', 1)
        lock.release()

        time.sleep(0.0001)

        tradeCount += 1

        id_tag = '{}-{}'.format(mer,getattr(self, '{}_tradeCount'.format(mer)))

        Logs('{} - Start trailing'.format(id_tag))
        setattr(self, id_tag, False)

        if side == BUY:
            s = 'buy'
            side_ = SELL
            s_ = 'sell'
            p_ = round(price - getattr(self, '{}_{}'.format(mer, UNIT_STOP_LOSS)), getattr(self, '{}_{}'.format(mer, PRICE_DECIMAL)))
            

        elif side == SELL:
            s = 'sell'
            side_ = BUY
            s_ = 'buy'
            p_ = round(price + getattr(self, '{}_{}'.format(mer, UNIT_STOP_LOSS)), getattr(self, '{}_{}'.format(mer, PRICE_DECIMAL)))

        Logs('{} - Open {}, price : {}, trade times : {}'.format(id_tag, s, price, tradeCount))


        do = True

        while do and flag == getattr(self, '{}_trailFlag'.format(mer)):
            
    
            lock.acquire()

            id_ = fixprotocol.order(mer, side_, int(getattr(self, '{}_{}'.format(mer, VOLUME))*tradeCount),
                                    STOP, 0, p_,
                                    '1', '', '', 1)

            Logs('{} - Make {} stop order, price : {}, now price : {}'.format(id_tag, s_, p_, price))

            fixprotocol.request_order_status('', id_)
            do_ = True
            while do_:
                res = fixprotocol.receive_execution_report()
                if res != '':
                    if 'ClOrdID' in res:
                        if res['ClOrdID'] == id_:    
                            if res['OrdStatus'] in ['0', '1', '2']:
                                do_ = False
                                do = False
                                setattr(self, id_tag, True)
                                Logs('{} - Make {} stop order successfully, status : {}'.format(id_tag, s_, res['OrdStatus']))

                            elif res['OrdStatus'] in ['4']:
                                do_ = False
                                do = False
                                Logs('{} - {} stop order was canceled, status : {}'.format(id_tag, s_, res['OrdStatus']))

                            elif res['OrdStatus'] in ['8']:
                                do_ = False
                                Logs('{} - Make {} stop oder failed, status : {}'.format(id_tag, s_, res['OrdStatus']))

                            else:
                                Logs('{} - {} stop order is pending-1, status : {}'.format(id_tag, s_, res['OrdStatus']))

                    else:
                        Logs('No ClOrdID - {}'.format(res))

            lock.release()
    
            while getattr(self, id_tag) and flag == getattr(self, '{}_trailFlag'.format(mer)):
                lock.acquire()

                fixprotocol.request_order_status('', id_)
                do_ = True
                while do_ and flag == getattr(self, '{}_trailFlag'.format(mer)):
                    res = fixprotocol.receive_execution_report()
                    if res != '':

                        if 'ClOrdID' in res:
                            if res['ClOrdID'] == id_:
                                if res['OrdStatus'] in ['2', '4', '8']:
                                    do_ = False
                                    setattr(self, id_tag, False)
                                    Logs('{} - {} stop order is end, trade times : {} , status : {}'.format(id_tag, s_, tradeCount, res['OrdStatus']))
                                    
                                    
                                elif res['OrdStatus'] in ['6', 'A', 'E']:
                                    Logs('{} - {} stop order is pending-2, status : {}'.format(id_tag, s_, res['OrdStatus']))

                                else:

                                    if side == BUY:
                                        nowPrice = getattr(self, '{}_ask'.format(mer))
                                        if not canStopProfit:
                                            if nowPrice > round(price + getattr(self, '{}_{}'.format(mer, UNIT_STOP_PROFIT)), getattr(self, '{}_{}'.format(mer, PRICE_DECIMAL))):
                                                canStopProfit = True
                                                fixprotocol.order(mer, side, getattr(self, '{}_{}'.format(mer, VOLUME)),
                                                                MARKET, nowPrice, 0,
                                                                '1', '', '', 1)
                                                time.sleep(0.0001)
                                                tradeCount += 1

                                                price = round(nowPrice - getattr(self, '{}_{}'.format(mer, UNIT_TRAILING)), getattr(self, '{}_{}'.format(mer, PRICE_DECIMAL)))
                                                id_ =  fixprotocol.order_cancel_replace('', id_, mer, side_, int(getattr(self, '{}_{}'.format(mer, VOLUME))*tradeCount), STOP, 0, price, '1', '')
                                               
                                                Logs('{} - Open plus {}, price : {}, trade times : {}'.format(id_tag, s, nowPrice, tradeCount))
                                                Logs('{} - Change {} stop order, price : {}'.format(id_tag, s_, price))

                                        else:
                                            if nowPrice > round(price + 2*getattr(self, '{}_{}'.format(mer, UNIT_TRAILING)), getattr(self, '{}_{}'.format(mer, PRICE_DECIMAL))):
                                                fixprotocol.order(mer, side, getattr(self, '{}_{}'.format(mer, VOLUME)),
                                                                MARKET, nowPrice, 0,
                                                                '1', '', '', 1)
                                                time.sleep(0.0001)
                                                tradeCount += 1

                                                price = round(nowPrice - getattr(self, '{}_{}'.format(mer, UNIT_TRAILING)), getattr(self, '{}_{}'.format(mer, PRICE_DECIMAL)))
                                                id_ =  fixprotocol.order_cancel_replace('', id_, mer, side_, int(getattr(self, '{}_{}'.format(mer, VOLUME))*tradeCount), STOP, 0, price, '1', '')
                                               
                                                Logs('{} - Open plus {}, price : {}, trade times : {}'.format(id_tag, s, nowPrice, tradeCount))
                                                Logs('{} - Change {} stop order, price : {}'.format(id_tag, s_, price))




                                    elif side == SELL:
                                        nowPrice = getattr(self, '{}_bid'.format(mer))
                                        if not canStopProfit:
                                            if nowPrice < round(price - getattr(self, '{}_{}'.format(mer, UNIT_STOP_PROFIT)), getattr(self, '{}_{}'.format(mer, PRICE_DECIMAL))):
                                                canStopProfit = True
                                                fixprotocol.order(mer, side, getattr(self, '{}_{}'.format(mer, VOLUME)),
                                                                MARKET, nowPrice, 0,
                                                                '1', '', '', 1)
                                                time.sleep(0.0001)
                                                tradeCount += 1

                                                price = round(nowPrice + getattr(self, '{}_{}'.format(mer, UNIT_TRAILING)), getattr(self, '{}_{}'.format(mer, PRICE_DECIMAL)))
                                                id_ =  fixprotocol.order_cancel_replace('', id_, mer, side_, int(getattr(self, '{}_{}'.format(mer, VOLUME))*tradeCount), STOP, 0, price, '1', '')
                                               
                                                Logs('{} - Open plus {}, price : {}, trade times : {}'.format(id_tag, s, nowPrice, tradeCount))
                                                Logs('{} - Change {} stop order, price : {}'.format(id_tag, s_, price))

                                        else:
                                            if nowPrice < round(price - 2*getattr(self, '{}_{}'.format(mer, UNIT_TRAILING)), getattr(self, '{}_{}'.format(mer, PRICE_DECIMAL))):
                                                fixprotocol.order(mer, side, getattr(self, '{}_{}'.format(mer, VOLUME)),
                                                                MARKET, nowPrice, 0,
                                                                '1', '', '', 1)
                                                time.sleep(0.0001)
                                                tradeCount += 1

                                                price = round(nowPrice + getattr(self, '{}_{}'.format(mer, UNIT_TRAILING)), getattr(self, '{}_{}'.format(mer, PRICE_DECIMAL)))
                                                id_ =  fixprotocol.order_cancel_replace('', id_, mer, side_, int(getattr(self, '{}_{}'.format(mer, VOLUME))*tradeCount), STOP, 0, price, '1', '')
                                               
                                                Logs('{} - Open plus {}, price : {}, trade times : {}'.format(id_tag, s, nowPrice, tradeCount))
                                                Logs('{} - Change {} stop order, price : {}'.format(id_tag, s_, price))

                                    
                                            
                                    do_ = False
                        else:
                            Logs('{} - No ClOrdID - {}'.format(id_tag, res))

                lock.release()


        delattr(self, id_tag)

        
        Logs('{} - Stop trailing'.format(id_tag))



    def __genTrailingThread(self, lock, fixprotocol, side, price, flag, mer):

        threading.Thread(target = self.__trailing,
                        args = (lock, fixprotocol, side, price, flag, mer)).start()


    def __genPunchThread(self, lock, fixprotocol, side, price, flag):

        threading.Thread(target = self.__openPunchPosition,
                        args = (lock, fixprotocol, side, price, flag)).start()

    def __openPunchPosition(self, lock, fixprotocol, side, price, flag):


        if not self.hasPunchPosition:
            self.hasPunchPosition = True

            lock.acquire()
            id_ = fixprotocol.order(MERCHANDISE, side, PUNCH_VOLUME,
                                MARKET, 0, price,
                                '1', '', '', 1)
            lock.release()
            self.punchCount += 1
            if side == BUY:
                s = 'buy'
            elif side == SELL:
                s = 'sell'


            Logs('Open punch position - {}, price : {}, times : {}'.format(s, price, self.punchCount))


            do = True
            while do and flag == self.trailFlag:
                
                if self.canPlus:
                    self.canPlus = False
                    if self.punchCount <= PUNCH_LIMIT:

                        if side == BUY:
                            price = self.ask_price
                        elif side == SELL:
                            price = self.bid_price


                        lock.acquire()
                        id_ = fixprotocol.order(MERCHANDISE, side, PUNCH_VOLUME,
                                            MARKET, 0, price,
                                            '1', '', '', 1)
                        lock.release()
                        self.punchCount += 1
                        

                        Logs('Open punch position - {}, price : {}, times : {}'.format(s, price, self.punchCount))
                    else:
                        do = False
                        Logs('Complete plus trade')

             
            Logs('Complete punch thread')
                
        else:
            Logs('Has Punch position')




    def resetTradeStatus(self, mer):
        Logs('Reset trade status')
        
        self.__resetTradeStatus(self.LOCK_1, self.fixprotocol_1, ACCOUNT_1, mer)

    def __resetTradeStatus(self, lock, fixprotocol, account, mer):
       
        lock.acquire()
        self.cancelAllOrder(fixprotocol, account, mer)
        self.closeAllPosition(fixprotocol, account, mer)
        lock.release()
     
    def queryPosition(self, fixprotocol, account, mer):

        res = fixprotocol.request_position(account, mer, 1)
        
        res = fixprotocol.request_position_ack()
        pre = dt.now()
        while res == '':  
            res = fixprotocol.request_position_ack()
            now = dt.now()
            if (now - pre).seconds > 15:
                Logs('Account : {}, merchandise : {} - Query position ack timeout'.format(account, mer))
                return ERROR, -1

        
        if 'PosReqResult' in res:
            if res['PosReqResult'] == '2':
                
                return '0', 0


        res = fixprotocol.receive_position_report()
        pre = dt.now()
        while res == '':  
            res = fixprotocol.receive_position_report()
            now = dt.now()
            if (now - pre).seconds > 15:
                Logs('Account : {}, merchandise : {}- Query position report timeout'.format(account, mer))
                return ERROR, -1

        
        posi = res['Positions'][0]
        if 'LongPos' in posi:
            vol = int(posi['LongPos'])
            side = BUY
        elif 'ShortPos' in posi:
            vol = int(posi['ShortPos'])
            side = SELL

       
        return side, vol

   
    def cancelAllOrder(self, fixprotocol, account, mer):
        Logs('Account : {}, merchandise : {} - Do cancel all order'.format(account, mer))

        try:
            fixprotocol.request_order_mass_status(7)

            do = True
        
            while do:
                Logs('Account : {}, merchandise : {} - cancel...'.format(account, mer))
                do_ = True
                pre = dt.now()
                while do_:
                    res = fixprotocol.receive_execution_report()
                    if res != '':
                        
                        if 'Text' in res:
                            
                            if res['Text'] == 'No open orders.':
                                do = False
                                do_ = False
                                Logs('Account : {}, merchandise : {} - No order to cancel'.format(account, mer))
                        else:
                            if res['OrdStatus'] == '0':
                                if res['Symbol'] == mer:
                                    fixprotocol.order_cancel('',
                                                            res['ClOrdID'],
                                                            res['Symbol'],
                                                            res['Side'],
                                                            int(res['LeavesQty']))
                                    time.sleep(0.0001)
                                if 'LastRptRequested' in res:
                                    if res['LastRptRequested'] == 'Y':
                                        Logs('Account : {}, merchandise : {} - Cancel All order successfully'.format(account, mer))
                                        do = False
                                        do_ = False

                        now = dt.now()
                        if (now - pre).seconds > 15:
                            Logs('Account : {}, merchandise : {}- Query orders timeout'.format(account, mer))
                            do_ = False
        except KeyError:
            print('--------------------')
            Logs('Account : {}, merchandise : {} - cancelAllOrder : KeyError'.format(account, mer))
            print(res)
            print('--------------------')
            self.cancelAllOrder(fixprotocol, account, mer)
   
    def closeAllPosition(self, fixprotocol, account, mer):

        Logs('Account : {}, merchandise : {} - Close all position'.format(account, mer))
        side, vol = self.queryPosition(fixprotocol, account, mer)

        if side == BUY:
            side = SELL
        elif side == SELL:
            side = BUY 
        elif side == ERROR:
            Logs('Account : {}, merchandise : {} - Close again...'.format(account, mer))
            self.closeAllPosition(fixprotocol, account, mer)

        if vol > 0:
            res = fixprotocol.order(mer, side, vol, MARKET, 0, 0, '1', '', '', 1)
            time.sleep(0.0001)
            Logs('Account : {}, merchandise : {} - Close all position successfully'.format(account, mer))
        elif vol == 0:
            Logs('Account : {}, merchandise : {} - No position to close'.format(account, mer))
    


    def run(self):
        stageFlag = 0
        while True:

            stage = self.check_stage()

            if stage == 'open':
                if not self.isTrading:
                    self.start()
                    stageFlag = 0
            elif stage == 'close':
                if self.isTrading:
                
                    self.stop()
                
                if stageFlag == 0:
                    
                    Logs('Not trading time')
                    stageFlag = 1

            time.sleep(60)     


    def start(self):

        resPrice = self.priceFix.login(PRICE_USERNAME, PRICE_PASSWORD)
        res1 = self.fixprotocol_1.login(USERNAME_1, PASSWORD_1)
      
        Logs('Res price {}'.format(resPrice))
        Logs('Res 1 {}'.format(res1))
       
        if resPrice and res1:
            
            self.isTrading = True
            
            self.prepare_data()
            self.__genPriceThread()
            time.sleep(2)
            
            for portf in PORTFOLIO:
                self.__genSignalThread(portf)
                self.__genTradeThread(portf)
                
        else:
        
            Logs('[CFH] Login failed')
            self.exit()


    def stop(self):
        
        self.isTrading = False
        self.isMonitor = False
        while self.positionThread.is_alive() or self.positionThread_1.is_alive():
            pass

        for portf in PORTFOLIO:
            while getattr(self, '{}_tradeThread'.format(portf)).is_alive():
                pass
            Logs('{} - Trading is rest'.format(portf))

            # self.resetTradeStatus(mer)


        self.priceFix.logout()
        self.fixprotocol_1.logout()
        Logs('[CFH] Logout succeed')

    def check_stage(self):
        # now = dt.now()
        # if now.weekday() == 5 or now.weekday() == 6:
        #     date = now.strftime('%F')
        #     stage0_from = dt.strptime(date + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
        #     stage0_to = dt.strptime(date + ' 04:30:00', '%Y-%m-%d %H:%M:%S')
        #     if now.weekday() == 5 and stage0_from <= now < stage0_to:
        #         return 'open'
        #     else:
        #         Logs('[System] It is weekend')
        #         return 'close'
        # else:
        #     date = now.strftime('%F')
        #     stage1_from = dt.strptime(date + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
        #     stage1_to = dt.strptime(date + ' 05:00:00', '%Y-%m-%d %H:%M:%S')

        #     stage2_from = dt.strptime(date + ' 04:30:00', '%Y-%m-%d %H:%M:%S')
        #     stage2_to = dt.strptime(date + ' 14:02:00', '%Y-%m-%d %H:%M:%S')

        #     if now.weekday() == 0 and stage1_from <= now < stage1_to:
        #         return 'close'
        #     elif stage2_from <= now < stage2_to:
        #         return 'close'
        #     else:
        #         return 'open'


        now = dt.now()
        if now.weekday() == 5 or now.weekday() == 6:
            date = now.strftime('%F')
            stage0_from = dt.strptime(date + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
            stage0_to = dt.strptime(date + ' 04:30:00', '%Y-%m-%d %H:%M:%S')
            if now.weekday() == 5 and stage0_from <= now < stage0_to:
                return 'open'
            else:
                # Logs('[System] It is weekend')
                return 'close'
        else:
            date = now.strftime('%F')
            stage1_from = dt.strptime(date + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
            stage1_to = dt.strptime(date + ' 05:00:00', '%Y-%m-%d %H:%M:%S')
            if now.weekday() == 0 and stage1_from <= now < stage1_to:
                return 'close'
            else:
                return 'open'


    def exit(self):
        self.priceFix.logout()
        self.fixprotocol_1.logout()

        Logs('Program is terminated')


if __name__ == '__main__':



    algo = AlgoBank()
    res = algo.LOGIN("xxxxx", sys.argv[2], sys.argv[3])
    Logs(res)
    if res['result']:

        priceFix = Fixprotocol(API_PATH)
        priceFix.initialize(PRICE_PATH)

        fixprotocol_1 = Fixprotocol(API_PATH)
        fixprotocol_1.initialize(ORDER_PATH_1)

       
        mt = MatchTrade(algo, priceFix, fixprotocol_1)
        mt.run()

    else:
        Logs('Algo login failed')