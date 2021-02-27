import os
import string
import threading
import pandas as pd
from datetime import datetime, time, timedelta
from pytz import tzinfo, timezone, utc as tzUTC

from ibapi.contract import Contract

from .taran_api import TaranApi


class TaranWrapper:

    def __init__(self):
        self.request_id = 1
        self.app = TaranApi()
        self.app.connect('127.0.0.1', 7497, 123)
        self.api_thread = threading.Thread(target=self.runLoop, daemon=True)
        self.api_thread.start()

    def _quit(self):
        self.app.disconnect()

    def __del__(self):
        self._quit()

    def runLoop(self):
        self.app.run()

    def getHistoricalPreSaleData(self, contract: Contract, timeFrom: time, timeTo: time, timeZ: tzinfo, type: string):
        #dayReq = (datetime.now() - timedelta(years=1)).date()
        dayReq = (datetime.now() - timedelta(days=30)).date()
        timeFromReq = timeFrom
        max_ticks = 1000

        with self.app.condition_lock:
            self.app.tickData = []
        dataCollected = []

        #the API can give only day by day data, we need to iterate through days
        while dayReq < datetime.now().date():
            #convert from market timezone to local IB timezone
            dateFromMarket = timeZ.localize(datetime(dayReq.year, dayReq.month, dayReq.day, timeFromReq.hour,
                                      timeFromReq.minute, timeFromReq.second, timeFromReq.microsecond))

            dateToMarket = timeZ.localize(datetime(dayReq.year, dayReq.month, dayReq.day, timeTo.hour,
                                    timeTo.minute, timeTo.second, timeTo.microsecond))

            dateFromUtc = dateFromMarket.astimezone(tzUTC)
            dateToUtc = dateToMarket.astimezone(tzUTC)

            localTimeZone = timezone(os.environ['TIMEZONE_USER'])
            dateFromLocal = dateFromUtc.astimezone(localTimeZone)

            #request data
            reqId = self.request_id
            self.app.reqHistoricalTicks(reqId, contract, dateFromLocal.strftime('%Y%m%d %H:%M:%S'), "", max_ticks, type, 0, True, [])
            self.request_id += 1

            #wait for the answer
            with self.app.condition_lock:
                while not reqId in self.app.requestsDone:
                    self.app.condition_lock.wait()
                    newData = self.app.tickData
                    self.app.tickData = []

            #check if the answer contains all data from the currently requested day or no
            if len(newData) > 0:
                lastTick = newData[-1]
                lastTickDate = tzUTC.localize(datetime.utcfromtimestamp(lastTick.time))
                if len(newData) < max_ticks or lastTickDate >= dateToUtc:
                    #move to the next day
                    dayReq = dayReq + timedelta(days=1)
                    timeFromReq = timeFrom
                else:
                    #move the from time so we get another set of ticks
                    timeFromReq = lastTickDate.astimezone(timeZ).time()

                newData = [tick for tick in newData if tzUTC.localize(datetime.utcfromtimestamp(tick.time)) < dateToUtc]
                dataCollected = dataCollected + newData
            else:
                # move to the next day
                dayReq = dayReq + timedelta(days=1)

        if type == 'TRADES':
            variables = ['time', 'price', 'size', 'exchange', 'specialConditions']
        elif type == 'BID_ASK':
            variables = ['time', 'priceBid', 'priceAsk', 'sizeBid', 'sizeAsk']
        else:
            raise NotImplementedError()

        df = pd.DataFrame([[getattr(tick, attr) for attr in variables] for tick in dataCollected], columns=variables)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        if type == 'TRADES':
            df['date'] = pd.to_datetime(df['time'].dt.date)
        return df

    def getHistoricalOpenData(self, contract:Contract):
        dateTo = datetime.now()

        with self.app.condition_lock:
            self.app.barData = []

        reqId = self.request_id
        self.app.reqHistoricalData(reqId, contract, dateTo.strftime('%Y%m%d %H:%M:%S'), "1 Y", "1 day", "TRADES", 1, 1, False, [])
        self.request_id = self.request_id + 1

        with self.app.condition_lock:
            while reqId not in self.app.requestsDone:
                self.app.condition_lock.wait()

        df = pd.DataFrame(data=[vars(s) for s in self.app.barData])
        if df.shape[0] > 0:
            df['date'] = pd.to_datetime(df['date'])

        return df
