import os
from datetime import datetime, timedelta, time

from ibapi.contract import Contract
from pytz import timezone
import pandas as pd

from .src.taran_wrapper import TaranWrapper
from ..taran_service import TaranService


class InteractiveBrokersDownloader(TaranService):

    def __init__(self):
        # appsettings
        self.end_date = datetime.today() - timedelta(days=1)
        self.history_length_days: int = None  # type: ignore
        self._load_and_unpack_appsettings()

        super().__init__()

        self.wrapper = TaranWrapper()
        self.contract = Contract()

    def run(self):

        # TODO for cycle
        symbol = self.tickers[0]

        self.contract.symbol = symbol
        self.contract.exchange = 'SMART'
        self.contract.secType = 'STK'
        self.contract.currency = 'USD'

        timeFrom = time(hour=4)
        timeTo = time(hour=9, minute=30)
        timeZ = timezone(os.environ['TIMEZONE_MARKET'])

        dataPreSaleTrades = self.wrapper.getHistoricalPreSaleData(self.contract, timeFrom, timeTo, timeZ, 'TRADES')
        dataPreSaleBidAsk = self.wrapper.getHistoricalPreSaleData(self.contract, timeFrom, timeTo, timeZ, 'BID_ASK')

        dataPreSaleBidAsk['timeBidAsk'] = dataPreSaleBidAsk['time']
        dataPreSale = pd.merge_asof(
            dataPreSaleTrades, dataPreSaleBidAsk,
            on='time',
            allow_exact_matches=False,
            direction='backward'
        )

        dataDaily = self.wrapper.getHistoricalOpenData(self.contract)

        # find previous date for close
        dataDaily1 = dataDaily[["date"]].copy(deep=True)
        dataDaily2 = dataDaily1.copy(deep=True)
        dataDaily1['key'] = 0
        dataDaily2['key'] = 0

        dataJoined = dataDaily1.merge(dataDaily2, on='key', suffixes=("", "_prev"))
        dataJoined = dataJoined[dataJoined["date"] > dataJoined["date_prev"]]
        dataJoined = dataJoined.drop('key', axis=1)
        dataPreviousDate = dataJoined.groupby("date").max()

        data = dataPreSale.merge(dataDaily, on='date')
        data = data.merge(dataPreviousDate, on='date')
        data = data.merge(dataDaily, left_on='date_prev', right_on='date', suffixes=("", "_prev"))
        data = data.drop('date_prev', axis=1)

        print(data)

        data.to_excel("data_trading.xlsx")
