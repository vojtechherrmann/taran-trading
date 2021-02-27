import json
import logging
import os
from typing import List
from datetime import datetime, timedelta, time

from ibapi.contract import Contract
from pytz import timezone
import pandas as pd

from src.taran_wrapper import TaranWrapper


class InteractiveBrokersDownloader:

    def __init__(self):
        self._init_logger()

        self.tickers: List[str] = []
        self.end_date = datetime.today() - timedelta(days=1)
        self.history_length_days: int = None  # type: ignore
        self._load_and_unpack_appsettings()

        self.wrapper = TaranWrapper()
        self.contract = Contract()

    def _init_logger(self):
        now = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        logging.basicConfig(filename=f"logs/api_{now}.log", level=logging.INFO)

    def _load_and_unpack_appsettings(self, path: str = "./appsettings.json") -> None:

        attrs_mandatory_error = ['history_length_days']
        attrs_mandatory_warning = ['tickers']
        attrs_optional = ['end_date']

        with open(path, 'r') as f:
            appsettings = json.load(f)

        for _attr in attrs_mandatory_error:
            if _attr in appsettings.keys():
                self.__setattr__(_attr, appsettings[_attr])
            else:
                raise RuntimeError(f"No {_attr} provided in appsettings")

        for _attr in attrs_mandatory_warning:
            if _attr in appsettings.keys():
                self.__setattr__(_attr, appsettings[_attr])
            else:
                logging.warning(f"No {_attr} provided in appsettings")

        for _attr in attrs_optional:
            if _attr in appsettings.keys():
                self.__setattr__(_attr, appsettings[_attr])

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
