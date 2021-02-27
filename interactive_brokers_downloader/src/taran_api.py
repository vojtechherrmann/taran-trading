import datetime
import threading
import logging

from ibapi.client import EClient, TickerId, BarData, ListOfHistoricalTickLast, ListOfHistoricalTickBidAsk
from ibapi.wrapper import EWrapper


class TaranApi(EWrapper, EClient):

    # according to https://www.quantstart.com/articles/connecting-to-the-interactive-brokers-native-python-api/
    _note_codes = [2104, 2106, 2158]

    def __init__(self):
        EClient.__init__(self, self)
        self.requestsDone = []
        self.tickData = []
        self.barData = []
        self.condition_lock = threading.Condition()

    def error(self, reqId: TickerId, errorCode: int, errorString: str) -> None:
        super().error(reqId, errorCode, errorString)
        error_type = "ERROR"
        if errorCode in self._note_codes:
            error_type = "NOTE"
        errStr = f"{error_type} ReqId: {reqId}, Code: {errorCode} Msg: {errorString}"
        logging.warning(errStr)
        print(errStr)
        with self.condition_lock:
            self.requestsDone.append(reqId)
            self.condition_lock.notify()

    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast, done: bool):
        for tick in ticks:
            logging.info(f"HistoricalTickLast. ReqId: {reqId}, {tick}")
        with self.condition_lock:
            self.requestsDone.append(reqId)
            self.tickData = self.tickData + ticks
            self.condition_lock.notify()

    def historicalTicksBidAsk(self, reqId: int, ticks: ListOfHistoricalTickBidAsk, done: bool):
        for tick in ticks:
            logging.info(f"historicalTicksBidAsk. ReqId: {reqId}, {tick}")
        with self.condition_lock:
            self.requestsDone.append(reqId)
            self.tickData = self.tickData + ticks
            self.condition_lock.notify()

    def historicalData(self, reqId: int, bar: BarData):
        logging.info(f"HistoricalData. ReqId: {reqId}, {bar}")
        with self.condition_lock:
            self.barData.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        logging.info(f"HistoricalDataEnd. ReqId: {reqId}, from: {start}, to: {end}")
        with self.condition_lock:
            self.requestsDone.append(reqId)
            self.condition_lock.notify()
