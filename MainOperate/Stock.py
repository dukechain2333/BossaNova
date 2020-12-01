#  @author Duke Chain
#  @File:Stock.py
#  @createTime 2020/12/01 21:18:01

import multiprocessing
from multiprocessing import Barrier, Lock, Process


class Stock:
    def __init__(self, stockID, dateList, moneyHold):
        self.stockID = stockID
        self.dateList = dateList
        self.moneyHold = moneyHold
        self.tradeFlags = [0, 0, 0, 0, 0]
        self.QUANT_AMOUNT = 5

    def main(self):
        barrier = Barrier(self.QUANT_AMOUNT)

