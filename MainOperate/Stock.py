#  @author Duke Chain
#  @File:Stock.py
#  @createTime 2020/12/01 21:18:01

import multiprocessing
from multiprocessing import Barrier, Lock, Process
from QuantAlg.BollMean import BollMean
from QuantAlg.DualThrust import DualThrust
from QuantAlg.Fairy import Fairy
from QuantAlg.RBreaker import RBreaker
from QuantAlg.TwoMeanStrategy import TwoMeanStrategy


class Stock(multiprocessing.Process):
    def __init__(self, stockID, dateList, moneyHold):
        super().__init__()
        self.stockID = stockID
        self.dateList = dateList
        self.moneyHold = moneyHold
        self.tradeFlags = [0, 0, 0, 0, 0]
        self.QUANT_AMOUNT = 5
        self.barrierMinute = Barrier(self.QUANT_AMOUNT)
        self.barrierDay = Barrier(self.QUANT_AMOUNT)

    def run(self):
        # 布林均值实例
        bollMean = BollMean(self.stockID, self.barrierMinute, self.barrierDay, self.dateList, self.tradeFlags)
        # DualThrust实例
        dualThrust = DualThrust(self.stockID, self.barrierMinute, self.barrierDay, self.dateList, self.tradeFlags)
        # 菲阿里四价实例
        fairy = Fairy(self.stockID, self.barrierMinute, self.barrierDay, self.dateList, self.tradeFlags)
        # RBreaker实例
        rBreaker = RBreaker(self.stockID, self.barrierMinute, self.barrierDay, self.dateList, self.tradeFlags)
        # 双均线实例
        twoMean = TwoMeanStrategy(self.stockID, self.barrierMinute, self.barrierDay, self.dateList, self.tradeFlags)
