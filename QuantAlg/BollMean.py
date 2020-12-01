#  @author Duke Chain
#  @File:BollMean.py
#  @createTime 2020/12/01 15:20:01

from DBOperate.QueryStockInfo import QueryStockInfo
from DBOperate.QueryTradeInfo import QueryTradeInfo
import numpy as np


class BollMean:
    """
    布林带是利用统计学中的均值和标准差联合计算得出的，分为均线，上轨线和下轨线。
    布林线均值回归策略认为，标的价格在上轨线和下轨线围成的范围内浮动，即使短期内突破上下轨，但长期内仍然会回归到布林带之中。
    因此，一旦突破上下轨，即形成买卖信号。突破下轨则形成卖信号
    中轨线 = N日移动平均线
    上轨线 = 中轨线 + n倍标准差
    下轨线 = 中轨线 - n倍标准差
    """

    def __init__(self, stockID, dateList, tradeFlags):
        """
        Args:
            stockID:股票ID
            dateList:传入日期列表
            tradeFlags:交易信号数组(4)
        """
        self.stockID = stockID
        self.dateList = dateList
        self.tradeFlags = tradeFlags
        # 均值和标准差计算长度
        self.WINDOW_LEN = 26
        # 确定布林带范围
        self.WIDTH = 1

    def _getDataMinute(self, tradeDate):
        """
        获取数据

        Args:
            tradeDate:交易日期

        Returns:
            minuteData:返回分钟数据
        """
        qry = QueryStockInfo(self.stockID, 'stock_info_minutes')
        minuteData = qry.query(tradeDate)

        return minuteData

    def _getDataDay(self, tradeDate):
        """
        获取数据

        Args:
            tradeDate:交易日期

        Returns:
            dailyData:返回日数据
        """
        qry = QueryStockInfo(self.stockID, 'stock_info_daily')
        dailyData = qry.query(tradeDate)

        return dailyData

    def main(self,barrierMinute, barrierDay):
        """
        主方法

        Args:
            barrierMinute:进程同步器(分钟)
            barrierDay:进程同步器(日)
        """
        dataList = []
        for day in range(len(self.dateList)):
            dayData = self._getDataDay(self.dateList[day])
            meanValue = np.mean(dataList)
            stdValue = np.std(dataList)

            if len(dataList) <= self.WINDOW_LEN:
                dataList.append(dayData[0][3])
            else:
                dataList.pop(0)
                dataList.append(dayData[0][3])

            # 计算上轨下轨
            bollCeil = meanValue + stdValue * self.WIDTH
            bollFloor = meanValue - stdValue * self.WIDTH

            minuteData = self._getDataMinute(self.dateList[day])

            for minute in minuteData:
                # 分钟数据大于上轨，卖出
                if minute[1] >= bollCeil:
                    self.tradeFlags[4] = -1
                    barrierMinute.wait()

                # 分钟数据小于下轨，买入
                elif minute[1] <= bollFloor:
                    self.tradeFlags[4] = 1
                    barrierMinute.wait()

                # 不作为
                else:
                    self.tradeFlags[4] = 0
                    barrierMinute.wait()

        # 日清算
        self.tradeFlags[4] = 0
        barrierDay.wait()
