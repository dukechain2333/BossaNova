#  @author Duke Chain
#  @File:DualThrust.py
#  @createTime 2020/11/17 20:24:17

from DBOperate.QueryStockInfo import QueryStockInfo


class DualThrust:
    """
    Dual Thrust是一个趋势跟踪系统
    计算前N天的最高价－收盘价和收盘价－最低价。然后取这2N个价差的最大值，乘以k值。把结果称为触发值。
    在今天的开盘，记录开盘价，然后在价格超过上轨（开盘＋触发值）时马上买入，或者价格低于下轨（开盘－触发值）时马上卖空。
    """

    def __init__(self, stockID, barrier, dateList, tradeFlags):
        """
        Args:
            stockID:传入股票代码
            barrier:进程同步器
            dateList:传入日期列表
            tradeFlags:交易信号数组(1)
        """
        self.stockID = stockID
        self.barrier = barrier
        self.dateList = dateList
        self.N = 5
        self.K = 0.2
        self.tradeFlags = tradeFlags

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

    def _rangeN(self, startPointer):
        """
        计算触发值
        计算前N天的最高价－收盘价和收盘价－最低价。然后取这2N个价差的最大值，乘以k值。把结果称为触发值。

        Args:
            startPointer:前N天的起始位置

        Returns:
            rangeN:触发值
        """
        NClose = []
        NHigh = []
        NLow = []

        for i in range(self.N):
            dayData = self._getDataDay(self.dateList[startPointer])
            NClose.append(dayData[0][1])
            NHigh.append(dayData[0][2])
            NLow.append(dayData[0][3])

        # 前N天最高价最大值
        highMax = max(NHigh)
        # 前N天收盘价最小值
        closeMin = min(NClose)
        # 前N天收盘价最大值
        closeMax = max(NClose)
        # 前N天最低价最小值
        lowMin = min(NLow)

        rangeN = max(highMax - closeMin, closeMax - lowMin) * self.K
        return rangeN

    def main(self):
        """
        主方法
        """
        for i in range(self.N - 1, len(self.dateList)):
            # 计算当天上轨与下轨
            dayOpen = self._getDataMinute(self.dateList)[0][1]
            rangeN = self._rangeN(i - (self.N - 1))
            # 上轨
            dayCeil = rangeN + dayOpen
            # 下轨
            dayFloor = rangeN - dayOpen

            # 获取当天的分钟数据
            minuteData = self._getDataMinute(self.dateList[i])

            for minute in range(len(minuteData)):
                # 分钟数据大于上轨，买入
                if minuteData[minute][1] > dayCeil:
                    self.tradeFlags[1] = 1
                    self.barrier.wait()
                # 分钟数据小于下轨，卖出
                elif minuteData[minute][1] < dayFloor:
                    self.tradeFlags[1] = -1
                    self.barrier.wait()
                # 分钟数据位于上下轨之间，不作为
                else:
                    self.tradeFlags[1] = 0
                    self.barrier.wait()
