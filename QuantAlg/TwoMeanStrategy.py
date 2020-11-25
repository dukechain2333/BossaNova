#  @author Duke Chain
#  @File:TwoMeanStrategy.py
#  @createTime 2020/11/10 21:58:10

from DBOperate.QueryStockInfo import QueryStockInfo


class TwoMeanStrategy:
    """
    双均线策略
    短周期默认为10，长周期默认为20，当短期均线由上向下穿越长期均线时卖出，当短期均线由下向上穿越长期均线时买入

    """

    def __init__(self, stockID, barrier, dateList, shortTerm=10, longTerm=20):
        """
        Args:
            stockID:传入股票代码
            barrier:进程同步器
            dateList:传入日期列表
            shortTerm:短周期(默认10)
            longTerm:长周期(默认20)
        """
        self.stockID = stockID
        self.barrier = barrier
        self.dateList = dateList
        self.shortTerm = shortTerm
        self.longTerm = longTerm
        self.tradeFlag = 0

    def _getData(self, tradeDate):
        """
        获取数据

        Args:
            tradeDate:交易日期

        Returns:
            data:返回指定交易日期的数据
        """
        qry = QueryStockInfo(self.stockID, 'stock_info_minutes')
        data = qry.query(tradeDate)

        return data

    def mainalg(self):

        sumShort = 0
        sumLong = 0
        # 均值存储容器
        shortMeanList = []
        longMeanList = []

        for date, time in self.dateList, range(1, len(self.dateList) + 1):
            # 均值清零
            if time % self.shortTerm == 0:
                sumShort = 0
            elif time % self.longTerm == 0:
                sumLong = 0

            # 获取数据并计算短期与长期均值
            data = self._getData(date)
            sumShort += data[0][1]
            sumLong += data[0][1]
            shortMean = sumShort / (time % self.shortTerm)
            longMean = sumLong / (time % self.longTerm)
            shortMeanList.append(shortMean)
            longMeanList.append(longMean)

            # 均线比较
            if len(shortMeanList) >= 2 and len(longMeanList) >= 2:
                # 均线下穿，卖出
                if longMeanList[-2] < shortMeanList[-2] and longMean >= shortMean:
                    self.barrier.wait()
                    self.tradeFlag = -1

                # 均线上穿，买入（默认moneyHold的10%）
                elif longMeanList[-2] > shortMeanList[-2] and longMean <= shortMean:
                    self.barrier.wait()
                    self.tradeFlag = 1

                # 持有，不作为
                else:
                    self.barrier.wait()
                    self.tradeFlag = 0
