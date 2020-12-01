#  @author Duke Chain
#  @File:Fairy.py
#  @createTime 2020/11/28 21:30:28


from DBOperate.QueryStockInfo import QueryStockInfo
from DBOperate.QueryTradeInfo import QueryTradeInfo


class Fairy:
    """
    菲阿里(Fairy)四价策略

    上轨=昨日最高点；
    下轨=昨日最低点；
    止损=今日开盘价;
    如果没有持仓，且现价大于了昨天最高价做多，小于昨天最低价做空。
    如果有多头持仓，当价格跌破了开盘价止损。
    如果有空头持仓，当价格上涨超过开盘价止损。
    本策略使用当天收盘时全平的方式来处理不持有隔夜单的情况。
    """

    def __init__(self, stockID, barrierMinute, barrierDay, dateList, tradeFlags):
        """
        Args:
            stockID:股票ID
            barrierMinute:进程同步器(分钟)
            barrierDay:进程同步器(日)
            dateList:传入日期列表
            tradeFlags:交易信号数组(3)
        """
        self.stockID = stockID
        self.barrierMinute = barrierMinute
        self.barrierDay = barrierDay
        self.dateList = dateList
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

    def _getTradeData(self):
        """
        获取交易信息

        Returns:
            tradeData:交易信息
        """
        qry = QueryTradeInfo(self.stockID)
        tradeData = qry.queryTradeInfo()

        return tradeData

    def main(self):
        """
        主方法
        """
        for day in range(len(self.dateList) - 1):
            preDayData = self._getDataDay(self.dateList[day])
            tradeData = self._getTradeData()
            minuteData = self._getDataMinute(self.dateList[day + 1])

            # 上轨=昨日最高点
            dayCeil = preDayData[0][2]
            # 下轨=昨日最低点
            dayFloor = preDayData[0][3]
            # 止损=今日开盘价
            dayOpen = minuteData[0][1]

            for minute in minuteData:
                # 空仓情况
                if tradeData[-1][3] == 0:
                    # 现价大于了昨天最高价做多
                    if minute[1] > dayCeil:
                        self.tradeFlags[3] = 1
                        self.barrierMinute.wait()

                    # 现价小于昨天最低价做空
                    elif minute[1] < dayFloor:
                        self.tradeFlags[3] = -1
                        self.barrierMinute.wait()

                    # 不作为
                    else:
                        self.tradeFlags[3] = 0
                        self.barrierMinute.wait()

                # 持仓情况
                else:
                    # 价格跌破了开盘价卖出（止损）
                    if minute[1] < dayOpen:
                        self.tradeFlags[3] = -1
                        self.barrierMinute.wait()

                    # 价格上涨超过开盘价买入（止损）
                    elif minute[1] > dayOpen:
                        self.tradeFlags[3] = 1
                        self.barrierMinute.wait()

                    # 不作为
                    else:
                        self.tradeFlags[3] = 0
                        self.barrierMinute.wait()

        # 当天结束全部卖出，不持有隔夜单
        self.tradeFlags[3] = -1
        self.barrierDay.wait()
