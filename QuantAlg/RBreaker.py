#  @author Duke Chain
#  @File:RBreaker.py
#  @createTime 2020/11/28 13:55:28

from DBOperate.QueryStockInfo import QueryStockInfo
from DBOperate.QueryTradeInfo import QueryTradeInfo


class RBreaker:
    """
    R-Breaker是一种短线日内交易策略
    根据前一个交易日的收盘价、最高价和最低价数据通过一定方式计算出六个价位，从大到小依次为：
    突破买入价、观察卖出价、反转卖出价、反转买入、观察买入价、突破卖出价。
    以此来形成当前交易日盘中交易的触发条件。

    a、b、c、d为策略参数
    观察卖出价（sSetup）= High + a * (Close – Low)
    观察买入（bSetup）= Low – a * (High – Close)
    反转卖出价（sEnter）= b / 2 * (High + Low) – c * Low
    反转买入价（bEnter）= b / 2 * (High + Low) – c * High
    突破卖出价（sBreak）= sSetup - d * (sSetup – bSetup)
    突破买入价（bBreak）= bSetup + d * (sSetup – bSetup)

    """

    def __init__(self, stockID, barrierMinute,barrierDay, dateList, tradeFlags):
        """
        Args:
            stockID:股票ID
            barrierMinute:进程同步器(分钟)
            barrierDay:进程同步器(日)
            dateList:传入日期列表
            tradeFlags:交易信号数组(2)
        """
        self.stockID = stockID
        self.barrierMinute = barrierMinute
        self.barrierDay = barrierDay
        self.dateList = dateList
        self.tradeFlags = tradeFlags
        # 策略参数
        self.SETUP_COEF = 0.25  # 参数a
        self.BREAK_COEF = 0.2  # 参数d
        self.ENTER_COEF_1 = 1.07  # 参数b
        self.ENTER_COEF_2 = 0.07  # 参数c

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

            # 观察卖出价（sSetup）= High + a * (Close – Low)
            sSetup = preDayData[0][2] + self.SETUP_COEF * (preDayData[0][1] - preDayData[0][3])
            # 观察买入（bSetup）= Low – a * (High – Close)
            bSetup = preDayData[0][3] - self.SETUP_COEF * (preDayData[0][2] - preDayData[0][1])
            # 反转卖出价（sEnter）= b / 2 * (High + Low) – c * Low
            sEnter = 0.5 * self.ENTER_COEF_1 * (preDayData[0][1] + preDayData[0][2]) - self.ENTER_COEF_2 * \
                     preDayData[0][3]
            # 反转买入价（bEnter）= b / 2 * (High + Low) – c * High
            bEnter = 0.5 * self.ENTER_COEF_1 * (preDayData[0][1] + preDayData[0][2]) - self.ENTER_COEF_2 * \
                     preDayData[0][2]
            # 突破卖出价（sBreak）= sSetup - d * (sSetup – bSetup)
            sBreak = sSetup - self.BREAK_COEF * (sSetup - bSetup)
            # 突破买入价（bBreak）= bSetup + d * (sSetup – bSetup)
            bBreak = bSetup - self.BREAK_COEF * (sSetup - bSetup)

            minuteData = self._getDataMinute(self.dateList[day + 1])
            dayHigh = float("-inf")

            for minute in minuteData:
                # 记录当天最高价格
                if minute[1] > dayHigh:
                    dayHigh = minute[1]

                tradeData = self._getTradeData()
                # 判断是否空仓
                # 空仓情况
                if tradeData[-1][3] == 0:
                    # 若价格>突破买入价，开仓做多
                    if minute[1] > bBreak:
                        self.tradeFlags[2] = 1
                        self.barrierMinute.wait()

                    # 若价格<突破卖出价，开仓做空
                    elif minute[1] < sBreak:
                        self.tradeFlags[2] = -1
                        self.barrierMinute.wait()

                    # 不作为
                    else:
                        self.tradeFlags[2] = 0
                        self.barrierMinute.wait()
                # 持仓情况
                else:
                    # 若日最高价>观察卖出价，然后下跌导致价格<反转卖出价，开仓做空
                    if dayHigh > sSetup and minute[1] < sEnter:
                        self.tradeFlags[2] = -1
                        self.barrierMinute.wait()

                    # 若日最低价<观察买入价，然后上涨导致价格>反转买入价，开仓做多
                    elif dayHigh < bSetup and minute[1] > bEnter:
                        self.tradeFlags[2] = 1
                        self.barrierMinute.wait()

                    # 不作为
                    else:
                        self.tradeFlags[2] = 0
                        self.barrierMinute.wait()

        # 日清算
        self.tradeFlags[2] = 0
        self.barrierDay.wait()
