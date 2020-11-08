#  @author Duke Chain
#  @File:WeeklyData.py
#  @createTime 2020/11/08 19:55:08

from threading import Thread
from DBOperate.CreateStockInfo import CreateStockInfo
from DBOperate.AddStockInfo import AddStockInfo


class WeeklyData(Thread):
    """
    获取Weekly数据并写入数据库，注意若传入日期为20201001-20201002仅录入20201002数据

     Args:
        pro:传入Tushare接口
        stockID:传入股票代码
        startDate:数据起始日期
        endDate:数据结束日期
    """
    def __init__(self, pro, stockID, startDate, endDate):
        super().__init__()
        self.pro = pro
        self.stockID = stockID
        self.startDate = startDate
        self.endDate = endDate

    def run(self):
        data = self.pro.daily(ts_code=self.stockID, start_date=self.startDate, end_date=self.endDate)

        open_price = data['open']
        high_price = data['high']
        low_price = data['low']
        close_price = data['close']
        pre_close = data['pre_close']
        chg = data['change']
        pct_chg = data['pct_change']
        vol = data['vol']
        amount = data['amount']

        createInfo = CreateStockInfo(self.stockID)
        createInfo.createTable()
        addInfo = AddStockInfo(self.stockID, self.endDate, open_price, high_price, low_price, close_price, pre_close,
                               chg, pct_chg, vol, amount)
        addInfo.addInfoWeekly()
