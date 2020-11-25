#  @author Duke Chain
#  @File:DailyData.py
#  @createTime 2020/11/05 22:23:05

import threading
from DBOperate.CreateStockInfo import CreateStockInfo
from DBOperate.AddStockInfo import AddStockInfo


class DailyData(threading.Thread):
    """
    获取Daily数据并写入数据库

     Args:
        ak:传入akshare接口
        stockID:传入股票代码
        startDate:数据起始日期(形如'20201103')
        endDate:数据结束日期(形如'20201103')
    """

    def __init__(self, ak, stockID, startDate, endDate):
        super().__init__()
        self.ak = ak
        self.stockID = stockID
        self.startDate = startDate
        self.endDate = endDate

    def run(self):
        data = self.ak.stock_zh_a_daily(symbol=self.stockID, start_date=self.startDate, end_date=self.endDate)

        try:
            date = data['date'][0]
            close_price = data['close'][0]
            high_price = data['high'][0]
            low_price = data['low'][0]
            open_price = data['open'][0]
            volume = data['volume'][0]
            outstanding_share = data['outstanding_share'][0]
            turnover = data['turnover'][0]

        except IndexError:
            print(self.endDate, '数据不存在，请修改时间！')

        else:
            createInfo = CreateStockInfo(self.stockID, 'stock_info_daily')
            createInfo.createTable()
            addInfo = AddStockInfo(self.stockID, date, close_price, high_price, low_price, open_price,
                                   volume, outstanding_share, turnover)
            addInfo.addInfoDaily()

