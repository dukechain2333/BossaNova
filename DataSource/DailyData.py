#  @author Duke Chain
#  @File:DailyData.py
#  @createTime 2020/11/05 22:23:05

from threading import Thread
import threading
from DBOperate.CreateStockInfo import CreateStockInfo
from DBOperate.AddStockInfo import AddStockInfo
import tushare as ts


class DailyData(threading.Thread):
    """
    获取Daily数据并写入数据库，注意若传入日期为20201001-20201002仅录入20201002数据

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

        try:
            open_price = data['open'][0]
            high_price = data['high'][0]
            low_price = data['low'][0]
            close_price = data['close'][0]
            pre_close = data['pre_close'][0]
            chg = data['change'][0]
            pct_chg = data['pct_chg'][0]
            vol = data['vol'][0]
            amount = data['amount'][0]

        except IndexError:
            print(self.endDate, '数据不存在，请修改时间！')

        else:
            createInfo = CreateStockInfo(self.stockID, 'stock_info_daily')
            createInfo.createTable()
            addInfo = AddStockInfo(self.stockID, self.endDate, open_price, high_price, low_price, close_price,
                                   pre_close,
                                   chg, pct_chg, vol, amount)
            addInfo.addInfoDaily()

# ts.set_token('6e32db130ef1ea433f2bf65245ae9026e355cb91d31e47ee50b76598')
# pro = ts.pro_api()
# test = DailyData(pro, '603385.SH', '20200104', '20200105')
# test.start()
