#  @author Duke Chain
#  @File:MinuteData.py
#  @createTime 2020/11/17 20:58:17

import threading
from DBOperate.CreateStockInfo import CreateStockInfo
from DBOperate.AddStockInfo import AddStockInfo
import akshare as ak


class MinuteData(threading.Thread):
    """
    获取Minute数据并写入数据库，注意若传入日期为20201001-20201002仅录入20201002数据

     Args:
        pro:传入Tushare接口
        stockID:传入股票代码
        startDate:数据起始日期
        endDate:数据结束日期
    """

    def __init__(self, ak, stockID, period='1'):
        super().__init__()
        self.ak = ak
        self.stockID = stockID
        self.period = period

    def run(self):
        data = self.ak.stock_zh_a_minute(symbol=self.stockID, period=self.period)
        createInfo = CreateStockInfo(self.stockID, 'stock_info_minutes', ifminute=True)
        createInfo.createTable()

        for i in range(19000):
            print('now processing ', self.stockID, ' No.', i)
            try:
                day = data['day'][i]
                open_price = data['open'][i]
                high_price = data['high'][i]
                low_price = data['low'][i]
                close_price = data['close'][i]
                volume = data['volume'][i]

            except IndexError:
                print('数据不存在，请修改参数！')

            else:
                # createInfo = CreateStockInfo(self.stockID, 'stock_info_minutes')
                # createInfo.createTable()
                addInfo = AddStockInfo(stockID=self.stockID, trade_date=day, open_price=open_price,
                                       high_price=high_price,
                                       low_price=low_price, close_price=close_price, volume=volume)
                addInfo.addInfoMinute()


if __name__ == '__main__':
    thread1 = MinuteData(ak, "sh601808")
    thread2 = MinuteData(ak, "sh601811")
    thread3 = MinuteData(ak, "sh601858")
    thread4 = MinuteData(ak, "sh601878")

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()

    print("ALL DONE!")
