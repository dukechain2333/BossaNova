#  @author Duke Chain
#  @File:TickData.py
#  @createTime 2020/12/08 15:25:08

import threading
from DBOperate.CreateStockInfo import CreateStockInfo
from DBOperate.AddStockInfo import AddStockInfo
import akshare as ak


class TickData(threading.Thread):
    """
       获取Tick数据并写入数据库

        Args:
           ak:传入akshare接口
           stockID:传入股票代码
            dateList:传入日期列表
       """

    def __init__(self, ak, stockID, dateList):
        super().__init__()
        self.ak = ak
        self.stockID = stockID
        self.dateList = dateList

    def run(self):
        createInfo = CreateStockInfo(self.stockID, 'stock_info_tick', 't')
        createInfo.createTable()
        for date in self.dateList:
            data = self.ak.stock_zh_a_tick_tx(code=self.stockID, trade_date=date)
            for i in range(data.shape[0]):
                try:
                    print(date + ' ' + data['成交时间'][i])
                    trade_date = date + ' ' + data['成交时间'][i]
                    stock_price = data['成交价格'][i]
                    chg = data['价格变动'][i]
                    volume = data['成交量(手)'][i]

                except IndexError:
                    print(date, '数据不存在，请修改时间！')

                else:
                    addInfo = AddStockInfo(self.stockID, trade_date=trade_date, close_price=stock_price, chg=chg,
                                           volume=volume)
                    addInfo.addInfoTick()

# if __name__ == '__main__':
#     dateList = ['20200907', '20200908', '20200909', '20200910', '20200911']
#     thread1 = TickData(ak, "sh601808", dateList)
#     thread2 = TickData(ak, "sh601811", dateList)
#     thread3 = TickData(ak, "sh601858", dateList)
#     thread4 = TickData(ak, "sh601878", dateList)
#
#     thread1.start()
#     thread2.start()
#     thread3.start()
#     thread4.start()
#
#     thread1.join()
#     thread2.join()
#     thread3.join()
#     thread4.join()
#
#     print("ALL DONE!")
