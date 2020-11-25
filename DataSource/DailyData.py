#  @author Duke Chain
#  @File:DailyData.py
#  @createTime 2020/11/05 22:23:05

import threading
from DBOperate.CreateStockInfo import CreateStockInfo
from DBOperate.AddStockInfo import AddStockInfo
import akshare as ak


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
        data = data.reset_index()
        for i in range(data.shape[0]):
            try:
                print('Now: ', i)
                date = data['date'][i]
                close_price = data['close'][i]
                high_price = data['high'][i]
                low_price = data['low'][i]
                open_price = data['open'][i]
                volume = data['volume'][i]
                outstanding_share = data['outstanding_share'][i]
                turnover = data['turnover'][i]

            except IndexError:
                print(self.endDate, '数据不存在，请修改时间！')

            else:
                createInfo = CreateStockInfo(self.stockID, 'stock_info_daily')
                createInfo.createTable()
                addInfo = AddStockInfo(self.stockID, date, close_price, high_price, low_price, open_price,
                                       volume, outstanding_share, turnover)
                addInfo.addInfoDaily()


# if __name__ == '__main__':
    # thread1 = DailyData(ak, "sh601808", '20200715', '20201118')
    # thread2 = DailyData(ak, "sh601811", '20200715', '20201118')
    # thread3 = DailyData(ak, "sh601858", '20200715', '20201118')
    # thread4 = DailyData(ak, "sh601878", '20200715', '20201118')
    #
    # thread1.start()
    # thread2.start()
    # thread3.start()
    # thread4.start()
    #
    # thread1.join()
    # thread2.join()
    # thread3.join()
    # thread4.join()
    #
    # print("ALL DONE!")
