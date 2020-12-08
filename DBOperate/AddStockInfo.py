#  @author Duke Chain
#  @File:AddStockInfo.py
#  @createTime 2020/11/08 19:14:08

import pymysql


class AddStockInfo:
    """
    在stock_info数据库的对应数据表中更新股票信息

    Args:
        stockID:股票代码
        trade_date:信息日期
        close_price:收盘价
        high_price:最高价
        low_price:最低价
        open_price:开盘价
        volume:成交量
        outstanding_share:流动股本
        turnover:换手率
        chg:价格变动
    """

    def __init__(self, stockID=None, trade_date=0, close_price=0, high_price=0, low_price=0, open_price=0, volume=0,
                 outstanding_share=0,
                 turnover=0, chg=0):
        # 初始化
        self.stockID = stockID
        self.trade_date = trade_date
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.volume = volume
        self.outstanding_share = outstanding_share
        self.turnover = turnover
        self.chg = chg

    def _connection(self, database):
        """
        建立和数据库的连接
        """
        conn = pymysql.connect("localhost", "root", "qian258046", database, charset='utf8')
        cursor = conn.cursor()
        return cursor, conn

    def addInfoDaily(self):
        """
        添加股票Daily信息
        """
        cursor, conn = self._connection('stock_info_daily')
        sql = 'INSERT INTO `{}`(trade_date,close_price,high_price,low_price,open_price,volume,outstanding_share,' \
              'turnover)' \
              'VALUES ("{}",{},{},{},{},{},{},{}) '.format(self.stockID, self.trade_date,
                                                           self.close_price, self.high_price, self.low_price,
                                                           self.open_price, self.volume, self.outstanding_share,
                                                           self.turnover)
        cursor.execute(sql)
        conn.commit()
        print(self.stockID + ' daily信息已更新')
        conn.close()

    def addInfoMinute(self):
        """
        添加股票Minutes信息
        """
        cursor, conn = self._connection('stock_info_minutes')
        sql = 'INSERT INTO `{}`(trade_date,open_price,high_price,low_price,close_price,volume)' \
              'VALUES ("{}",{},{},{},{},{}) '.format(self.stockID, self.trade_date,
                                                     self.open_price, self.high_price, self.low_price,
                                                     self.close_price, self.volume)
        cursor.execute(sql)
        conn.commit()
        print(self.stockID + ' minutes信息已更新')
        conn.close()

    def addInfoTick(self):
        """
        添加股票Tick信息
        """
        cursor, conn = self._connection('stock_info_tick')
        sql = 'INSERT INTO `{}`(trade_date,stock_price,chg,volume)' \
              'VALUES ("{}",{},{},{}) '.format(self.stockID, self.trade_date,
                                               self.close_price, self.chg, self.volume)
        cursor.execute(sql)
        conn.commit()
        print(self.stockID + ' tick信息已更新')
        conn.close()

# test = AddStockInfo('603385.SH', '20200101', 100, 200, 50, 150, 10, 10, 20, 30, 40)
# test.addInfoDaily()
