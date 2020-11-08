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
        open_price:开盘价
        high_price:最高价
        low_price:最低价
        close_price:收盘价
        pre_close:昨日收盘价
        chg:涨跌额
        pct_chg:涨跌幅
        vol:成交量（手）
        amount:成交额（千元）
    """

    def __init__(self, stockID, trade_date, open_price, high_price, low_price, close_price, pre_close, chg, pct_chg,
                 vol, amount):
        # 初始化
        self.stockID = stockID
        self.trade_date = trade_date
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.pre_close = pre_close
        self.chg = chg
        self.pct_chg = pct_chg
        self.vol = vol
        self.amount = amount

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
        sql = 'INSERT INTO stock_info_daily.%s(ts_code,trade_date,open_price,high_price,close_price,pre_close,chg,' \
              'pct_chg,vol,amount)' \
              'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
        value = (
            self.stockID, self.stockID, self.trade_date, self.open_price, self.high_price, self.close_price,
            self.pre_close, self.chg, self.pct_chg, self.vol, self.amount)
        cursor.execute(sql, value)
        conn.commit()
        print(self.stockID + ' daily信息已更新')
        conn.close()

    def addInfoWeekly(self):
        """
        添加股票Weekly信息
        """
        cursor, conn = self._connection('stock_info_weekly')
        sql = 'INSERT INTO stock_info_weekly.%s(ts_code,trade_date,open_price,high_price,close_price,pre_close,chg,' \
              'pct_chg,vol,amount)' \
              'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
        value = (
            self.stockID, self.stockID, self.trade_date, self.open_price, self.high_price, self.close_price,
            self.pre_close, self.chg, self.pct_chg, self.vol, self.amount)
        cursor.execute(sql, value)
        conn.commit()
        print(self.stockID + ' weekly信息已更新')
        conn.close()

    def addInfoMonthly(self):
        """
        添加股票Monthly信息
        """
        cursor, conn = self._connection('stock_info_monthly')
        sql = 'INSERT INTO stock_info_monthly.%s(ts_code,trade_date,open_price,high_price,close_price,pre_close,chg,' \
              'pct_chg,vol,amount)' \
              'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
        value = (
            self.stockID, self.stockID, self.trade_date, self.open_price, self.high_price, self.close_price,
            self.pre_close, self.chg, self.pct_chg, self.vol, self.amount)
        cursor.execute(sql, value)
        conn.commit()
        print(self.stockID + ' monthly信息已更新')
        conn.close()
