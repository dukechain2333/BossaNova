#  @author Duke Chain
#  @File:QueryStockInfo.py
#  @createTime 2020/11/10 22:12:10


import pymysql


class QueryStockInfo:
    """
    查询特定日期的股票数据

     Args:
        stockID:股票ID
        database:查询目标位于的数据库（daily,weekly,monthly）
        trade_date:特定日期（如20201111形式）
    """
    def __init__(self, stockID, database, trade_date):
        self.stockID = stockID
        self.database = database
        self.trade_date = trade_date

    def _connection(self):
        """
        建立和数据库的连接
        """
        conn = pymysql.connect("localhost", "root", "qian258046", self.database, charset='utf8')
        cursor = conn.cursor()
        return cursor, conn

    def query(self):
        """
        Returns:
             data:返回元组形式的数据
        """
        cursor, conn = self._connection()
        sql = "select open_price,high_price,low_price,close_price,pre_close,chg,pct_chg,vol,amount from `{}` " \
              "where trade_date='{}';".format(self.stockID, self.trade_date)
        cursor.execute(sql)
        data = cursor.fetchall()
        conn.close()
        return data


test = QueryStockInfo('603385.SH', 'stock_info_daily', '20200102')
data = test.query()
print(data)
