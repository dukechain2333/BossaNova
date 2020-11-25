#  @author Duke Chain
#  @File:QueryStockInfo.py
#  @createTime 2020/11/10 22:12:10


import pymysql


class QueryStockInfo:
    """
    查询特定日期的股票数据

     Args:
        stockID:股票ID
        database:查询目标位于的数据库（daily,weekly,monthly,minutes）
        trade_date:特定日期（如20201111形式）
    """

    def __init__(self, stockID, database):
        self.stockID = stockID
        self.database = database

    def _connection(self):
        """
        建立和数据库的连接
        """
        conn = pymysql.connect("localhost", "root", "qian258046", self.database, charset='utf8')
        cursor = conn.cursor()
        return cursor, conn

    def query(self, trade_date):
        """
        Returns:
             data:返回元组形式的数据
        """
        sql = None
        cursor, conn = self._connection()
        if self.database == 'stock_info_daily':
            sql = "select trade_date,close_price,high_price,low_price,open_price,pre_close,volume,outstanding_share,turnover from `{}`" \
                  "where trade_date like '{}%';".format(self.stockID, trade_date)
        elif self.database == 'stock_info_minutes':
            sql = "select trade_date,open_price,high_price,low_price,close_price,volume from `{}`" \
                  "where trade_date like '{}%';".format(self.stockID, trade_date)
        cursor.execute(sql)
        data = cursor.fetchall()
        conn.close()
        return data


if __name__ == '__main__':
    test = QueryStockInfo('sh601808', 'stock_info_minutes')
    data=test.query('2020-07-15')
    print(data)