#  @author Duke Chain
#  @File:QueryTradeInfo.py
#  @createTime 2020/11/28 16:38:28

import pymysql


class QueryTradeInfo:
    """
    查询特定交易信息

     Args:
            stockID:股票ID
    """
    def __init__(self, stockID):
        """
        Args:
            stockID:股票ID
        """
        self.stockID = stockID

    def _connection(self):
        """
        建立和数据库的连接
        """
        conn = pymysql.connect("localhost", "root", "qian258046", 'trade_info', charset='utf8')
        cursor = conn.cursor()
        return cursor, conn

    def queryTradeInfo(self):
        """
        查询详细交易信息
        """
        cursor, conn = self._connection()
        sql = 'select * from `{}` order by trade_time;'.format(self.stockID)
        cursor.execute(sql)
        data = cursor.fetchall()
        conn.close()
        return data

    def queryLogInfo(self, selectAll=True):
        """
        查询交易日志信息

        Args:
            selectAll:全选标识（默认全选）
        """
        cursor, conn = self._connection()
        if selectAll:
            sql = 'select * from trade_log order by trade_time;'.format(self.stockID)
        else:
            sql = "select * from trade_log where stockID='{}' order by trade_time;".format(self.stockID)
        cursor.execute(sql)
        data = cursor.fetchall()
        conn.close()
        return data
