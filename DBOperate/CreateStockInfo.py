#  @author Duke Chain
#  @File:CreateStockInfo.py
#  @createTime 2020/11/05 22:54:05

import pymysql


class CreateStockInfo:
    """
    为新收集的数据在stock_info数据库中创建表
    表命名规则：stockID

     Args:
        stockID:传入股票ID
        database:目标位于的数据库（daily,weekly,monthly）
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

    def createTable(self):
        """
        为stockID创建表
        ts_code trade_date open high low close pre_close chg pct_chg vol amount

        Returns:
            建表成功返回Ture，失败返回False
        """
        cursor, conn = self._connection()
        existence = cursor.execute("show tables like '%s';" % self.stockID)
        # 检查该表是否已经存在
        if existence == 1:
            print('该表已存在，不予重复创建')
            return False
        else:
            sql = """CREATE TABLE `{}`(
                    ts_code char(30),
                    trade_date char(30),
                    open_price float,
                    high_price float,
                    low_price float,
                    close_price float,
                    pre_close float,
                    chg float,
                    pct_chg float,
                    vol float,
                    amount float
                    )""".format(self.stockID)
            cursor.execute(sql)
            print(self.stockID + "信息表已创建！")
            conn.commit()
            conn.close()

            return True

# 测试信息
# test = CreateStockInfo('test_table')
# test.createTable()
