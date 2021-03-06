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
        content:判断数据库类型（d(日)，t(秒),m(分钟)）
    """

    def __init__(self, stockID, database, content='m'):
        self.stockID = stockID
        self.database = database
        self.content = content

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
            # 日级数据表
            if self.content == 'd':
                sql = """CREATE TABLE `{}`(
                                    trade_date char(30),
                                    close_price float,
                                    high_price float,
                                    low_price float,
                                    open_price float,
                                    pre_close float,
                                    volume float,
                                    outstanding_share float,
                                    turnover float
                                    )""".format(self.stockID)
            # 秒级数据表
            elif self.content == 't':
                sql = """CREATE TABLE `{}`(
                                    trade_date char(30),
                                    stock_price float,
                                    chg float,
                                    volume float
                                    )""".format(self.stockID)
            # 分钟级数据表
            else:
                sql = """CREATE TABLE `{}`(
                                    trade_date char(30),
                                    open_price float,
                                    high_price float,
                                    low_price float,
                                    close_price float,
                                    volume float
                                    )""".format(self.stockID)
            cursor.execute(sql)
            print(self.stockID + "信息表已创建！")
            conn.commit()
            conn.close()

            return True

# 测试信息
# test = CreateStockInfo('test_table')
# test.createTable()
