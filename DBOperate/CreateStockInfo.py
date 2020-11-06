#  @author Duke Chain
#  @File:CreateStockInfo.py
#  @createTime 2020/11/05 22:54:05

import pymysql


class CreateStockInfo:
    """
    为新收集的数据在stock_info数据库中创建表
    表命名规则：stockID
    """

    def __init__(self, stockID):
        self.stockID = stockID

    def connection(self):
        """
        建立和数据库的连接
        """
        conn = pymysql.connect("localhost", "root", "qian258046", "stock_info", charset='utf8')
        cursor = conn.cursor()
        return cursor, conn

    def createTable(self):
        """
        为stockID创建表
        ts_code trade_date open high close pre_close chg pct_chg vol amount

        Returns:
            建表成功返回Ture，失败返回False
        """
        cursor, conn = self.connection()
        existence = cursor.execute("show tables like '%s';" % self.stockID)
        # 检查该表是否已经存在
        if existence == 1:
            print('该表已存在，不予重复创建')
            return False
        else:
            sql = """CREATE TABLE %s(
                    ts_code char(30),
                    trade_date char(30),
                    open float,
                    high float,
                    close float,
                    pre_close float,
                    chg float,
                    pct_chg float,
                    vol float,
                    amount float
                    )""" % self.stockID
            cursor.execute(sql)
            print(self.stockID + "信息表已创建！")
            conn.commit()
            conn.close()

            return True

# 测试信息
# test = CreateStockInfo('test_table')
# test.createTable()
