#  @author Duke Chain
#  @File:AddTradeInfo.py
#  @createTime 2020/11/06 23:44:06

import pymysql
from DBOperate.QueryTradeInfo import QueryTradeInfo


class AddTradeInfo:
    """
    在trade_info数据库中插入交易信息

     Args:
        time:交易时间（必须以2020-11-06 23:49:45的形式）
        stockID:交易的股票代码
        pricePerAmount:每股价格
        stockAmount:交易的股票数量
        stockMoney:交易的股票金额
        money:账户中剩余金额
    """

    def __init__(self, time, stockID, pricePerAmount, stockAmount, stockMoney, money):
        self.time = time
        self.stockID = stockID
        self.pricePerAmount = pricePerAmount
        self.stockAmount = stockAmount
        self.stockMoney = stockMoney
        self.money = money

    def _connection(self):
        """
        建立和数据库的连接
        """
        conn = pymysql.connect("localhost", "root", "qian258046", "trade_info", charset='utf8')
        cursor = conn.cursor()
        return cursor, conn

    def addLogInfo(self):
        """
        在trade_log数据库中插入信息
        """
        cursor, conn = self._connection()
        sql = 'INSERT INTO trade_info.trade_log(time, stockID, pricePerAmount, stockAmount, stockMoney, money) VALUES ' \
              '(%s,%s,%s,%s,%s,%s) '
        value = (self.time, self.stockID, self.pricePerAmount, self.stockAmount, self.stockMoney, self.money)
        cursor.execute(sql, value)
        conn.commit()
        print(self.stockID + ' 交易信息已更新')
        conn.close()

    def addRepoInfo(self):
        """
        添加个体交易信息
        """
        cursor, conn = self._connection()
        existence = cursor.execute("show tables like '%s';" % self.stockID)

        if existence == 1:
            preTradeInfo = QueryTradeInfo(self.stockID)
            preTradeData = preTradeInfo.queryTradeInfo()
            repo_amount = preTradeData[0][3] + self.stockAmount
            sql = 'INSERT INTO `{}`(trade_time,trade_price,trade_amount,repo_amount)' \
                  'VALUES ("{}",{},{},{}) '.format(self.stockID, self.time, self.pricePerAmount, self.stockAmount,
                                                   repo_amount)
            cursor.execute(sql)
            conn.commit()
            print(self.stockID + ' 交易信息已更新')
            conn.close()
        else:
            sql = """CREATE TABLE `{}`(
                                    trade_time char(30),
                                    trade_price float,
                                    trade_amount float,
                                    repo_amount float
                                    )""".format(self.stockID)
            cursor.execute(sql)
            print(self.stockID + "信息表已创建！")
            conn.commit()
            conn.close()

# 测试信息
# test = AddTradeInfo('2020-11-06 23:49:45', '123', 34, 100, 3400, 10000)
# test.addInfo()
