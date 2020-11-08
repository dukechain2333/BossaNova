#  @author Duke Chain
#  @File:DataTest.py
#  @createTime 2020/11/05 22:25:05

import tushare as ts

ts.set_token('6e32db130ef1ea433f2bf65245ae9026e355cb91d31e47ee50b76598')
pro = ts.pro_api()
data = pro.weekly(ts_code='688598.SH', start_date='20201101', end_date='20201102')
print(data)
print(data.columns)