from datetime import datetime
import time
import re

lista=["zimo","star,sb,wzc"]
def explore_to_dict(list):
    user=list[0]
    # tagid_iter=iter(re.split(',',list[1]))
    tagid_iter=iter(list[1].split(','))
    user_tagid=[]
    for i in tagid_iter:
        user_tagid.append((user,i))
    return iter(user_tagid)


for i in explore_to_dict(lista):
    print(i)
# 将str转换为datetime类型
# str_time1="2022-12-06 16:13:29"
# datetime1=datetime.strptime(str_time1,"%Y-%m-%d %H:%M:%S")
# timestamp1=datetime1.timestamp()

# str_time2="2021-11-07 14:44:26"
# datetime2=datetime.strptime(str_time2,"%Y-%m-%d %H:%M:%S")
# timestamp2=datetime2.timestamp()
#
# print(timestamp1,timestamp2,int(timestamp1-timestamp2),sep='\t')

# str_time1="1989-04-16 16:13:29"
# datetime1=datetime.strptime(str_time1,"%Y-%m-%d %H:%M:%S")
# datestr=datetime.strftime(datetime1,"%Y-%m-%d %H:%M:%S")
# print(datestr[0:4])
# 都是包左不包右
# 正索引演示.
# if 1940 < int() < 1949:
#
#
# # 时间戳
# timestamp=round(time.time())
# print(timestamp)
# # 1662982140
#
# # 时间戳转换为datetime类型
# ts_datetime=datetime.fromtimestamp(timestamp)
# print(ts_datetime)
# # 2022-09-12 19:29:00
#
# # 将datetime转换为时间戳
# ts=ts_datetime.timestamp()
# print(round(ts))