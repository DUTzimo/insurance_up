#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "itcast team"

from pyspark.sql import functions as F


# 把五级标签的df转换为dict
def dataFrameToDict(fiveDF):
    return fiveDF.rdd.map(lambda row: (row.rule, row.id)).collectAsMap()

@F.udf
def fieldToTagsId(fieldName, fiveDictStr):
    fiveDict = eval(fiveDictStr)
    return fiveDict[str(fieldName)]


def datetimeTodatestamp(ts_datetime):
    dateint = int(ts_datetime.timestamp())
    return dateint

def datetimeTodatestamp2(buy_datetime, cliam_date):
    dateint = int(cliam_date.timestamp()) - int(buy_datetime.timestamp())
    return dateint

def updetetime(timea):
    return int(timea)*60*60*24

def subtimestrap(timestrap1):
    return (1670480639-int(timestrap1))

# rdd自定义函数
def avg(listx):
    lista=list(listx)
    lista.append(1670480639)
    sum=0
    for i in range(len(lista)-1):
        for j in range(len(lista)-i-1):
            if int(lista[j]) > int(lista[j+1]):
                lista[j], lista[j+1] = lista[j+1], lista[j]
    for i in range(len(lista)-1):
        sum+=int(lista[i+1])-int(lista[i])
    return int(sum/(len(lista)-1))

