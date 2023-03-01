#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "mjh"

from pyspark.sql import functions as F
import tag.base.TemplateClass as T

def MapHeight(heightstr):
    if 150 < heightstr < 159:
        return 12
    elif 160 < heightstr < 169:
        return 13
    elif 170 < heightstr < 179:
        return 14
    elif 180 < heightstr < 189:
        return 15
    elif 190 < heightstr < 199:
        return 16
    elif 200 < heightstr < 209:
        return 17
    else:
        return 18



class height(T.AbstractBaseModel):
    def compute(self, esDF, Tagslevel5df):
        change = self.ss.udf.register(name="change", f=lambda heightstr: MapHeight(heightstr))
        newDF = (
            esDF.select(
                F.col("user_id"),
                change(esDF.height).alias("tagsid")
            )
        )
        return newDF

# 实现Sex标签
height1=height("heightTask",11)
height1.execute()