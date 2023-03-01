#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "mjh"

from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_replace
from datetime import datetime
import tag.base.TemplateClass as T
def MapAge(birthday):

    dateint = int(datetime.strftime(birthday, "%Y-%m-%d %H:%M:%S")[0:4])
    if 1940 < dateint < 1949:
        return 20
    elif 1950 < dateint < 1959:
        return 21
    elif 1960 < dateint < 1969:
        return 22
    elif 1970 < dateint < 1979:
        return 23
    elif 1980 < dateint < 1989:
        return 24
    elif 1990 < dateint < 1999:
        return 25
    elif 2000 < dateint < 2009:
        return 26
    elif 2010 < dateint < 2019:
        return 27
    else:
        return 27


class birthday(T.AbstractBaseModel):
    def compute(self, esDF, Tagslevel5df):
        change = self.ss.udf.register(name="change", f=lambda birthday: MapAge(birthday))
        newDF = (
            esDF.select(
                F.col("user_id"),
                change(esDF.birthday).alias("tagsid")
            )
        )
        return newDF

# 实现Sex标签
birthday1=birthday("birthdayTask",19)
# birthday1.test()
# birthday1.executeWithoutInsert()
birthday1.execute()