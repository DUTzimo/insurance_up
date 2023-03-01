#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "mjh"

from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import datetimeTodatestamp,updetetime,subtimestrap

class BuyCycle(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        timechange = self.ss.udf.register(name="timechange", f=lambda x: datetimeTodatestamp(x))
        timetoday = self.ss.udf.register(name="timetoday", f=lambda x: updetetime(x))
        substrap = self.ss.udf.register(name="substrap", f=lambda x: subtimestrap(x))

        esDF2 = esDF.select(esDF.user_id, timechange(esDF.buy_datetime).alias("tagsid"))
        esDF3 = esDF2.groupBy(esDF2.user_id).agg(F.max(esDF2.tagsid).alias('tagsid'))
        esDF4 = esDF3.select(esDF3.user_id,substrap(esDF3.tagsid).alias('tagsid'))

        fiveDF2 = fiveDF.select(fiveDF.id,
                                F.split(fiveDF.rule, '-')[0].alias('start'),
                                F.split(fiveDF.rule, '-')[1].alias('end'),
                                )
        fiveDF3=fiveDF2.select(
            fiveDF2.id,
            timetoday(fiveDF2.start).alias('start'),
            timetoday(fiveDF2.end).alias('end')
        )

        newDF = (
            esDF4
            .join(fiveDF3)
            .where(esDF4.tagsid.between(fiveDF3.start, fiveDF3.end))
            .select(
                esDF4.user_id.alias('user_id'),
                fiveDF3.id.alias('tagsid')
            )
        )
        return newDF


# BuyCycles = BuyCycle('BuyCycleModel', 604)
# BuyCycles.execute()
