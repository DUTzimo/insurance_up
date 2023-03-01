#!/usr/bin/env python
# @desc :
__coding__ = "utf-8"
__author__ = "mjh"

from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel


class PolDates(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        esDF2 = esDF.select(
            esDF.user_id,
            esDF.keep_days
        )

        fiveDF2 = fiveDF.select(fiveDF.id,
                                F.split(fiveDF.rule, '-')[0].alias('start'),
                                F.split(fiveDF.rule, '-')[1].alias('end'),
        )

        newDF =(
            esDF2
            .join(fiveDF2)
            .where(esDF2.keep_days.between(fiveDF2.start, fiveDF2.end))
            .select(
                esDF2.user_id.alias('user_id'),
                fiveDF2.id.cast("string").alias('tagsid')
            )
        )

        return newDF


# pol_dates = PolDates('pol_datesModel', 634)
# pol_dates.executeWithoutInsert()