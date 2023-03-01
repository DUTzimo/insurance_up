# 匹配类标签 ：省份
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import dataFrameToDict, fieldToTagsId


class Provincechild(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        fiveDict = dataFrameToDict(fiveDF)
        newDF = esDF.select(esDF.user_id.alias('user_id'), fieldToTagsId(esDF.province, F.lit(str(fiveDict))).alias('tagsid'))

        return newDF

#
# province = Provincechild('proviceTask', 29)
# province.execute()
