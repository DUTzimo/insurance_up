# 匹配类标签 ：区域
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import dataFrameToDict, fieldToTagsId


class Direction(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        fiveDict = dataFrameToDict(fiveDF)
        newDF = esDF.select(esDF.user_id.alias('user_id'), fieldToTagsId(esDF.direction, F.lit(str(fiveDict))).alias('tagsid'))
        return newDF


# Direction = Direction('cityTask', 522)
# Direction.execute()
