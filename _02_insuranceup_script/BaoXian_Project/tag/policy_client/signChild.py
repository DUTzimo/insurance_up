# 匹配类标签 ：星座
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import dataFrameToDict, fieldToTagsId


class Signchild(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        fiveDict = dataFrameToDict(fiveDF)
        newDF = esDF.select(esDF.user_id.alias('user_id'), fieldToTagsId(esDF.sign, F.lit(str(fiveDict))).alias('tagsid'))

        return newDF


# Sign= Signchild('raceModel', 557)
# Sign.execute()
