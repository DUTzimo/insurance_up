# 匹配类标签 ：缴费期
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import dataFrameToDict, fieldToTagsId


class Pppchild(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        fiveDict = dataFrameToDict(fiveDF)
        newDF = esDF.select(
            esDF.user_id.alias('user_id'),
            fieldToTagsId(esDF.ppp, F.lit(str(fiveDict))).alias('tagsid')
        )
        return newDF


# PPP = Pppchild('PPPModel', 570)
# PPP.executeWithoutInsert()
