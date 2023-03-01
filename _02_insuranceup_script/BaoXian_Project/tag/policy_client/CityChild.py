# 匹配类标签 ：城市
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import dataFrameToDict,fieldToTagsId


class Citychild(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        fiveDict = dataFrameToDict(fiveDF)
        newDF = esDF.select(esDF.user_id.alias('user_id'), fieldToTagsId(esDF.city, F.lit(str(fiveDict))).alias('tagsid'))
        return newDF


# City = Citychild('cityTask', 62)
# City.execute()
