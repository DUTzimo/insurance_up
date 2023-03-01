# 匹配类标签 ：教育
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import dataFrameToDict, fieldToTagsId


class Educhild(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        fiveDict = dataFrameToDict(fiveDF)
        newDF = esDF.select(esDF.user_id.alias('user_id'), fieldToTagsId(esDF.edu, F.lit(str(fiveDict))).alias('tagsid'))
        return newDF


# Edu= Educhild('eduTask', 549)
# Edu.execute()
