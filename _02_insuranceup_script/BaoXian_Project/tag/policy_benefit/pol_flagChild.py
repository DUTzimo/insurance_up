# 匹配类标签 ：保险是否有效
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import dataFrameToDict, fieldToTagsId


class Pol_flagModel(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        fiveDict = dataFrameToDict(fiveDF)
        newDF = esDF.select(esDF.user_id.alias('user_id'), fieldToTagsId(esDF.pol_flag, F.lit(str(fiveDict))).alias('tagsid'))

        return newDF

#
# Pol_flag = Pol_flagModel('Pol_flagTask', 601)
# Pol_flag.execute()
