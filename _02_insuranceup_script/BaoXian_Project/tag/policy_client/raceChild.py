# 匹配类标签 ：民族
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import dataFrameToDict, fieldToTagsId


class Racechild(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        fiveDict = dataFrameToDict(fiveDF)
        newDF = esDF.select(esDF.user_id.alias('user_id'), fieldToTagsId(esDF.race, F.lit(str(fiveDict))).alias('tagsid'))

        return newDF


# Race= Racechild('raceTask', 537)
# Race.execute()
