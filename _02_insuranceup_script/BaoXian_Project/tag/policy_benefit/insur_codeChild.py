# 匹配类标签 ：保险类型
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import dataFrameToDict, fieldToTagsId


class Insur_codeModel(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        fiveDict = dataFrameToDict(fiveDF)
        newDF = esDF.select(esDF.user_id.alias('user_id'), fieldToTagsId(esDF.insur_code, F.lit(str(fiveDict))).alias('tagsid'))

        return newDF


# Insur_code = Insur_codeModel('InsurcodeTask', 595)
# Insur_code.execute()
