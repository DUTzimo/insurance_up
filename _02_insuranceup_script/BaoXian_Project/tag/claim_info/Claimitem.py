# 匹配类标签 ：理赔责任类型
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import dataFrameToDict, fieldToTagsId


class Claim_itemModel(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        fiveDict = dataFrameToDict(fiveDF)
        newDF = esDF.select(esDF.user_id.alias('user_id'), fieldToTagsId(esDF.claim_item, F.lit(str(fiveDict))).alias('tagsid'))

        return newDF


# Claim_item = Claim_itemModel('ClaimitemTask', 624)
# Claim_item.execute()
