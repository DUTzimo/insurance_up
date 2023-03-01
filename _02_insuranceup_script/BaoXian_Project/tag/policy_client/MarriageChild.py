# 匹配类标签： 婚姻状况
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import dataFrameToDict


@F.udf
def marriageToTagsId(marriage, fiveDictStr):
    fiveDict = eval(fiveDictStr)
    if marriage == '未婚':
        marriage = 1
    elif marriage == '已婚':
        marriage = 2
    elif marriage == '离异':
        marriage = 3

    return fiveDict[str(marriage)]


class Marriage(AbstractBaseModel):

    def compute(self,esDF, fiveDF):

        fiveDict = dataFrameToDict(fiveDF)
        newDF = esDF.select(esDF.user_id.alias('user_id'), marriageToTagsId(esDF.marriage_state, F.lit(str(fiveDict))).alias('tagsid'))

        return newDF


# Marriage1 = Marriage('MarriageTask', 545)
# Marriage1.execute()