# 匹配类标签：投保年龄
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel

class Age_Buy(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        esDF2 = esDF.select(esDF.user_id.alias('user_id'), esDF.age_buy.alias('tagsid'))
        esDF2.show()
        fiveDF2 = fiveDF.select(
            fiveDF.id,
            F.split(fiveDF.rule, '-')[0].alias('start'),
            F.split(fiveDF.rule, '-')[1].alias('end')
        )
        fiveDF2.show()
        newDF = (
            esDF2.join(fiveDF2)
            .where(esDF2.tagsid.between(fiveDF2.start, fiveDF2.end))
            .select(esDF2.user_id.alias('user_id'), fiveDF2.id.alias('tagsid'))
        )
        return newDF


# Age_buyModel = Age_Buy('Age_buyTask', 575)
# Age_buyModel.executeWithoutInsert()



