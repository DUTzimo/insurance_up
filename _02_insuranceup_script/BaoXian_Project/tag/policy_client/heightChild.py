# 匹配类标签：身高
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel

class height(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        esDF2 = esDF.select(esDF.user_id.alias('user_id'), esDF.height.alias('tagsid'))

        fiveDF2 = fiveDF.select(
            fiveDF.id,
            F.split(fiveDF.rule, '-')[0].alias('start'),
            F.split(fiveDF.rule, '-')[1].alias('end')
        )
        newDF =(
            esDF2.join(fiveDF2)
            .where(esDF2.tagsid.between(fiveDF2.start, fiveDF2.end))
            .select(esDF2.user_id.alias('user_id'), fiveDF2.id.alias('tagsid'))
        )
        return newDF

# sexModel = height('heightTask', 11)
# sexModel.execute()
