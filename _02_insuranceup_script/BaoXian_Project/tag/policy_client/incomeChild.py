# 匹配类标签：收入
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel

class IncomeModel(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        esDF2 = esDF.select(esDF.user_id.alias('user_id'), esDF.income.alias('tagsid'))

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

# Income = IncomeModel('IncomeTask', 530)
# Income.execute()
