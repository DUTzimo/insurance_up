from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel

@F.udf
def datatimeToStr(birthday):
     bir = birthday.strftime("%Y-%m-%d %I:%M:%S")
     return bir


class BirthChild(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        esDF2 = esDF.select(
            esDF.user_id,
            F.regexp_replace(F.substring(datatimeToStr(esDF.birthday), 0, 10), '-', '').alias('birth')
        )
        # 5.2 对fiveDF的rule进行处理，使用中间的“-”进行切分为两个字段，start和end
        fiveDF2 = fiveDF.select(fiveDF.id,
                                F.split(fiveDF.rule, '-')[0].alias('start'),
                                F.split(fiveDF.rule, '-')[1].alias('end'))

        # 5.3 对处理后esDF和fiveDF进行join，条件是birthday在start和end，返回userId 和 tagsId
        newDF = (
            esDF2
            .join(fiveDF2)
            .where(esDF2.birth.between(fiveDF2.start, fiveDF2.end))
            .select(
            esDF2.user_id.alias('user_id'),
            fiveDF2.id.alias('tagsid'))
        )


        return newDF


# birth = BirthChild('birthTask', 19)
# birth.execute()
