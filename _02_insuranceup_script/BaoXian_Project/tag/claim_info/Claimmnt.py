# 匹配类标签：理赔金额
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel

@F.udf
def strToInt(claim_mnt):
    claim_mnt = int(claim_mnt)
    return claim_mnt

class Claim_mntModel(AbstractBaseModel):

    def compute(self, esDF, fiveDF):
        esDF2 = esDF.select(esDF.user_id.alias('user_id'), strToInt(esDF.claim_mnt).alias('tagsid'))

        esDF3 = esDF2.groupBy(esDF2.user_id).agg(F.max(esDF2.tagsid).alias('tagsid'))
        esDF3.show(truncate=False)

        fiveDF2 = fiveDF.select(fiveDF.id, F.split(fiveDF.rule, '-')[0].alias('start'),
                                F.split(fiveDF.rule, '-')[1].alias('end'))
        fiveDF2.show(truncate=False)
        newDF = (
            esDF3.join(fiveDF2)
                .where(esDF3.tagsid.between(fiveDF2.start, fiveDF2.end))
                .select(
                    esDF3.user_id.alias('user_id'),
                    fiveDF2.id.alias('tagsid')
                    )
        )
        newDF.show(truncate=False)

        return newDF

# Claim_mnt = Claim_mntModel('ClaimmntTask', 630)
# Claim_mnt.execute()



