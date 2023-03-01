# 理赔周期表
from pyspark.sql import functions as F
from tag.base.TemplateClass import AbstractBaseModel
from tag.utils.TagLevel5Common import updetetime,avg,datetimeTodatestamp2


class claim_date(AbstractBaseModel):
    def compute(self, esDF, Tagslevel5df):
        timechange = self.ss.udf.register(name="timechange", f=lambda x,y: datetimeTodatestamp2(x,y))
        timetoday = self.ss.udf.register(name="timetoday", f=lambda x: updetetime(x))
        tempdf=(
            esDF.select(esDF.user_id,timechange(esDF.buy_datetime, esDF.claim_date).alias("tagsid"))
            .rdd
            .groupByKey()
            .map(lambda item:(item[0],avg(item[1])))
            .toDF(["user_id","avg"])
        )
        tagdf=Tagslevel5df.select(Tagslevel5df.id,
                                        F.split(Tagslevel5df.rule,'-')[0].alias('start'),
                                        F.split(Tagslevel5df.rule, '-')[1].alias('end')
                                      )
        temptagdf=tagdf.select( tagdf.id,
                                timetoday(tagdf.start).alias('start'),
                                timetoday(tagdf.end).alias('end')
                                )
        newDF = (tempdf
                .join(temptagdf)
                .where(tempdf.avg.between(temptagdf.start, temptagdf.end))
                .select(
                    tempdf.user_id,
                    temptagdf.id.alias('tagsid')
                    )
                )
        return newDF


# cliam_date = claim_date('ClaimdateTask', 614)
# cliam_date.execute()