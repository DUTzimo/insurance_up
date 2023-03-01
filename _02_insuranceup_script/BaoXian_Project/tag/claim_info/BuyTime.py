from pyspark.sql import functions as F
import tag.base.TemplateClass as T
from tag.utils.TagLevel5Common import datetimeTodatestamp,updetetime,avg


class buytime(T.AbstractBaseModel):
    def compute(self, esDF, Tagslevel5df):
        timechange = self.ss.udf.register(name="timechange", f=lambda x: datetimeTodatestamp(x))
        timetoday = self.ss.udf.register(name="timetoday", f=lambda x: updetetime(x))
        tempdf=(
            esDF.select(esDF.user_id,timechange(esDF.buy_datetime).alias("tagsid"))
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

# # 实现buy_datetime标签
# buytime1 = buytime("buytimeTask", 604)
# buytime1.execute()





