from pyspark.sql import functions as F
import tag.base.TemplateClass as T

def MapSex(sexstr):
    if sexstr.strip() == "M":
        return 9
    else:
        return 10


class sex(T.AbstractBaseModel):
    def compute(self, esDF, Tagslevel5df):
        change = self.ss.udf.register(name="change", f=lambda sexstr: MapSex(sexstr))
        newDF=(
            esDF.select(
                F.col("user_id"),
                change(esDF.sex).alias("tagsid")
            )
        )
        return newDF

# 实现Sex标签
# sex1=sex("sexTask",8)
# sex1.execute()