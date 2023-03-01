
#!/usr/bin/env python
# @desc : 年龄标签：规则匹配类
__coding__ = "utf-8"
__author__ = "itcast team"

import logging as logger
import os

from pyspark.sql import SparkSession, DataFrame, functions as F

from tag.entity.ESMeta import ESMeta

SPARK_HOME = '/export/server/spark'
os.environ['SPARK_HOME'] = SPARK_HOME

if __name__ == '__main__':
    """
        # 0.  创建spark执行环境，获得SparkSession对象
        # 1.  读取标签的规则数据：mysql中4级和5级标签数据
        # 2.  过滤标签规则数据，得到4级标签的数据
        # 3.  解析4级标签规则为esmeta对象，查询es的年龄标签原始数据
        # 4.  从标签规则数据中，过滤得到5级标签的数据
        # 5.  根据输入的es原始数据和5级标签规则数据，进行标签的计算逻辑
        # 6.  把计算后的结果数据写入到es结果表中
    """
    taskName = 'AgeModel'
    fourTagsId = 14
    # 0.  创建spark执行环境，获得SparkSession对象
    spark = SparkSession \
        .builder \
        .appName(taskName) \
        .master('local[1]') \
        .config('spark.shuffle.partition.nums', 1) \
        .getOrCreate()

    # 1.  读取标签的规则数据：mysql中4级和5级标签数据
    #    spark代码：在加载数据，尽可能只加载需要的数据：待优化
    mysqlDF = spark \
        .read \
        .format('jdbc') \
        .option('url',
                'jdbc:mysql://up01:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false&user=root&password=123456') \
        .option('query', 'select * from tags_new.tbl_basic_tag') \
        .load()
    mysqlDF.show(truncate=False)

    logger.warning("=================1.  读取标签的规则数据：mysql中4级和5级标签数据=======================>")

    # 2.  过滤标签规则数据，得到4级标签的数据
    fourDF: DataFrame = mysqlDF.where(f'id = {fourTagsId}').select(mysqlDF.id, mysqlDF.rule)
    fourDF.show(truncate=False)
    # mysqlDF.where(mysqlDF.id == 14)
    # mysqlDF.where(mysqlDF['id'] == 14)
    # mysqlDF.where('id' == 14)
    # mysqlDF.where(F.col('id') == 14)
    logger.warning("=================2.  过滤标签规则数据，得到4级标签的数据=======================>")

    # 3.  解析4级标签规则为esmeta对象，查询es的年龄标签原始数据
    #    如何从df中得到rule的值，查询df中的row对应rule属性的值?
    #       先把df转成rdd，调用map算子，对数据一对一处理，RDD(row.rule)
    fourRuleStr = fourDF.rdd.map(lambda row: row.rule).collect()[0]
    esMeta = ESMeta.fromStrToEsMeta(fourRuleStr)
    print(esMeta)
    esDF = spark \
        .read \
        .format('es') \
        .option('es.nodes', esMeta.esNodes) \
        .option('es.resource', esMeta.esIndex) \
        .option('es.read.field.include', esMeta.selectFields) \
        .load()
    esDF.show(truncate=False)

    logger.warning("=================3.  解析4级标签规则为esmeta对象，查询es的年龄标签原始数据=======================>")
    # 4.  从标签规则数据中，过滤得到5级标签的数据
    fiveDF = mysqlDF.where(f'pid = {fourTagsId}').select(mysqlDF.id, mysqlDF.rule)
    fiveDF.show(truncate=False)

    logger.warning("=================4.  从标签规则数据中，过滤得到5级标签的数据=======================>")
    # 5.  根据输入的es原始数据和5级标签规则数据，进行标签的计算逻辑
    #    年龄标签匹配规则
    # 5.1 对esDF的birthday进行处理，截取字符串，得到年月日，替换掉中间的"-"
    esDF2 = esDF.select(
        esDF.id,
        F.regexp_replace(F.substring(esDF.birthday, 0, 10), '-', '').alias('birth')
    )
    esDF2.printSchema()
    esDF2.show(truncate=False)
    # 5.2 对fiveDF的rule进行处理，使用中间的“-”进行切分为两个字段，start和end
    fiveDF2 = fiveDF.select(fiveDF.id,
                            F.split(fiveDF.rule, '-')[0].alias('start'),
                            F.split(fiveDF.rule, '-')[1].alias('end')
                            )
    fiveDF2.printSchema()
    fiveDF2.show(truncate=False)

    # 5.3 对处理后esDF和fiveDF进行join，条件是birthday在start和end，返回userId 和 tagsId
    resultDF = esDF2 \
        .join(fiveDF2) \
        .where(esDF2.birth.between(fiveDF2.start, fiveDF2.end)) \
        .select(
            esDF2.id.alias('userId'),
            fiveDF2.id.alias('tagsId')
        )
    resultDF.printSchema()
    resultDF.show(truncate=False)

    logger.warning("=================5.  根据输入的es原始数据和5级标签规则数据，进行标签的计算逻辑=======================>")
    # # 6.  把计算后的结果数据写入到es结果表中
    # resultDF\
    #     .write\
    #     .format('es') \
    #     .option('es.nodes', esMeta.esNodes) \
    #     .option('es.resource', 'tfec_up_result') \
    #     .option('es.mapping.id', 'userId') \
    #     .option('es.write.operation', 'upsert') \
    #     .mode('append') \
    #     .save()

    logger.warning("=================6.  把计算后的结果数据写入到es结果表中=======================>")
    logger.warning("%s 任务执行成功！！！", taskName)
