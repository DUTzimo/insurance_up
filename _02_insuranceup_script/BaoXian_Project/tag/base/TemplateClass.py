#!/usr/bin/env python
# @desc : 标签的基类：模板类
# 1.  从mysql中读取标签业务规则数据
# 2.  从标签规则数据中，过滤出4级标签数据
# 3.  根据4级标签的rule，获得esMeta元数据对象，查询标签对应源数据
# 4.  从标签规则数据中，过滤出5级标签数据
# 5.  实现性别标签匹配逻辑，把esDF和fiveDF通过gender字段值，匹配上fiveDF的rule，返回fiveDF的id作为tagsId，返回newDF
# 6.  读取es的标签计算结果表数据，作为oldDF
# 7.  合并当前标签计算的结果数据和之前的计算的标签结果数据，newDF和oldDF合并
# 8.  再把合并后结果插入更新结果索引表
__coding__ = "utf-8"
__author__ = "itcast team"

from pyspark.sql import SparkSession, functions as F
from tag.base.Esbase import ESMeta
import logging as logger
import os

import re
from datetime import datetime
import time

SPARK_HOME = '/export/server/spark'
os.environ['SPARK_HOME'] = SPARK_HOME


# @F.udf
def mergeDF(newTagsId, oldTagsId):
    if newTagsId is None:
        return oldTagsId
    if oldTagsId is None:
        return newTagsId
    oldTagsIdList = str(oldTagsId).split(',')
    oldTagsIdList.append(str(newTagsId))
    return ",".join(set(oldTagsIdList))

class AbstractBaseModel(object):

    def __init__(self, taskName, Tagslevel4Id):
        # 0.  创建spark执行环境，获得SparkSession对象
        self.taskName = taskName
        self.Tagslevel4Id = Tagslevel4Id
        self.ss = (
            SparkSession.builder
                .master("local[1]")  # 运行模式
                .appName(taskName)  # 程序名字
                .config("spark.sql.warehouse.dir", 'hdfs://up01:8020/user/hive/warehouse')
                .config('hive.metastore.uris', 'thrift://up01:9083')
                .config("spark.sql.shuffle.partitions", 1)
                .enableHiveSupport()
                .getOrCreate()  # 判断是否创建过ss
        )
        self.ss.sparkContext.setLogLevel("WARN")
        self.sc = self.ss.sparkContext
        self.mergeDF = self.ss.udf.register(name="mergeDF", f=lambda newTagsId, oldTagsId: mergeDF(newTagsId, oldTagsId))


    # 1.  从mysql中读取标签业务规则数据
    def __readMysqlRule(self):
        mysqlDF = (
            self.ss
            .read
            .format('jdbc')
            .option('url',
                        'jdbc:mysql://up01:3306/htv_rule?useUnicode=true&characterEncoding=UTF-8'
                        '&serverTimezone=UTC&useSSL=false&user=root&password=123456')
            .option('query',
                        f'select id, rule, pid from htv_rule.htv_basic_rules where id = '
                        f'{self.Tagslevel4Id} or pid = {self.Tagslevel4Id}')
            .load()
        )
        return mysqlDF

    # 2.  从标签规则数据中，过滤出4级标签数据
    def __readFourDF(self, mysqlDF):
        DFlevel4 = mysqlDF.where(f'id = {self.Tagslevel4Id}').select(mysqlDF.id, mysqlDF.rule)
        return DFlevel4



    # 3.  根据4级标签的rule，获得esMeta元数据对象，查询标签对应源数据
    def __readEsDF(self, DFlevel4):
        RuleStrlevel4 = DFlevel4.rdd.map(lambda row: row.rule).collect()[0]
        esMeta = ESMeta.fromStrToEsMeta(RuleStrlevel4)
        esDF = (
            self.ss
            .read
            .format('es')
            .option('es.nodes', esMeta.esNodes)
            .option('es.resource', esMeta.esIndex)
            .option('es.read.field.include', esMeta.selectFields)
            .load()
        )
        return esDF

    # 4.  从标签规则数据中，过滤出5级标签数据
    def __readFiveDF(self, mysqlDF):
        DFlevel5 = mysqlDF.where(f'pid = {self.Tagslevel4Id}').select(mysqlDF.id, mysqlDF.rule)
        return DFlevel5


    # 5.  实现标签匹配逻辑
    # todo: 需要重写实现
    def compute(self, esDF, Tagslevel5df):
        pass

    # 6.  读取es的标签计算结果表数据，作为oldDF
    def __readOldDF(self):
        oldDF = (
            self.ss
            .read
            .format('es')
            .option('es.nodes', "192.168.88.166:9200")
            .option('es.resource', 'htv_result')
            .option('es.read.field.include', 'user_id,tagsid')
            .load()
        )
        return oldDF

    # 7.  合并当前标签计算的结果数据和之前的计算的标签结果数据，newDF和oldDF合并
    def __mergeNewAndOldDF(self, newDF, oldDF):
        resultDF = (
            newDF
            .join(
                    oldDF,
                    newDF.user_id == oldDF.user_id,
                    'left'
                )
            .select(
                newDF.user_id,
                self.mergeDF(newDF.tagsid, oldDF.tagsid).alias("tagsid")
            )
        )
        return resultDF

    # 8.  再把合并后结果插入更新结果索引表
    def __writeResultToEs(self, resultDF):
        (
            resultDF
            .write
            .format('es')
            .option('es.nodes', "192.168.88.166:9200")
            .option('es.resource', 'htv_result')
            .option('es.mapping.id', 'user_id')
            .option('es.write.operation', 'upsert')
            .mode('append')
            .save()
        )
        logger.warning("%s 任务执行成功！！！", self.taskName)


    def __closecon(self):
        self.sc.stop()
        self.ss.stop()

    # 组合方法，组织标签计算整体逻辑
    def execute(self,first:bool=False):
        # step1 : 获取标签df
        mysqlDF = self.__readMysqlRule()
        # step2 :  获取四级标签df
        DFlevel4 = self.__readFourDF(mysqlDF)
        # step3 : 获取标签对应源数据
        esDF = self.__readEsDF(DFlevel4)
        # step4 :  获取五级标签df
        DFlevel5 = self.__readFiveDF(mysqlDF)
        # step5 ：实现标签匹配
        newDF = self.compute(esDF, DFlevel5)
        if first==False:
            # step6 :获取结果表数据
            oldDF = self.__readOldDF()
            # step7 :合并标签
            resultDF = self.__mergeNewAndOldDF(newDF, oldDF)
            # step8 :插入更新结果es
            self.__writeResultToEs(resultDF)
            self.__closecon()
        else:
            self.__writeResultToEs(newDF)
            self.__closecon()

    def test(self):
        mysqlDF = self.__readMysqlRule()
        # step2 :  获取四级标签df
        DFlevel4 = self.__readFourDF(mysqlDF)
        DFlevel4.show()
        # step3 : 获取标签对应源数据
        esDF = self.__readEsDF(DFlevel4)
        esDF.show()
        # step4 :  获取五级标签df
        DFlevel5 = self.__readFiveDF(mysqlDF)
        DFlevel5.show()
        newDF=self.compute(esDF, DFlevel5)
        self.__closecon()



    def executeWithoutInsert(self,first:bool=False):
        # step1 : 获取标签df
        mysqlDF = self.__readMysqlRule()
        # step2 :  获取四级标签df
        DFlevel4 = self.__readFourDF(mysqlDF)
        # step3 : 获取标签对应源数据
        esDF = self.__readEsDF(DFlevel4)
        # step4 :  获取五级标签df
        DFlevel5 = self.__readFiveDF(mysqlDF)
        # step5 ：实现标签匹配
        newDF = self.compute(esDF, DFlevel5)
        newDF.show()
        if first==False:
            # step6 :获取结果表数据
            oldDF = self.__readOldDF()
            # step7 :合并标签
            resultDF = self.__mergeNewAndOldDF(newDF, oldDF)
            resultDF.show()
            self.__closecon()
        else:
            newDF.show()
            self.__closecon()
