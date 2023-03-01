from pyspark.sql import SparkSession, functions as F

def explore_to_dict(list):
    user=list[0]
    tagid_iter=iter(list[1].split(','))
    user_tagid=[]
    for i in tagid_iter:
        user_tagid.append((user,i))
    return iter(user_tagid)

def Save_to_Mysql(mysqlname,user,password):
    ss = (
        SparkSession.builder
            .master("local[1]")  # 运行模式
            .appName("save_to_mysql")  # 程序名字
            .config("spark.sql.warehouse.dir", 'hdfs://up01:8020/user/hive/warehouse')
            .config('hive.metastore.uris', 'thrift://up01:9083')
            .config("spark.sql.shuffle.partitions", 1)
            .enableHiveSupport()
            .getOrCreate()  # 判断是否创建过ss
    )
    ss.sparkContext.setLogLevel("WARN")
    ESDF = (
            ss
            .read
            .format('es')
            .option('es.nodes', "192.168.88.166:9200")
            .option('es.resource', 'htv_result')
            .option('es.read.field.include', 'user_id,tagsid')
            .load()
    )
    ESDF1=(
        ESDF
        .select(ESDF.user_id, ESDF.tagsid)
        .rdd
        .flatMap(lambda x: explore_to_dict(x))
        .toDF(['user_id','tagsid'])
    )
    (
        ESDF1.select("user_id","tagsid")
        .write.mode("append").format("jdbc")
        .option("url", "jdbc:mysql://up01:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")
        .option("dbtable", mysqlname)
        .option("user", user)
        .option("password", password)
        .save()
    )
    ss.stop()
