--insurance_es

-- 创建数据库存放外部表
-- drop database insurance_es cascade ;
create database insurance_es;
-- 设置本地模式
set hive.exec.mode.local.auto=true;

-- 创建第一张外部表:理赔信息表
drop table if exists insurance_es.claim_info_es ;
CREATE EXTERNAL TABLE insurance_es.claim_info_es (
    pol_no       string comment '保单号',
    user_id      string comment '用户号',
    buy_datetime string comment '购买日期',
    insur_code   string comment '保险代码',
    claim_date   string comment '理赔日期',
    claim_item   string comment '理赔责任',
    claim_mnt    decimal(35, 6) comment '理赔金额',
    dt           string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
    'es.hosts' = 'up01:9200',
    'es.resource' = 'htv_claim_info/_doc',
    'es.mapping.id' = 'pol_no'
    );
-- 数据导入外部表hive-es
insert overwrite table insurance_es.claim_info_es
select pol_no,user_id,buy_datetime,insur_code,claim_date,claim_item,claim_mnt,dt from insurance_ods.claim_info ;

-- 创建第二张外部表:投保记录表
drop table if exists insurance_es.policy_benefit_es ;
CREATE EXTERNAL TABLE insurance_es.policy_benefit_es (
    pol_no       string comment '保单号',
    user_id      string comment '用户号',
    ppp          string comment '缴费期',
    age_buy      smallint comment '投保年龄',
    buy_datetime string comment '投保日期',
    insur_name   string comment '保险名称',
    insur_code   string comment '保险代码',
    pol_flag     smallint comment '保单状态，1有效，0失效',
    elapse_date  string comment '保单失效时间',
    dt           string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
    'es.hosts' = 'up01:9200',
    'es.resource' = 'htv_policy_benefit/_doc',
    'es.mapping.id' = 'pol_no'
    );
-- 数据导入外部表hive-es
insert overwrite table insurance_es.policy_benefit_es
select * from insurance_ods.policy_benefit ;

-- 创建第三张外部表:客户信息表
drop table if exists insurance_es.policy_client_es ;
CREATE EXTERNAL TABLE insurance_es.policy_client_es (
    user_id        string comment '用户号',
    name           string comment '姓名',
    id_card        string comment '身份证号',
    phone          string comment '手机号',
    sex            string comment '性别',
    height         int comment '升高',
    birthday       string comment '出生日期',
    province       string comment '省份',
    city           string comment '城市',
    direction      string comment '区域',
    income         int comment '收入',
    race           string comment '民族',
    marriage_state string comment '婚姻状况',
    edu            string comment '学历',
    sign           string comment '星座',
    dt             string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
    'es.hosts' = 'up01:9200',
    'es.resource' = 'htv_policy_client/_doc',
    'es.mapping.id' = 'user_id'
    );
-- 数据导入外部表hive-es
insert overwrite table insurance_es.policy_client_es
select * from insurance_ods.policy_client ;

-- 创建第四张外部表:退保记录表
drop table if exists insurance_es.policy_surrender_es ;
CREATE EXTERNAL TABLE insurance_es.policy_surrender_es (
    pol_no       string comment '保单号',
    user_id      string comment '用户号',
    buy_datetime string comment '投保日期',
    keep_days    int comment '持有天数',
    elapse_date  string comment '失效日期',
    dt           string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
    'es.hosts' = 'up01:9200',
    'es.resource' = 'htv_policy_surrender/_doc',
    'es.mapping.id' = 'pol_no'
    );
-- 数据导入外部表hive-es
insert overwrite table insurance_es.policy_surrender_es
select * from insurance_ods.policy_surrender ;


-------------------------------------
-- select count(1) from insurance_es.claim_info_es;         -- 114235
-- select count(1) from insurance_es.policy_benefit_es;     -- 350154
-- select count(1) from insurance_es.policy_client_es;      -- 100000
-- select count(1) from insurance_es.policy_surrender_es;   -- 60478