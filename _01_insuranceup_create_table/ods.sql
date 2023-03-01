--insurance_ods



-- 级联删除数据库时，数据库不为表，就会报错：工具提示元数据信息无法同步
drop database if exists insurance_ods cascade;
create database insurance_ods;

-- 保险投保客户：用户表
-- drop database insurance_userprofile cascade ;
-- drop database insurance_ods cascade ;

-- create database insurance_ods;
use insurance_ods;

-- 投保客户表
CREATE TABLE insurance_ods.policy_client (
  user_id STRING COMMENT '用户号',
  name STRING COMMENT '姓名',
  id_card STRING COMMENT '身份证号',
  phone STRING COMMENT '手机号',
  sex STRING COMMENT '性别',
  height INT  COMMENT '身高',
  birthday STRING COMMENT '出生日期',
  province STRING COMMENT '省份',
  city STRING COMMENT '城市',
  direction STRING COMMENT '区域',
  income INT COMMENT '收入',
  race STRING COMMENT '民族',
  marriage_state STRING COMMENT '婚姻状况',
  edu STRING  COMMENT '学历',
  sign STRING COMMENT '星座')
COMMENT '客户信息表'
    -- 是否设置表分区，取决于后续是否区分全量和增量导入数据
    partitioned by (dt string)
    row format delimited fields terminated by '\t';

-- 修改表创建新的分区
-- alter table insurance_ods.policy_client add if not exists partition(df='20221203') location '/user/hive/warehouse/insurance_ods.db/policy_client/20221203';
alter table insurance_ods.policy_client add if not exists partition(dt='20221203');
alter table insurance_ods.policy_client add if not exists partition(dt='20221202');


-- 退保记录表
CREATE TABLE insurance_ods.policy_surrender (
  pol_no STRING COMMENT '保单号',
  user_id STRING COMMENT '用户号',
  buy_datetime STRING COMMENT '投保日期',
  keep_days INT COMMENT '持有天数',
  elapse_date STRING COMMENT '失效日期')
COMMENT '退保记录表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t';


-- 投保详情表
drop table insurance_ods.policy_benefit;
CREATE TABLE insurance_ods.policy_benefit (
  pol_no STRING COMMENT '保单号',
  user_id STRING COMMENT '用户号',
  ppp STRING COMMENT '缴费期',
  age_buy smallint COMMENT '投保年龄',
  buy_datetime STRING COMMENT '投保日期',
  insur_name STRING COMMENT '保险名称',
  insur_code STRING COMMENT '保险代码',
  pol_flag SMALLINT COMMENT '保单状态，1有效，0失效',
  elapse_date STRING COMMENT '保单失效时间')
COMMENT '投保记录表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t';


-- 理赔信息表
drop table insurance_ods.claim_info;
CREATE TABLE insurance_ods.claim_info (
  pol_no STRING COMMENT '保单号',
  user_id STRING COMMENT '用户号',
  buy_datetime STRING COMMENT '购买日期',
  insur_code STRING COMMENT '保险代码',
  claim_date STRING COMMENT '理赔日期',
  claim_item STRING COMMENT '理赔责任',
  claim_mnt DECIMAL(35,6) COMMENT '理赔金额')
COMMENT '理赔信息表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t';

-- 修改表创建新的分区
-- alter table insurance_ods.policy_client add if not exists partition(df='20221203') location '/user/hive/warehouse/insurance_ods.db/policy_client/20221203';
alter table insurance_ods.policy_benefit add if not exists partition(dt='20221203');
alter table insurance_ods.policy_surrender add if not exists partition(dt='20221203');
alter table insurance_ods.claim_info add if not exists partition(dt='20221203');
