drop database if exists htv_rule;
create database htv_rule;
use htv_rule;
create table if not exists htv_rule.htv_basic_rules
(
    id    bigint auto_increment
        primary key,
    name  varchar(50)  null comment '标签名称',
    rule  varchar(300) null comment '标签规则',
    level int          null comment '标签等级',
    pid   bigint       null comment '父标签ID'
);
insert into htv_basic_rules
values (1, "保险", NULL, 1, -1);
insert into htv_basic_rules
values (2, "商业保险系统", NULL, 2, 1);
insert into htv_basic_rules
values (3, "用户指标（用户特征）", NULL, 3, 2);
insert into htv_basic_rules
values (4, "业务发展类指标（消费特征）", NULL, 3, 2);
insert into htv_basic_rules
values (5, "行为属性（兴趣特征）", NULL, 3, 2);
insert into htv_basic_rules
values (6, "成本费用(产品成本)", NULL, 3, 2);
insert into htv_basic_rules
values (7, "其他指标(资金运用)", NULL, 3, 2);

insert into htv_basic_rules (name, rule, level, pid)
values ("性别",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_client##esType=_doc##selectFields=user_id,sex",
        4,
        3);
insert into htv_basic_rules (name, rule, level, pid)
values ("男",
        "1",
        5,
        8);
insert into htv_basic_rules (name, rule, level, pid)
values ("女",
        "2",
        5,
        8);
insert into htv_basic_rules (name, rule, level, pid)
values ("身高",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_client##esType=_doc##selectFields=user_id,height",
        4,
        3);
insert into htv_basic_rules (name, rule, level, pid)
values ("150-159",
        "150-159",
        5,
        11);
insert into htv_basic_rules (name, rule, level, pid)
values ("160-169",
        "160-169",
        5,
        11);
insert into htv_basic_rules (name, rule, level, pid)
values ("170-179",
        "170-179",
        5,
        11);
insert into htv_basic_rules (name, rule, level, pid)
values ("180-189",
        "180-189",
        5,
        11);
insert into htv_basic_rules (name, rule, level, pid)
values ("190-199",
        "190-199",
        5,
        11);
insert into htv_basic_rules (name, rule, level, pid)
values ("200-209",
        "200-209",
        5,
        11);
insert into htv_basic_rules (name, rule, level, pid)
values ("210-240",
        "210-240",
        5,
        11);
insert into htv_basic_rules (name, rule, level, pid)
values ("年龄",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_client##esType=_doc##selectFields=user_id,birthday",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("40后",
        "19400101-19491231",
        5,
        19);

insert into htv_basic_rules (name, rule, level, pid)
values ("50后",
        "19500101-19591231",
        5,
        19);
insert into htv_basic_rules (name, rule, level, pid)
values ("60后",
        "19600101-19691231",
        5,
        19);

insert into htv_basic_rules (name, rule, level, pid)
values ("70后",
        "19700101-19791231",
        5,
        19);

insert into htv_basic_rules (name, rule, level, pid)
values ("80后",
        "19800101-19891231",
        5,
        19);

insert into htv_basic_rules (name, rule, level, pid)
values ("90后",
        "19900101-19991231",
        5,
        19);
insert into htv_basic_rules (name, rule, level, pid)
values ("00后",
        "20000101-20091231",
        5,
        19);

insert into htv_basic_rules (name, rule, level, pid)
values ("10后",
        "20100101-20191231",
        5,
        19);

insert into htv_basic_rules (name, rule, level, pid)
values ("20后",
        "20200101-20291231",
        5,
        19);

# select province,count(province) from insurance.policy_client group by province;

insert into htv_basic_rules (name, rule, level, pid)
values ("省份",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_client##esType=_doc##selectFields=user_id,province",
        4,
        3);

# insert into htv_basic_rules ( name, rule, level, pid)
# select province,t1.rule,5,29 from
# (select province,count(province),@rank := @rank + 1 as rule from insurance.policy_client,(select @rank := 0) as rule group by province order by rule) as t1;

insert into htv_basic_rules (name, rule, level, pid)
select t1.province, t1.province, 5, 29
from (select province from insurance.policy_client group by province) as t1;
set @@identity = 62;
alter table htv_basic_rules
    auto_increment = 1;


# truncate table htv_basic_rules;

insert into htv_basic_rules (name, rule, level, pid)
values ("城市",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_client##esType=_doc##selectFields=user_id,city",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
select city, city, 5, 62
from (select city from insurance.policy_client group by city) as t1;

set @@identity = 522;
alter table htv_basic_rules
    auto_increment = 1;

insert into htv_basic_rules (name, rule, level, pid)
values ("区域",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_client##esType=_doc##selectFields=user_id,direction",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
select direction, direction, 5, 522
from (select direction from insurance.policy_client group by direction) as t1;
set @@identity = 530;
alter table htv_basic_rules
    auto_increment = 1;


insert into htv_basic_rules (name, rule, level, pid)
values ("收入",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_client##esType=_doc##selectFields=user_id,income",
        4,
        3);

# select income,count(income) as i from insurance.policy_client group by income order by i;

insert into htv_basic_rules (name, rule, level, pid)
values ("低收入",
        "1-60000",
        5,
        530);

insert into htv_basic_rules (name, rule, level, pid)
values ("中等收入",
        "60000-120000",
        5,
        530);

insert into htv_basic_rules (name, rule, level, pid)
values ("中上等收入",
        "120000-240000",
        5,
        530);

insert into htv_basic_rules (name, rule, level, pid)
values ("高收入",
        "240000-600000",
        5,
        530);

insert into htv_basic_rules (name, rule, level, pid)
values ("超高收入",
        "600000-1200000",
        5,
        530);

insert into htv_basic_rules (name, rule, level, pid)
values ("富翁",
        "1200000-100000000",
        5,
        530);

insert into htv_basic_rules (name, rule, level, pid)
values ("民族",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_client##esType=_doc##selectFields=user_id,race",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("汉族",
        "汉族",
        5,
        537);

insert into htv_basic_rules (name, rule, level, pid)
values ("蒙古族",
        "蒙古族",
        5,
        537);

insert into htv_basic_rules (name, rule, level, pid)
values ("回族",
        "回族",
        5,
        537);

insert into htv_basic_rules (name, rule, level, pid)
values ("藏族",
        "藏族",
        5,
        537);

insert into htv_basic_rules (name, rule, level, pid)
values ("维吾尔族",
        "维吾尔族",
        5,
        537);

insert into htv_basic_rules (name, rule, level, pid)
values ("苗族",
        "苗族",
        5,
        537);

insert into htv_basic_rules (name, rule, level, pid)
values ("满族",
        "满族",
        5,
        537);

insert into htv_basic_rules (name, rule, level, pid)
values ("婚姻状况",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_client##esType=_doc##selectFields=user_id,marriage_state",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("未婚",
        "1",
        5,
        545);

insert into htv_basic_rules (name, rule, level, pid)
values ("已婚",
        "2",
        5,
        545);

insert into htv_basic_rules (name, rule, level, pid)
values ("离异",
        "3",
        5,
        545);

insert into htv_basic_rules (name, rule, level, pid)
values ("学历",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_client##esType=_doc##selectFields=user_id,edu",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("小学",
        "小学",
        5,
        549);

insert into htv_basic_rules (name, rule, level, pid)
values ("初中",
        "初中",
        5,
        549);

insert into htv_basic_rules (name, rule, level, pid)
values ("高中",
        "高中",
        5,
        549);

insert into htv_basic_rules (name, rule, level, pid)
values ("大专",
        "大专",
        5,
        549);

insert into htv_basic_rules (name, rule, level, pid)
values ("本科",
        "本科",
        5,
        549);

insert into htv_basic_rules (name, rule, level, pid)
values ("研究生",
        "研究生",
        5,
        549);

insert into htv_basic_rules (name, rule, level, pid)
values ("博士",
        "博士",
        5,
        549);

insert into htv_basic_rules (name, rule, level, pid)
values ("星座",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_client##esType=_doc##selectFields=user_id,sign",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("双子座",
        "双子座",
        5,
        557);

insert into htv_basic_rules (name, rule, level, pid)
values ("双鱼座",
        "双鱼座",
        5,
        557);

insert into htv_basic_rules (name, rule, level, pid)
values ("处女座",
        "处女座",
        5,
        557);

insert into htv_basic_rules (name, rule, level, pid)
values ("天秤座",
        "天秤座",
        5,
        557);

insert into htv_basic_rules (name, rule, level, pid)
values ("天蝎座",
        "天蝎座",
        5,
        557);

insert into htv_basic_rules (name, rule, level, pid)
values ("射手座",
        "射手座",
        5,
        557);

insert into htv_basic_rules (name, rule, level, pid)
values ("巨蟹座",
        "巨蟹座",
        5,
        557);

insert into htv_basic_rules (name, rule, level, pid)
values ("摩羯座",
        "摩羯座",
        5,
        557);

insert into htv_basic_rules (name, rule, level, pid)
values ("水瓶座",
        "水瓶座",
        5,
        557);

insert into htv_basic_rules (name, rule, level, pid)
values ("狮子座",
        "狮子座",
        5,
        557);

insert into htv_basic_rules (name, rule, level, pid)
values ("白羊座",
        "白羊座",
        5,
        557);

insert into htv_basic_rules (name, rule, level, pid)
values ("金牛座",
        "金牛座",
        5,
        557);

insert into htv_basic_rules (name, rule, level, pid)
values ("缴费期",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_benefit##esType=_doc##selectFields=user_id,ppp",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("10年",
        "10",
        5,
        570);

insert into htv_basic_rules (name, rule, level, pid)
values ("15年",
        "15",
        5,
        570);

insert into htv_basic_rules (name, rule, level, pid)
values ("20年",
        "20",
        5,
        570);

insert into htv_basic_rules (name, rule, level, pid)
values ("30年",
        "30",
        5,
        570);

insert into htv_basic_rules (name, rule, level, pid)
values ("投保年龄",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_benefit##esType=_doc##selectFields=user_id,age_buy",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("40后",
        "1940-1949",
        5,
        575);
insert into htv_basic_rules (name, rule, level, pid)
values ("50后",
        "1950-1959",
        5,
        575);

insert into htv_basic_rules (name, rule, level, pid)
values ("60后",
        "1960-1969",
        5,
        575);

insert into htv_basic_rules (name, rule, level, pid)
values ("70后",
        "1970-1979",
        5,
        575);

insert into htv_basic_rules (name, rule, level, pid)
values ("80后",
        "1980-1989",
        5,
        575);

insert into htv_basic_rules (name, rule, level, pid)
values ("90后",
        "1990-1999",
        5,
        575);

insert into htv_basic_rules (name, rule, level, pid)
values ("00后",
        "2000-2009",
        5,
        575);

insert into htv_basic_rules (name, rule, level, pid)
values ("10后",
        "2010-2019",
        5,
        575);
insert into htv_basic_rules (name, rule, level, pid)
values ("20后",
        "2020-2029",
        5,
        575);

insert into htv_basic_rules (name, rule, level, pid)
values ("最近购买周期",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_benefit##esType=_doc##selectFields=user_id,buy_datetime",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("7日",
        "0-7",
        5,
        585);

insert into htv_basic_rules (name, rule, level, pid)
values ("2周",
        "7-14",
        5,
        585);

insert into htv_basic_rules (name, rule, level, pid)
values ("1月",
        "14-30",
        5,
        585);

insert into htv_basic_rules (name, rule, level, pid)
values ("2月",
        "30-60",
        5,
        585);

insert into htv_basic_rules (name, rule, level, pid)
values ("3月",
        "60-90",
        5,
        585);

insert into htv_basic_rules (name, rule, level, pid)
values ("4月",
        "90-120",
        5,
        585);

insert into htv_basic_rules (name, rule, level, pid)
values ("5月",
        "120-150",
        5,
        585);

insert into htv_basic_rules (name, rule, level, pid)
values ("6月",
        "150-180",
        5,
        585);

insert into htv_basic_rules (name, rule, level, pid)
values ("半年以上",
        "180-9999",
        5,
        585);

insert into htv_basic_rules (name, rule, level, pid)
values ("保险类型",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_benefit##esType=_doc##selectFields=user_id,insur_code",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("e生保",
        "5599",
        5,
        595);

insert into htv_basic_rules (name, rule, level, pid)
values ("守护安康",
        "1188",
        5,
        595);

insert into htv_basic_rules (name, rule, level, pid)
values ("相伴一生护理保险",
        "8866",
        5,
        595);

insert into htv_basic_rules (name, rule, level, pid)
values ("福享一生",
        "6699",
        5,
        595);

insert into htv_basic_rules (name, rule, level, pid)
values ("财富鑫生",
        "2266",
        5,
        595);

insert into htv_basic_rules (name, rule, level, pid)
values ("保单是否生效",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_benefit##esType=_doc##selectFields=user_id,pol_flag",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("有效",
        "1",
        5,
        601);

insert into htv_basic_rules (name, rule, level, pid)
values ("无效",
        "0",
        5,
        601);

insert into htv_basic_rules (name, rule, level, pid)
values ("购买周期",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_claim_info##esType=_doc##selectFields=user_id,buy_datetime",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("三个月",
        "0-90",
        5,
        604);

insert into htv_basic_rules (name, rule, level, pid)
values ("六个月",
        "90-180",
        5,
        604);

insert into htv_basic_rules (name, rule, level, pid)
values ("一年",
        "180-365",
        5,
        604);

insert into htv_basic_rules (name, rule, level, pid)
values ("一年半",
        "365-545",
        5,
        604);

insert into htv_basic_rules (name, rule, level, pid)
values ("两年",
        "545-730",
        5,
        604);

insert into htv_basic_rules (name, rule, level, pid)
values ("俩年半",
        "730-910",
        5,
        604);

insert into htv_basic_rules (name, rule, level, pid)
values ("三年",
        "910-1095",
        5,
        604);

insert into htv_basic_rules (name, rule, level, pid)
values ("三年到五年",
        "1095-1825",
        5,
        604);

insert into htv_basic_rules (name, rule, level, pid)
values ("五年以上",
        "1825-9999",
        5,
        604);

insert into htv_basic_rules (name, rule, level, pid)
values ("理赔周期",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_claim_info##esType=_doc##selectFields=user_id,buy_datetime,claim_date",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("三个月",
        "0-90",
        5,
        614);

insert into htv_basic_rules (name, rule, level, pid)
values ("六个月",
        "90-180",
        5,
        614);

insert into htv_basic_rules (name, rule, level, pid)
values ("一年",
        "180-365",
        5,
        614);

insert into htv_basic_rules (name, rule, level, pid)
values ("一年半",
        "365-545",
        5,
        614);

insert into htv_basic_rules (name, rule, level, pid)
values ("两年",
        "545-730",
        5,
        614);

insert into htv_basic_rules (name, rule, level, pid)
values ("俩年半",
        "730-910",
        5,
        614);

insert into htv_basic_rules (name, rule, level, pid)
values ("三年",
        "910-1095",
        5,
        614);

insert into htv_basic_rules (name, rule, level, pid)
values ("三年到五年",
        "1095-1825",
        5,
        614);

insert into htv_basic_rules (name, rule, level, pid)
values ("五年以上",
        "1825-9999",
        5,
        614);

insert into htv_basic_rules (name, rule, level, pid)
values ("理赔责任类型",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_claim_info##esType=_doc##selectFields=user_id,claim_item",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("cqhlbxj",
        "cqhlbxj",
        5,
        624);

insert into htv_basic_rules (name, rule, level, pid)
values ("scbxj0",
        "scbxj0",
        5,
        624);

insert into htv_basic_rules (name, rule, level, pid)
values ("scbxj1",
        "scbxj1",
        5,
        624);

insert into htv_basic_rules (name, rule, level, pid)
values ("scbxj2",
        "scbxj2",
        5,
        624);

insert into htv_basic_rules (name, rule, level, pid)
values ("sgbxj",
        "sgbxj",
        5,
        624);

insert into htv_basic_rules (name, rule, level, pid)
values ("理赔金额",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_claim_info##esType=_doc##selectFields=user_id,claim_mnt",
        4,
        3);

insert into htv_basic_rules (name, rule, level, pid)
values ("低",
        "0-3000",
        5,
        630);

insert into htv_basic_rules (name, rule, level, pid)
values ("中",
        "3000-7000",
        5,
        630);

insert into htv_basic_rules (name, rule, level, pid)
values ("高",
        "7000-99999999",
        5,
        630);


insert into htv_basic_rules (name, rule, level, pid)
values ("保单持有周期",
        "inType=Elasticsearch##esNodes=192.168.88.166:9200##esIndex=htv_policy_surrender##esType=_doc##selectFields=user_id,keep_days",
        4,
        3);


insert into htv_basic_rules (name, rule, level, pid)
values ("三个月",
        "0-90",
        5,
        634);

insert into htv_basic_rules (name, rule, level, pid)
values ("六个月",
        "90-180",
        5,
        634);

insert into htv_basic_rules (name, rule, level, pid)
values ("一年",
        "180-365",
        5,
        634);

insert into htv_basic_rules (name, rule, level, pid)
values ("两年",
        "365-730",
        5,
        634);

insert into htv_basic_rules (name, rule, level, pid)
values ("三年",
        "730-1095",
        5,
        634);

insert into htv_basic_rules (name, rule, level, pid)
values ("三年到五年",
        "1095-1825",
        5,
        634);


insert into htv_basic_rules (name, rule, level, pid)
values ("五年以上",
        "1825-9999",
        5,
        634);

