# 可视化sql
#  用户年龄
select name as "年龄段",count(name) as "用户数量" from(
select t1.name from
               (select id, name from htv_basic_rules where id between 20 and 28)t1
                  left join
                  htv_result t2
                      on
                          t1.id=t2.tagsid
group by t1.name, t2.user_id)t3 group by name;

#  用户男女
select name as "性别",count(name) as "用户数量" from(
select t1.name from
               (select id, name from htv_basic_rules where id between 9 and 10)t1
                  left join
                  htv_result t2
                      on
                          t1.id=t2.tagsid
group by t1.name, t2.user_id)t3 group by name;

#  用户收入
select name as "收入",count(name) as "用户数量" from(
select t1.name from
               (select id, name from htv_basic_rules where id between 531 and 536)t1
                  left join
                  htv_result t2
                      on
                          t1.id=t2.tagsid
group by t1.name, t2.user_id)t3 group by name;

# 地区用户
select name as "地区",count(name) as "用户数量" from(
select t1.name from
               (select id, name from htv_basic_rules where id between 30 and 61)t1
                  left join
                  htv_result t2
                      on
                          t1.id=t2.tagsid
group by t1.name, t2.user_id)t3 group by name;

# 缴费期
select name as "缴费期",count(name) as "用户数量" from(
select t1.name from
               (select id, name from htv_basic_rules where id between 571 and 574)t1
                  left join
                  htv_result t2
                      on
                          t1.id=t2.tagsid
group by t1.name, t2.user_id)t3 group by name;

# 保险类型
select name as "保险类型",count(name) as "用户数量" from(
select t1.name from
               (select id, name from htv_basic_rules where id between 596 and 600)t1
                  left join
                  htv_result t2
                      on
                          t1.id=t2.tagsid
group by t1.name, t2.user_id)t3 group by name;

#理赔金额统计
select sum(claim_mnt) as "理赔总金额(元)" from insurance.claim_info;

# 保单持有周期
select name as "保单持有周期",count(name) as "用户数量" from(
select t1.name from
               (select id, name from htv_basic_rules where id between 635 and 641)t1
                  left join
                  htv_result t2
                      on
                          t1.id=t2.tagsid
group by t1.name, t2.user_id)t3 group by name;