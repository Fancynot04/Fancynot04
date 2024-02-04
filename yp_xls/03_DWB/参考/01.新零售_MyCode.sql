-- 细节: 数仓的分层是逻辑分层, 其实就是人为创建不同的Hive数据库, 实现分层.  这里, 我们先创建 ods层的Hive数据库.
-- 1. 创建ods层数据库.
create database yp_ods;     -- ods层建模规则: 项目名缩写_对应的层
use yp_ods;
show tables;

-- 2. 演示创建hive数据表, 有注释, 中文会乱码, 我们要解决它.
drop table yp_ods.t_district;       -- 修改完MySql码表后, 记得删除重建, 即可解决乱码.
CREATE TABLE yp_ods.t_district
(
    `id` string COMMENT '主键ID',
    `code` string COMMENT '区域编码',
    `name` string COMMENT '区域名称',
    `pid`  int COMMENT '父级ID',
    `alias` string COMMENT '别名'
)
comment '区域字典表'
row format delimited fields terminated by '\t'
    stored as orc tblproperties ('orc.compress'='ZLIB');

-- 3. 查看表结构.
desc t_district;

-- ------------------------------- 案例1: ODS层搭建之 全量覆盖 导入方式 -------------------------------
-- 问题: 为什么ods层的表使用的是 orc + zlib方式?
-- 答案: 因为 列存储更节省空间, 且ods层主要是临时存储, 读写操作相对不多, 我们更侧重于存储, 即: 压缩比.
-- 1. 在hive的 yp_ods数据库下, 建表.
show tables;
CREATE TABLE yp_ods.t_district
(
    `id` string COMMENT '主键ID',
    `code` string COMMENT '区域编码',
    `name` string COMMENT '区域名称',
    `pid`  int COMMENT '父级ID',
    `alias` string COMMENT '别名'
)
comment '区域字典表'
row format delimited fields terminated by '\t'
    stored as orc tblproperties ('orc.compress'='ZLIB');

-- 2. 通过Sqoop脚本(在CRT中执行)实现, 从MySQL中导入数据到Hive表中.
-- 如下的Sqoop脚本是在CRT中执行的.
/*
    /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
    --connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
    --username root \
    --password 123456 \
    --query "select * from t_district where 1=1 and  \$CONDITIONS" \
    --hcatalog-database yp_ods \
    --hcatalog-table t_district \
    -m 1
 */

-- 3. 查询结果.
select * from t_district;
select * from t_district where pid='410000';
select * from t_district where name='郑州市';


-- ------------------------------- 案例2: ODS层搭建之 增量导入(仅新增) 导入方式 -------------------------------
-- 1. 在hive的 yp_ods数据库下, 建表.
CREATE TABLE yp_ods.t_user_login(
   id string,
   login_user string,
   login_type string COMMENT '登录类型（登陆时使用）',
   client_id string COMMENT '推送标示id(登录、第三方登录、注册、支付回调、给用户推送消息时使用)',
   login_time string,
   login_ip string,
   logout_time string
)
COMMENT '用户登录记录表'
partitioned by (dt string)      -- 按照时间(天)分区,  2023-05-21,  2023-05-22
row format delimited fields terminated by '\t'
stored as orc tblproperties ('orc.compress' = 'ZLIB');  -- orc + zlib

-- 2. 通过Sqoop脚本(在CRT中执行)实现, 从MySQL中导入数据到Hive表中.
-- 如下的Sqoop脚本是在CRT中执行的.
/*
    Step1: 增量导入的第1步都是 全量导入.
    /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
    --connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
    --username root \
    --password 123456 \
    --query "select *, '2023-05-28' from t_user_login where 1=1 and  \$CONDITIONS" \
    --hcatalog-database yp_ods \
    --hcatalog-table t_user_login \
    -m 1

    Step2: 增量导入的第2步才是 增量导入(这里是 仅新增)
    /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
    --connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
    --username root \
    --password 123456 \
    --query "select *, '2023-05-28' from t_user_login where login_time between '2023-05-28 00:00:00' and '2023-05-28 23:59:59' and  \$CONDITIONS" \
    --hcatalog-database yp_ods \
    --hcatalog-table t_user_login \
    -m 1

    Step3: 最终写法如下, 时间是动态传入的.
    TD_DATE=`date -d '1 days ago' +"%Y-%m-%d"`          -- Linux变量, 用于获取 昨日时间的, 格式为: 2023-05-28
    /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
    --connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
    --username root \
    --password 123456 \
    --query "select *, '${TD_DATE}' from t_user_login where login_time between '${TD_DATE} 00:00:00' and '${TD_DATE} 23:59:59' and  \$CONDITIONS" \
    --hcatalog-database yp_ods \
    --hcatalog-table t_user_login \
    -m 1
 */

-- 3. 查询结果.
select * from t_user_login;


-- ------------------------------- 案例3: ODS层搭建之 增量导入(新增 + 修改) 导入方式 -------------------------------
-- 1. 在hive的 yp_ods数据库下, 建表.
CREATE TABLE yp_ods.t_store
(
    `id`                 string COMMENT '主键',
    `user_id`            string,
    `store_avatar`       string COMMENT '店铺头像',
    `address_info`       string COMMENT '店铺详细地址',
    `name`               string COMMENT '店铺名称',
    `store_phone`        string COMMENT '联系电话',
    `province_id`        INT COMMENT '店铺所在省份ID',
    `city_id`            INT COMMENT '店铺所在城市ID',
    `area_id`            INT COMMENT '店铺所在县ID',
    `mb_title_img`       string COMMENT '手机店铺 页头背景图',
    `store_description` string COMMENT '店铺描述',
    `notice`             string COMMENT '店铺公告',
    `is_pay_bond`        TINYINT COMMENT '是否有交过保证金 1：是0：否',
    `trade_area_id`      string COMMENT '归属商圈ID',
    `delivery_method`    TINYINT COMMENT '配送方式  1 ：自提 ；3 ：自提加配送均可; 2 : 商家配送',
    `origin_price`       DECIMAL,
    `free_price`         DECIMAL,
    `store_type`         INT COMMENT '店铺类型 22天街网店 23实体店 24直营店铺 33会员专区店',
    `store_label`        string COMMENT '店铺logo',
    `search_key`         string COMMENT '店铺搜索关键字',
    `end_time`           string COMMENT '营业结束时间',
    `start_time`         string COMMENT '营业开始时间',
    `operating_status`   TINYINT COMMENT '营业状态  0 ：未营业 ；1 ：正在营业',
    `create_user`        string,
    `create_time`        string,
    `update_user`        string,
    `update_time`        string,
    `is_valid`           TINYINT COMMENT '0关闭，1开启，3店铺申请中',
    `state`              string COMMENT '可使用的支付类型:MONEY金钱支付;CASHCOUPON现金券支付',
    `idCard`             string COMMENT '身份证',
    `deposit_amount`     DECIMAL(11,2) COMMENT '商圈认购费用总额',
    `delivery_config_id` string COMMENT '配送配置表关联ID',
    `aip_user_id`        string COMMENT '通联支付标识ID',
    `search_name`        string COMMENT '模糊搜索名称字段:名称_+真实名称',
    `automatic_order`    TINYINT COMMENT '是否开启自动接单功能 1：是  0 ：否',
    `is_primary`         TINYINT COMMENT '是否是总店 1: 是 2: 不是',
    `parent_store_id`    string COMMENT '父级店铺的id，只有当is_primary类型为2时有效'
)
comment '店铺表'
partitioned by (dt string)  -- 日期分区
    row format delimited fields terminated by '\t'
    stored as orc tblproperties ('orc.compress'='ZLIB');    -- orc + zlib


-- 2. 通过Sqoop脚本(在CRT中执行)实现, 从MySQL中导入数据到Hive表中.
-- 如下的Sqoop脚本是在CRT中执行的.
/*
    Step1: 增量导入的第1步都是 全量导入.
    /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
    --connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
    --username root \
    --password 123456 \
    --query "select *, '2023-05-28' as dt from t_store where 1=1 and  \$CONDITIONS" \
    --hcatalog-database yp_ods \
    --hcatalog-table t_store \
    -m 1

    Step2: 增量导入第2步才是具体的增量数据, 这里是 新增 + 修改.
    /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
    --connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
    --username root \
    --password 123456 \
    --query "select *, '2023-05-28' as dt from t_store where
        (
            (create_time between '2023-05-28 00:00:00' and '2023-05-28 23:59:59')           -- 新增数据
            or
            (update_time between '2023-05-28 00:00:00' and '2023-05-28 23:59:59')           -- 修改数据.
        ) and  \$CONDITIONS" \
    --hcatalog-database yp_ods \
    --hcatalog-table t_store \
    -m 1

    Step3: 最终写法如下, 时间是动态传入的.
    TD_DATE=`date -d '1 days ago' +"%Y-%m-%d"`          -- Linux变量, 用于获取 昨日时间的, 格式为: 2023-05-28
    /usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
    --connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
    --username root \
    --password 123456 \
    --query "select *, '${TD_DATE}' as dt from t_store where
        (
            (create_time between '${TD_DATE} 00:00:00' and '${TD_DATE} 23:59:59')           -- 新增数据
            or
            (update_time between '${TD_DATE} 00:00:00' and '${TD_DATE} 23:59:59')           -- 修改数据.
        ) and  \$CONDITIONS" \
    --hcatalog-database yp_ods \
    --hcatalog-table t_store \
    -m 1
 */

-- 3. 查询结果.
select * from t_store;

-- ------------------------------- 案例4: ODS层搭建之 完整版 -------------------------------
-- 1. DataGrip连接MySQL, Hive.
-- 2. 准备MySQL源数据, 创建Hive的 yp_ods数据库.
-- 3. 执行 yp_xls\01_ODS\create_ods_table.sql 脚本, 把ODS层所有的表创建出来.
-- 4. 查询yp_ods层下所有的数据表.    一共23张表.
-- 5. 执行 sqoop_import.sh, 将该文件上传到CRT中执行, 记得设置权限, 修改脚本中SQL的时间. 执行成功后, MySQL => ODS完成.
-- 6. 随便找几张表, 检查其中是否有数据, 有则搞定.


-- ------------------------------- ***** 如下是DWD层的搭建动作(Data warehouse detail, 数仓明细层) ***** -------------------------------
-- 作用: 1.清洗转换.   2.区分维度表和事实表.
-- 细节: dwd层表名规则, fact_表名: 事实表,  dim_表名: 维度表
create database yp_dwd;     -- 建模规则 = 库名 = 项目缩写_层的缩写
use yp_dwd;
show tables;

-- ------------------------------- DWD层, 案例1: 拉链表 -------------------------------
/*
    概述和作用:
        拉链表属于SCD2(缓慢渐变维2), 就是用来记录所有历史状态的数据的, 表必须有starttime 和 endtime这两列, 标记着 时间段.
    拉链表的实现步骤:
        1. 获取增量采集信息.
        2. 旧的拉链表 left join 增量采集信息, 只要满足条件, 就用 增量采集信息的 starttime - 1 作为 旧的拉链表的 endtime(结束时间)
        3. 用上述操作后的拉链表 union all 增量采集信息, 即为我们要的结果, 将其写到临时表中.
           细节: 实际开发中, 临时表不做也可以, 而是直接用结果覆盖原表.
        4. 用临时表的数据 全量覆盖 旧的拉链表数据, 即为最终结果.
        总结: 拉链表的核心公式
            (旧的拉链表 left join 增量采集信息) union all 增量采集信息
 */
-- 1. 创建表, 添加数据, 充当: 旧的拉链表数据.
use yp_dwd;
create table dw_zipper(
    userid string,
    phone string,
    nick string,
    gender int,
    addr string,
    starttime string,
    endtime string
) row format delimited fields terminated by '\t';

-- 添加表数据.  可以用put方式, 不能用 load data local...方式.
select * from dw_zipper;

-- 2. 创建表, 添加数据, 充当: 增量采集信息.
create table ods_zipper_update(
    userid string,
    phone string,
    nick string,
    gender int,
    addr string,
    starttime string,
    endtime string
) row format delimited fields terminated by '\t';

-- 3. 具体的拉链操作.  -- 拉链公式 = 增量采集信息 union all (旧的拉链表 left join 增量采集信息)
insert overwrite table tmp_zipper
select * from ods_zipper_update         -- 增量采集信息
union all
select
       a.userid, a.phone, a.nick, a.gender, a.addr, a.starttime,
       -- 没有交集的, 或者 a的结束时间已经修改过的, 这些数据不需要调整, 保留原始时间,  否则, 就用b的开始时间-1作为 a的结束时间.
       if(b.userid is null or a.endtime < '9999-12-31', a.endtime, date_sub(b.starttime, 1))
from
     dw_zipper a left join ods_zipper_update b on a.userid = b.userid;   --  (旧的拉链表 left join 增量采集信息)

-- 4. 创建临时表(实际开发中, 具体根据需求来, 可以不创建), 将处理后的数据写到临时表中.
create table tmp_zipper(
    userid string,
    phone string,
    nick string,
    gender int,
    addr string,
    starttime string,
    endtime string
) row format delimited fields terminated by '\t';

select * from tmp_zipper;

-- 5. 用临时表覆盖旧的拉链表, 即为最新的拉链表数据.
insert overwrite table dw_zipper
select * from tmp_zipper;

select * from dw_zipper;        -- 13条.


-- ------------------------------- DWD层, 案例2: 拉链导入(增量导入, 新增 + 修改) -------------------------------
-- 1. 这里我们用 订单事实表举例, 先建表.
DROP TABLE if EXISTS yp_dwd.fact_shop_order;
CREATE TABLE yp_dwd.fact_shop_order(
  id string COMMENT '根据一定规则生成的订单编号',
  order_num string COMMENT '订单序号',
  buyer_id string COMMENT '买家的userId',
  store_id string COMMENT '店铺的id',
  order_from string COMMENT '此字段可以转换 1.安卓\; 2.ios\; 3.小程序H5 \; 4.PC',
  order_state int COMMENT '订单状态:1.已下单\; 2.已付款, 3. 已确认 \;4.配送\; 5.已完成\; 6.退款\;7.已取消',
  create_date string COMMENT '下单时间',
  finnshed_time timestamp COMMENT '订单完成时间,当配送员点击确认送达时,进行更新订单完成时间,后期需要根据订单完成时间,进行自动收货以及自动评价',
  is_settlement tinyint COMMENT '是否结算\;0.待结算订单\; 1.已结算订单\;',
  is_delete tinyint COMMENT '订单评价的状态:0.未删除\;  1.已删除\;(默认0)',
  evaluation_state tinyint COMMENT '订单评价的状态:0.未评价\;  1.已评价\;(默认0)',
  way string COMMENT '取货方式:SELF自提\;SHOP店铺负责配送',
  is_stock_up int COMMENT '是否需要备货 0：不需要    1：需要    2:平台确认备货  3:已完成备货 4平台已经将货物送至店铺 ',
  create_user string,
  create_time string,
  update_user string,
  update_time string,
  is_valid tinyint COMMENT '是否有效  0: false\; 1: true\;   订单是否有效的标志',
  end_date string COMMENT '拉链结束日期')
COMMENT '订单表'
partitioned by (start_date string)      -- start_date字段: 即充当分区字段, 也充当拉链的开始时间.
row format delimited fields terminated by '\t'
stored as orc tblproperties ('orc.compress' = 'SNAPPY');        -- orc + snappy: 因为从dwd层开始, 对数据的读写要求相对较高, 所以推荐用这个组合.

-- 2. 拉链导入(增量导入, 新增+更新)本质还是增量导入, 所有增量导入第一步都是: 全量导入.
-- 首次导入, 即: 全量导入, 先执行下如下的 set 参数, 再执行SQL语句.
--分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.created.files=150000;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;

-- 具体导入数据的动作.
insert into yp_dwd.fact_shop_order partition (start_date)       -- 动态分区.
select
    id,
    order_num,
    buyer_id,
    store_id,
    case order_from         -- 数据的清洗转换, 即: ETL动作.
        when 1 then 'Android'
        when 2 then 'IOS'
        when 3 then 'miniApp'
        when 4 then 'PC'
        when 5 then '空'
    end as order_from,
    order_state,
    create_date,
    finnshed_time,
    is_settlement,
    is_delete,
    evaluation_state,
    way,
    is_stock_up,
    create_user,
    create_time,
    update_user,
    update_time,
    is_valid,
    '9999-12-31' as end_date,
    dt as start_date        -- 既充当分区字段, 又充当拉链开始时间.
from yp_ods.t_shop_order;

-- 3. 循环导入, 拉链导入(增量采集, 新增 + 修改)
-- 3.1 查看ods层的数据
select * from yp_ods.t_shop_order;      --  3155 => 3157条

-- 3.2 将ods层的数据, 拉链导入到dwd层.
-- 拉链导入公式: (旧的拉链表 left join 增量采集信息) union all 增量采集信息
-- 旧的拉链表:  select * from yp_dwd.fact_shop_order;

-- 增量信息:    select * from yp_ods.t_shop_order where create_time between '' and ''  or update_time between '' and '';
select * from yp_ods.t_shop_order where     -- 增量信息
    create_time between '2023-05-29 00:00:00' and '2023-05-29 23:59:59'     -- 新增
    or
    update_time between '2023-05-29 00:00:00' and '2023-05-29 23:59:59';    -- 修改,  总计: 2条(修改1条, 新增1条)

-- 3.3 创建中间表, 用来存储结果.
DROP TABLE if EXISTS yp_dwd.fact_shop_order_tmp;
CREATE TABLE yp_dwd.fact_shop_order_tmp(
  id string COMMENT '根据一定规则生成的订单编号',
  order_num string COMMENT '订单序号',
  buyer_id string COMMENT '买家的userId',
  store_id string COMMENT '店铺的id',
  order_from string COMMENT '此字段可以转换 1.安卓\; 2.ios\; 3.小程序H5 \; 4.PC',
  order_state int COMMENT '订单状态:1.已下单\; 2.已付款, 3. 已确认 \;4.配送\; 5.已完成\; 6.退款\;7.已取消',
  create_date string COMMENT '下单时间',
  finnshed_time timestamp COMMENT '订单完成时间,当配送员点击确认送达时,进行更新订单完成时间,后期需要根据订单完成时间,进行自动收货以及自动评价',
  is_settlement tinyint COMMENT '是否结算\;0.待结算订单\; 1.已结算订单\;',
  is_delete tinyint COMMENT '订单评价的状态:0.未删除\;  1.已删除\;(默认0)',
  evaluation_state tinyint COMMENT '订单评价的状态:0.未评价\;  1.已评价\;(默认0)',
  way string COMMENT '取货方式:SELF自提\;SHOP店铺负责配送',
  is_stock_up int COMMENT '是否需要备货 0：不需要    1：需要    2:平台确认备货  3:已完成备货 4平台已经将货物送至店铺 ',
  create_user string,
  create_time string,
  update_user string,
  update_time string,
  is_valid tinyint COMMENT '是否有效  0: false\; 1: true\;   订单是否有效的标志',
  end_date string COMMENT '拉链结束日期')
COMMENT '订单表'
partitioned by (start_date string)
row format delimited fields terminated by '\t'
stored as orc
tblproperties ('orc.compress' = 'SNAPPY');

-- 3.4 将处理后的数据, 添加到 中间表中.
insert overwrite table yp_dwd.fact_shop_order_tmp partition (start_date)        -- 往临时表添加数据.
select *
from (
   --1、ods表的新分区数据(有新增和更新的数据)
         select id,
                order_num,
                buyer_id,
                store_id,
                case order_from
                    when 1
                        then 'android'
                    when 2
                        then 'ios'
                    when 3
                        then 'miniapp'
                    when 4
                        then 'pcweb'
                    else 'other'
                    end
                    as order_from,
                order_state,
                create_date,
                finnshed_time,
                is_settlement,
                is_delete,
                evaluation_state,
                way,
                is_stock_up,
                create_user,
                create_time,
                update_user,
                update_time,
                is_valid,
                '9999-99-99' end_date,
          '2023-05-29' as start_date
         from yp_ods.t_shop_order
         where dt='2023-05-29'          --  增量采集信息

         union all

    -- 2、历史拉链表数据，并根据up_id判断更新end_time有效期
         select
             fso.id,
             fso.order_num,
             fso.buyer_id,
             fso.store_id,
             fso.order_from,
             fso.order_state,
             fso.create_date,
             fso.finnshed_time,
             fso.is_settlement,
             fso.is_delete,
             fso.evaluation_state,
             fso.way,
             fso.is_stock_up,
             fso.create_user,
             fso.create_time,
             fso.update_user,
             fso.update_time,
             fso.is_valid,
             --3、更新end_time：如果没有匹配到变更数据，或者当前已经是无效的历史数据，则保留原始end_time过期时间；否则变更end_time时间为前天（昨天之前有效）
             if (tso.id is null or fso.end_date<'9999-12-31', fso.end_date, date_add(tso.dt, -1)) end_time,
             fso.start_date
         from yp_dwd.fact_shop_order fso left join (select * from yp_ods.t_shop_order where dt='2023-05-29') tso
         on fso.id=tso.id
     ) his
order by his.id, start_date;        -- (旧的拉链表 left join 增量采集信息)

-- 3.5 查询中间表的数据, 有结果则说明 拉链导入成功.
select * from fact_shop_order_tmp  where id = 'dd1910223851672f32';   -- 3157

-- 4. 用中间表, 全量覆盖旧的拉链表, 即为: 最终结果(新的拉链表)
INSERT OVERWRITE TABLE yp_dwd.fact_shop_order partition (start_date)
SELECT * from yp_dwd.fact_shop_order_tmp;

-- 5. 查看结果数据.
select * from yp_dwd.fact_shop_order;       -- 3155条  => 3157

-- 6. 验证数据.
select * from yp_dwd.fact_shop_order where id = 'dd1910223851672f32';
select * from yp_dwd.fact_shop_order where id = 'dd9999999999999999';

-- ------------------------------- DWD层, 案例3: 全量覆盖 -------------------------------
-- 1. 建表.
DROP TABLE if EXISTS yp_dwd.dim_district;
CREATE TABLE yp_dwd.dim_district(
  id string COMMENT '主键ID',
  code string COMMENT '区域编码',
  name string COMMENT '区域名称',
  pid string COMMENT '父级ID',
  alias string COMMENT '别名')
COMMENT '区域字典表'
row format delimited fields terminated by '\t'
stored as orc
tblproperties ('orc.compress' = 'SNAPPY');

-- 2. 全量覆盖.
insert overwrite table yp_dwd.dim_district
select * from yp_ods.t_district where code is not null and pid is not null;

-- 3. 查询表结果.
select * from yp_dwd.dim_district;

-- ------------------------------- DWD层, 案例4: 增量导入(增量导入, 仅新增) -------------------------------
-- 1. 建表.
DROP TABLE if EXISTS yp_dwd.fact_goods_evaluation;
CREATE TABLE yp_dwd.fact_goods_evaluation(
  id string,
  user_id string COMMENT '评论人id',
  store_id string COMMENT '店铺id',
  order_id string COMMENT '订单id',
  geval_scores int COMMENT '综合评分',
  geval_scores_speed int COMMENT '送货速度评分0-5分(配送评分)',
  geval_scores_service int COMMENT '服务评分0-5分',
  geval_isanony tinyint COMMENT '0-匿名评价，1-非匿名',
  create_user string,
  create_time string,
  update_user string,
  update_time string,
  is_valid tinyint COMMENT '0 ：失效，1 ：开启')
COMMENT '订单评价表'
partitioned by (dt string)      -- 分区字段
row format delimited fields terminated by '\t'
stored as orc tblproperties ('orc.compress' = 'SNAPPY');

-- 2. 增量导入(仅新增), 首次必定是: 全量导入.
insert into table yp_dwd.fact_goods_evaluation partition (dt)
select id,
       user_id,
       store_id,
       order_id,
       geval_scores,
       geval_scores_speed,
       geval_scores_service,
       geval_isanony,
       create_user,
       create_time,
       update_user,
       update_time,
       is_valid,
       substr(create_time, 1, 10) as dt     -- 按照数据的 添加日期(年月日)来进行分区.
from yp_ods.t_goods_evaluation;

-- 3. 循环(重复)做: 增量导入(仅新增).
insert into table yp_dwd.fact_goods_evaluation partition (dt)
select id,
       user_id,
       store_id,
       order_id,
       geval_scores,
       geval_scores_speed,
       geval_scores_service,
       geval_isanony,
       create_user,
       create_time,
       update_user,
       update_time,
       is_valid,
       substr(create_time, 1, 10) as dt     -- 按照数据的 添加日期(年月日)来进行分区.
from yp_ods.t_goods_evaluation where create_time between '2023-05-28 00:00:00' and '2023-05-28 23:59:59';

-- 4. 查看结果.
select * from yp_dwd.fact_goods_evaluation;


-- ------------------------------- DWD层, 案例5: 完整实现 -------------------------------
-- 名词解释: FT: fact table 事实表,  DT: dimension table 维度表.

-- 细节:
-- 1. 记得把set这一坨设置放到 insert-**.sql 文件中, 设置参数后, 再插入数据.
-- 2. 记得修改方言为 hive.

-- 如下是执行步骤:
-- 1. 执行 02_DWD\DT\create_dim_table.sql脚本, 创建所有的维度表.
-- 2. 执行 02_DWD\DT\insert-dim.sql脚本, 给维度表添加数据.

-- 3. 执行 02_DWD\FT\create_fact_table.sql脚本, 创建所有的事实表.
-- 4. 执行 02_DWD\FT\insert-fact.sql脚本, 给事实表添加数据.


-- --------------------------------------------- 如下是和 DWB层 有关的内容 ---------------------------------------------
/*
DWB层介绍:
    概述:
        Data Warehouse Base, 数仓基础层, 也叫数仓中间层(Middle).
    作用/目的:
        降维, 形成宽表, 即: 把我们要用的字段从多张表抽取出来, 放到一张表中.
    好处:
        1. 方便查询, 提高查询效率.   单表效率 > 多表效率
        2. 开发难度相对降低.
    弊端:
        存在一定程度上的数据冗余, 且已经不符合3范式的要求了.
    建宽表原则:
        宁滥勿缺.
    细节:
        1. 因为我们亿品新零售项目要做3个主题的研发, 分别是: 销售主题, 商品主题, 用户主题.
        2. 基于上述的3个主题, 我们搞出来了3张宽表:
            订单宽表    yp_dwb.dwb_order_detail         难点在于: 表多
            店铺宽表    yp_dwb.dwb_shop_detail          难点在于: (区域表)自关联查询
            商品宽表    yp_dwb.dwb_goods_detail         难点在于: join错乱
*/
-- 1. 建模(建库)
create database if not exists yp_dwb;
-- 2. 切库, 查表.
use yp_dwb;
show tables;

-- ----------------------------------- 案例1: 订单宽表    yp_dwb.dwb_order_detail         难点在于: 表多 -----------------------------------
-- 1. 建表, 订单宽表的字段来源于 yp_dwd(数仓明细层)的各张和 订单相关的表.
CREATE TABLE yp_dwb.dwb_order_detail(
--订单主表
  order_id string COMMENT '根据一定规则生成的订单编号',
  order_num string COMMENT '订单序号',
  buyer_id string COMMENT '买家的userId',
  store_id string COMMENT '店铺的id',
  order_from string COMMENT '渠道类型：android、ios、miniapp、pcweb、other',
  order_state int COMMENT '订单状态:1.已下单\; 2.已付款, 3. 已确认 \;4.配送\; 5.已完成\; 6.退款\;7.已取消',
  create_date string COMMENT '下单时间',
  finnshed_time timestamp COMMENT '订单完成时间,当配送员点击确认送达时,进行更新订单完成时间,后期需要根据订单完成时间,进行自动收货以及自动评价',
  is_settlement tinyint COMMENT '是否结算\;0.待结算订单\; 1.已结算订单\;',
  is_delete tinyint COMMENT '订单评价的状态:0.未删除\;  1.已删除\;(默认0)',
  evaluation_state tinyint COMMENT '订单评价的状态:0.未评价\;  1.已评价\;(默认0)',
  way string COMMENT '取货方式:SELF自提\;SHOP店铺负责配送',
  is_stock_up int COMMENT '是否需要备货 0：不需要    1：需要    2:平台确认备货  3:已完成备货 4平台已经将货物送至店铺 ',
--  订单副表
  order_amount decimal(36,2) COMMENT '订单总金额:购买总金额-优惠金额',
  discount_amount decimal(36,2) COMMENT '优惠金额',
  goods_amount decimal(36,2) COMMENT '用户购买的商品的总金额+运费',
  is_delivery string COMMENT '0.自提；1.配送',
  buyer_notes string COMMENT '买家备注留言',
  pay_time string,
  receive_time string,
  delivery_begin_time string,
  arrive_store_time string,
  arrive_time string COMMENT '订单完成时间,当配送员点击确认送达时,进行更新订单完成时间,后期需要根据订单完成时间,进行自动收货以及自动评价',
  create_user string,
  create_time string,
  update_user string,
  update_time string,
  is_valid tinyint COMMENT '是否有效  0: false\; 1: true\;   订单是否有效的标志',
--  订单组
  group_id string COMMENT '订单分组id',
  is_pay tinyint COMMENT '订单组是否已支付,0未支付,1已支付',
--  订单组支付
  group_pay_amount decimal(36,2) COMMENT '订单总金额\;',
--  退款单
  refund_id string COMMENT '退款单号',
  apply_date string COMMENT '用户申请退款的时间',
  refund_reason string COMMENT '买家退款原因',
  refund_amount decimal(36,2) COMMENT '订单退款的金额',
  refund_state tinyint COMMENT '1.申请退款\;2.拒绝退款\; 3.同意退款,配送员配送\; 4:商家同意退款,用户亲自送货 \;5.退款完成',
--  结算
  settle_id string COMMENT '结算单号',
  settlement_amount decimal(36,2) COMMENT '如果发生退款,则结算的金额 = 订单的总金额 - 退款的金额',
  dispatcher_user_id string COMMENT '配送员id',
  dispatcher_money decimal(36,2) COMMENT '配送员的配送费(配送员的运费(如果退货方式为1:则买家支付配送费))',
  circle_master_user_id string COMMENT '圈主id',
  circle_master_money decimal(36,2) COMMENT '圈主分润的金额',
  plat_fee decimal(36,2) COMMENT '平台应得的分润',
  store_money decimal(36,2) COMMENT '商家应得的订单金额',
  status tinyint COMMENT '0.待结算；1.待审核 \; 2.完成结算；3.拒绝结算',
  settle_time string COMMENT ' 结算时间',
-- 订单评价
  evaluation_id string,
  evaluation_user_id string COMMENT '评论人id',
  geval_scores int COMMENT '综合评分',
  geval_scores_speed int COMMENT '送货速度评分0-5分(配送评分)',
  geval_scores_service int COMMENT '服务评分0-5分',
  geval_isanony tinyint COMMENT '0-匿名评价，1-非匿名',
  evaluation_time string,
-- 订单配送
  delievery_id string COMMENT '主键id',
  dispatcher_order_state tinyint COMMENT '配送订单状态:0.待接单.1.已接单,2.已到店.3.配送中 4.商家普通提货码完成订单.5.商家万能提货码完成订单。6，买家完成订单',
  delivery_fee decimal(36,2) COMMENT '配送员的运费',
  distance int COMMENT '配送距离',
  dispatcher_code string COMMENT '收货码',
  receiver_name string COMMENT '收货人姓名',
  receiver_phone string COMMENT '收货人电话',
  sender_name string COMMENT '发货人姓名',
  sender_phone string COMMENT '发货人电话',
  delievery_create_time string,
-- 商品快照
  order_goods_id string COMMENT '--商品快照id',
  goods_id string COMMENT '购买商品的id',
  buy_num int COMMENT '购买商品的数量',
  goods_price decimal(36,2) COMMENT '购买商品的价格',
  total_price decimal(36,2) COMMENT '购买商品的价格 = 商品的数量 * 商品的单价 ',
  goods_name string COMMENT '商品的名称',
  goods_specification string COMMENT '商品规格',
  goods_type string COMMENT '商品分类     ytgj:进口商品    ytsc:普通商品     hots爆品',
  goods_brokerage decimal(36,2) COMMENT '商家设置的商品分润的金额',
  is_goods_refund tinyint COMMENT '0.不退款\; 1.退款'
)
COMMENT '订单明细表'
PARTITIONED BY(dt STRING)
row format delimited fields terminated by '\t'
stored as orc
tblproperties ('orc.compress' = 'SNAPPY');

-- 2. 往表中添加数据.
/*
    insert into table yp_dwb.dwb_order_detail partition (dt)
    select
        -- 来源于 9张表的字段
        '2023-05-29' as dt
    from
        yp_dwd.fact_shop_order o
            left join yp_dwd.fact_shop_order_address_detail ad on o.id = ad.id and ad.end_date='9999-99-99'
            left join ....    其它的表
*/

-- 3. 查询表数据.
select * from yp_dwb.dwb_order_detail;      -- 4623条


-- ----------------------------------- 案例2: 店铺宽表    yp_dwb.dwb_shop_detail          难点在于: (区域表)自关联查询 -----------------------------------
-- 1. 建表, 店铺信息不是急剧变化, 所以无需 分区表.
CREATE TABLE yp_dwb.dwb_shop_detail(
--  店铺
  id string,
  address_info string COMMENT '店铺详细地址',
  store_name string COMMENT '店铺名称',
  is_pay_bond tinyint COMMENT '是否有交过保证金 1：是0：否',
  trade_area_id string COMMENT '归属商圈ID',
  delivery_method tinyint COMMENT '配送方式  1 ：自提 ；3 ：自提加配送均可\; 2 : 商家配送',
  store_type int COMMENT '店铺类型 22天街网店 23实体店 24直营店铺 33会员专区店',
  is_primary tinyint COMMENT '是否是总店 1: 是 2: 不是',
  parent_store_id string COMMENT '父级店铺的id，只有当is_primary类型为2时有效',
--  商圈
  trade_area_name string COMMENT '商圈名称',
--  区域-店铺
  province_id string COMMENT '店铺所在省份ID',
  city_id string COMMENT '店铺所在城市ID',
  area_id string COMMENT '店铺所在县ID',
  province_name string COMMENT '省份名称',
  city_name string COMMENT '城市名称',
  area_name string COMMENT '县名称'
  )
COMMENT '店铺明细表'
row format delimited fields terminated by '\t'
stored as orc tblproperties ('orc.compress' = 'SNAPPY');

-- 2. 关于 行政区域表 dim_district的自关联查询, 获取省市区的所有信息.
select * from yp_dwd.dim_district;

-- 设计思路1: 一张表存完, 弊端: 冗余度太高了.
-- 省份id  省份名 城市id  城市名  县区id  县区名
-- 410000 河南省 410100 郑州市  410101  金水区
-- 410000 河南省 410100 郑州市  410102  二七区
-- 410000 河南省 410100 郑州市  410103  航空港区

-- 设计思路2: 三张表存, 弊端: 查询效率相对较低, 多表查询.
-- 省份id  省份名 父级id
-- 410000 河南省  0

-- 城市id  城市名 父级id
-- 410100 郑州市 410000

-- 县区id  县区名 父级id
-- 410101 金水区 410101

-- 设计思路3: 一张表实现所有.
-- 自身id  自身名称 父级id
-- 410000 河南省   0
-- 410100 郑州市   410000
-- 410101 金水区   410100
-- 410102 二七区   410100
-- 410103 航空港区 410100
select
    t3.code, t3.name,       -- 省的编号, 名字
    t2.code, t2.name,       -- 市的编号, 名字
    t1.code, t1.name        -- 县区的编号, 名字
from yp_dwd.dim_district t1                                 -- t1 县区
    left join yp_dwd.dim_district t2 on t1.pid = t2.id      -- t2 市
    left join yp_dwd.dim_district t3 on t2.pid = t3.id;     -- t3 省

-- 3. 添加表数据.
insert into table yp_dwb.dwb_shop_detail
select
-- 来源于: 店铺表.
    s.id,
    s.address_info,
    s.name as store_name,
    s.is_pay_bond,
    s.trade_area_id,
    s.delivery_method,
    s.store_type,
    s.is_primary,
    s.parent_store_id,
-- 来源于: 商圈表
    ta.name as trade_area_name,
-- 来源于: 区域-店铺
    t3.id as province_id,
    t2.id as city_id,
    t1.id as area_id,
    t3.name as province_name,
    t2.name as city_name,
    t1.name as area_name
from
    yp_dwd.dim_store s      -- 店铺表.
        left join yp_dwd.dim_trade_area ta on s.id = ta.id and ta.end_date = '9999-99-99'  -- 商圈表
        left join yp_dwd.dim_location lc on lc.correlation_id = s.id and lc.type = 2 and lc.end_date='9999-99-99'      -- 地址信息表(不仅有商圈地址, 还有店铺地址, 用户地址, 买家地址)
        left join yp_dwd.dim_district t1 on t1.code = lc.adcode        -- t1: 县区
        left join yp_dwd.dim_district t2 on t1.pid = t2.code           -- t2: 市
        left join yp_dwd.dim_district t3 on t2.pid = t3.code           -- t3: 省
where s.end_date='9999-99-99';


-- 4. 查询表数据.
select * from yp_dwb.dwb_shop_detail;


-- ----------------------------------- 案例3: 商品宽表    yp_dwb.dwb_goods_detail         难点在于: join错乱 -----------------------------------
-- 1. 建表.
CREATE TABLE yp_dwb.dwb_goods_detail(
  id string,
  store_id string COMMENT '所属商店ID',
  class_id string COMMENT '分类id:只保存最后一层分类id',
  store_class_id string COMMENT '店铺分类id',
  brand_id string COMMENT '品牌id',
  goods_name string COMMENT '商品名称',
  goods_specification string COMMENT '商品规格',
  search_name string COMMENT '模糊搜索名称字段:名称_+真实名称',
  goods_sort int COMMENT '商品排序',
  goods_market_price decimal(36,2) COMMENT '商品市场价',
  goods_price decimal(36,2) COMMENT '商品销售价格(原价)',
  goods_promotion_price decimal(36,2) COMMENT '商品促销价格(售价)',
  goods_storage int COMMENT '商品库存',
  goods_limit_num int COMMENT '购买限制数量',
  goods_unit string COMMENT '计量单位',
  goods_state tinyint COMMENT '商品状态 1正常，2下架,3违规（禁售）',
  goods_verify tinyint COMMENT '商品审核状态: 1通过，2未通过，3审核中',
  activity_type tinyint COMMENT '活动类型:0无活动1促销2秒杀3折扣',
  discount int COMMENT '商品折扣(%)',
  seckill_begin_time string COMMENT '秒杀开始时间',
  seckill_end_time string COMMENT '秒杀结束时间',
  seckill_total_pay_num int COMMENT '已秒杀数量',
  seckill_total_num int COMMENT '秒杀总数限制',
  seckill_price decimal(36,2) COMMENT '秒杀价格',
  top_it tinyint COMMENT '商品置顶：1-是，0-否',
  create_user string,
  create_time string,
  update_user string,
  update_time string,
  is_valid tinyint COMMENT '0 ：失效，1 ：开启',
--  商品小类
  min_class_id string COMMENT '分类id:只保存最后一层分类id',
  min_class_name string COMMENT '店铺内分类名字',
--  商品中类
  mid_class_id string COMMENT '分类id:只保存最后一层分类id',
  mid_class_name string COMMENT '店铺内分类名字',
--  商品大类
  max_class_id string COMMENT '分类id:只保存最后一层分类id',
  max_class_name string COMMENT '店铺内分类名字',
--  品牌
  brand_name string COMMENT '品牌名称'
  )
COMMENT '商品明细表'
row format delimited fields terminated by '\t'
stored as orc tblproperties ('orc.compress' = 'SNAPPY');

-- 2. 演示join错乱的问题, 我们管理商品是通过 3级标题法来管理的, 即: 大类管理中类, 中类管理小类.  这里: 1(大类), 2(中类), 3(小类)
-- 问题: 我们要的数据, level应该都是3(小类), 然后自关联即可找到对应的中类, 对应的大类. 但是我们发现, 分类表中, 小类, 中类, 大类都有.
-- 如何解决? 在自关联查询数据的时候, 做筛选判断即可.
select * from yp_dwd.dim_goods_class;       -- 407条, level(有1, 有2, 有3), join错乱.
select level, count(id) as total_cnt from yp_dwd.dim_goods_class group by level;     -- 24.3s

-- 3. 添加表数据.
insert into table yp_dwb.dwb_goods_detail
select
-- 字段来源于: yp_dwd.dim_goods表
    goods.id,
    goods.store_id,
    goods.class_id,
    goods.store_class_id,
    goods.brand_id,
    goods.goods_name,
    goods.goods_specification,
    goods.search_name,
    goods.goods_sort,
    goods.goods_market_price,
    goods.goods_price,
    goods.goods_promotion_price,
    goods.goods_storage,
    goods.goods_limit_num,
    goods.goods_unit,
    goods.goods_state,
    goods.goods_verify,
    goods.activity_type,
    goods.discount,
    goods.seckill_begin_time,
    goods.seckill_end_time,
    goods.seckill_total_pay_num,
    goods.seckill_total_num,
    goods.seckill_price,
    goods.top_it,
    goods.create_user,
    goods.create_time,
    goods.update_user,
    goods.update_time,
    goods.is_valid,
-- 商品分类信息, 来源于: yp_dwd.dim_goods_class 商品分类表, 但是存在join错乱的问题, 需要对level级别做判断.
-- 小类判断, 只有:  class1的 level = 3 才有意义, 因为小类 => 中类 => 大类
    case
        when class1.level = 3 then class1.id else null
    end as min_class_id,

    case
        when class1.level = 3 then class1.name else null
    end as min_class_name,

-- 中类判断, 只有:  class1(小类), class2(中类)的 level = 2 才有意义, 因为 中类 => 大类
    case
        when class1.level = 2 then class1.id
        when class2.level = 2 then class2.id
        else null
    end as mid_class_id,

    case
        when class1.level = 2 then class1.name
        when class2.level = 2 then class2.name
        else null
    end as mid_class_name,

-- 大类判断, 只有:  class1(小类), class2(中类), class3(大类)的 level = 1 才有意义.
    case
        when class1.level = 1 then class1.id
        when class2.level = 1 then class2.id
        when class3.level = 1 then class3.id
        else null
    end as max_class_id,

    case
        when class1.level = 1 then class1.name
        when class2.level = 1 then class2.name
        when class3.level = 1 then class3.name
        else null
    end as max_class_name,

-- 品牌名, 来源于: yp_dwd.dim_brand 品牌表
    brand.brand_name
from
-- 商品Sku表
    yp_dwd.dim_goods goods
-- 商品分类表
    left join yp_dwd.dim_goods_class class1 on goods.store_class_id = class1.id and class1.end_date='9999-99-99'    -- 充当小类(3), 类似于: 县区
    left join yp_dwd.dim_goods_class class2 on class1.parent_id = class2.id and class2.end_date='9999-99-99'        -- 充当中类(2), 类似于: 市
    left join yp_dwd.dim_goods_class class3 on class2.parent_id = class3.id and class3.end_date='9999-99-99'        -- 充当大类(1), 类似于: 省
-- 品牌表
    left join yp_dwd.dim_brand brand on goods.brand_id = brand.id and brand.end_date='9999-99-99'
where goods.end_date='9999-99-99';

-- 4. 查询表数据.
select * from yp_dwb.dwb_goods_detail;      -- 2453条

