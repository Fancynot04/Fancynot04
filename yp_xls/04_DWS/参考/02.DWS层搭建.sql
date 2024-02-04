-- --------------------------------- 案例1: 演示Hive的 聚合函数增强 grouping sets()函数 ---------------------------------
-- 1. 切库, 查看所有数据表.
use test;
show tables;
-- 2. 建表, 上传源文件(表数据).
create table test.t_cookie(
    month string,       -- 月
    day string,         -- 天
    cookieid string)    -- 用户id
row format delimited fields terminated by ',';

-- 3. 查询表数据.
select * from t_cookie;

-- 4. 至此, 准备动作完成, 接下来是我们的需求.
-- 需求: 分别按照 月（month）、天（day）、月和天（month,day）统计 来访用户cookieid个数，并获取三者的结果集（一起插入到目标宽表中）。
-- 即, 目标表结果为: month   day   cnt_nums

-- 思路1: group by 分组实现(逐个实现), 传统方式, 代码繁琐, 且写的语句较多, 执行速度慢.
-- 4.1 按照 月 分组, 统计 来访用户cookieid个数
select month, count(cookieid) cnt_nums from t_cookie group by month;
-- 4.2 按照 天 分组, 统计 来访用户cookieid个数
select day, count(cookieid) cnt_nums from t_cookie group by day;
-- 4.3 按照 月,天 分组, 统计 来访用户cookieid个数
select month, day, count(cookieid) cnt_nums from t_cookie group by month, day;
-- 4.4 上述的SQL虽然初步达到了效果, 但是没有达到最终效果, 即, 要把上述的三张结果表, 合成1张表, 所以使用 union all 来做.
select month, null as day, count(cookieid) cnt_nums from t_cookie group by month             -- 按月统计
union all
select null as month, day, count(cookieid) cnt_nums from t_cookie group by day                 -- 按天统计
union all
select month, day, count(cookieid) cnt_nums from t_cookie group by month, day;   -- 按 月和天 统计    运行了 63 秒

-- 思路2: grouping sets 聚合函数增强 实现, 代码简洁, 且效率高.
select month, day, count(cookieid) cnt_nums from t_cookie
group by month, day     -- 按照月, 天进行分组.
grouping sets
    (month, day, (month, day));     -- 具体的3种维度: 月, 天, 月和天       运行了 18 秒


-- --------------------------------- 案例2: 演示Hive的 聚合函数增强 cube() 和 rollup()函数 ---------------------------------
-- cube()函数: 实现任意维度的组合, 即: 你传入3个维度, 则等价于 2 ^ 3 = 8种组合, cube()也叫任意维度组合.
select month, day, count(cookieid) cnt_nums from t_cookie
group by
cube(month, day);

-- 上述的SQL等价于, 如下的SQL
select month, day, count(cookieid) cnt_nums from t_cookie
group by month, day
grouping sets
    ((), month, day, (month, day));     -- 空, 月, 天, 月和天, 共计 4 种维度.


-- rollup()函数, 从右往左 依次递减, 例如: rollup(a, b, c) 等价于 grouping sets((a,b,c), (a,b), (a), ())
select month, day, count(cookieid) cnt_nums from t_cookie
group by
rollup(month, day);

-- 上述的SQL等价于, 如下的SQL
select month, day, count(cookieid) cnt_nums from t_cookie
group by month, day
grouping sets
    ((month, day), month, ());     -- 月和天, 月, 空 共计 3 种维度.

-- --------------------------------- 案例3: 演示Hive的 聚合函数增强 grouping()函数 ---------------------------------
-- grouping()函数: 用来判断当前的结果属于哪种维度组合的, 规则: 0代表有, 1代表没有, 且这里的0和1是二进制的, 如果是多维度, 最终结果会显示成对应的 十进制结果.
-- 例如: 二进制 10, 对应的十进制是 2,   二进制 11 对应的 十进制 3, 二进制 01对应的十进制 1.
/*
    二进制:    0   0   0   0   0   0   0   0
    十进制:    128 64  32  16  8   4   2   1
*/
-- 入门版
select month,
       day,
       count(cookieid),
       grouping(month)      as m,
       grouping(day)        as d,
       grouping(month, day) as m_d
from test.t_cookie
group by month, day
   grouping sets (month, day, (month, day));

-- 进阶版, 实际开发写法, 多维度组合判断.
select
       month, day,
       count(cookieid) cnt_nums,
       case grouping(month, day)
        when 0 then '月和天'       -- 00(二进制) => 0 + 0 = 0(十进制)
        when 1 then '月'          -- 01(二进制) => 0 + 1 = 1(十进制)
        when 2 then '天'          -- 10(二进制) => 2 + 0 = 2(十进制)
        when 3 then '空'          -- 11(二进制) => 2 + 1 = 3(十进制)
       end as dimension
from t_cookie
group by month, day
grouping sets
    ((), month, day, (month, day));     -- 4种维度, 空, 月, 天, 月和天



-- --------------------------------- 案例4: dws层, 建模 ---------------------------------
-- DWS全称叫: Data warehouse Service, 数仓服务层, 目的: 基于前边各层的数据, 按照主题进行划分, 获取各主题的 日统计宽表, 便于下一步做粗粒度汇总(年月周).
-- 1. 建库建表, 要通过 hive 来实现.
create database if not exists yp_dws;
-- 2. 切库.
use yp_dws;
-- 3. 查看数据表
show tables;

-- 4. 因为我们的亿品新零售项目, 主题有3个(分别是:销售主题, 商品主题, 用户主题), 所以我们也应该有 3 张表. 这里我们先构建DWS层的第1张表: 销售主题日统计宽表.
CREATE TABLE yp_dws.dws_sale_daycount(
  --维度, 一共 14个字段(7个维度, id + 姓名)
   city_id string COMMENT '城市id',
   city_name string COMMENT '城市name',
   trade_area_id string COMMENT '商圈id',
   trade_area_name string COMMENT '商圈名称',
   store_id string COMMENT '店铺的id',
   store_name string COMMENT '店铺名称',
   brand_id string COMMENT '品牌id',
   brand_name string COMMENT '品牌名称',
   max_class_id string COMMENT '商品大类id',
   max_class_name string COMMENT '大类名称',
   mid_class_id string COMMENT '中类id',
   mid_class_name string COMMENT '中类名称',
   min_class_id string COMMENT '小类id',
   min_class_name string COMMENT '小类名称',

   -- 标记着计算出来的数据, 具体属于哪种维度.
   group_type string COMMENT '分组类型：store，trade_area，city，brand，min_class，mid_class，max_class，all',

   --   =======日统计, 16个指标 = 16个字段 =======
   --   销售收入
   sale_amt DECIMAL(38,2) COMMENT '销售收入',
   --   平台收入
   plat_amt DECIMAL(38,2) COMMENT '平台收入',
   -- 配送成交额
   deliver_sale_amt DECIMAL(38,2) COMMENT '配送成交额',
   -- 小程序成交额
   mini_app_sale_amt DECIMAL(38,2) COMMENT '小程序成交额',
   -- 安卓APP成交额
   android_sale_amt DECIMAL(38,2) COMMENT '安卓APP成交额',
   --  苹果APP成交额
   ios_sale_amt DECIMAL(38,2) COMMENT '苹果APP成交额',
   -- PC商城成交额
   pcweb_sale_amt DECIMAL(38,2) COMMENT 'PC商城成交额',
   -- 成交单量
   order_cnt BIGINT COMMENT '成交单量',
   -- 参评单量
   eva_order_cnt BIGINT COMMENT '参评单量comment=>cmt',
   -- 差评单量
   bad_eva_order_cnt BIGINT COMMENT '差评单量negtive-comment=>ncmt',
   -- 配送成交单量
   deliver_order_cnt BIGINT COMMENT '配送单量',
   -- 退款单量
   refund_order_cnt BIGINT COMMENT '退款单量',
   -- 小程序成交单量
   miniapp_order_cnt BIGINT COMMENT '小程序成交单量',
   -- 安卓APP订单量
   android_order_cnt BIGINT COMMENT '安卓APP订单量',
   -- 苹果APP订单量
   ios_order_cnt BIGINT COMMENT '苹果APP订单量',
   -- PC商城成交单量
   pcweb_order_cnt BIGINT COMMENT 'PC商城成交单量'
)
COMMENT '销售主题日统计宽表'
PARTITIONED BY(dt STRING)       -- 既充当分区字段, 还充当 日期维度.
ROW format delimited fields terminated BY '\t'
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');

-- 5. 查询表数据.
select * from yp_dws.dws_sale_daycount;

-- --------------------------------- 案例5: 演示 DWS层 销售主题日统计宽表, 简化版(简单模型, 分组统计), 快速上手, 然后做DWS层. ---------------------------------
-- 1. 建表, 添加表数据(上传源文件), 在hive中创建
create table test.t_order_detail(
    oid string comment '订单ID',
    goods_id string comment '商品ID',
    o_price int comment '订单总金额',
    g_num int comment '商品数量',
    g_price int comment '商品单价',
    brand_id string comment '品牌ID',
    dt string comment '日期'
) comment '订单详情宽表_简易模型'
row format delimited fields terminated by ',';

-- 2. 查询表数据.
use test;
select * from test.t_order_detail;

-- 3. 需求: 指标：订单量、销售额,  维度：日期、日期+品牌      如下的代码, 在Presto中.
-- 3.1 思路1: 分解版.
-- 3.2 Step1: 统计每天的订单量、销售额.           维度: 日期,      指标: 订单量、销售额

-- 3.3 Step2: 统计每天每个品牌的订单量、销售额.    维度: 日期, 品牌  指标: 订单量、销售额

-- 3.4 把上述的结果合并到一起, 这里用union all思路写.

-- 4. 用 聚合函数增强 来优化上述的需求.



-- --------------------------------- 案例6: 演示 DWS层 销售主题日统计宽表, 简化版(复杂模型, 去重), 快速上手, 然后做DWS层. ---------------------------------
-- 1. 问题: 为什么要演示如何去重呢?  答案: 因为我们目前的表数据中, 有大量的数据是完全重复的, 如下.
-- 需求1: 根据订单id分组，找出订单商品数最多的
select order_id, count(order_id) order_cnt from yp_dwb.dwb_order_detail group by order_id order by order_cnt desc;

-- 需求2: 查找订单编号为 dd190227318021f41f 的订单数据.
select * from yp_dwb.dwb_order_detail where order_id = 'dd190227318021f41f';

-- 2. 如下是用来演示如何对数据进行去重操作的.
-- 建表, 上传源文件.
create table test.t_order_detail_dup(
    oid string comment '订单ID',
    goods_id string comment '商品ID',
    o_price int comment '订单总金额',
    g_num int comment '商品数量',
    g_price int comment '商品单价',
    brand_id string comment '品牌ID',
    dt string comment '日期'
) comment '订单详情宽表_复杂模型'
row format delimited fields terminated by ',';

-- 3. 查看表数据.
select * from test.t_order_detail_dup;

-- 4. 演示如何去重.  不能使用distinct, 不能使用group by, 因为它们会直接修改表结构, 表的数据格式等就变化了.
-- 小技巧: 根据谁(哪个字段)去重, 就根据谁(哪个字段)进行分组.
-- 4.1 需求1: 只以订单oid去重,  思路: row_number() + CRT表达式.
select *, row_number() over(partition by oid) id_rn from test.t_order_detail_dup;
-- 4.2 需求2:
-- 4.3 需求3:
-- 4.4 需求4: 把上述所有的去重条件, 合并到一起(写).
-- 上述的代码, 详见Presto中的代码.


-- --------------------------------- 案例7: 演示DWS层 销售主题日统计宽表搭建, 查询数据, 然后插入到表中  ---------------------------------
-- 该案例代码, 详见Presto.



-- --------------------------------- 案例8: 演示DWS层 商品主题日统计宽表搭建, 查询数据, 然后插入到表中  ---------------------------------
/*
细节:
    1. 建表语法需要在Hive中写, 因为Presto不支持.
    2. 给变量起名主要有四种规范, 大多数语言都支持前两种, 后两种, 部分语言支持, 部分语言不支持.
        规范1: 大驼峰命名法, 也叫 双峰驼命名法, 即: 每个单词的首字母都大写, 其它小写.
            例如: HelloWorld, MaxValue
        规范2: 小驼峰命名法, 也叫 单峰驼命名法, 即: 从第二个单词开始, 每个单词的首字母都大写, 其它小写.
            例如: helloWorld, maxValue, zhangSanAge
        规范3: 蛇形命名法, 单词间用下划线隔开.
            例如: max_value, min_value, hello_world, zhang_san_age
        规范4: 串行命名法, 单词间用中划线隔开.
            例如: max-value, min-value, zhang-san-age
    3. 分享1个好用的起名网站, 大家可以参考: https://unbug.github.io/codelf/
*/

-- 1. 创建 dws层 商品主题日统计宽表, 该表 15个指标, 1种维度组合(2个维度字段)
use yp_dws;
show tables;
create table yp_dws.dws_sku_daycount(
    -- 维度字段, 日期, 商品id, 商品名
    dt STRING,
    sku_id string comment 'sku_id',
    sku_name string comment '商品名称',
    -- 指标字段, 15个.
    order_count bigint comment '被下单次数',
    order_num bigint comment '被下单件数',
    order_amount decimal(38,2) comment '被下单金额',
    payment_count bigint  comment '被支付次数',
    payment_num bigint comment '被支付件数',
    payment_amount decimal(38,2) comment '被支付金额',
    refund_count bigint  comment '被退款次数',
    refund_num bigint comment '被退款件数',
    refund_amount  decimal(38,2) comment '被退款金额',
    cart_count bigint comment '被加入购物车次数',
    cart_num bigint comment '被加入购物车件数',
    favor_count bigint comment '被收藏次数',
    evaluation_good_count bigint comment '好评数',
    evaluation_mid_count bigint comment '中评数',
    evaluation_bad_count bigint comment '差评数'
) COMMENT '每日商品行为'
--PARTITIONED BY(dt STRING)
ROW format delimited fields terminated BY '\t'
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');

-- 2. 往表中插入数据.
-- 3. 查询表数据, 上述步骤详见Presto代码.
select * from yp_dws.dws_sku_daycount;

-- 2023-06-01 gid001 联想Y10000P 下单10次 下单30件 下单金额10W 被支付次数6次 支付21件.....
-- 2023-06-01 gid002 华为P100    下单10次 下单30件 下单金额10W 被支付次数6次 支付21件.....



-- --------------------------------- 案例9: 演示DWS层 用户主题日统计宽表搭建, 查询数据, 然后插入到表中  ---------------------------------
-- 1. 建表, 在hive中做.   维度: 用户, 日期.    指标: 9个
drop table if exists yp_dws.dws_user_daycount;
create table yp_dws.dws_user_daycount (
   user_id string comment '用户 id',
    login_count bigint comment '登录次数',
    store_collect_count bigint comment '店铺收藏数量',
    goods_collect_count bigint comment '商品收藏数量',
    cart_count bigint comment '加入购物车次数',
    cart_amount decimal(38,2) comment '加入购物车金额',
    order_count bigint comment '下单次数',
    order_amount    decimal(38,2)  comment '下单金额',
    payment_count   bigint      comment '支付次数',
    payment_amount  decimal(38,2) comment '支付金额'
) COMMENT '每日用户行为'
PARTITIONED BY(dt STRING)
ROW format delimited fields terminated BY '\t'
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');

-- 2. 添加表数据到上述的表中, 思路和 商品主题日统计宽表一模一样.

-- 3. 查询表数据(结果)

-- --------------------------------- 案例10: Hive的索引问题  ---------------------------------
/*
Hive的索引介绍:
    1. Hive从0.7X开始支持索引, 只不过功能较弱, 因为索引会额外存储到一张 索引表中(物理存储), 且需要手动更新维护, 还会额外开启1个新的MR程序.
    2. Hive从3.X开始移除了索引, 取而代之的是 物化视图(自动更新维护索引)  和 推荐使用列存储格式(例如:Orc, Parquet)自带的索引功能.
    3. 这里我们要重点讲解的是: Orc(列存储)格式自带的 行组索引 和 布隆过滤器索引.


*/


















