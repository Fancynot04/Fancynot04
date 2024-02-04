-- DM层解释: 全称叫Data Mark(数据集市层), 主要是基于 DWS层的数据, 上卷出 年, 月, 周, 日的统计.        -- 该项目中对于DM的使用.
-- DM官方解释: 实际开发中, DM层指的是 某个业务线, 或者某个工作组的数据.
-- 面试题: 数仓 和 数据集市的区别?
-- 答案: 数仓是整个集团(公司)所有的业务, 数据集市是某个业务线 或者 某个工作组所需数据.

-- 小细节: 我们的新零售项目, yp_dws层做的是细粒度统计(天),  yp_dm层做的是粗粒度汇总(年月周), 这样设计的目的是为了方便大家学习.
-- 实际开发中, 如果是类似于我们的这种需求, 可以不写dm层, 而是直接在 yp_dws层中完成所有统计.

-- 1. 创建dm层.
create database if not exists yp_dm;
-- 2. 切库, 查看数据表.
use yp_dm;
show tables;

-- ------------------------------------ 案例1: DM层 销售主题统计宽表(年统计) ------------------------------------
-- 1. 建表, 销售主题统计宽表, 基于: DWS层的 销售主题日统计宽表 上卷出 销售主题 年月周日 统计宽表.
-- 指标: 16个指标    维度: 32种组合情况(年 + 8种), (月 + 8种), (周 + 8种), (日 + 8种)
-- 格式
create table yp_dm.dm_sale(
    -- 新增的维度字段(时间相关)
    dt string,      -- 统计时间, 无意义, 例如: 2023-06-04
    year_code string,   -- 年    2023
    year_month string,  -- 年月  2023-06
    month_code string,  -- 月    06
    day_month_num string, -- 月中的第几天, 4
    dim_date_id string,   -- 具体的日期, 2023-06-04
    week_year_num string, -- 年的第几周

    -- time_type, 日期类型, date, week, month, year
    time_type string,

    -- 分组类型, store, trade_area, city, brand, maxclass, midclass, minclass, all
    group_type string,


    -- 14个维度字段.  城市, 商圈, 店铺, 品牌, 大类, 中类, 小类 => id, name
    city_id string,
    city_name string,

    --16个指标
    sal_amount decimal(38, 2)
) comment ''
row format delimited fields terminated by '\t'
stored as orc tblproperties ('orc.compress'='snappy');

-- 具体的建表语法.
CREATE TABLE yp_dm.dm_sale(
   date_time string COMMENT '统计日期,不能用来分组统计',
   time_type string COMMENT '统计时间维度：year、month、week、date(就是天day)',
   year_code string COMMENT '年code',
   year_month string COMMENT '年月',
   month_code string COMMENT '月份编码',
   day_month_num string COMMENT '一月第几天',
   dim_date_id string COMMENT '日期',
   year_week_name_cn string COMMENT '年中第几周',

   group_type string COMMENT '分组类型：store，trade_area，city，brand，min_class，mid_class，max_class，all',
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
   --   =======统计=======
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
COMMENT '销售主题宽表'
ROW format delimited fields terminated BY '\t'
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');

-- 2. 插入表数据到上述的表中.  Presto中完成.

-- 3. 查询表数据.  Presto中完成.


-- ------------------------------------ 案例2: DM层 销售主题统计宽表(年月周日) ------------------------------------
-- 代码详见Presto中的写法.

-- ------------------------------------ 案例3: DM层 商品主题统计宽表(总累计, 近30天累计) ------------------------------------
-- 商品主题统计宽表, 我们不再计算每天的了, 而是改为: 周期性进行统计, 这里统计的是: 总累计量, 近30天累计.
-- 1. 建表.
create table yp_dm.dm_sku(
    dt                    string,
    sku_id                string comment 'sku_id',

    last_30d_order_count           bigint comment '被下单次数',
    last_30d_order_num             bigint comment '被下单件数',
    last_30d_order_amount          decimal(38, 2) comment '被下单金额',

    order_count           bigint comment '被下单次数 总累计',
    order_num             bigint comment '被下单件数 总累计',
    order_amount          decimal(38, 2) comment '被下单金额 总累计',


    last_30d_payment_count         bigint comment '被支付次数',
    last_30d_payment_num           bigint comment '被支付件数',
    last_30d_payment_amount        decimal(38, 2) comment '被支付金额',

    payment_count         bigint comment '被支付次数',
    payment_num           bigint comment '被支付件数',
    payment_amount        decimal(38, 2) comment '被支付金额',
    refund_count          bigint comment '被退款次数',
    refund_num            bigint comment '被退款件数',
    refund_amount         decimal(38, 2) comment '被退款金额',
    cart_count            bigint comment '被加入购物车次数',
    cart_num              bigint comment '被加入购物车件数',
    favor_count           bigint comment '被收藏次数',
    evaluation_good_count bigint comment '好评数',
    evaluation_mid_count  bigint comment '中评数',
    evaluation_bad_count  bigint comment '差评数'
) comment '商品主题宽表'
row format delimited fields terminated by '\t'
stored as orc tblproperties ('orc.compress'='snappy');


-- 2. 插入表数据.

-- 3. 查询表数据.

create table yp_dm.dm_sku           -- yp_dm层的 商品主题统计宽表.
(
    sku_id string comment 'sku_id',
    order_last_30d_count bigint comment '最近30日被下单次数',
    order_last_30d_num bigint comment '最近30日被下单件数',
    order_last_30d_amount decimal(38,2)  comment '最近30日被下单金额',
    order_count bigint comment '累积被下单次数',
    order_num bigint comment '累积被下单件数',
    order_amount decimal(38,2) comment '累积被下单金额',
    payment_last_30d_count   bigint  comment '最近30日被支付次数',
    payment_last_30d_num bigint comment '最近30日被支付件数',
    payment_last_30d_amount  decimal(38,2) comment '最近30日被支付金额',
    payment_count   bigint  comment '累积被支付次数',
    payment_num bigint comment '累积被支付件数',
    payment_amount  decimal(38,2) comment '累积被支付金额',
    refund_last_30d_count bigint comment '最近三十日退款次数',
    refund_last_30d_num bigint comment '最近三十日退款件数',
    refund_last_30d_amount decimal(38,2) comment '最近三十日退款金额',
    refund_count bigint comment '累积退款次数',
    refund_num bigint comment '累积退款件数',
    refund_amount decimal(38,2) comment '累积退款金额',
    cart_last_30d_count bigint comment '最近30日被加入购物车次数',
    cart_last_30d_num bigint comment '最近30日被加入购物车件数',
    cart_count bigint comment '累积被加入购物车次数',
    cart_num bigint comment '累积被加入购物车件数',
    favor_last_30d_count bigint comment '最近30日被收藏次数',
    favor_count bigint comment '累积被收藏次数',
    evaluation_last_30d_good_count bigint comment '最近30日好评数',
    evaluation_last_30d_mid_count bigint comment '最近30日中评数',
    evaluation_last_30d_bad_count bigint comment '最近30日差评数',
    evaluation_good_count bigint comment '累积好评数',
    evaluation_mid_count bigint comment '累积中评数',
    evaluation_bad_count bigint comment '累积差评数'
)
COMMENT '商品主题宽表'
ROW format delimited fields terminated BY '\t'
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');



create table yp_dm.dm_sku_tmp           -- yp_dm层的 商品主题统计宽表_临时表, 用于记录: 循环计算的 总累计 和 近30天累计.
(
    sku_id string comment 'sku_id',
    order_last_30d_count bigint comment '最近30日被下单次数',
    order_last_30d_num bigint comment '最近30日被下单件数',
    order_last_30d_amount decimal(38,2)  comment '最近30日被下单金额',
    order_count bigint comment '累积被下单次数',
    order_num bigint comment '累积被下单件数',
    order_amount decimal(38,2) comment '累积被下单金额',
    payment_last_30d_count   bigint  comment '最近30日被支付次数',
    payment_last_30d_num bigint comment '最近30日被支付件数',
    payment_last_30d_amount  decimal(38,2) comment '最近30日被支付金额',
    payment_count   bigint  comment '累积被支付次数',
    payment_num bigint comment '累积被支付件数',
    payment_amount  decimal(38,2) comment '累积被支付金额',
    refund_last_30d_count bigint comment '最近三十日退款次数',
    refund_last_30d_num bigint comment '最近三十日退款件数',
    refund_last_30d_amount decimal(38,2) comment '最近三十日退款金额',
    refund_count bigint comment '累积退款次数',
    refund_num bigint comment '累积退款件数',
    refund_amount decimal(38,2) comment '累积退款金额',
    cart_last_30d_count bigint comment '最近30日被加入购物车次数',
    cart_last_30d_num bigint comment '最近30日被加入购物车件数',
    cart_count bigint comment '累积被加入购物车次数',
    cart_num bigint comment '累积被加入购物车件数',
    favor_last_30d_count bigint comment '最近30日被收藏次数',
    favor_count bigint comment '累积被收藏次数',
    evaluation_last_30d_good_count bigint comment '最近30日好评数',
    evaluation_last_30d_mid_count bigint comment '最近30日中评数',
    evaluation_last_30d_bad_count bigint comment '最近30日差评数',
    evaluation_good_count bigint comment '累积好评数',
    evaluation_mid_count bigint comment '累积中评数',
    evaluation_bad_count bigint comment '累积差评数'
)
COMMENT '商品主题宽表'
ROW format delimited fields terminated BY '\t'
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');


-- ------------------------------------ 案例4: DM层 用户主题统计宽表(总累计, 近30天累计) ------------------------------------
-- 思路: 同商品主题一致, 这里不做过多赘述了.
create table yp_dm.dm_user
(
   date_time string COMMENT '统计日期',
    user_id string  comment '用户id',
--    登录
    login_date_first string  comment '首次登录时间',
    login_date_last string  comment '末次登录时间',
    login_count bigint comment '累积登录天数',
    login_last_30d_count bigint comment '最近30日登录天数',

--     store_collect_date_first bigint comment '首次店铺收藏时间',
--     store_collect_date_last bigint comment '末次店铺收藏时间',
--     store_collect_count bigint comment '累积店铺收藏数量',
--     store_collect_last_30d_count bigint comment '最近30日店铺收藏数量',
--
--     goods_collect_date_first bigint comment '首次商品收藏时间',
--     goods_collect_date_last bigint comment '末次商品收藏时间',
--     goods_collect_count bigint comment '累积商品收藏数量',
--     goods_collect_last_30d_count bigint comment '最近30日商品收藏数量',

    --购物车
    cart_date_first string comment '首次加入购物车时间',
    cart_date_last string comment '末次加入购物车时间',
    cart_count bigint comment '累积加入购物车次数',
    cart_amount decimal(38,2) comment '累积加入购物车金额',
    cart_last_30d_count bigint comment '最近30日加入购物车次数',
    cart_last_30d_amount decimal(38,2) comment '最近30日加入购物车金额',
   --订单
    order_date_first string  comment '首次下单时间',
    order_date_last string  comment '末次下单时间',
    order_count bigint comment '累积下单次数',
    order_amount decimal(38,2) comment '累积下单金额',
    order_last_30d_count bigint comment '最近30日下单次数',
    order_last_30d_amount decimal(38,2) comment '最近30日下单金额',
   --支付
    payment_date_first string  comment '首次支付时间',
    payment_date_last string  comment '末次支付时间',
    payment_count bigint comment '累积支付次数',
    payment_amount decimal(38,2) comment '累积支付金额',
    payment_last_30d_count bigint comment '最近30日支付次数',
    payment_last_30d_amount decimal(38,2) comment '最近30日支付金额'
)
COMMENT '用户主题宽表'
ROW format delimited fields terminated BY '\t'
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');
