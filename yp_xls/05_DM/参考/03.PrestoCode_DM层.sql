-- ------------------------------------ 案例1: DM层 销售主题统计宽表(年统计) ------------------------------------
-- 1. 建表(在Hive中完成), 销售主题统计宽表, 基于: DWS层的 销售主题日统计宽表 上卷出 销售主题 年月周日 统计宽表.
-- 指标: 16个指标    维度: 32种组合情况(年 + 8种), (月 + 8种), (周 + 8种), (日 + 8种)

-- 2. 插入表数据到上述的表中.  Presto中完成.
-- step1: 梳理表关系,  销售主题日统计宽表 left join yp_dwd.dim_date 日期维度表
select
-- step5: 日期维度字段
    '2023-06-04' as date_time,
    d.year_code,    -- 年, 例如: 2023
-- step4: 除(日期维度)外, 其它所有的维度, 即: 城市, 商圈, 店铺, 品牌, 大类, 中类, 小类.
    -- 1个分组类型字段, 分组类型：store，trade_area，city，brand，min_class，mid_class，max_class，all
    case
        when grouping(store_id, store_name)=0 then 'store'                  -- 日期 + 城市 + 商圈 + 店铺
        when grouping(trade_area_id, trade_area_name)=0 then 'trade_area'   -- 日期 + 城市 + 商圈
        when grouping(city_id, city_name)=0 then 'city'                     -- 日期 + 城市
        when grouping(brand_id, brand_name)=0 then 'brand'                  -- 日期 + 品牌
        when grouping(min_class_id, min_class_name)=0 then 'min_class'      -- 日期 + 大类 + 中类 + 小类
        when grouping(mid_class_id, mid_class_name)=0 then 'mid_class'      -- 日期 + 大类 + 中类
        when grouping(max_class_id, max_class_name)=0 then 'max_class'      -- 日期 + 大类
        else 'all'                                                          -- 日期
    end as group_type,
    -- 14个维度字段
    city_id,
    city_name,
    trade_area_id,
    trade_area_name,
    store_id,
    store_name,
    brand_id,
    brand_name,
    max_class_id,
    max_class_name,
    mid_class_id,
    mid_class_name,
    min_class_id,
    min_class_name,
-- step3: 计算 16项 指标
    sum(sale_amt) as sale_amt,
    sum(plat_amt) as plat_amt,
    sum(deliver_sale_amt) as deliver_sale_amt,
    sum(mini_app_sale_amt) as mini_app_sale_amt,
    sum(android_sale_amt) as android_sale_amt,
    sum(ios_sale_amt) as ios_sale_amt,
    sum(pcweb_sale_amt) as pcweb_sale_amt,
    sum(order_cnt) as order_cnt,
    sum(eva_order_cnt) as eva_order_cnt,
    sum(bad_eva_order_cnt) as bad_eva_order_cnt,
    sum(deliver_order_cnt) as deliver_order_cnt,
    sum(refund_order_cnt) as refund_order_cnt,
    sum(miniapp_order_cnt) as miniapp_order_cnt,
    sum(android_order_cnt) as android_order_cnt,
    sum(ios_order_cnt) as ios_order_cnt,
    sum(pcweb_order_cnt) as pcweb_order_cnt
from yp_dws.dws_sale_daycount dc                            -- dws层的 销售主题日统计宽表
    left join yp_dwd.dim_date d on dc.dt = d.date_code      -- dwd层的 日期维度表
-- step2: 按年进行分组, 结合8种维度, 进行统计.
group by
grouping sets(
    -- 按年统计, 8种维度.
    (d.year_code),            -- 日期( 年 )
    (d.year_code, city_id, city_name),            -- 日期( 年 ) + 城市
    (d.year_code, city_id, city_name, trade_area_id, trade_area_name),  -- 日期( 年 ) + 城市 + 商圈
    (d.year_code, city_id, city_name, trade_area_id, trade_area_name, store_id, store_name),  -- 日期( 年 ) + 城市 + 商圈 + 店铺
    (d.year_code, brand_id, brand_name),  -- 日期( 年 ) + 品牌
    (d.year_code, max_class_id, max_class_name),  -- 日期( 年 ) + 大类
    (d.year_code, max_class_id, max_class_name, mid_class_id, mid_class_name),  -- 日期( 年 ) + 大类 + 中类
    (d.year_code, max_class_id, max_class_name, mid_class_id, mid_class_name, min_class_id, min_class_name)  -- 日期( 年 ) + 大类 + 中类 + 小类
);

-- 3. 查询表数据.  Presto中完成.
select * from yp_dm.dm_sale;

-- ------------------------------------ 案例2: DM层 销售主题统计宽表(年月周日) ------------------------------------
-- 1. DM层 销售主题统计宽表(年月周日) 完整代码实现如下.
/*
基于上述的 按年统计的代码, 改造如下:
    1. yp_dwd.dim_date日期维度表的字段太多了, 我们只需要抽取出我们要用的字段, 然后放到CTE表达式中, 最后和 dws层的销售主题日统计宽表 做 连接查询.
    2. 改造grouping sets()的代码, 加上: 月(8种维度), 周(8种维度), 日(8种维度)  结合已经实现的 年(8种维度), 共计 32 种维度组合.
    3. 完善和日期维度有关的字段.
    4. 目前我们判断日期类型(time_type) 和 维度类型(group_type)都是使用 简单版思路(找不同), 筛掉哪些具有独立属性的, 最终能甄别出每一种维度组合.
       其实这个代码可以用 grouping(维度1, 维度2, 维度3....) = 二进制对应的十进制数据 来实现精准校验.
       例如:
                   0           0            1(16)    1(8)      1(4)          1(2)           1(1)
        grouping(city_id, trade_area_id, store_id, brand_id, max_class_id, mid_class_id, min_class_id) = 31      日期 + 城市 + 商圈
*/
-- 改造1: 从yp_dwd.dim_date日期维度表抽取我们用的字段, 放到CTE中, 然后和 yp_dws.dws_sale_daycount 表 做连接查询.
insert into hive.yp_dm.dm_sale
-- 获取日期数据（周、月的环比/同比日期）
with dt1 as (
  select
    dim_date_id,
    date_code
    ,date_id_mom -- 与本月环比的上月日期
    ,date_id_mym -- 与本月同比的上年日期
    ,year_code
    ,month_code
    ,year_month     --年月
    ,day_month_num --几号
    ,week_day_code --周几
    ,year_week_name_cn  --年周
    from yp_dwd.dim_date
)
-- step1: 梳理表关系,  销售主题日统计宽表 left join yp_dwd.dim_date 日期维度表
select
-- 改造3: 完善和日期维度有关的字段.
-- step5: 日期维度字段
    '2023-06-04' as date_time,
    --  time_type字段具体的值: string comment '统计时间维度：year、month、week、date(就是天day)',
    case
        when grouping(year_code, month_code, day_month_num, dim_date_id)=0 then 'date'
        when grouping(year_code, year_week_name_cn)=0 then 'week'
        when grouping(year_code, month_code, year_month)=0 then 'month'
        else 'year'
    end as time_type,
    dt1.year_code,    -- 年, 例如: 2023
    dt1.year_month,   -- 年月, 例如: 202303
    dt1.month_code,   -- 月, 例如: 03
    dt1.day_month_num, -- 月中的第几天, 例如: 21
    dt1.dim_date_id,   -- 具体的日期, 例如: 20230321
    dt1.year_week_name_cn, -- 年中的第几周
-- step4: 除(日期维度)外, 其它所有的维度, 即: 城市, 商圈, 店铺, 品牌, 大类, 中类, 小类.
    -- 1个分组类型字段, 分组类型：store，trade_area，city，brand，min_class，mid_class，max_class，all
    case
        when grouping(store_id, store_name)=0 then 'store'                  -- 日期 + 城市 + 商圈 + 店铺
        when grouping(trade_area_id, trade_area_name)=0 then 'trade_area'   -- 日期 + 城市 + 商圈
        when grouping(city_id, city_name)=0 then 'city'                     -- 日期 + 城市
        when grouping(brand_id, brand_name)=0 then 'brand'                  -- 日期 + 品牌
        when grouping(min_class_id, min_class_name)=0 then 'min_class'      -- 日期 + 大类 + 中类 + 小类
        when grouping(mid_class_id, mid_class_name)=0 then 'mid_class'      -- 日期 + 大类 + 中类
        when grouping(max_class_id, max_class_name)=0 then 'max_class'      -- 日期 + 大类
        else 'all'                                                          -- 日期
    end as group_type,
    -- 14个维度字段
    city_id,
    city_name,
    trade_area_id,
    trade_area_name,
    store_id,
    store_name,
    brand_id,
    brand_name,
    max_class_id,
    max_class_name,
    mid_class_id,
    mid_class_name,
    min_class_id,
    min_class_name,
-- step3: 计算 16项 指标
    sum(sale_amt) as sale_amt,
    sum(plat_amt) as plat_amt,
    sum(deliver_sale_amt) as deliver_sale_amt,
    sum(mini_app_sale_amt) as mini_app_sale_amt,
    sum(android_sale_amt) as android_sale_amt,
    sum(ios_sale_amt) as ios_sale_amt,
    sum(pcweb_sale_amt) as pcweb_sale_amt,
    sum(order_cnt) as order_cnt,
    sum(eva_order_cnt) as eva_order_cnt,
    sum(bad_eva_order_cnt) as bad_eva_order_cnt,
    sum(deliver_order_cnt) as deliver_order_cnt,
    sum(refund_order_cnt) as refund_order_cnt,
    sum(miniapp_order_cnt) as miniapp_order_cnt,
    sum(android_order_cnt) as android_order_cnt,
    sum(ios_order_cnt) as ios_order_cnt,
    sum(pcweb_order_cnt) as pcweb_order_cnt
from yp_dws.dws_sale_daycount dc                            -- dws层的 销售主题日统计宽表
    left join dt1 on dc.dt = dt1.date_code      -- dwd层的 日期维度表
-- step2: 按年进行分组, 结合8种维度, 进行统计.
group by
grouping sets(
-- 改造2: grouping sets()的代码, 加上: 月(8种维度), 周(8种维度), 日(8种维度)  结合已经实现的 年(8种维度), 共计 32 种维度组合.
    -- 按年统计, 8种维度.
    (dt1.year_code),            -- 日期( 年 )
    (dt1.year_code, city_id, city_name),            -- 日期( 年 ) + 城市
    (dt1.year_code, city_id, city_name, trade_area_id, trade_area_name),  -- 日期( 年 ) + 城市 + 商圈
    (dt1.year_code, city_id, city_name, trade_area_id, trade_area_name, store_id, store_name),  -- 日期( 年 ) + 城市 + 商圈 + 店铺
    (dt1.year_code, brand_id, brand_name),  -- 日期( 年 ) + 品牌
    (dt1.year_code, max_class_id, max_class_name),  -- 日期( 年 ) + 大类
    (dt1.year_code, max_class_id, max_class_name, mid_class_id, mid_class_name),  -- 日期( 年 ) + 大类 + 中类
    (dt1.year_code, max_class_id, max_class_name, mid_class_id, mid_class_name, min_class_id, min_class_name),  -- 日期( 年 ) + 大类 + 中类 + 小类

     -- 按月统计, 8种维度.
    (dt1.year_code, dt1.month_code, dt1.year_month),            -- 日期( 月 )
    (dt1.year_code, dt1.month_code, dt1.year_month, city_id, city_name),            -- 日期( 月 ) + 城市
    (dt1.year_code, dt1.month_code, dt1.year_month, city_id, city_name, trade_area_id, trade_area_name),  -- 日期( 月 ) + 城市 + 商圈
    (dt1.year_code, dt1.month_code, dt1.year_month, city_id, city_name, trade_area_id, trade_area_name, store_id, store_name),  -- 日期( 月 ) + 城市 + 商圈 + 店铺
    (dt1.year_code, dt1.month_code, dt1.year_month, brand_id, brand_name),  -- 日期( 月 ) + 品牌
    (dt1.year_code, dt1.month_code, dt1.year_month, max_class_id, max_class_name),  -- 日期( 月 ) + 大类
    (dt1.year_code, dt1.month_code, dt1.year_month, max_class_id, max_class_name, mid_class_id, mid_class_name),  -- 日期( 月 ) + 大类 + 中类
    (dt1.year_code, dt1.month_code, dt1.year_month, max_class_id, max_class_name, mid_class_id, mid_class_name, min_class_id, min_class_name),  -- 日期( 年 ) + 大类 + 中类 + 小类

     -- 按周统计, 8种维度.
    (dt1.year_code, dt1.year_week_name_cn),            -- 日期( 周 )
    (dt1.year_code, dt1.year_week_name_cn,  city_id, city_name),            -- 日期( 周 ) + 城市
    (dt1.year_code, dt1.year_week_name_cn,  city_id, city_name, trade_area_id, trade_area_name),  -- 日期( 周 ) + 城市 + 商圈
    (dt1.year_code, dt1.year_week_name_cn,  city_id, city_name, trade_area_id, trade_area_name, store_id, store_name),  -- 日期( 周 ) + 城市 + 商圈 + 店铺
    (dt1.year_code, dt1.year_week_name_cn,  brand_id, brand_name),  -- 日期( 周 ) + 品牌
    (dt1.year_code, dt1.year_week_name_cn,  max_class_id, max_class_name),  -- 日期( 周 ) + 大类
    (dt1.year_code, dt1.year_week_name_cn,  max_class_id, max_class_name, mid_class_id, mid_class_name),  -- 日期( 周 ) + 大类 + 中类
    (dt1.year_code, dt1.year_week_name_cn,  max_class_id, max_class_name, mid_class_id, mid_class_name, min_class_id, min_class_name),  -- 日期( 周 ) + 大类 + 中类 + 小类

     -- 按天统计, 8种维度.
    (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id),            -- 日期( 天 )
    (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id,  city_id, city_name),            -- 日期( 天 ) + 城市
    (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id,  city_id, city_name, trade_area_id, trade_area_name),  -- 日期( 天 ) + 城市 + 商圈
    (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id,  city_id, city_name, trade_area_id, trade_area_name, store_id, store_name),  -- 日期( 天 ) + 城市 + 商圈 + 店铺
    (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id,  brand_id, brand_name),  -- 日期( 天 ) + 品牌
    (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id,  max_class_id, max_class_name),  -- 日期( 天 ) + 大类
    (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id,  max_class_id, max_class_name, mid_class_id, mid_class_name),  -- 日期( 天 ) + 大类 + 中类
    (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id,  max_class_id, max_class_name, mid_class_id, mid_class_name, min_class_id, min_class_name)  -- 日期( 天 ) + 大类 + 中类 + 小类
);

-- 2. 查询表数据.  Presto中完成.
select * from yp_dm.dm_sale;


-- ------------------------------------ 案例3: DM层 商品主题统计宽表(总累计, 近30天累计), 首次计算. ------------------------------------
-- 商品主题统计宽表, 我们不再计算每天的了, 而是改为: 周期性进行统计, 这里统计的是: 总累计量, 近30天累计.
-- 第一次计算 总累计:       开始时间 ~ 当前时间, 根据商品id分组, 对应的数值全部累加
-- 第一次计算 近30天累计:   today - 30 ~ today, 根据商品id分组, 对应的数值全部累加

-- 1. 建表, 在hive中写.

-- 2. 插入表数据.
insert into hive.yp_dm.dm_sku
-- 2.1 计算 总累计,  第一次计算:  从开始时间 累加到 当前时间.
with all_count as (
    select
        sku_id,
        sum(order_count) as order_count,
        sum(order_num) as order_num,
        sum(order_amount) as order_amount,
        sum(payment_count) as payment_count,
        sum(payment_num) as payment_num,
        sum(payment_amount) as payment_amount,
        sum(refund_count) as refund_count,
        sum(refund_num) as refund_num,
        sum(refund_amount) as refund_amount,
        sum(cart_count) as cart_count,
        sum(cart_num) as cart_num,
        sum(favor_count) as favor_count,
        sum(evaluation_good_count) as evaluation_good_count,
        sum(evaluation_mid_count) as evaluation_mid_count,
        sum(evaluation_bad_count) as evaluation_bad_count
    from yp_dws.dws_sku_daycount
    group by sku_id
),
-- 2.2 计算 近30天累计, 第一次计算,  (today-30, today)
last_30d as (
    select
        sku_id,
        sum(order_count) as order_last_30d_count,
        sum(order_num) as order_last_30d_num,
        sum(order_amount) as order_last_30d_amount,
        sum(payment_count) as payment_last_30d_count,
        sum(payment_num) as payment_last_30d_num,
        sum(payment_amount) as payment_last_30d_amount,
        sum(refund_count) as refund_last_30d_count,
        sum(refund_num) as refund_last_30d_num,
        sum(refund_amount) as refund_last_30d_amount,
        sum(cart_count) as cart_last_30d_count,
        sum(cart_num) as cart_last_30d_num,
        sum(favor_count) as favor_last_30d_count,
        sum(evaluation_good_count) as evaluation_last_30d_good_count,
        sum(evaluation_mid_count) as evaluation_last_30d_mid_count,
        sum(evaluation_bad_count) as evaluation_last_30d_bad_count
    from yp_dws.dws_sku_daycount
    -- where dt between '当前时间 - 30天' and '当前时间'     -- 正确写法, 但是可能会没有数据, 因为我们现在是模拟数据.
    where dt > cast(date_add('day', -30, date '2020-05-08') as varchar)     -- 细节: 记得类型转换
    group by sku_id
)
-- 2.3 把上述 首次计算的 总累计 和 近30天累计结果, 写到 yp_dm.dm_sku 中.
select
    ac.sku_id,
    l30.order_last_30d_count,
    l30.order_last_30d_num,
    l30.order_last_30d_amount,
    ac.order_count,
    ac.order_num,
    ac.order_amount,
    payment_last_30d_count,
    payment_last_30d_num,
    payment_last_30d_amount,
    payment_count,
    payment_num,
    payment_amount,
    refund_last_30d_count,
    refund_last_30d_num,
    refund_last_30d_amount,
    refund_count,
    refund_num,
    refund_amount,
    cart_last_30d_count,
    cart_last_30d_num,
    cart_count,
    cart_num,
    favor_last_30d_count,
    favor_count,
    evaluation_last_30d_good_count,
    evaluation_last_30d_mid_count,
    evaluation_last_30d_bad_count,
    evaluation_good_count,
    evaluation_mid_count,
    evaluation_bad_count
from all_count ac
    left join last_30d l30 on ac.sku_id = l30.sku_id;

-- 2.4 演示presto的date_add()方法, 实现: 时间往前推 30天.
select date_add('day', -30, date '2023-06-04');     -- 当前时间 - 30天

-- 3. 查询表数据.
select * from yp_dm.dm_sku;

-- ------------------------------------ 案例4: DM层 商品主题统计宽表(总累计, 近30天累计), 循环计算. ------------------------------------
/*
循环计算:
    循环计算, 总累计:
        思路1: 从开始时间 从新计算到 当前时间, 会存在大量的冗余计算, 效率低, 可以但是没必要这样做.
        思路2: 旧的总累计 + 新增1天的数据 = 新的总累计.

    循环计算, 近30天累计:
         today - 30 ~ today, 根据商品id分组, 对应的数值全部累加
细节:
    1. 当我们在计算近30天的累计时, 应该写今天的时间(2023-06-04), 但是因为是模拟数据, 这样写时间可能没有数据, 所以我们写了1个靠前的时间. 即: 2020-05-08
    2. 因为首次计算近30天累计, 我们是用 2020-05-08, 所以重复计算近30天累计, 我们应该算 2020-05-09往前推30天.
        近30天累计:
            第一次:        2020-05-08 减去 30天  ~ 2020-05-08
            第2次(循环):   2020-05-09 减去 30天  ~ 2020-05-09
            第3次(循环):   2020-05-10 减去 30天  ~ 2020-05-10
*/
-- 1. 创建临时表, 用于存储最新的(循环计算的) 总累计 和 近30天累计结果.

-- 3. 把上述的计算结果, 添加到 临时表中.
insert into hive.yp_dm.dm_sku_tmp
-- 2. 循环计算 总累计 和 近30天累计.
-- 2.1 获取旧的总累计.
with old as (
    select * from yp_dm.dm_sku
),
-- 2.2 计算新的近30天累计, 因为近30天的累计, 包含最新一天的数据, 所以捎带着, 我们把最新一天的累计, 也计算出来.
new as (
    select
        sku_id,
-- 计算最新1天(这里是: 2020-05-09)的累计
        sum(if(dt='2020-05-09', order_count, 0)) as order_count_1d,
        sum(if(dt='2020-05-09', order_num, 0)) as order_num_1d,
        sum(if(dt='2020-05-09', order_amount, 0)) as order_amount_1d,
        sum(if(dt='2020-05-09', payment_count, 0)) as payment_count_1d,
        sum(if(dt='2020-05-09', payment_num, 0)) as payment_num_1d,
        sum(if(dt='2020-05-09', payment_amount, 0)) as payment_amount_1d,
        sum(if(dt='2020-05-09', refund_count, 0)) as refund_count_1d,
        sum(if(dt='2020-05-09', refund_num, 0)) as refund_num_1d,
        sum(if(dt='2020-05-09', refund_amount, 0)) as refund_amount_1d,
        sum(if(dt='2020-05-09', cart_count, 0)) as cart_count_1d,
        sum(if(dt='2020-05-09', cart_num, 0)) as cart_num_1d,
        sum(if(dt='2020-05-09', favor_count, 0)) as favor_count_1d,
        sum(if(dt='2020-05-09', evaluation_good_count, 0)) as evaluation_good_count_1d,
        sum(if(dt='2020-05-09', evaluation_mid_count, 0)) as evaluation_mid_count_1d,
        sum(if(dt='2020-05-09', evaluation_bad_count, 0)) as evaluation_bad_count_1d,
-- 最新的 近30天累计 数据
        sum(order_count) as order_count30,
        sum(order_num) as order_num30,
        sum(order_amount) as order_amount30,
        sum(payment_count) as payment_count30,
        sum(payment_num) as payment_num30,
        sum(payment_amount) as payment_amount30,
        sum(refund_count) as refund_count30,
        sum(refund_num) as refund_num30,
        sum(refund_amount) as refund_amount30,
        sum(cart_count) as cart_count30,
        sum(cart_num) as cart_num30,
        sum(favor_count) as favor_count30,
        sum(evaluation_good_count) as evaluation_good_count30,
        sum(evaluation_mid_count) as evaluation_mid_count30,
        sum(evaluation_bad_count) as evaluation_bad_count30
    from yp_dws.dws_sku_daycount
    where dt > cast(date_add('day', -30, date '2020-05-09') as varchar)     -- 细节: 记得类型转换
    group by sku_id
)
-- 2.3 计算最终结果, 即: 新的总累计(等于 旧的总累计 + 最新1天的数据),  新的近30天累计(这个在new中, 算完了)
select
    coalesce(new.sku_id, old.sku_id) as sku_id,
    coalesce(new.order_count30, 0) order_last_30d_count,       -- 近30天累计, 下单量
    coalesce(new.order_num30, 0) order_last_30d_num,              -- 近30天累计, 下单件数
    coalesce(new.order_amount30, 0) order_last_30d_amount,     -- 近30天累计, 下单金额

    -- 新的总累计 = 旧的总累计 + 新增1天累计
    coalesce(old.order_count, 0) + coalesce(new.order_count_1d, 0) as order_count,        -- 总累计, 下单量
    coalesce(old.order_num, 0) + coalesce(new.order_num_1d, 0) order_num,          -- 总累计, 下单件数
    coalesce(old.order_amount, 0) + coalesce(new.order_amount_1d, 0) order_amount,       -- 总累计, 下单金额

  --        支付单 30天数据
      coalesce(new.payment_count30,0) payment_last_30d_count,
      coalesce(new.payment_num30,0) payment_last_30d_num,
      coalesce(new.payment_amount30,0) payment_last_30d_amount,
  --        支付单 累积历史数据
      coalesce(old.payment_count,0) + coalesce(new.payment_count_1d,0) payment_count,
      coalesce(old.payment_num,0) + coalesce(new.payment_count_1d,0) payment_num,
      coalesce(old.payment_amount,0) + coalesce(new.payment_count_1d,0) payment_amount,
  --        退款单 30天数据
      coalesce(new.refund_count30,0) refund_last_30d_count,
      coalesce(new.refund_num30,0) refund_last_30d_num,
      coalesce(new.refund_amount30,0) refund_last_30d_amount,
  --        退款单 累积历史数据
      coalesce(old.refund_count,0) + coalesce(new.refund_count_1d,0) refund_count,
      coalesce(old.refund_num,0) + coalesce(new.refund_num_1d,0) refund_num,
      coalesce(old.refund_amount,0) + coalesce(new.refund_amount_1d,0) refund_amount,
  --        购物车 30天数据
      coalesce(new.cart_count30,0) cart_last_30d_count,
      coalesce(new.cart_num30,0) cart_last_30d_num,
  --        购物车 累积历史数据
      coalesce(old.cart_count,0) + coalesce(new.cart_count_1d,0) cart_count,
      coalesce(old.cart_num,0) + coalesce(new.cart_num_1d,0) cart_num,
  --        收藏 30天数据
      coalesce(new.favor_count30,0) favor_last_30d_count,
  --        收藏 累积历史数据
      coalesce(old.favor_count,0) + coalesce(new.favor_count_1d,0) favor_count,
  --        评论 30天数据
      coalesce(new.evaluation_good_count30,0) evaluation_last_30d_good_count,
      coalesce(new.evaluation_mid_count30,0) evaluation_last_30d_mid_count,
      coalesce(new.evaluation_bad_count30,0) evaluation_last_30d_bad_count,
  --        评论 累积历史数据
      coalesce(old.evaluation_good_count,0) + coalesce(new.evaluation_good_count_1d,0) evaluation_good_count,
      coalesce(old.evaluation_mid_count,0) + coalesce(new.evaluation_mid_count_1d,0) evaluation_mid_count,
      coalesce(old.evaluation_bad_count,0) + coalesce(new.evaluation_bad_count_1d,0) evaluation_bad_count
from

    -- 满外连接, 结果 = 左表的全集 + 右表的全集 + 交集.
    old full join new on old.sku_id = new.sku_id;

-- 4. 因为Presto不支持 insert overwrite的写法, 所以删除 yp_dm.dm_sku 表中的数据(旧的总累计 和 近30天累计)
delete from yp_dm.dm_sku;
select * from yp_dm.dm_sku;

-- 5. 用临时表(新的总累计 和 近30天累计) 追加到 yp_dm.dm_sku中即可, 此时: yp_dm.dm_sku记录的就是 循环计算后的 新的总累计 和 近30天累计.
insert into yp_dm.dm_sku select * from yp_dm.dm_sku_tmp;

-- 6. 查看结果.
select * from yp_dm.dm_sku_tmp;



-- 2.5 计算 近30天累计, (循环)计算.  (today-30, today)

-- 2.  计算 总累计, (循环)计算.  旧的总累计 + 新增1天的数据 = 新的总累计.

-- ------------------------------------ 案例5: DM层 用户主题统计宽表(总累计, 近30天累计) ------------------------------------
-- 思路: 同商品主题一致, 这里不做过多赘述了.
-- 首次执行.
insert into yp_dm.dm_user
with login_count as (
    select
        min(dt) as login_date_first,
        max (dt) as login_date_last,
        sum(login_count) as login_count,
       user_id
    from yp_dws.dws_user_daycount
    where login_count > 0
    group by user_id
),
cart_count as (
    select
        min(dt) as cart_date_first,
        max(dt) as cart_date_last,
        sum(cart_count) as cart_count,
        sum(cart_amount) as cart_amount,
       user_id
    from yp_dws.dws_user_daycount
    where cart_count > 0
    group by user_id
),
order_count as (
    select
        min(dt) as order_date_first,
        max(dt) as order_date_last,
        sum(order_count) as order_count,
        sum(order_amount) as order_amount,
       user_id
    from yp_dws.dws_user_daycount
    where order_count > 0
    group by user_id
),
payment_count as (
    select
        min(dt) as payment_date_first,
        max(dt) as payment_date_last,
        sum(payment_count) as payment_count,
        sum(payment_amount) as payment_amount,
       user_id
    from yp_dws.dws_user_daycount
    where payment_count > 0
    group by user_id
),
last_30d as (
    select
        user_id,
        sum(if(login_count>0,1,0)) login_last_30d_count,
        sum(cart_count) cart_last_30d_count,
        sum(cart_amount) cart_last_30d_amount,
        sum(order_count) order_last_30d_count,
        sum(order_amount) order_last_30d_amount,
        sum(payment_count) payment_last_30d_count,
        sum(payment_amount) payment_last_30d_amount
    from yp_dws.dws_user_daycount
    where dt>=cast(date_add('day', -30, date '2019-05-07') as varchar)
    group by user_id
)
select
    '2019-05-07' date_time,
    last30.user_id,
--    登录
    l.login_date_first,
    l.login_date_last,
    l.login_count,
    last30.login_last_30d_count,
--    购物车
    cc.cart_date_first,
    cc.cart_date_last,
    cc.cart_count,
    cc.cart_amount,
    last30.cart_last_30d_count,
    last30.cart_last_30d_amount,
--    订单
    o.order_date_first,
    o.order_date_last,
    o.order_count,
    o.order_amount,
    last30.order_last_30d_count,
    last30.order_last_30d_amount,
--    支付
    p.payment_date_first,
    p.payment_date_last,
    p.payment_count,
    p.payment_amount,
    last30.payment_last_30d_count,
    last30.payment_last_30d_amount
from last_30d last30
left join login_count l on last30.user_id=l.user_id
left join order_count o on last30.user_id=o.user_id
left join payment_count p on last30.user_id=p.user_id
left join cart_count cc on last30.user_id=cc.user_id
;


-- 查询结果.
select * from yp_dm.dm_user;