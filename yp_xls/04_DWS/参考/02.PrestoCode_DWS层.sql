-- --------------------------------- 案例1: 演示Hive的 聚合函数增强  ---------------------------------
-- 1. grouping sets() 在Hive 和 Presto中的作用一模一样, 但是写法稍有不同, 在Presto中写的时候, group by 后边不要写字段名(写了就报错)
use test;
select month, day, count(cookieid) cnt_nums from t_cookie
group by        -- 这里不要写字段, 写了就错, 因为Presto会根据如下的维度, 自动填充.
grouping sets
    (month, day, (month, day));     -- 具体的3种维度: 月, 天, 月和天       运行了 3 秒


-- 2. cube()函数: 实现任意维度的组合, 即: 你传入3个维度, 则等价于 2 ^ 3 = 8种组合, cube()也叫任意维度组合.
select month, day, count(cookieid) cnt_nums from t_cookie
group by
cube(month, day);

-- 上述的SQL等价于, 如下的SQL
select month, day, count(cookieid) cnt_nums from t_cookie
group by
grouping sets
    ((), month, day, (month, day));     -- 空, 月, 天, 月和天, 共计 4 种维度.

-- 3. rollup()函数, 从右往左 依次递减, 例如: rollup(a, b, c) 等价于 grouping sets((a,b,c), (a,b), (a), ())
select month, day, count(cookieid) cnt_nums from t_cookie
group by
rollup(month, day);

-- 上述的SQL等价于, 如下的SQL
select month, day, count(cookieid) cnt_nums from t_cookie
group by
grouping sets
    ((month, day), month, ());     -- 月和天, 月, 空 共计 3 种维度.


-- 4. grouping()函数: 用来判断当前的结果属于哪种维度组合的, 规则: 0代表有, 1代表没有, 且这里的0和1是二进制的, 如果是多维度, 最终结果会显示成对应的 十进制结果.
/*
    例如: 二进制 10, 对应的十进制是 2 + 0 = 2,   二进制 11 对应的 十进制 2 + 1 = 3, 二进制 01对应的十进制 0 + 1 = 1.
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
group by
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
group by
grouping sets
    ((), month, day, (month, day));     -- 4种维度, 空, 月, 天, 月和天



-- --------------------------------- 案例2: 演示DWS层 销售主题日统计宽表搭建, 准备动作之 简单模型  ---------------------------------
-- 3. 需求: 指标：订单量、销售额,  维度：日期、日期+品牌
-- 3.1 思路1: 分解版.
-- 3.2 Step1: 统计每天的订单量、销售额.           维度: 日期,      指标: 订单量、销售额
select
       dt,
       count(distinct oid) "总订单量",
       sum(g_price) "总销售额"
from test.t_order_detail group by dt;

-- 3.3 Step2: 统计每天每个品牌的订单量、销售额.    维度: 日期, 品牌  指标: 订单量、销售额
select
       dt,
       brand_id as "品牌id",
       count(distinct oid) "各品牌订单量",
       sum(g_price) "各品牌销售额"
from test.t_order_detail group by dt, brand_id;

-- 3.4 把上述的结果合并到一起, 这里用union all思路写.
select
       dt,
       null as "品牌id",
       count(distinct oid) "总订单量",
       null as "各品牌订单量",
       sum(g_price) "总销售额",
       null as "各品牌销售额",
       1 as group_type          -- 分组类型, 这里的1表示 总订单量, 总销售额
from test.t_order_detail group by dt
union all       -- 把数据合并到一起.
select
       dt,
       brand_id as "品牌id",
       null as "总订单量",
       count(distinct oid) "各品牌订单量",
       null as "总销售额",
       sum(g_price) "各品牌销售额",
       2 as group_type              -- 分组类型, 这里的2表示: 各品牌订单量, 各品牌销售额
from test.t_order_detail group by dt, brand_id;

-- 4. 用 聚合函数增强 来优化上述的需求, 即: 需求: 指标：订单量、销售额,  维度：日期、日期+品牌
-- grouping()规则: 0:有, 1: 没有
-- 4.1 思路1: 取巧了, 因为就两个维度, 1个有品牌, 1个没有品牌.
select
    dt,
    case when grouping(brand_id)=1 then null else brand_id end "品牌id",
    case when grouping(brand_id)=1 then count(distinct oid) else null end "总订单量",
    case when grouping(brand_id)=0 then count(distinct oid) else null end "各品牌订单量",
    case when grouping(brand_id)=1 then sum(g_price) else null end "总销售额",
    case when grouping(brand_id)=0 then sum(g_price) else null end "各品牌销售额",
    case
        when grouping(brand_id)=1 then '1'      -- 日期维度
        when grouping(brand_id)=0 then '2'      -- 日期 + 品牌维度
    end as group_type
from test.t_order_detail
group by
grouping sets
    (dt, (dt, brand_id));


-- 4.2 思路2: 严谨写法, grouping(维度1, 维度2, 维度3....) = 十进制值  标准判断
select
    dt,
    case when grouping(dt, brand_id)=1 then null else brand_id end "品牌id",                  -- 01 => 1
    case when grouping(dt, brand_id)=1 then count(distinct oid) else null end "总订单量",      -- 01 => 1
    case when grouping(dt, brand_id)=0 then count(distinct oid) else null end "各品牌订单量",   -- 00 => 0
    case when grouping(dt, brand_id)=1 then sum(g_price) else null end "总销售额",
    case when grouping(dt, brand_id)=0 then sum(g_price) else null end "各品牌销售额",
    case grouping(dt, brand_id)
        when 1 then '1'          -- 01 => 1, 有日期, 没品牌
        when 0 then '2'          -- 00 => 0, 有日期, 有品牌
    end as group_type
from test.t_order_detail
group by
grouping sets
    (dt, (dt, brand_id));


-- --------------------------------- 案例3: 演示DWS层 销售主题日统计宽表搭建, 准备动作之 复杂模型  ---------------------------------
-- 目的: 告诉大家, 如果未来我们要处理的数据, 有大量的重复, 且去重条件都不一致, 该如何高效的实现去重.

-- 需求1: 根据订单id分组，找出订单商品数最多的
select order_id, count(order_id) order_cnt from yp_dwb.dwb_order_detail group by order_id order by order_cnt desc;

-- 需求2: 查找订单编号为 dd190227318021f41f 的订单数据.
select * from yp_dwb.dwb_order_detail where order_id = 'dd190227318021f41f';

-- 1. 演示如何去重.  不能使用distinct, 不能使用group by, 因为它们会直接修改表结构, 表的数据格式等就变化了.
-- 小技巧: 根据谁(哪个字段)去重, 就根据谁(哪个字段)进行分组.
-- 1.1 需求1: 只以订单oid去重,  思路: row_number() + CRT表达式.
with t1 as (
    select
       *,
       row_number() over(partition by oid) id_rn
    from test.t_order_detail_dup
)
select * from t1 where id_rn = 1;

-- 1.2 需求2: 以订单oid+品牌brand_id去重
with t1 as (
    select
           *,
           row_number() over(partition by oid, brand_id) rn
    from test.t_order_detail_dup
)
select * from t1 where rn = 1;

-- 1.3 需求3: 再比如以订单oid+品牌brand_id+商品goods_id去重
with t1 as (
    select
           *,
           row_number() over(partition by oid, brand_id, goods_id) rn
    from test.t_order_detail_dup
)
select * from t1 where rn = 1;

-- 1.4 需求4: 把上述所有的去重条件, 合并到一起(写).
with t1 as (
    select
           *,
           row_number() over(partition by oid) rn1,                     -- 按照 oid 去重
           row_number() over(partition by oid, brand_id) rn2,           -- 按照 oid, brand_id 去重
           row_number() over(partition by oid, brand_id, goods_id) rn3  -- 按照 oid, brand_id, goods_id 去重
    from test.t_order_detail_dup
)
select * from t1 where rn1 = 1;


-- --------------------------------- 案例4: 演示DWS层 销售主题日统计宽表搭建, 查询数据, 然后插入到表中  ---------------------------------
-- 细节: 搭建DWS层所需的数据不仅只来源于它的上一层(DWB), 还可以来源于其它的层(例如: DWD, ODS), 只不过稍显不规范, 但是可以这样做.

-- Step7: 把下述的查询结果插入到表中.
-- insert into table yp_dws.dws_sale_daycount partition(dt)

-- Step3: 用CTE包裹如下的内容(基础数据), 然后进行多维度分组查询.
with tmp as (
    -- Step1: 明确表关系 及 提取计算该主题的指标 和 维度 所需的字段.
    select
        -- 维度字段
        o.dt as create_date,        -- 日期维度
        s.city_id,
        s.city_name,                -- 城市
        s.trade_area_id,
        s.trade_area_name,          -- 商圈
        s.id as store_id,
        s.store_name,               -- 店铺
        g.brand_id,
        g.brand_name,               -- 品牌
        g.max_class_id,
        g.max_class_name,           -- 大类
        g.mid_class_id,
        g.mid_class_name,           -- 中类
        g.min_class_id,
        g.min_class_name,            -- 小类

        -- 订单指标
        o.order_id,         -- 订单id
        o.goods_id,         -- 商品id

        -- 金额相关
        o.order_amount,     -- 订单总金额 = 购买总金额 - 优惠金额
        o.plat_fee,         -- 平台分润
        o.dispatcher_money, -- 配送费
        o.total_price,      -- 购买商品的价格= 商品的数量 * 商品的单价

        -- 和判断相关的字段
        o.order_from,       -- 渠道类型, android, ios, miniapp, pcweb
        o.evaluation_id,    -- 评论id, 不为空说明有评论
        o.geval_scores,     -- 综合评分
        o.delievery_id,     -- 配送id, 不为空说明有配送
        o.refund_id,         -- 退款id, 不为空说明有退款

        -- step2: 按照需求, 对数据进行去重, 不要用distinct, group by, 会对实际数据产生影响, 这里用 row_number(), 根据需求去重即可.
        -- 注意: 如下的去重条件不一定全部用到, 用到了来这里找就行.   实际开发中, 去重的操作一般是最后写的, 遇到问题了, 再来改代码即可.
        -- 订单总价: 日期, 城市, 商圈, 店铺.            商品总价: 品牌, 大类, 中类, 小类
        row_number() over(partition by order_id) as order_rn,
        row_number() over(partition by order_id,g.brand_id) as brand_rn,
        row_number() over(partition by order_id,g.max_class_name) as maxclass_rn,
        row_number() over(partition by order_id,g.max_class_name,g.mid_class_name) as midclass_rn,
        row_number() over(partition by order_id,g.max_class_name,g.mid_class_name,g.min_class_name) as minclass_rn,

        --下面分组加入goods_id
        row_number() over(partition by order_id,g.brand_id,o.goods_id) as brand_goods_rn,
        row_number() over(partition by order_id,g.max_class_name,o.goods_id) as maxclass_goods_rn,
        row_number() over(partition by order_id,g.max_class_name,g.mid_class_name,o.goods_id) as midclass_goods_rn,
        row_number() over(partition by order_id,g.max_class_name,g.mid_class_name,g.min_class_name,o.goods_id) as minclass_goods_rn

    from
        yp_dwb.dwb_order_detail o       -- 订单明细宽表
        left join yp_dwb.dwb_goods_detail g on o.goods_id = g.id    -- 商品id 关联,  商品明细宽表
        left join yp_dwb.dwb_shop_detail s on o.store_id = s.id    -- 店铺id 关联,  店铺明细宽表
)

-- 多维度分组查询.
select
    -- Step4: 明确维度字段, 即: 判断当前维度组合是否包含该维度字段, 如果包含再显示维度字段值, 如果不包含, 维度字段值直接设置为null.
    -- grouping()规则: 0有, 1没有.
    case when grouping(city_id) = 0   --如果分组中包含city_id 则grouping为0 那么就返回city_id
		then city_id
		else null end as city_id ,
	case when grouping(city_id) = 0
		then city_name
		else null end as city_name ,
	case when grouping(trade_area_id) = 0--商圈
		then trade_area_id
		else null end as trade_area_id ,
	case when grouping(trade_area_id) = 0
		then trade_area_name
		else null end as trade_area_name ,
	case when grouping(store_id) = 0 --店铺
		then store_id
		else null end as store_id ,
	case when grouping(store_id) = 0
		then store_name
		else null end as store_name ,
	case when grouping(brand_id) = 0 --品牌
		then brand_id
		else null end as brand_id ,
	case when grouping(brand_id) = 0
		then brand_name
		else null end as brand_name ,
	case when grouping(max_class_id) = 0 --大类
		then max_class_id
		else null end as max_class_id ,
	case when grouping(max_class_id) = 0
		then max_class_name
		else null end as max_class_name ,
	case when grouping(mid_class_id) = 0 --中类
		then mid_class_id
		else null end as mid_class_id ,
	case when grouping(mid_class_id) = 0
		then mid_class_name
		else null end as mid_class_name ,
	case when grouping(min_class_id) = 0--小类
		then min_class_id
		else null end as min_class_id ,
	case when grouping(min_class_id) = 0
		then min_class_name
		else null end as min_class_name ,

    -- 明确分组类型, 即: 属于哪种维度组合. 分组类型：store，trade_area，city，brand，min_class，mid_class，max_class，all
    -- 方式1: 取巧版, 找不同维度组合的规律.
    case
        when grouping(store_id)=0 then 'store'
        when grouping(trade_area_id)=0 then 'trade_area'
        when grouping(city_id)=0 then 'city'
        when grouping(brand_id)=0 then 'brand'
        when grouping(min_class_id)=0 then 'min_class'
        when grouping(mid_class_id)=0 then 'mid_class'
        when grouping(max_class_id)=0 then 'max_class'
        when grouping(create_date)=0 then 'all'
    end as group_type,

    -- 方式2: 标准版, grouping()函数用来判断属于 哪种维度.   0:有, 1:没有
--     case
--         --          0           0          0             0         1         1             1               1
--         grouping(create_date, city_id, trade_area_id, store_id, brand_id, max_class_id, mid_class_id, min_class_id) = 15 then 'store'
--         --          0           0          0             1         1         1             1               1
--         grouping(create_date, city_id, trade_area_id, store_id, brand_id, max_class_id, mid_class_id, min_class_id) = 31 then 'trade_area_id'
--         --          0           0          1             1         1         1             1               1
--         grouping(create_date, city_id, trade_area_id, store_id, brand_id, max_class_id, mid_class_id, min_class_id) = 63 then 'city'
--         --          0           1          1             1         0         1             1               1
--         grouping(create_date, city_id, trade_area_id, store_id, brand_id, max_class_id, mid_class_id, min_class_id) = 119 then 'brand'
--     end as group_type

    -- Step5: 计算各种 金额指标.
    -- 指标1: 销售收入
    case
        -- 日期 + 城市 + 商圈 + 店铺维度            订单去重   并且   店铺id不为空(说明有数据)  订单总额    否则为0
        when grouping(store_id)=0 then sum(if(order_rn = 1 and store_id is not null, order_amount, 0))

        -- 更细腻, 严谨的一种判断, 有可能, 订单总额是个null, 所以 sum(null)的时候, 整体结果就为null了, 所以要对null值过滤.
        -- when grouping(store_id)=0 then sum(if(order_rn = 1 and store_id is not null, coalesce(order_amount, 0), 0))

        -- 日期 + 城市 + 商圈   订单总额
        when grouping(trade_area_id)=0 then sum(if(order_rn = 1 and trade_area_id is not null, order_amount, 0))
        -- 日期 + 城市    订单总额
        when grouping(city_id)=0 then sum(if(order_rn = 1 and city_id is not null, order_amount, 0))

        -- 日期 + 品牌    商品总额
        when grouping(brand_id)=0 then sum(if(brand_goods_rn = 1 and brand_id is not null, total_price, 0))
        -- 日期 + 大类 + 中类 + 小类        商品总额
        when grouping(min_class_id)=0 then sum(if(minclass_goods_rn = 1 and min_class_id is not null, total_price, 0))
        -- 日期 + 大类 + 中类     商品总额
        when grouping(mid_class_id)=0 then sum(if(midclass_goods_rn = 1 and mid_class_id is not null, total_price, 0))
        -- 日期 + 大类      商品总额
        when grouping(max_class_id)=0 then sum(if(maxclass_goods_rn = 1 and max_class_id is not null, total_price, 0))
        -- 日期            订单总额
        when grouping(create_date)=0 then sum(if(order_rn = 1 and max_class_id is not null, order_amount, 0))
    end as sale_amt,

    -- 指标2: 平台收入(按照订单来算的)
    case
        -- 日期 + 城市 + 商圈 + 店铺维度            订单去重   并且   店铺id不为空(说明有数据)  平台收入    否则为0
        when grouping(store_id)=0 then sum(if(order_rn = 1 and store_id is not null, plat_fee, 0))
        -- 日期 + 城市 + 商圈   平台收入
        when grouping(trade_area_id)=0 then sum(if(order_rn = 1 and trade_area_id is not null, plat_fee, 0))
        -- 日期 + 城市    平台收入
        when grouping(city_id)=0 then sum(if(order_rn = 1 and city_id is not null, plat_fee, 0))

        -- 日期 + 品牌    平台收入: 无
        when grouping(brand_id)=0 then 0
        -- 日期 + 大类 + 中类 + 小类        平台收入: 无
        when grouping(min_class_id)=0 then 0
        -- 日期 + 大类 + 中类     平台收入: 无
        when grouping(mid_class_id)=0 then 0
        -- 日期 + 大类      平台收入: 无
        when grouping(max_class_id)=0 then 0
        -- 日期            平台收入
        when grouping(create_date)=0 then sum(if(order_rn = 1 and max_class_id is not null, plat_fee, 0))
    end as plat_amt,

    -- Step6: 计算各种 (订单)量指标.
    -- 成交单量
    case
        -- 日期 + 城市 + 商圈 + 店铺, 总订单量
        when grouping(store_id)=0 then count(if(order_rn=1 and store_id is not null, order_id, null))
        -- 日期 + 城市 + 商圈, 总订单量
        when grouping(trade_area_id)=0 then count(if(order_rn=1 and trade_area_id is not null, order_id, null))
        -- 日期 + 城市, 总订单量
        when grouping(city_id)=0 then count(if(order_rn=1 and city_id is not null, order_id, null))

        -- 日期 + 品牌, 总订单量
        when grouping(brand_id)=0 then count(if(brand_rn=1 and brand_id is not null, order_id, null))

        -- 日期 + 大类 + 中类 + 小类, 总订单量
        when grouping(min_class_id)=0 then count(if(minclass_rn=1 and min_class_id is not null, order_id, null))
         -- 日期 + 大类 + 中类, 总订单量
        when grouping(mid_class_id)=0 then count(if(midclass_rn=1 and mid_class_id is not null, order_id, null))
         -- 日期 + 大类, 总订单量
        when grouping(max_class_id)=0 then count(if(maxclass_rn=1 and max_class_id is not null, order_id, null))

        -- 日期 , 总订单量
        when grouping(create_date)=0 then count(if(order_rn=1 and create_date is not null, order_id, null))
    end as order_cnt,

    -- 参评单量
     case
        -- 日期 + 城市 + 商圈 + 店铺, 参评单量
        when grouping(store_id)=0 then count(if(order_rn=1 and store_id is not null and evaluation_id is not null, order_id, null))
        -- 日期 + 城市 + 商圈, 参评单量
        when grouping(trade_area_id)=0 then count(if(order_rn=1 and trade_area_id is not null and evaluation_id is not null, order_id, null))
        -- 日期 + 城市, 参评单量
        when grouping(city_id)=0 then count(if(order_rn=1 and city_id is not null and evaluation_id is not null, order_id, null))

        -- 日期 + 品牌, 参评单量
        when grouping(brand_id)=0 then count(if(brand_rn=1 and brand_id is not null and evaluation_id is not null, order_id, null))

        -- 日期 + 大类 + 中类 + 小类, 参评单量
        when grouping(min_class_id)=0 then count(if(minclass_rn=1 and min_class_id is not null and evaluation_id is not null, order_id, null))
         -- 日期 + 大类 + 中类, 参评单量
        when grouping(mid_class_id)=0 then count(if(midclass_rn=1 and mid_class_id is not null and evaluation_id is not null, order_id, null))
         -- 日期 + 大类, 参评单量
        when grouping(max_class_id)=0 then count(if(maxclass_rn=1 and max_class_id is not null and evaluation_id is not null, order_id, null))

        -- 日期 , 参评单量
        when grouping(create_date)=0 then count(if(order_rn=1 and create_date is not null and evaluation_id is not null, order_id, null))
    end as eva_order_cnt
from tmp
group by
grouping sets(
    (create_date),                                  -- 日期 维度
    (create_date, city_id, city_name),              -- 日期 + 城市 维度
    (create_date, city_id, city_name, trade_area_id, trade_area_name),  -- 日期 + 城市 + 商圈 维度
    (create_date, city_id, city_name, trade_area_id, trade_area_name, store_id, store_name),  -- 日期 + 城市 + 商圈 + 店铺 维度
    (create_date, brand_id, brand_name),            -- 日期 + 品牌 维度
    (create_date, max_class_id, max_class_name),    -- 日期 + 大类 维度
    (create_date, max_class_id, max_class_name, mid_class_id, mid_class_name),    -- 日期 + 大类 + 中类 维度
    (create_date, max_class_id, max_class_name, mid_class_id, mid_class_name, min_class_id, min_class_name)        -- 日期 + 大类 + 中类 + 小类 维度
);


-- 执行脚本后, yp_dws.dws_sale_daycount表(销售主题日统计宽表)就有数据了.
-- Presto的1个Bug: 用Presto插入的数据, 用Hive查询不了.
select * from yp_dws.dws_sale_daycount;


-- --------------------------------- 案例5: 演示DWS层 商品主题日统计宽表搭建, 查询数据, 然后插入到表中  ---------------------------------
-- 1. 建表, 详见Hive代码.

-- 2. 往表中插入数据.
-- 2.1 分析我们要的数据来源于哪些表的哪些字段.
/*
目标: 商品主题日统计宽表
指标: 下单次数、下单件数、下单金额、被支付次数、被支付件数、被支付金额、被退款次数、被退款件数、被退款金额、被加入购物车次数、被加入购物车件数、被收藏次数、好评数、中评数、差评数
维度: 日期 + 商品(id, 名字)
001号订单 华为P100  3台
001号订单 华为P100  6台
001号订单 华为M100  9台
002号订单 华为P100  2台

结论:
    组1: 下单次数、下单件数、下单金额
        表:  yp_dwb.dwb_order_detail
        字段: dt, order_id, goods_id, goods_name,
              buy_num(购买商品的数量), total_price(购买商品的价格)
    组2: 被支付次数、被支付件数、被支付金额
        表:   yp_dwb.dwb_order_detail
        字段:  dt, order_id, goods_id, goods_name,
              buy_num, total_price, is_pay(0:未支付, 1:已支付)
    组3: 被退款次数、被退款件数、被退款金额
        表:   yp_dwb.dwb_order_detail
        字段:  dt, order_id, goods_id, goods_name,
              buy_num, total_price, refund_id(不为null说明有退款)
    组4: 被加入购物车次数、被加入购物车件数
        表:   yp_dwd.fact_shop_cart
        字段: id, buy_num, goods_id, end_date='9999-99-99'
    组5: 被收藏次数
        表:  yp_dwd.fact_goods_collect
        字段: id, goods_id, end_date='9999-99-99'
    组6: 好评数、中评数、差评数
        表:  yp_dwd.fact_goods_evaluation_detail
        字段: dt, goods_id, geval_scores_goods(商品评分: 0 ~ 10分)
            评分规则: >=9 好评,    >6 <9 中评,  <=6 差评
*/

-- 2.2 以下是具体的插入数据的动作.
-- step10: 把下述的内容, 插入到 yp_dws.dws_sku_daycount 表中即可.
insert into hive.yp_dws.dws_sku_daycount
--step1: 准备数据源, 用于计算:  下单次数、下单件数、下单金额、       被支付次数、被支付件数、被支付金额、     被退款次数、被退款件数、被退款金额
with order_base as (
    select
        dt,             -- 订单日期, 维度
        order_id,       -- 订单id
        goods_id,       -- 商品id,
        goods_name,     -- 商品名
        buy_num,        -- 购买商品的数量,
        total_price,    -- 购买商品的总价格
        is_pay,         -- 标记是否已支付, 0:未支付, 1:已支付
        refund_id,      -- 标记是否有退款, 不为null, 说明有退款.
        row_number() over(partition by order_id, goods_id) rn   -- 根据订单id, 商品id去重.
    from yp_dwb.dwb_order_detail
),
-- step2: 计算 下单次数、下单件数、下单金额
order_count as (
    select
           dt,                              -- 日期维度
           goods_id as sku_id,              -- 商品id, 维度
           goods_name as sku_name,          -- 商品名, 维度
           count(order_id) as order_count,  -- 被下单次数
           sum(buy_num) as order_num,       -- 被下单件数
           sum(total_price) as order_amount -- 被下单金额
    from order_base
    where rn = 1
    group by dt, goods_id, goods_name
),
-- step3: 计算 被支付次数、被支付件数、被支付金额
payment_count as (
    select
           dt,                              -- 日期维度
           goods_id as sku_id,              -- 商品id, 维度
           goods_name as sku_name,          -- 商品名, 维度
           count(order_id) as payment_count,  -- 被支付次数
           sum(buy_num) as payment_num,       -- 被支付件数
           sum(total_price) as payment_amount -- 被支付金额
    from order_base
    where rn = 1 and is_pay=1       -- rn=1 去重, is_pay = 1 已支付
    group by dt, goods_id, goods_name
),
-- step4: 计算 被退款次数、被退款件数、被退款金额
refund_count as (
    select
           dt,                              -- 日期维度
           goods_id as sku_id,              -- 商品id, 维度
           goods_name as sku_name,          -- 商品名, 维度
           count(order_id) as refund_count,  -- 被退款次数
           sum(buy_num) as refund_num,       -- 被退款件数
           sum(total_price) as refund_amount -- 被退款金额
    from order_base
    where rn = 1 and refund_id is not null    -- rn=1 去重, refund_id is not null 不为空, 说明有退款
    group by dt, goods_id, goods_name
),
-- step5: 计算 被加入购物车次数、被加入购物车件数、
cart_count as (
    select
        substring(create_time, 1, 10) as dt,    -- 日期
        goods_id as sku_id,                     -- 商品id
        count(id) as cart_count,                -- 被加入购物车次数
        sum(buy_num) as cart_num                -- 被加入购物车件数
    from yp_dwd.fact_shop_cart
    where end_date='9999-99-99'
    group by substring(create_time, 1, 10), goods_id
),
-- step6: 被收藏次数
favor_count as (
    select
        substring(create_time, 1, 10) as dt,    -- 日期
        goods_id as sku_id,                     -- 商品id
        count(id) as favor_count                -- 被收藏次数
    from yp_dwd.fact_goods_collect
    where end_date='9999-99-99'
    group by substring(create_time, 1, 10), goods_id
),
-- step7: 好评数、中评数、差评数, 规则: >=9 好评, >6 <9 中评, <= 6 差评
evaluation_count as (
    select
        substring(create_time, 1, 10) as dt,    -- 日期
        goods_id as sku_id,                     -- 商品id
        count(if(geval_scores_goods >= 9, 1, null)) as evaluation_good_count, -- 好评数, if(评分 >= 9, 1, null), 因为count(1)会被统计, count(null)会被忽略
        count(if(geval_scores_goods > 6 and geval_scores_goods < 9 , 1, null)) as evaluation_mid_count, -- 中评数
        count(if(geval_scores_goods <= 6, 1, null)) as evaluation_bad_count -- 差评数
    from yp_dwd.fact_goods_evaluation_detail
    group by substring(create_time, 1, 10), goods_id
),
-- step8: 对上述的6个结果做合并, 即: 合并 order_count, payment_count, refund_count, cart_count, favor_count, evaluation_count
unionall as (
    -- 1. order_count,  下单次数、下单件数、下单金额、
    select
        dt, sku_id, sku_name,
        order_count,
        order_num,
        order_amount,
        0 as payment_count,
        0 as payment_num,
        0 as payment_amount,
        0 as refund_count,
        0 as refund_num,
        0 as refund_amount,
        0 as cart_count,
        0 as cart_num,
        0 as favor_count,
        0 as evaluation_good_count,
        0 as evaluation_mid_count,
        0 as evaluation_bad_count
    from order_count
union all
    -- 2. payment_count, 被支付次数、被支付件数、被支付金额、
    select
        dt, sku_id, sku_name,
        0 as order_count,
        0 as order_num,
        0 as order_amount,
        payment_count,
        payment_num,
        payment_amount,
        0 as refund_count,
        0 as refund_num,
        0 as refund_amount,
        0 as cart_count,
        0 as cart_num,
        0 as favor_count,
        0 as evaluation_good_count,
        0 as evaluation_mid_count,
        0 as evaluation_bad_count
    from payment_count
union all
    -- 3. refund_count,  被退款次数、被退款件数、被退款金额、
    select
        dt, sku_id, sku_name,
        0 as order_count,
        0 as order_num,
        0 as order_amount,
        0 as payment_count,
        0 as payment_num,
        0 as payment_amount,
        refund_count,
        refund_num,
        refund_amount,
        0 as cart_count,
        0 as cart_num,
        0 as favor_count,
        0 as evaluation_good_count,
        0 as evaluation_mid_count,
        0 as evaluation_bad_count
    from refund_count
union all
    -- 4. cart_count,    被加入购物车次数、被加入购物车件数、     没有商品名
    select
        dt, sku_id, null as sku_name,
        0 as order_count,
        0 as order_num,
        0 as order_amount,
        0 as payment_count,
        0 as payment_num,
        0 as payment_amount,
        0 as refund_count,
        0 as refund_num,
        0 as refund_amount,
        cart_count,
        cart_num,
        0 as favor_count,
        0 as evaluation_good_count,
        0 as evaluation_mid_count,
        0 as evaluation_bad_count
    from cart_count
union all
    -- 5. favor_count,   被收藏次数、                        没有商品名
     select
        dt, sku_id, null as sku_name,
        0 as order_count,
        0 as order_num,
        0 as order_amount,
        0 as payment_count,
        0 as payment_num,
        0 as payment_amount,
        0 as refund_count,
        0 as refund_num,
        0 as refund_amount,
        0 as cart_count,
        0 as cart_num,
        favor_count,
        0 as evaluation_good_count,
        0 as evaluation_mid_count,
        0 as evaluation_bad_count
    from favor_count
union all
    -- 6. evaluation_count 好评数、中评数、差评数              没有商品名
    select
        dt, sku_id, null as sku_name,
        0 as order_count,
        0 as order_num,
        0 as order_amount,
        0 as payment_count,
        0 as payment_num,
        0 as payment_amount,
        0 as refund_count,
        0 as refund_num,
        0 as refund_amount,
        0 as cart_count,
        0 as cart_num,
        0 as favor_count,
        evaluation_good_count,
        evaluation_mid_count,
        evaluation_bad_count
    from evaluation_count
)
-- select * from unionall; -- 遇到的Bug, 因为我们是union all行合并, 所以同样的数据(日期, 商品id相同的数据) 对应的指标没有被放到一行, 我们要解决它.
-- step9: 合并后, 因为用的是union all, 所以相同数据(dt, sku_id均一样)的指标没有在一行, 所以可以通过 重新分组 + 聚合函数解决它, 具体如下:
select
        dt, sku_id,
        max(sku_name) as sku_name,
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
from unionall
group by dt, sku_id;
-- having dt='2020-07-18' and sku_id='34d6ba09801c11e998ec7cd30ad32e2e';  测试的


-- 3. 查询表数据, 上述步骤详见Presto代码.
select * from yp_dws.dws_sku_daycount;



-- --------------------------------- 案例6: 演示DWS层 用户主题日统计宽表搭建, 查询数据, 然后插入到表中  ---------------------------------
-- 1. 建表, 在hive中做.   维度: 用户, 日期.    指标: 9个

-- 2. 添加表数据到上述的表中, 思路和 商品主题日统计宽表一模一样.
-- 思路: 用多个CTE表达式存储每部分的值, 然后对这些值做合并操作, 可以是union all, 也可以是 full join
-- 此处略, 我直接跑脚本了.

-- 3. 查询表数据(结果)
select * from yp_dws.dws_user_daycount;





-- --------------------------------- 案例7: 演示DWS层 商品主题日统计宽表搭建, 查询数据, 然后插入到表中  ---------------------------------










































