1. 完成DM层-销售主题统计宽表(年月周日)
-- 动作1: 求年的8种维度组合情况.
-- step1: 梳理表关系.
select
	-- step5: 和日期相关的所有维度
	dt,
	year_code,
	year_month,
	month_code,
	day_of_month,
	dim_date_id,
	year_week_name_cnt,
	case
		when grouping(dim_date_id)=0 then 'date'
		......
	end as time_type, 
	-- step4: 完成出日期维度外, 其它维度的计算.
	-- group_type 分组类型.
	case
		when grouping(store_id, store_name)=0 then 'store'				-- 日期 + 城市 + 商圈 + 店铺
		when grouping(trade_area_id, trade_area_name)=0 then 'trade'	-- 日期 + 城市 + 商圈
		......
	end as group_type,
	
	-- 14个维度字段
	city_id, city_name,
	......
	
	-- step3: 完成 16个 指标计算.
	sum(sale_amt) as sale_amt,
	sum(plat_amt) as plat_amt,
	......
from
	yp_dws.dws_sale_daycount dc		-- 销售主题日统计宽表
	left join yp_dwd.dim_date d on dc.dt = d.date_code		-- 日期维度表.
-- step2: 年的8种维度.
group by
grouping sets(
	(year_code),							-- 日期(年)
	(year_code, city_id, city_name),		-- 日期(年) + 城市
	......
);

-- 动作2: 基于上述的SQL, 改造成最终的SQL即可. 
	1. 用CTE表达式封装, 去 yp_dwd.dim_date表中抽取我们要的字段, 然后和 yp_dws.dws_sale_daycount 关联.
	2. 去grouping sets()中完成 32个维度的操作.
	3. 完善日期相关的维度字段的操作. 
	4. 如果要先使用精准判断, 则可以通过 grouping(列1, 列2, 列3, 列4...) = 二进制转成的十进制的值
	
	
2. 完成DM层-商品主题统计宽表(总累计, 近30天累计)
-- 第1阶段: 首次计算 总累计(从开始时间 ~ 当前时间, 按照sku_id分组, sum累加即可)
-- 			首次计算 近30天累计(today - 30 ~ today), 按照sku_id分组, sum累加即可

-- step1: 创建 yp_dm.dm_sku 表,    Hive中完成. 
create table yp_dm.dm_sku(
	-- 商品id
	sku_id string,
	-- 各种指标, 总累计, 近30天累计
) comment '商品主题统计宽表'
row format delimited fields terminated by '\t'
stored as orc tblproperties('orc.compress'='snappy');

-- step3: 把上述结果插入到 yp_dm.dm_sku表中.

insert into hive.yp_dm.dm_sku
-- step2: 首次计算 总累计 和 近30天累计.
with all_count as (
	select 
		sku_id,
		sum(order_count) as order_count, 		-- 下单次数, 总累计
		sum(order_num) as order_num, 			-- 下单件数
		sum(order_amount) as order_amount, 		-- 下单金额
		....
	from
		yp_dws.dws_sku_daycount
	group by sku_id
), 
last_30d as (
	select 
		sku_id,
		sum(order_count) as order_last_30d_count, 		-- 下单次数, 近30天累计
		sum(order_num) as order_last_30d_num, 			-- 下单件数
		sum(order_amount) as order_last_30d_amount, 	-- 下单金额
		....
	from
		yp_dws.dws_sku_daycount
	where dt >= cast(date_add('day', -30, date '2020-05-08') as varchar)
	group by sku_id
)
-- 把上述的结果(总累计, 30天累计)汇总.
select 
	ac.sku_id,
	ac.order_count,
	ac.order_num,
	ac.order_amount,
	
	l30.order_last_30d_count,
	l30.order_last_30d_num,
	l30.order_last_30d_amount,
	
from all_count ac left join last_30d l30 on ac.sku_id = l30.sku_id;


-- step4: 查询结果.
select * from yp_dm.dm_sku;



3. 第2阶段: 循环计算 总累计: 	  旧的总累计 + 新增1天的数据.
			循环计算 近30天累计: (today - 30 ~ today), 按照sku_id分组, sum累加即可
-- step1: 创建 yp_dm.dm_sku_tmp表, 用于存储最新的数据(总累计, 近30天累计)			
create table yp_dm.dm_sku_tmp(...)...;
			

-- step4: 把下述的结果插入到 临时包中.
insert into yp_dm.dm_sku_tmp

-- step2: 循环计算 总累计 和 近30天累计
with old as (
	select * from yp_dm.dm_sku		-- 旧的总累计
),
new as (
	select 
		sku_id,		-- 商品id
	-- 计算 最新1天累计.
		sum(if(dt='2020-05-09', order_count, 0)) as order_count_1d,
		sum(if(dt='2020-05-09', order_num, 0)) as order_num_1d,
		sum(if(dt='2020-05-09', order_amount, 0)) as order_amount_1d,
		
	-- 计算 近30天累计
		sum(order_count) as order_count30,
		sum(order_num) as order_num30,
		sum(order_amount) as order_amount30,
	from
		yp_dws.dws_sku_daycount
	where dt >= cast(date_add('day', -30, date '2020-05-09') as varchar)
	group by sku_id	
)
-- step3: 基于上述的数据, 计算 最终结果.
select
	coalesce(old.sku_id, new_sku_id) as sku_id,
	-- 最新的30天累计
	coalesce(new.order_count30, 0) as order_count,
	coalesce(new.order_num30, 0) as order_num,
	coalesce(new.order_amount30, 0) as order_amount,
	...
	
	-- 新的总累计 = 旧的总累计 + 最新1天的数据.
	coalesce(old.order_count, 0) + coalesce(new.order_count_1d, 0) as order_count,
	coalesce(old.order_num, 0) + coalesce(new.order_num_1d, 0) as order_num,
	coalesce(old.order_amount, 0) + coalesce(new.order_amount_1d, 0) as order_amount,
	
from
	old full join new on old.sku_id = new.sku_id;


-- step5: 删除旧的累计数据.
delete from yp_dm.dm_sku;

-- step6: 用临时表(最新数据) 覆盖 yp_dm.dm_sku;
insert into yp_dm.dm_sku select * from yp_dm.dm_sku_tmp;

-- step7: 查询最终结果.
select * from yp_dm.dm_sku;

