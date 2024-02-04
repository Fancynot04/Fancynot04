1. DWS层--商品主题日统计宽表实现.
-- 1.1 建表, 在Hive中写.
create table yp_dws.dws_sku_daycount(
	-- 维度字段, 日期 + 商品
	dt string,
	sku_id string,
	sku_name string,
	
	-- 指标字段, 15个指标
	order_count int,
	order_num int,
	order_amount decimal(38, 2)
	......
) comment '商品主题日统计宽表'
row format delimited fields terminated by '\t'
stored as orc tblproperties('orc.compress'='snappy');

-- 1.2 往表中插入数据.  思路: 多组CTE表达式存储结果, 然后拼接即可. 

-- step10: 把下述的数据, 插入到 yp_dws.dws_sku_daycount 表中.
insert into hive.yp_dws.dws_sku_daycount

-- step1: 准备计算 下单, 支付, 退款相关指标所需的基础数据.
with order_base as (....),
-- step2: 计算 下单次数, 件数, 金额指标
order_count as (....), 
-- step3: 计算 被支付次数, 件数, 金额指标
payment_count as (....), 
-- step4: 计算 退款次数, 件数, 金额指标
refund_count as (....), 
-- step5: 计算 购物车次数, 件数
cart_count as (....), 
-- step6: 计算 被收藏次数
favor_count as (....), 
-- step7: 计算 好评, 中评, 差评数
evaluation_count as (....), 
-- step8: 把上述的数据合并到一起, 思路1: union all,  思路2: full outer join
unionall as (
	order_count
	union all
	payment_count
	......
)
-- step9: 把相同数据(dt, sku_id均相同)的多个指标, 合并到一行.
select 
	dt, sku_id, max(sku_name),
	sum(order_count),
	sum(order_num),
	sum(order_amount),
	-- ......
from
	unionall
group by dt, sku_id;

-- 1.3 查询表数据, 因为是Presto方式插入的数据, 所以去Presto中查询.
select * from yp_dws.dws_sku_daycount;


2. DWS层--用户主题日统计宽表实现.
	-- 思路 和 商品主题一致, 只不过是表, 字段不同, 这里不再赘述.
	
	
3. 关于索引的问题.
	回顾MySQL中的索引:
		概述:
		好处:
		弊端:
		分类(常用):
			1. 主键索引
			2. 唯一索引
			3. 普通索引
	Hive中的索引问题:
		Hive0.7X开始支持索引, 3.X开始移除了索引.
		
		Orc(列存储)格式自带索引:
			Row Group Index: 行组索引
			
			Bloom Filter Index: 布隆过滤器索引