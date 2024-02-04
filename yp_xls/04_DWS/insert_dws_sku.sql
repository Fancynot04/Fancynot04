----注意，如果session无法选中presto数据源----------
----把sql文件语法方言支持调整为Generic sql模式----------

insert into hive.yp_dws.dws_sku_daycount
with order_cnt as (select
	dt ,pay_time ,apply_date ,goods_id , goods_name , order_state ,is_pay ,refund_id ,refund_state ,
	order_id ,buy_num ,total_price ,
	row_number() over(partition by order_id , goods_id) as rn1
from hive.yp_dwb.dwb_order_detail),

goods_order_1  as (select
	dt,
	goods_id,
	goods_name,
	count(order_id) as order_count,
	sum(buy_num) as order_num,
	sum(total_price) as order_amount
from order_cnt where rn1 = 1
group by goods_id,goods_name , dt) ,

goods_pay_2 as (select
	substring(pay_time,1,10) as dt,
	goods_id,
	goods_name,
	count(order_id) as payment_count,
	sum(buy_num) as payment_num,
	sum(total_price) as payment_amount
from order_cnt where rn1 = 1 and  order_state not in(1,7) and is_pay=1
group by goods_id,goods_name, substring(pay_time,1,10)),

goods_refund_3 as(select
	substring(apply_date,1,10) as dt,
	goods_id,
	goods_name,
	count(order_id) as refund_count,
	sum(buy_num) as refund_num,
	sum(total_price) as refund_amount
from order_cnt where rn1 = 1 and refund_id is not null and refund_state = 5
group by  goods_id , goods_name , substring(apply_date,1,10)),

goods_cart_4 as (select
	 substring(c.create_time ,1,10) as  dt,
	 c.goods_id  ,
	 d1.goods_name ,
	 count(c.id) as cart_count,
	 sum(c.buy_num) as cart_num
from hive.yp_dwd.fact_shop_cart c join hive.yp_dwb.dwb_goods_detail  d1
	on c.goods_id  = d1.id
group by c.goods_id  , d1.goods_name , substring(c.create_time ,1,10)),

goods_favor_5 as (select
	 substring(gc.create_time ,1,10) as dt,
	 gc.goods_id ,
	 d2.goods_name ,
	 count(gc.id) as favor_count
from hive.yp_dwd.fact_goods_collect gc join hive.yp_dwb.dwb_goods_detail  d2
	on gc.goods_id  = d2.id
group  by gc.goods_id , d2.goods_name , substring(gc.create_time ,1,10)) ,

goods_eval_6 as (select
	substring(g.create_time ,1,10)  as dt,
	g.goods_id,
	d3.goods_name,
	-- 低于 6分  差评 , 6~8分(包含)  8以上好评
	count(
		if( g.geval_scores_goods is null OR g.geval_scores_goods > 8 , g.id ,null )
		) as evaluation_good_count,
	count(
		if( g.geval_scores_goods is not null and g.geval_scores_goods between 6 and 8 , g.id ,null )
		) as evaluation_mid_count,
	count(
		if( g.geval_scores_goods is not null and g.geval_scores_goods < 6 , g.id ,null )
		) as evaluation_bad_count
from hive.yp_dwd.fact_goods_evaluation_detail  g join hive.yp_dwb.dwb_goods_detail  d3
	on  g.goods_id  = d3.id
group by  g.goods_id,d3.goods_name,substring(g.create_time ,1,10)),

temp as (select
	coalesce(goods_order_1.dt,goods_pay_2.dt,goods_refund_3.dt,goods_cart_4.dt,goods_favor_5.dt,goods_eval_6.dt) as dt,
	coalesce(goods_order_1.goods_id,goods_pay_2.goods_id,goods_refund_3.goods_id,goods_cart_4.goods_id,goods_favor_5.goods_id,goods_eval_6.goods_id) as sku_id,
	coalesce(goods_order_1.goods_name,goods_pay_2.goods_name,goods_refund_3.goods_name,goods_cart_4.goods_name,goods_favor_5.goods_name,goods_eval_6.goods_name) as sku_name,

	coalesce(goods_order_1.order_count,0) as order_count,
	coalesce(goods_order_1.order_num,0) as order_num,
	coalesce(goods_order_1.order_amount,0) as order_amount,

	coalesce(goods_pay_2.payment_count,0) as payment_count,
	coalesce(goods_pay_2.payment_num,0) as payment_num,
	coalesce(goods_pay_2.payment_amount,0) as payment_amount,

	coalesce(goods_refund_3.refund_count,0) as refund_count,
	coalesce(goods_refund_3.refund_num,0) as refund_num,
	coalesce(goods_refund_3.refund_amount,0) as refund_amount,

	coalesce(goods_cart_4.cart_count,0) as cart_count,
	coalesce(goods_cart_4.cart_num,0) as cart_num,

	coalesce(goods_favor_5.favor_count,0) as favor_count,

	coalesce(goods_eval_6.evaluation_good_count,0) as evaluation_good_count,
	coalesce(goods_eval_6.evaluation_mid_count,0) as evaluation_mid_count,
	coalesce(goods_eval_6.evaluation_bad_count,0) as evaluation_bad_count
from  goods_order_1
	full join goods_pay_2 on goods_order_1.goods_id=goods_pay_2.goods_id and goods_order_1.dt = goods_pay_2.dt
	full join goods_refund_3 on goods_order_1.goods_id = goods_refund_3.goods_id and goods_order_1.dt = goods_refund_3.dt
	full join goods_cart_4 on goods_order_1.goods_id = goods_cart_4.goods_id and goods_order_1.dt = goods_cart_4.dt
	full join goods_favor_5 on goods_order_1.goods_id = goods_favor_5.goods_id and goods_order_1.dt = goods_favor_5.dt
	full join goods_eval_6 on goods_order_1.goods_id = goods_eval_6.goods_id and goods_order_1.dt = goods_eval_6.dt)

select
	dt,
	sku_id, sku_name,
	sum(order_count),
	sum(order_num),
	sum(order_amount),
	sum(payment_count),
	sum(payment_num),
	sum(payment_amount),
	sum(refund_count),
	sum(refund_num),
	sum(refund_amount),
	sum(cart_count),
	sum(cart_num),
	sum(favor_count),
	sum(evaluation_good_count),
	sum(evaluation_mid_count),
	sum(evaluation_bad_count)
from  temp
group by sku_id, sku_name,dt;

