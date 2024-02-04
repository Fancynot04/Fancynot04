----注意，如果session无法选中presto数据源----------
----把sql文件语法方言支持调整为Generic sql模式----------

insert into  hive.yp_dws.dws_sale_daycount
with  temp as (select
   -- 判断
	o.order_id , -- 订单id
	o.goods_id , -- 商品id
	o.order_from , --渠道信息: 小程序, 安卓...
	o.evaluation_id , -- 评价id(判断是否评价)
	o.geval_scores , -- 综合评分
	o.delievery_id , -- 配送id
	o.refund_id , --退款id
	o.dt as create_date, -- 下单日期
	-- 统计计算
	o.order_amount ,-- 订单金额
	o.total_price , --商品的金额
	o.plat_fee ,  --平台分润
	o.dispatcher_money , -- 配送费
	-- 维度字段
	s.city_id ,s.city_name ,  --城市
	s.trade_area_id ,s.trade_area_name , --商圈
	s.id as store_id ,s.store_name  ,-- 店铺
	g.brand_id ,g.brand_name , -- 品牌
	g.min_class_id ,g.min_class_name , --小类
	g.mid_class_id ,g.mid_class_name , --中类
	g.max_class_id ,g.max_class_name, --大类
	row_number() over(partition by order_id) as order_rn,
	row_number() over(partition by order_id,g.brand_id) as brand_rn,
	row_number() over(partition by order_id,g.max_class_name) as maxclass_rn,
	row_number() over(partition by order_id,g.max_class_name,g.mid_class_name) as midclass_rn,
	row_number() over(partition by order_id,g.max_class_name,g.mid_class_name,g.min_class_name) as minclass_rn,

	row_number() over(partition by order_id,g.brand_id,o.goods_id) as brand_goods_rn,
	row_number() over(partition by order_id,g.max_class_name,o.goods_id) as maxclass_goods_rn,
	row_number() over(partition by order_id,g.max_class_name,g.mid_class_name,o.goods_id) as midclass_goods_rn,
	row_number() over(partition by order_id,g.max_class_name,g.mid_class_name,g.min_class_name,o.goods_id) as minclass_goods_rn
from  hive.yp_dwb.dwb_order_detail o
	left join hive.yp_dwb.dwb_goods_detail g on o.goods_id  = g.id
	left join hive.yp_dwb.dwb_shop_detail  s on o.store_id = s.id )

select
	case when grouping(city_id) = 0
		then city_id
		else null end as city_id ,
	case when grouping(city_id) = 0
		then city_name
		else null end as city_name ,
	case when grouping(trade_area_id) = 0
		then trade_area_id
		else null end as trade_area_id ,
	case when grouping(trade_area_id) = 0
		then trade_area_name
		else null end as trade_area_name ,
	case when grouping(store_id) = 0
		then store_id
		else null end as store_id ,
	case when grouping(store_id) = 0
		then store_name
		else null end as store_name ,
	case when grouping(brand_id) = 0
		then brand_id
		else null end as brand_id ,
	case when grouping(brand_id) = 0
		then brand_name
		else null end as brand_name ,
	case when grouping(max_class_id) = 0
		then max_class_id
		else null end as max_class_id ,
	case when grouping(max_class_id) = 0
		then max_class_name
		else null end as max_class_name ,
	case when grouping(mid_class_id) = 0
		then mid_class_id
		else null end as mid_class_id ,
	case when grouping(mid_class_id) = 0
		then mid_class_name
		else null end as mid_class_name ,
	case when grouping(min_class_id) = 0
		then min_class_id
		else null end as min_class_id ,
	case when grouping(min_class_id) = 0
		then min_class_name
		else null end as min_class_name ,

	case when grouping(store_id,store_name) = 0
		then 'store'
		when grouping(trade_area_id ,trade_area_name) = 0
		then 'trade_area'
		when grouping (city_id,city_name) = 0
		then 'city'
		when grouping (brand_id,brand_name) = 0
		then 'brand'
		when grouping (min_class_id,min_class_name) = 0
		then 'min_class'
		when grouping (mid_class_id,mid_class_name) = 0
		then 'mid_class'
		when grouping (max_class_id,max_class_name) = 0
		then 'max_class'
		when grouping (create_date) = 0
		then 'all'
		else 'other' end as group_type,

		-- 销售收入
		case when grouping(store_id,store_name) =0
			then sum(if( order_rn = 1 and store_id is not null ,order_amount,0))
			when grouping (trade_area_id ,trade_area_name) = 0
			then sum(if( order_rn = 1 and trade_area_id is not null ,order_amount,0))
			when grouping (city_id,city_name) = 0
			then sum(if( order_rn = 1 and city_id is not null,order_amount,0))
			when grouping (brand_id,brand_name) = 0
			then sum(if(brand_goods_rn = 1 and brand_id is not null,total_price,0))
			when grouping (min_class_id,min_class_name) = 0
			then sum(if(minclass_goods_rn = 1 and min_class_id is not null ,total_price,0))
			when grouping (mid_class_id,mid_class_name) = 0
			then sum(if(midclass_goods_rn = 1 and mid_class_id is not null,total_price,0))
			when grouping (max_class_id,max_class_name) = 0
			then sum(if(maxclass_goods_rn = 1 and max_class_id is not null ,total_price,0))
			when grouping (create_date) = 0
			then sum(if(order_rn=1 and create_date is not null,order_amount,0))
			else null end  as sale_amt ,

		-- 平台收入
		case when grouping(store_id,store_name) =0
			then sum(if( order_rn = 1 and store_id is not null ,plat_fee,0))
			when grouping (trade_area_id ,trade_area_name) = 0
			then sum(if( order_rn = 1 and trade_area_id is not null ,plat_fee,0))
			when grouping (city_id,city_name) = 0
			then sum(if( order_rn = 1 and city_id is not null,plat_fee,0))
			when grouping (brand_id,brand_name) = 0
			then null
			when grouping (min_class_id,min_class_name) = 0
			then null
			when grouping (mid_class_id,mid_class_name) = 0
			then null
			when grouping (max_class_id,max_class_name) = 0
			then null
			when grouping (create_date) = 0
			then sum(if(order_rn=1 and create_date is not null,plat_fee,0))
			else null end  as plat_amt ,

		-- 配送成交额
		case when grouping(store_id,store_name) =0
			then sum(if( order_rn = 1 and store_id is not null and delievery_id is not null ,dispatcher_money,0))
			when grouping (trade_area_id ,trade_area_name) = 0
			then sum(if( order_rn = 1 and trade_area_id is not null and delievery_id is not null,dispatcher_money,0))
			when grouping (city_id,city_name) = 0
			then sum(if( order_rn = 1 and city_id is not null and delievery_id is not null,dispatcher_money,0))
			when grouping (brand_id,brand_name) = 0
			then null
			when grouping (min_class_id,min_class_name) = 0
			then null
			when grouping (mid_class_id,mid_class_name) = 0
			then null
			when grouping (max_class_id,max_class_name) = 0
			then null
			when grouping (create_date) = 0
			then sum(if(order_rn=1 and create_date is not null and delievery_id is not null ,dispatcher_money,0))
			else null end  as deliver_sale_amt ,

		-- 小程序成交额
		case when grouping(store_id,store_name) =0
			then sum(if( order_rn = 1 and store_id is not null and order_from='miniapp' ,order_amount,0))
			when grouping (trade_area_id ,trade_area_name) = 0
			then sum(if( order_rn = 1 and trade_area_id is not null and order_from='miniapp',order_amount,0))
			when grouping (city_id,city_name) = 0
			then sum(if( order_rn = 1 and city_id is not null and order_from='miniapp',order_amount,0))
			when grouping (brand_id,brand_name) = 0
			then sum(if(brand_goods_rn = 1 and brand_id is not null and order_from='miniapp',total_price,0))
			when grouping (min_class_id,min_class_name) = 0
			then sum(if(minclass_goods_rn = 1 and min_class_id is not null and order_from='miniapp',total_price,0))
			when grouping (mid_class_id,mid_class_name) = 0
			then sum(if(midclass_goods_rn = 1 and mid_class_id is not null and order_from='miniapp',total_price,0))
			when grouping (max_class_id,max_class_name) = 0
			then sum(if(maxclass_goods_rn = 1 and max_class_id is not null and order_from='miniapp',total_price,0))
			when grouping (create_date) = 0
			then sum(if(order_rn=1 and create_date is not null and order_from='miniapp',order_amount ,0))
			else null end  as mini_app_sale_amt ,

		-- 安卓成交额
		case when grouping(store_id,store_name) =0
			then sum(if( order_rn = 1 and store_id is not null and order_from='android' ,order_amount,0))
			when grouping (trade_area_id ,trade_area_name) = 0
			then sum(if( order_rn = 1 and trade_area_id is not null and order_from='android',order_amount,0))
			when grouping (city_id,city_name) = 0
			then sum(if( order_rn = 1 and city_id is not null and order_from='android',order_amount,0))
			when grouping (brand_id,brand_name) = 0
			then sum(if(brand_goods_rn = 1 and brand_id is not null and order_from='android',total_price,0))
			when grouping (min_class_id,min_class_name) = 0
			then sum(if(minclass_goods_rn = 1 and min_class_id is not null and order_from='android',total_price,0))
			when grouping (mid_class_id,mid_class_name) = 0
			then sum(if(midclass_goods_rn = 1 and mid_class_id is not null and order_from='android',total_price,0))
			when grouping (max_class_id,max_class_name) = 0
			then sum(if(maxclass_goods_rn = 1 and max_class_id is not null and order_from='android',total_price,0))
			when grouping (create_date) = 0
			then sum(if(order_rn=1 and create_date is not null and order_from='android',order_amount ,0))
			else null end  as android_sale_amt ,

		-- 苹果成交额
		case when grouping(store_id,store_name) =0
			then sum(if( order_rn = 1 and store_id is not null and order_from='ios' ,order_amount,0))
			when grouping (trade_area_id ,trade_area_name) = 0
			then sum(if( order_rn = 1 and trade_area_id is not null and order_from='ios',order_amount,0))
			when grouping (city_id,city_name) = 0
			then sum(if( order_rn = 1 and city_id is not null and order_from='ios',order_amount,0))
			when grouping (brand_id,brand_name) = 0
			then sum(if(brand_goods_rn = 1 and brand_id is not null and order_from='ios',total_price,0))
			when grouping (min_class_id,min_class_name) = 0
			then sum(if(minclass_goods_rn = 1 and min_class_id is not null and order_from='ios',total_price,0))
			when grouping (mid_class_id,mid_class_name) = 0
			then sum(if(midclass_goods_rn = 1 and mid_class_id is not null and order_from='ios',total_price,0))
			when grouping (max_class_id,max_class_name) = 0
			then sum(if(maxclass_goods_rn = 1 and max_class_id is not null and order_from='ios',total_price,0))
			when grouping (create_date) = 0
			then sum(if(order_rn=1 and create_date is not null and order_from='ios',order_amount ,0))
			else null end  as ios_sale_amt ,

		-- pc成交额
		case when grouping(store_id,store_name) =0
			then sum(if( order_rn = 1 and store_id is not null and order_from='pcweb' ,order_amount,0))
			when grouping (trade_area_id ,trade_area_name) = 0
			then sum(if( order_rn = 1 and trade_area_id is not null and order_from='pcweb',order_amount,0))
			when grouping (city_id,city_name) = 0
			then sum(if( order_rn = 1 and city_id is not null and order_from='pcweb',order_amount,0))
			when grouping (brand_id,brand_name) = 0
			then sum(if(brand_goods_rn = 1 and brand_id is not null and order_from='pcweb',total_price,0))
			when grouping (min_class_id,min_class_name) = 0
			then sum(if(minclass_goods_rn = 1 and min_class_id is not null and order_from='pcweb',total_price,0))
			when grouping (mid_class_id,mid_class_name) = 0
			then sum(if(midclass_goods_rn = 1 and mid_class_id is not null and order_from='pcweb',total_price,0))
			when grouping (max_class_id,max_class_name) = 0
			then sum(if(maxclass_goods_rn = 1 and max_class_id is not null and order_from='pcweb',total_price,0))
			when grouping (create_date) = 0
			then sum(if(order_rn=1 and create_date is not null and order_from='pcweb',order_amount ,0))
			else null end  as pcweb_sale_amt ,

		-- 订单量
		case when grouping(store_id,store_name) =0
			then count(if(order_rn=1 and store_id is not null , order_id,null))
			when grouping (trade_area_id ,trade_area_name) = 0
			then count(if(order_rn=1 and trade_area_id is not null , order_id,null))
			when grouping (city_id,city_name) = 0
			then count(if(order_rn=1 and city_id is not null , order_id,null))
			when grouping (brand_id,brand_name) = 0
			then count(if(brand_rn=1 and brand_id is not null , order_id,null))
			when grouping (min_class_id,min_class_name) = 0
			then count(if(minclass_rn=1 and min_class_id is not null , order_id,null))
			when grouping (mid_class_id,mid_class_name) = 0
			then count(if(midclass_rn=1 and mid_class_id is not null , order_id,null))
			when grouping (max_class_id,max_class_name) = 0
			then count(if(maxclass_rn=1 and max_class_id is not null , order_id,null))
			when grouping (create_date) = 0
			then count(if(order_rn=1 , order_id,null))
			else null end  as order_cnt ,

		-- 参评单量
		case when grouping(store_id,store_name) =0
			then count(if(order_rn=1 and store_id is not null and evaluation_id is not null , order_id,null))
			when grouping (trade_area_id ,trade_area_name) = 0
			then count(if(order_rn=1 and trade_area_id is not null and evaluation_id is not null , order_id,null))
			when grouping (city_id,city_name) = 0
			then count(if(order_rn=1 and city_id is not null and evaluation_id is not null , order_id,null))
			when grouping (brand_id,brand_name) = 0
			then count(if(brand_rn=1 and brand_id is not null and evaluation_id is not null , order_id,null))
			when grouping (min_class_id,min_class_name) = 0
			then count(if(minclass_rn=1 and min_class_id is not null and evaluation_id is not null , order_id,null))
			when grouping (mid_class_id,mid_class_name) = 0
			then count(if(midclass_rn=1 and mid_class_id is not null  and evaluation_id is not null, order_id,null))
			when grouping (max_class_id,max_class_name) = 0
			then count(if(maxclass_rn=1 and max_class_id is not null  and evaluation_id is not null, order_id,null))
			when grouping (create_date) = 0
			then count(if(order_rn=1 and evaluation_id is not null, order_id,null))
			else null end  as eva_order_cnt ,
		--差评单量
		case when grouping(store_id,store_name) =0
			then count(if(order_rn=1 and store_id is not null and evaluation_id is not null and coalesce(geval_scores,0) <6 , order_id,null))
			when grouping (trade_area_id ,trade_area_name) = 0
			then count(if(order_rn=1 and trade_area_id is not null and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
			when grouping (city_id,city_name) = 0
			then count(if(order_rn=1 and city_id is not null and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
			when grouping (brand_id,brand_name) = 0
			then count(if(brand_rn=1 and brand_id is not null and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
			when grouping (min_class_id,min_class_name) = 0
			then count(if(minclass_rn=1 and min_class_id is not null and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
			when grouping (mid_class_id,mid_class_name) = 0
			then count(if(midclass_rn=1 and mid_class_id is not null  and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
			when grouping (max_class_id,max_class_name) = 0
			then count(if(maxclass_rn=1 and max_class_id is not null  and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
			when grouping (create_date) = 0
			then count(if(order_rn=1 and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
			else null end  as bad_eva_order_cnt ,

		--配送单量
		case when grouping(store_id,store_name) =0
			then count(if(order_rn=1 and store_id is not null and delievery_id is not null, order_id,null))
			when grouping (trade_area_id ,trade_area_name) = 0
			then count(if(order_rn=1 and trade_area_id is not null and delievery_id is not null, order_id,null))
			when grouping (city_id,city_name) = 0
			then count(if(order_rn=1 and city_id is not null and delievery_id is not null, order_id,null))
			when grouping (brand_id,brand_name) = 0
			then count(if(brand_rn=1 and brand_id is not null and delievery_id is not null, order_id,null))
			when grouping (min_class_id,min_class_name) = 0
			then count(if(minclass_rn=1 and min_class_id is not null and delievery_id is not null, order_id,null))
			when grouping (mid_class_id,mid_class_name) = 0
			then count(if(midclass_rn=1 and mid_class_id is not null  and delievery_id is not null, order_id,null))
			when grouping (max_class_id,max_class_name) = 0
			then count(if(maxclass_rn=1 and max_class_id is not null and delievery_id is not null, order_id,null))
			when grouping (create_date) = 0
			then count(if(order_rn=1 and delievery_id is not null, order_id,null))
			else null end  as deliver_order_cnt ,

		--退款单量
		case when grouping(store_id,store_name) =0
			then count(if(order_rn=1 and store_id is not null and refund_id is not null, order_id,null))
			when grouping (trade_area_id ,trade_area_name) = 0
			then count(if(order_rn=1 and trade_area_id is not null and refund_id is not null, order_id,null))
			when grouping (city_id,city_name) = 0
			then count(if(order_rn=1 and city_id is not null and refund_id is not null, order_id,null))
			when grouping (brand_id,brand_name) = 0
			then count(if(brand_rn=1 and brand_id is not null and refund_id is not null, order_id,null))
			when grouping (min_class_id,min_class_name) = 0
			then count(if(minclass_rn=1 and min_class_id is not null and refund_id is not null, order_id,null))
			when grouping (mid_class_id,mid_class_name) = 0
			then count(if(midclass_rn=1 and mid_class_id is not null  and refund_id is not null, order_id,null))
			when grouping (max_class_id,max_class_name) = 0
			then count(if(maxclass_rn=1 and max_class_id is not null and refund_id is not null, order_id,null))
			when grouping (create_date) = 0
			then count(if(order_rn=1 and refund_id is not null, order_id,null))
			else null end  as refund_order_cnt ,

		-- 小程序订单量
		case when grouping(store_id,store_name) =0
			then count(if(order_rn=1 and store_id is not null and order_from = 'miniapp', order_id,null))
			when grouping (trade_area_id ,trade_area_name) = 0
			then count(if(order_rn=1 and trade_area_id is not null and order_from = 'miniapp', order_id,null))
			when grouping (city_id,city_name) = 0
			then count(if(order_rn=1 and city_id is not null and order_from = 'miniapp', order_id,null))
			when grouping (brand_id,brand_name) = 0
			then count(if(brand_rn=1 and brand_id is not null and order_from = 'miniapp', order_id,null))
			when grouping (min_class_id,min_class_name) = 0
			then count(if(minclass_rn=1 and min_class_id is not null and order_from = 'miniapp', order_id,null))
			when grouping (mid_class_id,mid_class_name) = 0
			then count(if(midclass_rn=1 and mid_class_id is not null  and order_from = 'miniapp', order_id,null))
			when grouping (max_class_id,max_class_name) = 0
			then count(if(maxclass_rn=1 and max_class_id is not null and order_from = 'miniapp', order_id,null))
			when grouping (create_date) = 0
			then count(if(order_rn=1 and order_from = 'miniapp', order_id,null))
			else null end  as miniapp_order_cnt ,

		-- android订单量
		case when grouping(store_id,store_name) =0
			then count(if(order_rn=1 and store_id is not null and order_from = 'android', order_id,null))
			when grouping (trade_area_id ,trade_area_name) = 0
			then count(if(order_rn=1 and trade_area_id is not null and order_from = 'android', order_id,null))
			when grouping (city_id,city_name) = 0
			then count(if(order_rn=1 and city_id is not null and order_from = 'android', order_id,null))
			when grouping (brand_id,brand_name) = 0
			then count(if(brand_rn=1 and brand_id is not null and order_from = 'android', order_id,null))
			when grouping (min_class_id,min_class_name) = 0
			then count(if(minclass_rn=1 and min_class_id is not null and order_from = 'android', order_id,null))
			when grouping (mid_class_id,mid_class_name) = 0
			then count(if(midclass_rn=1 and mid_class_id is not null  and order_from = 'android', order_id,null))
			when grouping (max_class_id,max_class_name) = 0
			then count(if(maxclass_rn=1 and max_class_id is not null and order_from = 'android', order_id,null))
			when grouping (create_date) = 0
			then count(if(order_rn=1 and order_from = 'android', order_id,null))
			else null end  as android_order_cnt ,

		-- ios订单量
		case when grouping(store_id,store_name) =0
			then count(if(order_rn=1 and store_id is not null and order_from = 'ios', order_id,null))
			when grouping (trade_area_id ,trade_area_name) = 0
			then count(if(order_rn=1 and trade_area_id is not null and order_from = 'ios', order_id,null))
			when grouping (city_id,city_name) = 0
			then count(if(order_rn=1 and city_id is not null and order_from = 'ios', order_id,null))
			when grouping (brand_id,brand_name) = 0
			then count(if(brand_rn=1 and brand_id is not null and order_from = 'ios', order_id,null))
			when grouping (min_class_id,min_class_name) = 0
			then count(if(minclass_rn=1 and min_class_id is not null and order_from = 'ios', order_id,null))
			when grouping (mid_class_id,mid_class_name) = 0
			then count(if(midclass_rn=1 and mid_class_id is not null  and order_from = 'ios', order_id,null))
			when grouping (max_class_id,max_class_name) = 0
			then count(if(maxclass_rn=1 and max_class_id is not null and order_from = 'ios', order_id,null))
			when grouping (create_date) = 0
			then count(if(order_rn=1 and order_from = 'ios', order_id,null))
			else null end  as ios_order_cnt ,

		-- pcweb订单量
		case when grouping(store_id,store_name) =0
			then count(if(order_rn=1 and store_id is not null and order_from = 'pcweb', order_id,null))
			when grouping (trade_area_id ,trade_area_name) = 0
			then count(if(order_rn=1 and trade_area_id is not null and order_from = 'pcweb', order_id,null))
			when grouping (city_id,city_name) = 0
			then count(if(order_rn=1 and city_id is not null and order_from = 'pcweb', order_id,null))
			when grouping (brand_id,brand_name) = 0
			then count(if(brand_rn=1 and brand_id is not null and order_from = 'pcweb', order_id,null))
			when grouping (min_class_id,min_class_name) = 0
			then count(if(minclass_rn=1 and min_class_id is not null and order_from = 'pcweb', order_id,null))
			when grouping (mid_class_id,mid_class_name) = 0
			then count(if(midclass_rn=1 and mid_class_id is not null  and order_from = 'pcweb', order_id,null))
			when grouping (max_class_id,max_class_name) = 0
			then count(if(maxclass_rn=1 and max_class_id is not null and order_from = 'pcweb', order_id,null))
			when grouping (create_date) = 0
			then count(if(order_rn=1 and order_from = 'pcweb', order_id,null))
			else null end  as pcweb_order_cnt ,
		create_date as dt
from  temp
group by
 	grouping sets (
 		create_date,
 		(create_date,city_id,city_name),
 		(create_date,city_id,city_name,trade_area_id ,trade_area_name),
 		(create_date,city_id,city_name,trade_area_id ,trade_area_name,store_id,store_name),
 		(create_date,brand_id,brand_name),
 		(create_date,max_class_id,max_class_name),
 		(create_date,max_class_id,max_class_name,mid_class_id,mid_class_name),
 		(create_date,max_class_id,max_class_name,mid_class_id,mid_class_name,min_class_id,min_class_name)
 	);