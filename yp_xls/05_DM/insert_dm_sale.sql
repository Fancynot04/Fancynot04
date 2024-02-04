insert into yp_dm.dm_sale
-- 获取日期数据（周、月的环比/同比日期）
with dt1 as (
  select
   dim_date_id, date_code
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
select
-- 统计日期
   '2021-03-17' as date_time,
-- 时间维度      year、month、date
   case when grouping(dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id) = 0
      then 'date'
       when grouping(dt1.year_code, dt1.year_week_name_cn) = 0
      then 'week'
      when grouping(dt1.year_code, dt1.month_code, dt1.year_month) = 0
      then 'month'
      when grouping(dt1.year_code) = 0
      then 'year'
   end
   as time_type,
   dt1.year_code,
   dt1.year_month,
   dt1.month_code,
   dt1.day_month_num, --几号
   dt1.dim_date_id,
    dt1.year_week_name_cn,  --第几周
-- 产品维度类型：store，trade_area，city，brand，min_class，mid_class，max_class，all
   CASE WHEN grouping(dc.city_id, dc.trade_area_id, dc.store_id)=0
         THEN 'store'
         WHEN grouping(dc.city_id, dc.trade_area_id)=0
         THEN 'trade_area'
         WHEN grouping(dc.city_id)=0
         THEN 'city'
         WHEN grouping(dc.brand_id)=0
         THEN 'brand'
         WHEN grouping(dc.max_class_id, dc.mid_class_id, dc.min_class_id)=0
         THEN 'min_class'
         WHEN grouping(dc.max_class_id, dc.mid_class_id)=0
         THEN 'mid_class'
         WHEN grouping(dc.max_class_id)=0
         THEN 'max_class'
         ELSE 'all'
         END as group_type,
   dc.city_id,
   dc.city_name,
   dc.trade_area_id,
   dc.trade_area_name,
   dc.store_id,
   dc.store_name,
   dc.brand_id,
   dc.brand_name,
   dc.max_class_id,
   dc.max_class_name,
   dc.mid_class_id,
   dc.mid_class_name,
   dc.min_class_id,
   dc.min_class_name,
-- 统计值
    sum(dc.sale_amt) as sale_amt,
   sum(dc.plat_amt) as plat_amt,
   sum(dc.deliver_sale_amt) as deliver_sale_amt,
   sum(dc.mini_app_sale_amt) as mini_app_sale_amt,
   sum(dc.android_sale_amt) as android_sale_amt,
   sum(dc.ios_sale_amt) as ios_sale_amt,
   sum(dc.pcweb_sale_amt) as pcweb_sale_amt,

   sum(dc.order_cnt) as order_cnt,
   sum(dc.eva_order_cnt) as eva_order_cnt,
   sum(dc.bad_eva_order_cnt) as bad_eva_order_cnt,
   sum(dc.deliver_order_cnt) as deliver_order_cnt,
   sum(dc.refund_order_cnt) as refund_order_cnt,
   sum(dc.miniapp_order_cnt) as miniapp_order_cnt,
   sum(dc.android_order_cnt) as android_order_cnt,
   sum(dc.ios_order_cnt) as ios_order_cnt,
   sum(dc.pcweb_order_cnt) as pcweb_order_cnt
from yp_dws.dws_sale_daycount dc
   left join dt1 on dc.dt = dt1.date_code
group by
grouping sets (
-- 年，注意养成加小括号的习惯
   (dt1.year_code),
   (dt1.year_code, city_id, city_name),
   (dt1.year_code, city_id, city_name, trade_area_id, trade_area_name),
   (dt1.year_code, city_id, city_name, trade_area_id, trade_area_name, store_id, store_name),
    (dt1.year_code, brand_id, brand_name),
    (dt1.year_code, max_class_id, max_class_name),
    (dt1.year_code, max_class_id, max_class_name,mid_class_id, mid_class_name),
    (dt1.year_code, max_class_id, max_class_name,mid_class_id, mid_class_name,min_class_id, min_class_name),
--  月
   (dt1.year_code, dt1.month_code, dt1.year_month),
   (dt1.year_code, dt1.month_code, dt1.year_month, city_id, city_name),
   (dt1.year_code, dt1.month_code, dt1.year_month, city_id, city_name, trade_area_id, trade_area_name),
   (dt1.year_code, dt1.month_code, dt1.year_month, city_id, city_name, trade_area_id, trade_area_name, store_id, store_name),
    (dt1.year_code, dt1.month_code, dt1.year_month, brand_id, brand_name),
    (dt1.year_code, dt1.month_code, dt1.year_month, max_class_id, max_class_name),
    (dt1.year_code, dt1.month_code, dt1.year_month, max_class_id, max_class_name,mid_class_id, mid_class_name),
    (dt1.year_code, dt1.month_code, dt1.year_month, max_class_id, max_class_name,mid_class_id, mid_class_name,min_class_id, min_class_name),
-- 日
   (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id),
   (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id, city_id, city_name),
   (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id, city_id, city_name, trade_area_id, trade_area_name),
   (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id, city_id, city_name, trade_area_id, trade_area_name, store_id, store_name),
    (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id, brand_id, brand_name),
    (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id, max_class_id, max_class_name),
    (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id, max_class_id, max_class_name,mid_class_id, mid_class_name),
    (dt1.year_code, dt1.month_code, dt1.day_month_num, dt1.dim_date_id, max_class_id, max_class_name,mid_class_id, mid_class_name,min_class_id, min_class_name),
--  周
   (dt1.year_code, dt1.year_week_name_cn),
   (dt1.year_code, dt1.year_week_name_cn, city_id, city_name),
   (dt1.year_code, dt1.year_week_name_cn, city_id, city_name, trade_area_id, trade_area_name),
   (dt1.year_code, dt1.year_week_name_cn, city_id, city_name, trade_area_id, trade_area_name, store_id, store_name),
    (dt1.year_code, dt1.year_week_name_cn, brand_id, brand_name),
    (dt1.year_code, dt1.year_week_name_cn, max_class_id, max_class_name),
    (dt1.year_code, dt1.year_week_name_cn, max_class_id, max_class_name,mid_class_id, mid_class_name),
    (dt1.year_code, dt1.year_week_name_cn, max_class_id, max_class_name,mid_class_id, mid_class_name,min_class_id, min_class_name)
)
-- order by time_type desc
;