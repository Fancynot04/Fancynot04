-- 首次执行，需要计算总累计值
insert into yp_dm.dm_sku
with all_count as (
    select
        sum(order_count) as order_count,
        sum(order_num) as order_num,
        sum(order_amount) as order_amount,
        sum(payment_count) payment_count,
        sum(payment_num) payment_num,
        sum(payment_amount) payment_amount,
        sum(refund_count) refund_count,
        sum(refund_num) refund_num,
        sum(refund_amount) refund_amount,
        sum(cart_count) cart_count,
        sum(cart_num) cart_num,
        sum(favor_count) favor_count,
        sum(evaluation_good_count)   evaluation_good_count,
        sum(evaluation_mid_count)    evaluation_mid_count,
        sum(evaluation_bad_count)    evaluation_bad_count,
       sku_id
    from yp_dws.dws_sku_daycount
--     where order_count > 0
    group by sku_id
),
last_30d as (
    select
        sum(order_count) order_last_30d_count,
        sum(order_num) order_last_30d_num,
        sum(order_amount) as order_last_30d_amount,

        sum(payment_count) payment_last_30d_count,
        sum(payment_num) payment_last_30d_num,
        sum(payment_amount) payment_last_30d_amount,

        sum(refund_count) refund_last_30d_count,
        sum(refund_num) refund_last_30d_num,
        sum(refund_amount) refund_last_30d_amount,

        sum(cart_count) cart_last_30d_count,
        sum(cart_num) cart_last_30d_num,

        sum(favor_count) favor_last_30d_count,

        sum(evaluation_good_count) evaluation_last_30d_good_count,
        sum(evaluation_mid_count)  evaluation_last_30d_mid_count,
        sum(evaluation_bad_count)  evaluation_last_30d_bad_count,

        sku_id
    from yp_dws.dws_sku_daycount
    where dt>=cast(date_add('day', -30, date '2019-05-07') as varchar)
    group by sku_id
)
select
    ac.sku_id,
    l30.order_last_30d_count,
    l30.order_last_30d_num,
    l30.order_last_30d_amount,
    ac.order_count,
    ac.order_num,
    ac.order_amount,
    l30.payment_last_30d_count,
    l30.payment_last_30d_num,
    l30.payment_last_30d_amount,
    ac.payment_count,
    ac.payment_num,
    ac.payment_amount,
    l30.refund_last_30d_count,
    l30.refund_last_30d_num,
    l30.refund_last_30d_amount,
    ac.refund_count,
    ac.refund_num,
    ac.refund_amount,
    l30.cart_last_30d_count,
    l30.cart_last_30d_num,
    ac.cart_count,
    ac.cart_num,
    l30.favor_last_30d_count,
    ac.favor_count,
    l30.evaluation_last_30d_good_count,
    l30.evaluation_last_30d_mid_count,
    l30.evaluation_last_30d_bad_count,
    ac.evaluation_good_count,
    ac.evaluation_mid_count,
    ac.evaluation_bad_count
from all_count ac
left join last_30d l30 on ac.sku_id=l30.sku_id;


--每日循环执行
--1.重建临时表
/*drop table if exists yp_dm.dm_sku_tmp;
create table yp_dm.dm_sku_tmp
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
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');*/

--2.合并新旧数据
insert into yp_dm.dm_sku_tmp
select
    coalesce(new.sku_id,old.sku_id) sku_id,
--        订单 30天数据
    coalesce(new.order_count30,0) order_last_30d_count,
    coalesce(new.order_num30,0) order_last_30d_num,
    coalesce(new.order_amount30,0) order_last_30d_amount,
--        订单 累积历史数据
    coalesce(old.order_count,0) + coalesce(new.order_count,0) order_count,
    coalesce(old.order_num,0) + coalesce(new.order_num,0) order_num,
    coalesce(old.order_amount,0) + coalesce(new.order_amount,0) order_amount,
--        支付单 30天数据
    coalesce(new.payment_count30,0) payment_last_30d_count,
    coalesce(new.payment_num30,0) payment_last_30d_num,
    coalesce(new.payment_amount30,0) payment_last_30d_amount,
--        支付单 累积历史数据
    coalesce(old.payment_count,0) + coalesce(new.payment_count,0) payment_count,
    coalesce(old.payment_num,0) + coalesce(new.payment_count,0) payment_num,
    coalesce(old.payment_amount,0) + coalesce(new.payment_count,0) payment_amount,
--        退款单 30天数据
    coalesce(new.refund_count30,0) refund_last_30d_count,
    coalesce(new.refund_num30,0) refund_last_30d_num,
    coalesce(new.refund_amount30,0) refund_last_30d_amount,
--        退款单 累积历史数据
    coalesce(old.refund_count,0) + coalesce(new.refund_count,0) refund_count,
    coalesce(old.refund_num,0) + coalesce(new.refund_num,0) refund_num,
    coalesce(old.refund_amount,0) + coalesce(new.refund_amount,0) refund_amount,
--        购物车 30天数据
    coalesce(new.cart_count30,0) cart_last_30d_count,
    coalesce(new.cart_num30,0) cart_last_30d_num,
--        购物车 累积历史数据
    coalesce(old.cart_count,0) + coalesce(new.cart_count,0) cart_count,
    coalesce(old.cart_num,0) + coalesce(new.cart_num,0) cart_num,
--        收藏 30天数据
    coalesce(new.favor_count30,0) favor_last_30d_count,
--        收藏 累积历史数据
    coalesce(old.favor_count,0) + coalesce(new.favor_count,0) favor_count,
--        评论 30天数据
    coalesce(new.evaluation_good_count30,0) evaluation_last_30d_good_count,
    coalesce(new.evaluation_mid_count30,0) evaluation_last_30d_mid_count,
    coalesce(new.evaluation_bad_count30,0) evaluation_last_30d_bad_count,
--        评论 累积历史数据
    coalesce(old.evaluation_good_count,0) + coalesce(new.evaluation_good_count,0) evaluation_good_count,
    coalesce(old.evaluation_mid_count,0) + coalesce(new.evaluation_mid_count,0) evaluation_mid_count,
    coalesce(old.evaluation_bad_count,0) + coalesce(new.evaluation_bad_count,0) evaluation_bad_count
from
(
--     dm旧数据
    select
        sku_id,
        order_last_30d_count,
        order_last_30d_num,
        order_last_30d_amount,
        order_count,
        order_num,
        order_amount  ,
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
    from yp_dm.dm_sku
)old
full outer join
(
--     30天 和 昨天 的dws新数据
    select
        sku_id,
        sum(if(dt='2019-05-07', order_count,0 )) order_count,
        sum(if(dt='2019-05-07',order_num ,0 ))  order_num,
        sum(if(dt='2019-05-07',order_amount,0 )) order_amount ,
        sum(if(dt='2019-05-07',payment_count,0 )) payment_count,
        sum(if(dt='2019-05-07',payment_num,0 )) payment_num,
        sum(if(dt='2019-05-07',payment_amount,0 )) payment_amount,
        sum(if(dt='2019-05-07',refund_count,0 )) refund_count,
        sum(if(dt='2019-05-07',refund_num,0 )) refund_num,
        sum(if(dt='2019-05-07',refund_amount,0 )) refund_amount,
        sum(if(dt='2019-05-07',cart_count,0 )) cart_count,
        sum(if(dt='2019-05-07',cart_num,0 )) cart_num,
        sum(if(dt='2019-05-07',favor_count,0 )) favor_count,
        sum(if(dt='2019-05-07',evaluation_good_count,0 )) evaluation_good_count,
        sum(if(dt='2019-05-07',evaluation_mid_count,0 ) ) evaluation_mid_count ,
        sum(if(dt='2019-05-07',evaluation_bad_count,0 )) evaluation_bad_count,
        sum(order_count) order_count30 ,
        sum(order_num) order_num30,
        sum(order_amount) order_amount30,
        sum(payment_count) payment_count30,
        sum(payment_num) payment_num30,
        sum(payment_amount) payment_amount30,
        sum(refund_count) refund_count30,
        sum(refund_num) refund_num30,
        sum(refund_amount) refund_amount30,
        sum(cart_count) cart_count30,
        sum(cart_num) cart_num30,
        sum(favor_count) favor_count30,
        sum(evaluation_good_count) evaluation_good_count30,
        sum(evaluation_mid_count) evaluation_mid_count30,
        sum(evaluation_bad_count) evaluation_bad_count30
    from yp_dws.dws_sku_daycount
    where dt >= cast(date_add('day', -30, date '2019-05-07') as varchar)
    group by sku_id
)new
on new.sku_id = old.sku_id;


--3.临时表覆盖宽表
delete from yp_dm.dm_sku;
insert into yp_dm.dm_sku
select * from yp_dm.dm_sku_tmp;


