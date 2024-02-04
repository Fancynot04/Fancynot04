--首次执行，计算总累计值
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


--每日循环执行
--1.建立临时表
/*
drop table if exists yp_dm.dm_user_tmp;
create table yp_dm.dm_user_tmp
(
   date_time string COMMENT '统计日期',
    user_id string  comment '用户id',
--    登录
    login_date_first string  comment '首次登录时间',
    login_date_last string  comment '末次登录时间',
    login_count bigint comment '累积登录天数',
    login_last_30d_count bigint comment '最近30日登录天数',

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

*/

--2.合并新旧数据
insert into yp_dm.dm_user_tmp
select
    '2019-05-08' date_time,
    coalesce(new.user_id,old.user_id) user_id,
--     登录
    if(old.login_date_first is null and new.login_count>0,'2019-05-08',old.login_date_first) login_date_first,
    if(new.login_count>0,'2019-05-08',old.login_date_last) login_date_last,
    coalesce(old.login_count,0)+if(new.login_count>0,1,0) login_count,
    coalesce(new.login_last_30d_count,0) login_last_30d_count,
--     购物车
    if(old.cart_date_first is null and new.cart_count>0,'2019-05-08',old.cart_date_first) cart_date_first,
    if(new.cart_count>0,'2019-05-08',old.cart_date_last) cart_date_last,
    coalesce(old.cart_count,0)+if(new.cart_count>0,1,0) cart_count,
    coalesce(old.cart_amount,0)+coalesce(new.cart_amount,0) cart_amount,
    coalesce(new.cart_last_30d_count,0) cart_last_30d_count,
    coalesce(new.cart_last_30d_amount,0) cart_last_30d_amount,
--     订单
    if(old.order_date_first is null and new.order_count>0,'2019-05-08',old.order_date_first) order_date_first,
    if(new.order_count>0,'2019-05-08',old.order_date_last) order_date_last,
    coalesce(old.order_count,0)+coalesce(new.order_count,0) order_count,
    coalesce(old.order_amount,0)+coalesce(new.order_amount,0) order_amount,
    coalesce(new.order_last_30d_count,0) order_last_30d_count,
    coalesce(new.order_last_30d_amount,0) order_last_30d_amount,
--     支付
    if(old.payment_date_first is null and new.payment_count>0,'2019-05-08',old.payment_date_first) payment_date_first,
    if(new.payment_count>0,'2019-05-08',old.payment_date_last) payment_date_last,
    coalesce(old.payment_count,0)+coalesce(new.payment_count,0) payment_count,
    coalesce(old.payment_amount,0)+coalesce(new.payment_amount,0) payment_amount,
    coalesce(new.payment_last_30d_count,0) payment_last_30d_count,
    coalesce(new.payment_last_30d_amount,0) payment_last_30d_amount
from
(
    select * from yp_dm.dm_user
    where date_time=cast((date '2019-05-08' - interval '1' day) as varchar)
) old
full outer join
(
    select
        user_id,
--         登录次数
        sum(if(dt='2019-05-08',login_count,0)) login_count,
--         收藏
        sum(if(dt='2019-05-08',store_collect_count,0)) store_collect_count,
        sum(if(dt='2019-05-08',goods_collect_count,0)) goods_collect_count,
--         购物车
        sum(if(dt='2019-05-08',cart_count,0)) cart_count,
        sum(if(dt='2019-05-08',cart_amount,0)) cart_amount,
--         订单
        sum(if(dt='2019-05-08',order_count,0)) order_count,
        sum(if(dt='2019-05-08',order_amount,0)) order_amount,
--         支付
        sum(if(dt='2019-05-08',payment_count,0)) payment_count,
        sum(if(dt='2019-05-08',payment_amount,0)) payment_amount,
--         30天
        sum(if(login_count>0,1,0)) login_last_30d_count,
        sum(store_collect_count) store_collect_last_30d_count,
        sum(goods_collect_count) goods_collect_last_30d_count,
        sum(cart_count) cart_last_30d_count,
        sum(cart_amount) cart_last_30d_amount,
        sum(order_count) order_last_30d_count,
        sum(order_amount) order_last_30d_amount,
        sum(payment_count) payment_last_30d_count,
        sum(payment_amount) payment_last_30d_amount
    from yp_dws.dws_user_daycount
    where dt>=cast(date_add('day', -30, date '2019-05-08') as varchar)
    group by user_id
) new
on old.user_id=new.user_id;


--3.临时表覆盖宽表
delete from yp_dm.dm_user;
insert into yp_dm.dm_user
select * from yp_dm.dm_user_tmp;
