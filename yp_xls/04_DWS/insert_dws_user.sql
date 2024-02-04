insert into yp_dws.dws_user_daycount
-- 登录次数
with login_count as (
   select
      count(id) as login_count,
      login_user as user_id, dt
   from yp_dwd.fact_user_login
   group by login_user, dt
),
-- 店铺收藏数
store_collect_count as (
   select
      count(id) as store_collect_count,
      user_id, substring(create_time, 1, 10) as dt
   from yp_dwd.fact_store_collect
   where end_date='9999-99-99'
   group by user_id, substring(create_time, 1, 10)
),
-- 商品收藏数
goods_collect_count as (
   select
      count(id) as goods_collect_count,
      user_id, substring(create_time, 1, 10) as dt
   from yp_dwd.fact_goods_collect
    where end_date='9999-99-99'
   group by user_id, substring(create_time, 1, 10)
),
-- 加入购物车次数和金额
cart_count_amount as (
   select
      count(cart.id) as cart_count,
      sum(g.goods_promotion_price) as cart_amount,
      buyer_id as user_id, substring(cart.create_time, 1, 10) as dt
   from yp_dwd.fact_shop_cart cart, yp_dwb.dwb_goods_detail g
   where cart.end_date='9999-99-99' and cart.goods_id=g.id
   group by buyer_id, substring(cart.create_time, 1, 10)
),
-- 下单次数和金额
order_count_amount as (
   select
      count(o.id) as order_count,
      sum(order_amount) as order_amount,
      buyer_id as user_id, substring(create_date, 1, 10) as dt
   from yp_dwd.fact_shop_order o, yp_dwd.fact_shop_order_address_detail od
   where o.id=od.id
     and o.is_valid=1 and o.end_date='9999-99-99' and od.end_date='9999-99-99'
   group by buyer_id, substring(create_date, 1, 10)
),
-- 支付次数和金额
payment_count_amount as (
   select
      count(id) as payment_count,
      sum(trade_true_amount) as payment_amount,
      user_id, substring(create_time, 1, 10) as dt
   from yp_dwd.fact_trade_record
   where is_valid=1 and trade_type in (1,11) and status=1
   group by user_id, substring(create_time, 1, 10)
)
select
--        dt,
       user_id,
      -- 登录次数
       sum(login_count) login_count,
       -- 店铺收藏数
       sum(store_collect_count) store_collect_count,
       -- 商品收藏数
       sum(goods_collect_count) goods_collect_count,
       -- 加入购物车次数和金额
       sum(cart_count) cart_count,
       sum(cart_amount) cart_amount,
       -- 下单次数和金额
       sum(order_count) order_count,
       sum(order_amount) order_amount,
       -- 支付次数和金额
       sum(payment_count) payment_count,
       sum(payment_amount) payment_amount
      ,dt
from
(
    select lc.login_count,
           0 store_collect_count,
           0 goods_collect_count,
           0 cart_count, 0 cart_amount,
           0 order_count, 0 order_amount,
           0 payment_count, 0 payment_amount,
           user_id, dt
    from login_count lc
    union all
    select
           0 login_count,
           scc.store_collect_count,
           0 goods_collect_count,
           0 cart_count, 0 cart_amount,
           0 order_count, 0 order_amount,
           0 payment_count, 0 payment_amount,
           user_id, dt
    from store_collect_count scc
    union all
    select
           0 login_count,
           0 store_collect_count,
           gcc.goods_collect_count,
           0 cart_count, 0 cart_amount,
           0 order_count, 0 order_amount,
           0 payment_count, 0 payment_amount,
           user_id, dt
    from goods_collect_count gcc
    union all
    select
           0 login_count,
           0 store_collect_count,
           0 goods_collect_count,
           cca.cart_count, cart_amount,
           0 order_count, 0 order_amount,
           0 payment_count, 0 payment_amount,
           user_id, dt
    from cart_count_amount cca
    union all
    select
           0 login_count,
           0 store_collect_count,
           0 goods_collect_count,
           0 cart_count, 0 cart_amount,
           oca.order_count, order_amount,
           0 payment_count, 0 payment_amount,
           user_id, dt
    from order_count_amount oca
    union all
    select
           0 login_count,
           0 store_collect_count,
           0 goods_collect_count,
           0 cart_count, 0 cart_amount,
           0 order_count, 0 order_amount,
           pca.payment_count, payment_amount,
           user_id, dt
    from payment_count_amount pca
) user_count
group by user_id, dt
;