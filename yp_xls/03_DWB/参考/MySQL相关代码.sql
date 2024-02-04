-- 1. 解决hive中文注释乱码问题.
use hive;
show tables;

alter table COLUMNS_V2 modify column COMMENT varchar(256) character set utf8;
alter table TABLE_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8;
alter table PARTITION_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8 ;
alter table PARTITION_KEYS modify column PKEY_COMMENT varchar(4000) character set utf8;
alter table INDEX_PARAMS modify column PARAM_VALUE varchar(4000) character set utf8;


-- 2. Sqoop导入数据后, 如何验真?
# 思路: 总量校验,  条件校验,  样本脚本.
use yipin;
select * from t_district;
select * from t_district where pid='410000';
select * from t_district where name='郑州市';


select * from t_user_login;     -- 15829条
select * from t_store;          -- 44条


-- 如下是为了演示 dwd层的拉链导入(增量导入, 新增 + 修改)
-- 修改mysql中t_shop_order表数据
select * from t_shop_order;         -- 3155条 => 3156条数据, 新增1条, 修改1条.
delete from t_shop_order where id ='dd9999999999999999';

-- 新增订单
INSERT INTO yipin.t_shop_order (id, order_num, buyer_id, store_id, order_from, order_state, create_date, finnshed_time, is_settlement, is_delete, evaluation_state, way, is_stock_up, create_user, create_time, update_user, update_time, is_valid) VALUES ('dd9999999999999999', '251', '2f322c3f55e211e998ec7cd30ad32e2e', 'e438ca06cdf711e998ec7cd30ad32e2e', 3, 2, '2023-05-29 17:52:23', null, 0, 0, 0, 'SELF', 0, '2f322c3f55e211e998ec7cd30ad32e2e', '2023-05-29 17:52:23', '2f322c3f55e211e998ec7cd30ad32e2e', '2023-05-29 18:52:34', 1);

-- 更新订单
UPDATE t_shop_order SET order_num=666 WHERE id='dd1910223851672f32';
UPDATE t_shop_order SET update_time='2023-05-29 13:14:21' WHERE id='dd1910223851672f32';


select * from t_shop_order where
    create_time between '2023-05-29 00:00:00' and '2023-05-29 23:59:59'     -- 新增
    or
    update_time between '2023-05-29 00:00:00' and '2023-05-29 23:59:59';    -- 修改,  总计: 2条(修改1条, 新增1条)