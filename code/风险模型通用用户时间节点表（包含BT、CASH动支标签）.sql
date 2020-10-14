CREATE TABLE dmining.dmining_f_risk_model_lon_info_daily (
`uid` STRING COMMENT 'uid',
`ato_tim` STRING COMMENT '申完时间',
`adt_tim` STRING COMMENT '授信时间',
`fst_ord_tim` STRING COMMENT '首次动支时间',
`lon_adt_dif` INT COMMENT '首次动支距授信天数',
`is_lon` INT COMMENT '至今是否动支',
`is_0d_lon` INT COMMENT '是否授信当天动支',
`is_7d_lon` INT COMMENT '是否授信7天内动支',
`is_15d_lon` INT COMMENT '是否授信15天内动支',
`is_30d_lon` INT COMMENT '是否授信30天内动支',
`is_90d_lon` INT COMMENT '是否授信90天内动支',
`is_180d_lon` INT COMMENT '是否授信180天内动支'
)
COMMENT '风险模型通用用户时间节点表（包含BT、CASH动支标签）-- 每日增量表'
PARTITIONED BY (day string)
    
STORED AS ORC

-- 每日新增更新
-- 新进用户
-- 新申完用户
-- 新授信用户
-- 新BT、CASH动支用户
insert overwrite table dmining.dmining_f_risk_model_lon_info_daily partition(day = '{{params.yesterday}}')
select aa.uid
      ,aa.ato_tim
      ,aa.adt_tim
      ,aa.fst_ord_tim
      ,aa.lon_adt_dif
      ,case when aa.lon_adt_dif is null then 1 else 0 end as is_lon
      ,case when aa.lon_adt_dif = 0 then 1 else 0 end as is_0d_lon
      ,case when aa.lon_adt_dif <= 7 then 1 else 0 end as is_7d_lon
      ,case when aa.lon_adt_dif <= 15 then 1 else 0 end as is_15d_lon
      ,case when aa.lon_adt_dif <= 30 then 1 else 0 end as is_30d_lon
      ,case when aa.lon_adt_dif <= 90 then 1 else 0 end as is_90d_lon
      ,case when aa.lon_adt_dif <= 180 then 1 else 0 end as is_180d_lon
from (
    select aa.uid
          ,substr(aa.ato_tim,1,10) as ato_tim
          ,substr(aa.adt_tim,1,10) as adt_tim
          ,substr(bb.fst_ord_tim,1,10) as fst_ord_tim
          ,datediff(bb.fst_ord_tim,aa.adt_tim) as lon_adt_dif
    from (
        select *
        from dbank.dbank_f_all_milestone
        where day = '{{params.yesterday}}'
          and ato_tim is not null
    ) as aa 
    -- 取CASH和BT业务的首次动支时间
    left join (
        select uid
              ,min(crt_tim) as fst_ord_tim 
        from dbank.loan_f_order_info
        where day = '{{params.yesterday}}' 
          and bsy_typ in ('CASH','BALANCE_TRANSFER') 
          and ord_stt in ('LENDING','PAY_OFF','SOLD','EXCEED')
        group by uid
    ) as bb
    on aa.uid = bb.uid
) as aa 
left join ( select * from dmining.dmining_f_risk_model_lon_info where day = date_add('{{params.yesterday}}',-1)) as bb 
on aa.uid = bb.uid
where bb.uid is null
   or (aa.ato_tim is not null and bb.ato_tim is null)
   or (aa.adt_tim is not null and bb.adt_tim is null)
   or (aa.fst_ord_tim is not null and bb.fst_ord_tim is null)




CREATE TABLE dmining.dmining_f_risk_model_lon_info (
`uid` STRING COMMENT 'uid',
`ato_tim` STRING COMMENT '申完时间',
`adt_tim` STRING COMMENT '授信时间',
`fst_ord_tim` STRING COMMENT '首次动支时间',
`lon_adt_dif` INT COMMENT '首次动支距授信天数',
`is_lon` INT COMMENT '至今是否动支',
`is_0d_lon` INT COMMENT '是否授信当天动支',
`is_7d_lon` INT COMMENT '是否授信7天内动支',
`is_15d_lon` INT COMMENT '是否授信15天内动支',
`is_30d_lon` INT COMMENT '是否授信30天内动支',
`is_90d_lon` INT COMMENT '是否授信90天内动支',
`is_180d_lon` INT COMMENT '是否授信180天内动支',
`day` string 
)
COMMENT '风险模型通用用户时间节点表（包含BT、CASH动支标签）'
-- PARTITIONED BY (day string)
    
STORED AS ORC

-- 以2020-10-12作为历史基础数据
insert overwrite table dmining.dmining_f_risk_model_lon_info
select aa.uid
      ,aa.ato_tim
      ,aa.adt_tim
      ,aa.fst_ord_tim
      ,aa.lon_adt_dif
      ,case when aa.lon_adt_dif is null then 1 else 0 end as is_lon
      ,case when aa.lon_adt_dif = 0 then 1 else 0 end as is_0d_lon
      ,case when aa.lon_adt_dif <= 7 then 1 else 0 end as is_7d_lon
      ,case when aa.lon_adt_dif <= 15 then 1 else 0 end as is_15d_lon
      ,case when aa.lon_adt_dif <= 30 then 1 else 0 end as is_30d_lon
      ,case when aa.lon_adt_dif <= 90 then 1 else 0 end as is_90d_lon
      ,case when aa.lon_adt_dif <= 180 then 1 else 0 end as is_180d_lon
      ,'2020-10-12' as day
from (
    select aa.uid
          ,substr(aa.ato_tim,1,10) as ato_tim
          ,substr(aa.adt_tim,1,10) as adt_tim
          ,substr(bb.fst_ord_tim,1,10) as fst_ord_tim
          ,datediff(bb.fst_ord_tim,aa.adt_tim) as lon_adt_dif
    from (
        select *
        from dbank.dbank_f_all_milestone
        where day = '2020-10-12'
          and ato_tim is not null
    ) as aa 
    -- 取CASH和BT业务的首次动支时间
    left join (
        select uid
              ,min(crt_tim) as fst_ord_tim 
        from dbank.loan_f_order_info
        where day = '2020-10-12' 
          and bsy_typ in ('CASH','BALANCE_TRANSFER') 
          and ord_stt in ('LENDING','PAY_OFF','SOLD','EXCEED')
        group by uid
    ) as bb
    on aa.uid = bb.uid
) as aa

-- 全量更新表
insert overwrite table dmining.dmining_f_risk_model_lon_info
select aa.uid
      ,aa.ato_tim
      ,aa.adt_tim
      ,aa.fst_ord_tim
      ,aa.lon_adt_dif
      ,aa.is_lon
      ,aa.is_0d_lon
      ,aa.is_7d_lon
      ,aa.is_15d_lon
      ,aa.is_30d_lon
      ,aa.is_90d_lon
      ,aa.is_180d_lon
      ,'{{params.yesterday}}' as day
from (
    select *
          ,row_number () over (partition by aa.uid order by aa.day desc) as n
    from (
        select * from dmining.dmining_f_risk_model_lon_info as aa union all 
        select * from dmining.dmining_f_risk_model_lon_info_daily as bb where bb.day = '{{params.yesterday}}'
    ) as aa 
) as aa 
where aa.n = 1