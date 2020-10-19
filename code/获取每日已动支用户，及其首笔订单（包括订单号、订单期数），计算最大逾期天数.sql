CREATE TABLE dmining.dmining_f_risk_model_fst_ord_info (
`uid` STRING COMMENT 'uid',
`ato_tim` STRING COMMENT '申完时间',
`adt_tim` STRING COMMENT '授信时间',
`fst_ord_tim` STRING COMMENT '首次动支时间',
`lon_adt_dif` INT COMMENT '首次动支距授信天数',
`ord_no` INT COMMENT '首次动支订单号',
`ord_stt` INT COMMENT '首次动支订单状态',
`prc_amt` INT COMMENT '首次动支订单金额',
`stg_typ` INT COMMENT '首次动支订单期数',
`max_ovd_day_his` INT COMMENT '历史最大逾期天数',
`max_ovd_day_mob_1` INT COMMENT 'MOB1最大逾期天数',
`max_ovd_day_mob_2` INT COMMENT 'MOB2最大逾期天数',
`max_ovd_day_mob_3` INT COMMENT 'MOB3最大逾期天数',
`max_ovd_day_mob_4` INT COMMENT 'MOB4最大逾期天数',
`max_ovd_day_mob_5` INT COMMENT 'MOB5最大逾期天数',
`max_ovd_day_mob_6` INT COMMENT 'MOB6最大逾期天数',
`max_ovd_day_mob_7` INT COMMENT 'MOB7最大逾期天数',
`max_ovd_day_mob_8` INT COMMENT 'MOB8最大逾期天数',
`max_ovd_day_mob_9` INT COMMENT 'MOB9最大逾期天数',
`max_ovd_day_mob_10` INT COMMENT 'MOB10最大逾期天数',
`max_ovd_day_mob_11` INT COMMENT 'MOB11最大逾期天数',
`max_ovd_day_mob_12` INT COMMENT 'MOB12最大逾期天数',
`day` string 
)
COMMENT '获取每日已动支用户，及其首笔订单（包括订单号、订单期数），计算最大逾期天数'
-- PARTITIONED BY (day string)
    
STORED AS ORC


insert overwrite table dmining.dmining_f_risk_model_fst_ord_info
-- 获取每日已动支用户，及其首笔订单（包括订单号、订单期数）
select aa.uid
      ,aa.ato_tim
      ,aa.adt_tim
      ,aa.fst_ord_tim
      ,aa.lon_adt_dif
      ,aa.ord_no
      ,aa.ord_stt
      ,aa.prc_amt
      ,aa.stg_typ
      ,max(aa.due_day) as max_ovd_day_his
      ,max(case when aa.stg_no <= 1  then aa.due_day end) as max_ovd_day_mob_1 
      ,max(case when aa.stg_no <= 2  then aa.due_day end) as max_ovd_day_mob_2 
      ,max(case when aa.stg_no <= 3  then aa.due_day end) as max_ovd_day_mob_3 
      ,max(case when aa.stg_no <= 4  then aa.due_day end) as max_ovd_day_mob_4 
      ,max(case when aa.stg_no <= 5  then aa.due_day end) as max_ovd_day_mob_5 
      ,max(case when aa.stg_no <= 6  then aa.due_day end) as max_ovd_day_mob_6 
      ,max(case when aa.stg_no <= 7  then aa.due_day end) as max_ovd_day_mob_7 
      ,max(case when aa.stg_no <= 8  then aa.due_day end) as max_ovd_day_mob_8 
      ,max(case when aa.stg_no <= 9  then aa.due_day end) as max_ovd_day_mob_9 
      ,max(case when aa.stg_no <= 10 then aa.due_day end) as max_ovd_day_mob_10
      ,max(case when aa.stg_no <= 11 then aa.due_day end) as max_ovd_day_mob_11
      ,max(case when aa.stg_no <= 12 then aa.due_day end) as max_ovd_day_mob_12
      ,'{{params.yesterday}}' as day      
from (
    select aa.uid
          ,aa.ato_tim
          ,aa.adt_tim
          ,aa.fst_ord_tim
          ,aa.lon_adt_dif
          ,aa.ord_no
          ,aa.ord_stt
          ,aa.prc_amt
          ,bb.stg_no
          ,bb.stg_typ
          ,bb.rep_dte
          ,bb.rep_tim
          ,datediff(substr(nvl(bb.rep_tim,'{{params.yesterday}}'),1,10),bb.rep_dte) as due_day
    from (
        select aa.uid
              ,aa.ato_tim
              ,aa.adt_tim
              ,aa.fst_ord_tim
              ,aa.lon_adt_dif
              ,bb.ord_no
              ,bb.ord_stt
              ,bb.prc_amt
        from (
            -- 获取每日已动支用户
            select aa.uid
                  ,aa.ato_tim
                  ,aa.adt_tim
                  ,aa.fst_ord_tim
                  ,aa.lon_adt_dif
            from dmining.dmining_f_risk_model_lon_info as aa 
            where aa.is_lon = 1
        ) as aa 
        left join (
            -- 获取动支用户首笔订单
            select aa.uid
                  ,aa.crt_tim
                  ,aa.ord_no
                  ,aa.ord_stt
                  ,aa.prc_amt
            from (
                select aa.uid
                      ,aa.crt_tim
                      ,aa.ord_no
                      ,aa.ord_stt
                      ,aa.prc_amt
                      ,row_number () over (partition by aa.uid order by aa.crt_tim) as n
                from dbank.loan_f_order_info as aa 
                where aa.day = '{{params.yesterday}}' 
                  and aa.bsy_typ in ('CASH','BALANCE_TRANSFER') 
                  and aa.ord_stt in ('LENDING','PAY_OFF','SOLD','EXCEED')
            ) as aa 
            where aa.n = 1
        ) as bb
        on aa.uid = bb.uid
    ) as aa 
    left join (
        select uid
              ,ord_no
              ,stg_no
              ,stg_typ
              ,rep_dte
              ,rep_tim 
        from dbank.loan_f_stage_plan
        where day = '{{params.yesterday}}'
    ) as bb
    on aa.ord_no = bb.ord_no
) as aa
group by aa.uid
        ,aa.ato_tim
        ,aa.adt_tim
        ,aa.fst_ord_tim
        ,aa.lon_adt_dif
        ,aa.ord_no
        ,aa.ord_stt
        ,aa.prc_amt
        ,aa.stg_typ
        ,'{{params.yesterday}}'

-- 全量更新表
insert overwrite table dmining.dmining_f_risk_model_fst_ord_info
select aa.uid
      ,aa.ato_tim
      ,aa.adt_tim
      ,aa.fst_ord_tim
      ,aa.lon_adt_dif
      ,aa.ord_no
      ,aa.ord_stt
      ,aa.prc_amt
      ,aa.stg_typ
      ,aa.max_ovd_day_his
      ,aa.max_ovd_day_mob_1 
      ,aa.max_ovd_day_mob_2 
      ,aa.max_ovd_day_mob_3 
      ,aa.max_ovd_day_mob_4 
      ,aa.max_ovd_day_mob_5 
      ,aa.max_ovd_day_mob_6 
      ,aa.max_ovd_day_mob_7 
      ,aa.max_ovd_day_mob_8 
      ,aa.max_ovd_day_mob_9 
      ,aa.max_ovd_day_mob_10
      ,aa.max_ovd_day_mob_11
      ,aa.max_ovd_day_mob_12 
      ,'{{params.yesterday}}' as day
from (
    select *
          ,row_number () over (partition by aa.uid order by aa.day desc) as n
    from (
        select * from dmining.dmining_f_risk_model_fst_ord_info as aa union all 
        select * from dmining.dmining_f_risk_model_fst_ord_info_daily as bb where bb.day = '{{params.yesterday}}'
    ) as aa 
) as aa 
where aa.n = 1
;


CREATE TABLE dmining.dmining_f_risk_model_fst_ord_info_daily (
`uid` STRING COMMENT 'uid',
`ato_tim` STRING COMMENT '申完时间',
`adt_tim` STRING COMMENT '授信时间',
`fst_ord_tim` STRING COMMENT '首次动支时间',
`lon_adt_dif` INT COMMENT '首次动支距授信天数',
`ord_no` INT COMMENT '首次动支订单号',
`ord_stt` INT COMMENT '首次动支订单状态',
`prc_amt` INT COMMENT '首次动支订单金额',
`stg_typ` INT COMMENT '首次动支订单期数',
`max_ovd_day_his` INT COMMENT '历史最大逾期天数',
`max_ovd_day_mob_1` INT COMMENT 'MOB1最大逾期天数',
`max_ovd_day_mob_2` INT COMMENT 'MOB2最大逾期天数',
`max_ovd_day_mob_3` INT COMMENT 'MOB3最大逾期天数',
`max_ovd_day_mob_4` INT COMMENT 'MOB4最大逾期天数',
`max_ovd_day_mob_5` INT COMMENT 'MOB5最大逾期天数',
`max_ovd_day_mob_6` INT COMMENT 'MOB6最大逾期天数',
`max_ovd_day_mob_7` INT COMMENT 'MOB7最大逾期天数',
`max_ovd_day_mob_8` INT COMMENT 'MOB8最大逾期天数',
`max_ovd_day_mob_9` INT COMMENT 'MOB9最大逾期天数',
`max_ovd_day_mob_10` INT COMMENT 'MOB10最大逾期天数',
`max_ovd_day_mob_11` INT COMMENT 'MOB11最大逾期天数',
`max_ovd_day_mob_12` INT COMMENT 'MOB12最大逾期天数'
)
COMMENT '获取每日已动支用户，及其首笔订单（包括订单号、订单期数），计算最大逾期天数，日更新'
PARTITIONED BY (day string)
    
STORED AS ORC

insert overwrite table dmining.dmining_f_risk_model_fst_ord_info_daily partition(day = '{{params.yesterday}}')
-- 获取每日已动支用户，及其首笔订单（包括订单号、订单期数）
select aa.uid
      ,aa.ato_tim
      ,aa.adt_tim
      ,aa.fst_ord_tim
      ,aa.lon_adt_dif
      ,aa.ord_no
      ,aa.ord_stt
      ,aa.prc_amt
      ,aa.stg_typ
      ,max(aa.due_day) as max_ovd_day_his
      ,max(case when aa.stg_no <= 1  then aa.due_day end) as max_ovd_day_mob_1 
      ,max(case when aa.stg_no <= 2  then aa.due_day end) as max_ovd_day_mob_2 
      ,max(case when aa.stg_no <= 3  then aa.due_day end) as max_ovd_day_mob_3 
      ,max(case when aa.stg_no <= 4  then aa.due_day end) as max_ovd_day_mob_4 
      ,max(case when aa.stg_no <= 5  then aa.due_day end) as max_ovd_day_mob_5 
      ,max(case when aa.stg_no <= 6  then aa.due_day end) as max_ovd_day_mob_6 
      ,max(case when aa.stg_no <= 7  then aa.due_day end) as max_ovd_day_mob_7 
      ,max(case when aa.stg_no <= 8  then aa.due_day end) as max_ovd_day_mob_8 
      ,max(case when aa.stg_no <= 9  then aa.due_day end) as max_ovd_day_mob_9 
      ,max(case when aa.stg_no <= 10 then aa.due_day end) as max_ovd_day_mob_10
      ,max(case when aa.stg_no <= 11 then aa.due_day end) as max_ovd_day_mob_11
      ,max(case when aa.stg_no <= 12 then aa.due_day end) as max_ovd_day_mob_12   
from (
    select aa.uid
          ,aa.ato_tim
          ,aa.adt_tim
          ,aa.fst_ord_tim
          ,aa.lon_adt_dif
          ,aa.ord_no
          ,aa.ord_stt
          ,aa.prc_amt
          ,bb.stg_no
          ,bb.stg_typ
          ,bb.rep_dte
          ,bb.rep_tim
          ,datediff(substr(nvl(bb.rep_tim,'{{params.yesterday}}'),1,10),bb.rep_dte) as due_day
    from (
        select aa.uid
              ,aa.ato_tim
              ,aa.adt_tim
              ,aa.fst_ord_tim
              ,aa.lon_adt_dif
              ,bb.ord_no
              ,bb.ord_stt
              ,bb.prc_amt
        from (
            -- 获取每日已动支用户
            select aa.uid
                  ,aa.ato_tim
                  ,aa.adt_tim
                  ,aa.fst_ord_tim
                  ,aa.lon_adt_dif
            from dmining.dmining_f_risk_model_lon_info as aa 
            where aa.is_lon = 1
        ) as aa 
        left join (
            -- 获取动支用户首笔订单
            select aa.uid
                  ,aa.crt_tim
                  ,aa.ord_no
                  ,aa.ord_stt
                  ,aa.prc_amt
            from (
                select aa.uid
                      ,aa.crt_tim
                      ,aa.ord_no
                      ,aa.ord_stt
                      ,aa.prc_amt
                      ,row_number () over (partition by aa.uid order by aa.crt_tim) as n
                from dbank.loan_f_order_info as aa 
                where aa.day = '{{params.yesterday}}' 
                  and aa.bsy_typ in ('CASH','BALANCE_TRANSFER') 
                  and aa.ord_stt in ('LENDING','PAY_OFF','SOLD','EXCEED')
            ) as aa 
            where aa.n = 1
        ) as bb
        on aa.uid = bb.uid
    ) as aa 
    left join (
        select uid
              ,ord_no
              ,stg_no
              ,stg_typ
              ,rep_dte
              ,rep_tim 
        from dbank.loan_f_stage_plan
        where day = '{{params.yesterday}}'
    ) as bb
    on aa.ord_no = bb.ord_no
) as aa
where aa.ord_stt != 'PAY_OFF' or aa.ord_stt is null
group by aa.uid
        ,aa.ato_tim
        ,aa.adt_tim
        ,aa.fst_ord_tim
        ,aa.lon_adt_dif
        ,aa.ord_no
        ,aa.ord_stt
        ,aa.prc_amt
        ,aa.stg_typ
