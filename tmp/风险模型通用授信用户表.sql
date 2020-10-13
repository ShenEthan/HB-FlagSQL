CREATE TABLE dmining.dmining_f_risk_model_audit_user (
`uid` STRING COMMENT 'uid',
`ato_tim` STRING COMMENT '申完时间',
`adt_tim` STRING COMMENT '授信时间',
`fst_ord_tim` STRING COMMENT '首次动支时间',
`lon_adt_dif` INT COMMENT '首次动支距授信天数'
)
COMMENT '风险模型通用授信用户表'
PARTITIONED BY (day string)
    
STORED AS ORC

insert into table dmining.dmining_f_risk_model_audit_user partition(day = '{{params.yesterday}}')
select aa.uid
      ,substr(aa.ato_tim,1,10) as ato_tim
      ,substr(aa.adt_tim,1,10) as adt_tim
      ,substr(cc.fst_ord_tim,1,10) as fst_ord_tim
      ,datediff(cc.fst_ord_tim,aa.adt_tim) as lon_adt_dif
from (
    select *
    from dbank.dbank_f_all_milestone
    where day = '{{params.yesterday}}'
) as aa 
-- 去除重审用户
inner join (
    select distinct uid 
    from ods_loan.ods_loan_dsloan_credit_mgr_law_info 
    where audit_stage in ('FIRST_AUDIT','H5_APPLY','APP_RE_AUDIT')
      and law_status like '%ACCEPT%' 
) as bb
on aa.uid = bb.uid 
-- 取CASH和BT业务的首次动支时间
left join (
    select uid
          ,min(crt_tim) as fst_ord_tim 
    from dbank.loan_f_order_info
    where day = '{{params.yesterday}}' 
      and bsy_typ in ('CASH','BALANCE_TRANSFER') 
      and ord_stt in ('LENDING','PAY_OFF','SOLD','EXCEED')
    group by uid
) as cc
on aa.uid = cc.uid
;