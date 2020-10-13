CREATE TABLE dmining.dmining_f_risk_model_audit_user_flag (
`uid` STRING COMMENT 'uid',
`ato_tim` STRING COMMENT '申完时间',
`adt_tim` STRING COMMENT '授信时间',
`fst_ord_tim` STRING COMMENT '首次动支时间',
`lon_adt_dif` INT COMMENT '首次动支距授信天数',
`flg_dz_7` INT COMMENT '7天内动支标签',
`flg_dz_15` INT COMMENT '15天内动支标签',
`flg_dz_30` INT COMMENT '30天内动支标签',
`if_mob_6_60` INT COMMENT '是否满6期',
`if_mob_y_6_60` INT COMMENT '是否满6期+60',
`flg_6_60` INT COMMENT '6期60+标签',
`if_mob_3_30` INT COMMENT '是否满3期',
`if_mob_y_3_30` INT COMMENT '是否满3期+30',
`flg_3_30` INT COMMENT '3期30+标签',
`if_mob_1_15` INT COMMENT '是否满1期',
`if_mob_y_1_15` INT COMMENT '是否满1期+15',
`flg_1_15` INT COMMENT '1期15+标签'
)
COMMENT '风险模型通用授信用户标签表'
PARTITIONED BY (day string)
    
STORED AS ORC

insert into table dmining.dmining_f_risk_model_audit_user_flag partition(day = '{{params.yesterday}}')
select uid
	  ,ato_tim
	  ,adt_tim
	  ,fst_ord_tim
	  ,lon_adt_dif
	  ,case when lon_adt_dif <= 7 then 1 
	  		when lon_adt_dif > 7 then 0
	  		else null end as flg_dz_7
	  ,case when lon_adt_dif <= 15 then 1 
	  		when lon_adt_dif > 15 then 0
	  		else null end as flg_dz_15
	  ,case when lon_adt_dif <= 30 then 1 
	  		when lon_adt_dif > 30 then 0
	  		else null end as flg_dz_30
	  ,if_mob_6_60
	  ,if_mob_y_6_60
	  ,case when due_day_6_60 >= 60 then 1
	  	    when due_day_6_60 <= 0 and if_mob_6_60 = 1 then 0
			else -99 end as flg_6_60
	  ,if_mob_3_30
	  ,if_mob_y_3_30
	  ,case when due_day_3_30 >= 30 then 1
	  	    when due_day_3_30 <= 0 and if_mob_3_30 = 1 then 0
			else -99 end as flg_3_30
	  ,if_mob_1_15
	  ,if_mob_y_1_15
	  ,case when due_day_1_15 >= 15 then 1
	  	    when due_day_1_15 <= 0 and if_mob_1_15 = 1 then 0
			else -99 end as flg_1_15
from (
	select uid
		  ,ato_tim
		  ,adt_tim
		  ,fst_ord_tim
		  ,lon_adt_dif
		  ,due_day_6_60
		  ,case when add_months(date(fst_ord_tim), 6)<'{{params.yesterday}}' then 1 else 0 end if_mob_6_60 -- 是否满足6个月表现期（不包括观察正样本的60d+）
		  ,case when date_add(add_months(date(fst_ord_tim), 6), 60)<'{{params.yesterday}}' then 1 else 0 end if_mob_y_6_60
		  ,due_day_3_30
		  ,case when add_months(date(fst_ord_tim), 3)<'{{params.yesterday}}' then 1 else 0 end if_mob_3_30 -- 是否满足3个月表现期（不包括观察正样本的30d+）
		  ,case when date_add(add_months(date(fst_ord_tim), 3), 30)<'{{params.yesterday}}' then 1 else 0 end if_mob_y_3_30
          ,due_day_1_15
		  ,case when add_months(date(fst_ord_tim), 1)<'{{params.yesterday}}' then 1 else 0 end if_mob_1_15 -- 是否满足1个月表现期（不包括观察正样本的15d+）
		  ,case when date_add(add_months(date(fst_ord_tim), 1), 15)<'{{params.yesterday}}' then 1 else 0 end if_mob_y_1_15
	from (
		select aa.uid
			  ,aa.ato_tim
			  ,aa.adt_tim
              ,aa.fst_ord_tim
              ,aa.lon_adt_dif
			  ,max(datediff(aa.rec_dt_6_60,aa.rep_dte)) as due_day_6_60
			  ,max(datediff(aa.rec_dt_3_30,aa.rep_dte)) as due_day_3_30
              ,max(datediff(aa.rec_dt_1_15,aa.rep_dte)) as due_day_1_15
  		from (
  			select aa.uid
			      ,aa.ato_tim
			      ,aa.adt_tim
			      ,aa.fst_ord_tim
			      ,aa.lon_adt_dif
			      ,bb.rep_dte
			      ,bb.rep_tim
			      ,case when bb.rep_tim is null and date_add(add_months(aa.fst_ord_tim, 6), 60)<'{{params.yesterday}}'  then date_add(add_months(aa.fst_ord_tim, 6),60)
				  		when bb.rep_tim is null and date_add(add_months(aa.fst_ord_tim, 6), 60)>='{{params.yesterday}}'  then '{{params.yesterday}}' 
						when bb.rep_tim is not null and date_add(add_months(aa.fst_ord_tim, 6), 60)<substr(rep_tim,1,10) then date_add(add_months(aa.fst_ord_tim, 6), 60)
						else substr(bb.rep_tim,1,10) end rec_dt_6_60
			      ,case when bb.rep_tim is null and date_add(add_months(aa.fst_ord_tim, 3), 30)<'{{params.yesterday}}'  then date_add(add_months(aa.fst_ord_tim, 3),30)
				  		when bb.rep_tim is null and date_add(add_months(aa.fst_ord_tim, 3), 30)>='{{params.yesterday}}'  then '{{params.yesterday}}' 
						when bb.rep_tim is not null and date_add(add_months(aa.fst_ord_tim, 3), 30)<substr(rep_tim,1,10) then date_add(add_months(aa.fst_ord_tim, 3), 30)
						else substr(bb.rep_tim,1,10) end rec_dt_3_30
			      ,case when bb.rep_tim is null and date_add(add_months(aa.fst_ord_tim, 1), 15)<'{{params.yesterday}}'  then date_add(add_months(aa.fst_ord_tim, 1),15)
				  		when bb.rep_tim is null and date_add(add_months(aa.fst_ord_tim, 1), 15)>='{{params.yesterday}}'  then '{{params.yesterday}}' 
						when bb.rep_tim is not null and date_add(add_months(aa.fst_ord_tim, 1), 15)<substr(rep_tim,1,10) then date_add(add_months(aa.fst_ord_tim, 1), 15)
						else substr(bb.rep_tim,1,10) end rec_dt_1_15
			from dmining.dmining_f_risk_model_audit_user as aa
			left join (
				select aa.uid
					  ,aa.ord_no
					  ,bb.rep_dte
					  ,bb.rep_tim 
				from (
					select uid
						  ,ord_no
					from dbank.loan_f_order_info
					where day = '{{params.yesterday}}'
					  and bsy_typ in ('CASH','BALANCE_TRANSFER') 
					  and ord_stt in ('LENDING','PAY_OFF','SOLD','EXCEED')
			    ) as aa
			    left join (
					select uid
						  ,ord_no
						  ,rep_dte
						  ,rep_tim 
					from dbank.loan_f_stage_plan
					where day = '{{params.yesterday}}'
				) as bb
			    on aa.ord_no = bb.ord_no
			) as bb
			on aa.uid = bb.uid 
			where add_months(aa.fst_ord_tim, 6) >= bb.rep_dte
  		) as aa 
  		group by aa.uid
				,aa.ato_tim
				,aa.adt_tim
                ,aa.fst_ord_tim
                ,aa.lon_adt_dif
	) as aa 
) as bb 
;
