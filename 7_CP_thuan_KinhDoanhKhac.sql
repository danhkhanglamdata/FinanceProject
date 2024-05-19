-- Tính Chi phí thuần kinh doanh khác
with head as (
select 
	sum(amount) as total_head
from fact_txn_month_raw_data_xlsx ftmrdx 
where extract(year from transaction_date) = 2023 
	and extract(month from transaction_date) in (1,2)
	and analysis_code like 'HEAD%'
	and account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001) ) 
, rule1_step1 as (	
select 
	* , 
	substring(analysis_code,9,1) as ma_vung 
from fact_txn_month_raw_data_xlsx ftmrdx 
where extract(year from transaction_date) = 2023 
	and extract(month from transaction_date) in (1,2)
	and analysis_code like 'DVML%'
	and account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001) )
, rule1_step2 as (
select 
	ma_vung , 
	sum(amount) as total_rule1 
from rule1_step1 
group by 1 ) 

,rule2_step1 as (
select 
	kpi_month , 
	mx.ma_khuvuc ,
	sum(outstanding_principal) as op_per_month
from fact_kpi_month_raw_data_xlsx f
inner join makhuvuc2_xlsx mx 
on f.pos_city = mx.pos_city 
	and kpi_month  in (202301,202302)
group by 1,2 
order by 2,1 ) 
, rule2_step2 as (
select 
	ma_khuvuc , 
	avg(op_per_month) as ave 
from rule2_step1 
group by 1 ) 
, rule2_step3 as (
select 
	ma_khuvuc , 
	ave / (select sum(ave) from rule2_step2 ) as ty_trong
from rule2_step2 ) 

select 
	distinct 
	r1.ma_vung  , 
	round(((ty_trong * (select total_head from head)) + total_rule1 ) / 1000000 , 2 ) as CP_ThuanKinhDoanhKhac
from rule2_step3 as r2 
inner join rule1_step2 as r1 
on r2.ma_khuvuc = r1.ma_vung ;

