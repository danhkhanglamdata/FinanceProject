-- Tính phí thanh toán chậm , thu từ ngoại bảng , ...
with head as (
select 
	sum(amount) as total_head
from fact_txn_month_raw_data_xlsx ftmrdx 
where account_code in ( 719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
	and extract(year from transaction_date) = 2023 
	and extract(month from transaction_date) in (1,2) 
	and analysis_code like 'HEAD%' )

,rule1_step1 as (
select 
	* ,
	substring(analysis_code,9,1) as ma_vung
from fact_txn_month_raw_data_xlsx ftmrdx  
where account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
	and extract(year from transaction_date) = 2023 
	and extract(month from transaction_date) in (1,2) 
	and analysis_code like 'DVML%' ) 
,rule1_step2 as (	
select 
	ma_vung ,
	sum(amount) as total_rule1 
from rule1_step1 
group by 1 ) 

,rule2_step1 as (
select 
	* 
from fact_kpi_month_raw_data_xlsx f
inner join makhuvuc2_xlsx mx 
on f.pos_city = mx.pos_city 
	and kpi_month in (202301,202302)
	and (max_bucket between 2 and 5))
, rule2_step2 as (
select 
	kpi_month ,
	ma_khuvuc ,
	sum(outstanding_principal) as oppermonth
from rule2_step1 
group by 1,2 
order by 2,1 )

,rule2_step3 as (
select 
	ma_khuvuc , 
	avg(oppermonth) as ave_khuvuc
from rule2_step2 
group by 1 )
, rule2_step4 as (
select 
	ma_khuvuc , 
	(ave_khuvuc * (select total_head from head) / (select sum(ave_khuvuc) from rule2_step3 )) as phanbo
from rule2_step3 ) 

select 
	distinct 
	r1.ma_vung ,
	round((phanbo + total_rule1)/1000000,2) as Phi_thanh_toan_cham
from rule2_step4 as r2
inner join rule1_step2 as r1
on r2.ma_khuvuc = r1.ma_vung ; 


