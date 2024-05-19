-- Tính CP Dự phòng 
with head as (
select 
	sum(amount) as total_head
from fact_txn_month_raw_data_xlsx ftmrdx 
where extract(year from transaction_date) = 2023 
	and extract(month from transaction_date) in (1,2)
	and analysis_code like 'HEAD%'
	and account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001, 882200050101, 882200020101, 882200060001,790000050101 ,882200030101))
, rule1_step1 as (
select 
	*  ,
	substring(analysis_code,9,1) as ma_vung 
from fact_txn_month_raw_data_xlsx ftmrdx 
where extract(year from transaction_date) = 2023 
	and extract(month from transaction_date) in (1,2)
	and analysis_code like 'DVML%'
	and account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001, 882200050101, 882200020101, 882200060001,790000050101 ,882200030101))
, rule1_step2 as (
select 
	ma_vung , 
	sum(amount) as total_rule1 
from rule1_step1 
group by 1 ) 
, rule2_step1 as (
select
	mx.ma_khuvuc ,
	da.area_name , 
	count(sale_name) as so_luong_ASM
from "ds_ASM" da 
inner join makhuvuc23_xlsx mx 
on da.area_name = mx.area_name 
	and jan is not null 
	and feb is not null
group by 1,2
order by mx.ma_khuvuc asc ) 
, rule2_step2 as (
select 
	ma_khuvuc ,
	so_luong_ASM / (select sum(so_luong_ASM) from rule2_step1 ) as ty_trong
from rule2_step1 )

select 
	r1.ma_vung , 
	round(((ty_trong *(select total_head from head)) + total_rule1)/1000000,2) as CP_DuPhong
from rule2_step2 as r2 
inner join rule1_step2 as r1 
on r2.ma_khuvuc = r1.ma_vung 
