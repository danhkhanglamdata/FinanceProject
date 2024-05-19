# Detailed Coding 
<div align="center">
  <img src="https://github.com/danhkhanglamdata/FinanceProject-SalesKPI/assets/153256289/ee48fbe1-b251-470d-a442-de8d185d041e" alt="anh6" width ="503px">
</div>

As illustrated in the diagram above, this is the process I will follow to produce the analysis results. Here, I will create a Summary Report using two datasets: `fact_kpi_month_dataraw` and `fact_txn_month_dataraw`. By generating the Summary Report, we can load the data into Power BI to produce comprehensive dashboards on revenue and expenses.

To calculate the metrics in the Summary Report, specific business knowledge is required. Below are two files that will support you in understanding the necessary business knowledge: `huondan_data` and `dim_data`

In this file, I will focus on calculating the Summary Report. The `ASM KPI` file will follow a similar process, and I will include the code for it in the repository for reference.

<div align="center">
  <img src="https://github.com/danhkhanglamdata/FinanceProject-SalesKPI/assets/153256289/1a4f1f53-2387-428a-9aa2-97083c5e7ecd" alt="anh7" >
</div>

The Summary Report includes numerous metrics. Some of these are aggregate metrics derived from other metrics. Therefore, I will categorize them into two levels:

#### Level 1 Metrics:
- **lai_trong_han**: Interest within the term
- **lai_qua_han**: Overdue interest
- **phi_bao_hiem**: Insurance fee
- **phi_tang_han_muc**: Limit increase fee
- **phi_thanh_toan_cham**: Late payment fee
- **dt_kinhdoanh**: Business revenue
- **cp_hoahong**: Commission costs
- **cp_thuan_kinhdoanhkhac**: Other net business costs
- **cp_nhanvien**: Employee costs
- **cp_quanly**: Management costs
- **cp_taisan**: Asset costs
- **cp_duphong**: Provision costs
- **so_luong_nhansu** : number saleman

#### Level 2 Metrics:
- **cp_von_tt2**: Cost of capital TT2
- **cp_von_cctg**: Cost of capital CCTG

#### Aggregate Metrics:
- **thu_nhap_hoat_dong_the**: Operating income, calculated as:
  - `lai_trong_han + lai_qua_han + phi_bao_hiem + phi_tang_han_muc + phi_thanh_toan_cham`
- **Cp_thuan_KDV**: Net business costs, calculated as:
  - `cp_von_tt2 + cp_von_cctg`
- **cp_thuan_hoat_dong_khac**: Net other operating costs, calculated as:
  - `dt_kinhdoanh + cp_hoahong + cp_thuan_kinhdoanhkhac`
- **tong_thu_nhap_hoat_dong**: Total operating income, calculated as:
  - `thu_nhap_hoat_dong_the + Cp_thuan_KDV + cp_thuan_hoat_dong_khac`
- **tong_chi_phi_hoat_dong**: Total operating expenses, calculated as:
  - `cp_quanly + cp_nhanvien + cp_taisan`
- **loi_nhuan_truoc_thue**: Profit before tax, calculated as:
  - `tong_thu_nhap_hoat_dong - tong_chi_phi_hoat_dong - cp_duphong`
- **cir**: Cost-to-income ratio, calculated as:
  - `tong_chi_phi_hoat_dong / tong_thu_nhap_hoat_dong`
- **tong_doanh_thu**: Total revenue, calculated as:
  - `dt_kinhdoanh + thu_nhap_hoat_dong_the`
- **margin**: Margin, calculated as:
  - `loi_nhuan_truoc_thue / tong_doanh_thu`
- **hs_von**: Capital efficiency, calculated as:
  - `loi_nhuan_truoc_thue / Cp_thuan_KDV`
- **hieu_suat_BQ**: Average efficiency, calculated as:
  - `loi_nhuan_truoc_thue / so_luong_nhansu`


## Table of Contents
1. Calculate REPORT1
2. Calculate SUMMARY Report

## 1.Calculate REPORT1
Below is a diagram illustrating the calculation process in step 1.

<div align="center">
  <img src="https://github.com/danhkhanglamdata/FinanceProject-SalesKPI/assets/153256289/70ba39d8-b003-4fb2-9c8d-c033fd22ae06" alt="anh7" width="600px">
</div>

After importing the two data files, `fact_kpi_month_dataraw` and `fact_txn_month_dataraw`, along with the two guide files `huongdan_data`, we will sequentially calculate the metrics at level 1. Since this project requires the calculation of numerous metrics, to facilitate easy modification and maintenance of the code, I will use functions and procedures. 

After calculating the metrics at level 1, I will create a function to store all these metrics in temporary tables in PostgreSQL. Finally, I will use a procedure to consolidate these temporary tables into a single table `report1` that contains all the level 1 metrics.

Let's canculate Metrics level 1 😎. Here is a diagram illustrating the calculation process for the level 1 metrics.

<div align="center">
  <img src="https://github.com/danhkhanglamdata/FinanceProject-SalesKPI/assets/153256289/b94d606a-6f82-4894-a1d7-03c0558dce27" alt="step2" width="503px">
</div>

I will calculate the first metric: `lai_trong_han`.
~~~sql
-- Tinh Lai Trong Han
with head as (
select 
	sum(amount) as total_head
from fact_txn_month_raw_data_xlsx ftmrdx 
where account_code in (702000030002, 702000030001,702000030102)
	and extract(year from transaction_date) = 2023 
	and extract(month from transaction_date) in (1,2) 
	and analysis_code like 'HEAD%' )

,rule1_step1 as (
select 
	* ,
	substring(analysis_code,9,1) as ma_vung
from fact_txn_month_raw_data_xlsx ftmrdx  
where account_code in (702000030002, 702000030001,702000030102)
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
	and coalesce(max_bucket,1) = 1 )
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
	mx.area_name ,
	round((phanbo + total_rule1),2) as Lai_Trong_Han
from rule2_step4 as r2
inner join rule1_step2 as r1
on r2.ma_khuvuc = r1.ma_vung
inner join makhuvuc23_xlsx mx 
on r1.ma_vung = mx.ma_khuvuc 
order by 2 desc 
~~~

and result 
| Khu Vuc             | lai_trong_han                        |
| ------------------- | ------------------------------------ |
| Tây Nam Bộ          |                  252.252.873.483,34  |
| Đông Nam Bộ         |                  104.784.917.684,64  |
| Đồng Bằng Sông Hồng |                    96.090.675.423,85 |
| Đông Bắc Bộ         |                    71.210.401.715,68 |
| Nam Trung Bộ        |                    54.408.780.960,00 |
| Tây Bắc Bộ          |                    40.355.250.939,22 |
| Bắc Trung Bộ        |                    19.822.618.842,27 |

Similarly, for the other level 1 metrics, I will provide complete calculation scripts for each metric in the repository for your reference.

Next, I will use a function to create a temporary table containing the metrics ☺️😚. I will only provide an example of inserting one metric, as including all the level 1 metrics would be too lengthy. You can find the complete script file in the repository.

I will pass a parameter `month_param` to both the function and procedure, allowing for calculations to be performed for the desired time period.

~~~sql
create or replace
function create_temp_table(month_pram int)
returns void 
as 
$$
begin
-- Tạo bảng tạm lãi trong hạn 
	create temp table lai_trong_hann as 
	-- Tinh Lai Trong Han
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where account_code in (702000030002, 702000030001,702000030102)
		and extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%' )
	
	,rule1_step1 as (
	select 
		* ,
		substring(analysis_code,9,1) as ma_vung
	from fact_txn_month_raw_data_xlsx ftmrdx  
	where account_code in (702000030002, 702000030001,702000030102)
		and extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
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
		and coalesce(max_bucket,1) = 1 )
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
		r1.ma_vung  ,
		(phanbo + total_rule1) as Lai_Trong_Han
	from rule2_step4 as r2
	inner join rule1_step2 as r1
	on r2.ma_khuvuc = r1.ma_vung;
end ;
$$ language plpgsql ;
~~~
Now, after creating the temporary tables for all the metrics, I will use a procedure to generate `Report1`, which contains all the level 1 metrics, from these temporary tables.

~~~sql
create or replace procedure generate_report(month_pram int ) 
language plpgsql 
as $$ 
begin 
	perform create_temp_table(month_pram) ; 
	
	drop table if exists report1 ; 
	create table report1(
		tenkhuvuc varchar , 
		lai_trong_han int8 , 
		lai_qua_han int8 ,
		phi_bao_hiem int8 , 
		phi_tang_han_muc int8 ,
		phi_thanh_toan_cham int8 , 
		doanhthu_kinhdoanh int8 ,
		cp_hoahongg int8 , 
		cp_thuankdkhac int8 ,
		cp_nhanvienn int8 ,
		cp_quanlyy int8 , 
		cp_taisann int8 ,
		cp_duphongg int8	
	) ;

	insert into report1
	select 
		m2.area_name  , 
		lai_trong_han ,
		lai_qua_han ,
		phi_bao_hiem , 
		phi_tang_han_muc ,
		phi_thanh_toan_cham ,
		dt_kinhdoanh ,
		cp_hoahong ,
		CP_ThuanKinhDoanhKhac , 
		cp_nhanvien , 
		cp_quanly ,
		cp_taisan , 
		cp_duphong 
	from lai_trong_hann lt
	inner join lai_qua_hann lq
	on lt.ma_vung  = lq.ma_vung 
	inner join phi_bao_hiemm pb 
	on lt.ma_vung = pb.ma_vung 
	inner join phi_tang_han_mucc pt 
	on lt.ma_vung = pt.ma_vung 
	inner join phi_thanh_toan_chamm pc 
	on lt.ma_vung = pc.ma_vung 
	inner join dt_kinhdoanhh dk 
	on lt.ma_vung = dk.ma_vung 
	inner join cp_hoahongg ch
	on lt.ma_vung = ch.ma_vung 
	inner join cp_thuan_kd_khacc ct 
	on lt.ma_vung = ct.ma_vung 
	inner join cp_nhanvienn cn 
	on lt.ma_vung = cn.ma_vung 
	inner join cp_quanlyy 
	on lt.ma_vung = cp_quanlyy.ma_vung 
	inner join cp_taisann
	on lt.ma_vung = cp_taisann.ma_vung 
	inner join cp_duphongg 
	on lt.ma_vung = cp_duphongg.ma_vung 
	inner join makhuvuc23_xlsx m2 
	on lt.ma_vung = m2.ma_khuvuc ;

	drop table if exists lai_trong_hann ;
	drop table if exists lai_qua_hann ;
	drop table if exists phi_bao_hiemm ; 
	drop table if exists phi_tang_han_mucc ; 
	drop table if exists phi_thanh_toan_chamm ; 
	drop table if exists dt_kinhdoanhh; 
	drop table if exists cp_thuan_kd_khacc; 
	drop table if exists cp_nhanvienn ; 
	drop table if exists cp_quanlyy ; 
	drop table if exists cp_taisann ; 
	drop table if exists cp_duphongg ; 
	drop table if exists cp_hoahongg ;
end ;
$$ ;
~~~

When calling the procedure and passing a specific time period, such as 2, the code inside will be executed, and simultaneously, the previously created temporary tables will be deleted to free up memory.

~~~sql
call generate_report(2) ;
select *
from report1 ;
~~~
| tenkhuvuc           | lai_trong_han      | lai_qua_han       | phi_bao_hiem       | phi_tang_han_muc           | phi_thanh_toan_cham                | doanhthu_kinhdoanh                 | cp_hoahongg   | cp_thuankdkhac   | cp_nhanvienn     | cp_quanlyy      | cp_taisann      | cp_duphongg       |
| ------------------- | ------------------ | ----------------- | ------------------ | -------------------------- | ---------------------------------- | ---------------------------------- | ------------- | ---------------- | ---------------- | --------------- | --------------- | ----------------- |
| Đông Bắc Bộ         |     71.210.401.716 |     64.191.654    |    2.248.781.647   |              3.625.425.756 |                        634.722.991 |                          5.445.450 | \-74.515.640  | \-5.112.847.483  | \-9.172.655.789  | \-356.162.358   | \-894.738.097   | \-40.615.928.951  |
| Tây Bắc Bộ          |     40.355.250.939 |     55.514.989    |       574.178.105  |              2.113.532.018 |                        399.710.741 |                          3.400.433 | \-46.315.116  | \-3.240.656.297  | \-6.115.861.629  | \-240.733.533   | \-588.318.832   | \-23.318.975.760  |
| Đồng Bằng Sông Hồng |     96.090.675.424 |     87.499.120    |       944.207.739  |              5.004.389.958 |                        741.649.960 |                          6.684.151 | \-91.043.735  | \-6.169.920.131  | \-13.799.752.480 | \-544.890.364   | \-1.249.420.516 | \-43.413.672.058  |
| Bắc Trung Bộ        |     19.822.618.842 |           765.223 |         60.962.181 |              1.040.022.295 |                        156.193.505 |                          1.774.191 | \-25.259.007  | \-1.630.922.532  | \-3.477.629.404  | \-136.693.619   | \-319.438.705   | \-6.498.522.434   |
| Nam Trung Bộ        |     54.408.780.960 |     53.184.939    |    1.325.958.386   |              2.789.252.822 |                        270.150.876 |                          3.333.393 | \-44.184.241  | \-3.188.264.670  | \-5.715.311.356  | \-222.914.654   | \-554.295.559   | \-22.308.118.789  |
| Tây Nam Bộ          |   252.252.873.483  |     63.407.212    |    3.895.335.021   |           12.950.572.262   |                     2.886.946.171  |                        17.351.807  | \-237.313.124 | \-16.485.262.716 | \-26.733.389.392 | \-1.026.441.571 | \-2.753.969.173 | \-109.179.627.167 |
| Đông Nam Bộ         |   104.784.917.685  |   233.448.619     |    2.485.006.743   |              5.490.859.663 |                     1.481.077.682  |                          8.582.864 | \-116.750.438 | \-7.878.503.667  | \-15.110.980.529 | \-589.810.980   | \-1.448.148.541 | \-85.553.424.634  |

## 2.Calculate SUMMARY report
Below is a diagram illustrating the calculation process in step 2.

<div align="center">
  <img src="https://github.com/danhkhanglamdata/FinanceProject-SalesKPI/assets/153256289/8feb04ed-bad8-421d-b4f5-bae221718150" alt="anh8" width="600px">
</div>




















