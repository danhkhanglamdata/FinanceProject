# Detailed Coding 

## Table of Contents
1. [Metrics](#metrics)
2. [Calculate Report1](#report1)
3. [Calculate Summary Report](#summaryreport)

<div align="center">
  <img src="https://github.com/danhkhanglamdata/FinanceProject-SalesKPI/assets/153256289/ee48fbe1-b251-470d-a442-de8d185d041e" alt="anh6" width ="503px">
</div>

As illustrated in the diagram above, this is the process I will follow to produce the analysis results. Here, I will create a Summary Report using two datasets: `fact_kpi_month_dataraw` and `fact_txn_month_dataraw`. By generating the Summary Report, we can load the data into Power BI to produce comprehensive dashboards on revenue and expenses.

To calculate the metrics in the Summary Report, specific business knowledge is required. Below are two files that will support you in understanding the necessary business knowledge: `huondan_data` and `dim_data`

In this file, I will focus on calculating the Summary Report. The `ASM KPI` file will follow a similar process, and I will include the code for it in the repository for reference.

<div align="center">
  <img src="https://github.com/danhkhanglamdata/FinanceProject-SalesKPI/assets/153256289/1a4f1f53-2387-428a-9aa2-97083c5e7ecd" alt="anh7" >
</div>

<div id='metrics' />
	
## 1.Metrics
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



<div id='report1' />
	
## 2.Calculate REPORT1
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

<div id='summaryreport' />
	
## 3.Calculate SUMMARY Report 
Below is a diagram illustrating the calculation process in step 2.

<div align="center">
  <img src="https://github.com/danhkhanglamdata/FinanceProject-SalesKPI/assets/153256289/8feb04ed-bad8-421d-b4f5-bae221718150" alt="anh8" width="600px">
</div>

After constructing Report1, I will utilize a function to create temporary tables for calculating level 2 metrics based on the level 1 metrics data from Report1. Subsequently, I will employ a procedure to compute aggregate metrics and generate the summary report

Now, I will create a function to calculate the two metrics, cp_von_tt2 and cp_von_cctg, from Report1.

~~~sql
create or replace function cp(month_pram int) 
returns void 
as 
$$ 
begin 
	-- Tạo bảng tạm tính cp_cctg 
	create temp table cp_cctgg as 
	with head as (
	select 
		sum(amount) as total_head 
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%'
		and account_code  = 803000000001 ) 
	, step1 as (
	select 
		tenkhuvuc , 
		lai_trong_han + lai_qua_han + phi_bao_hiem + phi_tang_han_muc + phi_thanh_toan_cham as thu_nhap_tu_hd_the , 
		sum(doanhthu_kinhdoanh) over() as total_dt_kinhdoanh
	from report1 ) 
	, step2 as (
	select 
		tenkhuvuc , 
		thu_nhap_tu_hd_the  , 
		sum(thu_nhap_tu_hd_the) over() as total_thunhap_hdthe , 
		total_dt_kinhdoanh 
	from step1 ) 
	
	select 
		tenkhuvuc , 
		((select total_head from head ) * thu_nhap_tu_hd_the / (total_thunhap_hdthe+total_dt_kinhdoanh)) as cp_von_CCTG
	from step2 ;

	-- Bảng tạm tính cp_tt2 
	create temp table cp_tt22 as 
	with head as (
	select 
		sum(amount) as total_head 
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%'
		and account_code  in (801000000001,802000000001))
	, step1 as (
	select 
		tenkhuvuc , 
		lai_trong_han + lai_qua_han + phi_bao_hiem + phi_tang_han_muc + phi_thanh_toan_cham as thu_nhap_tu_hd_the , 
		sum(doanhthu_kinhdoanh) over() as total_dt_kinhdoanh
	from report1 ) 
	, step2 as (
	select 
		tenkhuvuc , 
		thu_nhap_tu_hd_the  , 
		sum(thu_nhap_tu_hd_the) over() as total_thunhap_hdthe , 
		total_dt_kinhdoanh 
	from step1 ) 
	select 
		tenkhuvuc , 
		((select total_head from head ) * thu_nhap_tu_hd_the / (total_thunhap_hdthe+total_dt_kinhdoanh)) as cp_von_tt2
	from step2 ;

	-- 1 bảng tạm để kết hợp từ report trên + 2 chỉ số vừa tính được ở trên 
	-- để tạo ra 1 report cơ bản tính toán các chỉ số 
	-- dưới procedure chỉ tính toán cả chỉ số tổng hợp 
	create temp table summary_report as
	select 
		r.* , 
		c.cp_von_CCTG ,
		t.cp_von_tt2
	from report1 r 
	inner join cp_cctgg c 
	on r.tenkhuvuc = c.tenkhuvuc 
	inner join cp_tt22 t 
	on r.tenkhuvuc = t.tenkhuvuc ;
	
end ; 
$$ language plpgsql ;
~~~
Finally, I will use a procedure to calculate aggregate metrics and build the summary report from the temporary tables created earlier.

~~~sql
create or replace procedure gen_final_report(month_pram int ) 
language plpgsql 
as $$ 
begin 
	perform cp(month_pram ) ; 
	
	drop table if exists final_report ; 
	create table final_report(
	tenkhuvuc varchar , 
	"1.Lợi nhuận trước thuế " int8  ,
	 "Thu nhập từ hoạt động thẻ"  int8 ,
	 "Lãi trong hạn " int8 ,
	 "Lãi quá hạn " int8 ,
	 "Phí Bảo hiểm " int8 ,
	 "Phí tăng hạn mức " int8 ,
	" Phí thanh toán chậm, thu từ ngoại bảng, khác… " int8 ,
	" Chi phí thuần KDV"  int8 ,
	"CP vốn TT 2 " int8 ,
	 "CP vốn CCTG  " int8 ,
	 "Chi phí thuần hoạt động khác"  int8 ,
	"DT Kinh doanh " int8 ,
	 "CP hoa hồng "  int8 ,
	 "CP thuần KD khác " int8 ,
	"Tổng thu nhập hoạt động" int8 ,
	"Tổng chi phí hoạt động" int8 ,
	"CP nhân viên " int8 ,
	"CP quản lý" int8 ,
	"CP tài sản" int8 ,
	"Chi phí dự phòng" int8 ,
	"2. Số lượng nhân sự ( Sale Manager )" int8 ,
	"CIR (%)" numeric ,
	"Margin (%)" numeric ,
	"Hiệu suất trên/vốn (%)" numeric ,
	"Hiệu suất BQ/ Nhân sự "numeric 
	) ;

	insert into final_report
	with cte1 as (
select 
	s1.tenkhuvuc , 
	(lai_trong_han + lai_qua_han + phi_bao_hiem + phi_tang_han_muc + phi_thanh_toan_cham) as thu_nhap_tu_hd_the , 
	lai_trong_han , 
	lai_qua_han ,
	phi_bao_hiem ,
	phi_tang_han_muc ,
	phi_thanh_toan_cham ,
	(cp_von_tt2 + cp_von_CCTG) as chi_phi_thuan_KDV , 
	cp_von_tt2 ,
	cp_von_cctg ,
	(doanhthu_kinhdoanh + cp_hoahongg + cp_thuankdkhac) as chi_phi_thuan_hd_khac , 
	doanhthu_kinhdoanh , 
	cp_hoahongg , 
	cp_thuankdkhac ,
	(cp_nhanvienn + cp_quanlyy + cp_taisann) as tong_chi_phi_hoat_dong , 
	cp_nhanvienn , 
	cp_quanlyy , 
	cp_taisann , 
	cp_duphongg ,
	s2.sl_nhansu 
from summary_report s1 
inner join -- thêm metric số lượng nhân sự 
	(select 	
		area_name , 
		count(sale_name) as sl_nhansu
	from "ds_ASM" da 
	group by 1 ) s2 
on s1.tenkhuvuc = s2.area_name ) 
, cte2 as (
select 
	* ,
	(thu_nhap_tu_hd_the + chi_phi_thuan_KDV + chi_phi_thuan_hd_khac) as tong_thu_nhap_hd
from cte1 ) 
, cte3 as (
select 
	* , 
	(tong_thu_nhap_hd+ tong_chi_phi_hoat_dong+cp_duphongg) as loi_nhuan_truoc_thue ,
	round((tong_chi_phi_hoat_dong*-1/tong_thu_nhap_hd),2) as CIR , 
	round((tong_thu_nhap_hd+ tong_chi_phi_hoat_dong+cp_duphongg) / (thu_nhap_tu_hd_the + doanhthu_kinhdoanh) , 2) as Margin ,
	round((tong_thu_nhap_hd+ tong_chi_phi_hoat_dong+cp_duphongg)*-1 / chi_phi_thuan_KDV  , 2) as hieusuat_von ,
	round((tong_thu_nhap_hd+ tong_chi_phi_hoat_dong+cp_duphongg)/ sl_nhansu  , 2) as hieusuat_bq 
from cte2 ) 

select 
	tenkhuvuc ,
	loi_nhuan_truoc_thue , 
	thu_nhap_tu_hd_the ,
	lai_trong_han ,
	lai_qua_han ,
	phi_bao_hiem , 
	phi_tang_han_muc ,
	phi_thanh_toan_cham ,
	chi_phi_thuan_KDV  ,
	cp_von_tt2 ,
	cp_von_cctg ,
	chi_phi_thuan_hd_khac ,
	doanhthu_kinhdoanh ,
	cp_hoahongg ,
	cp_thuankdkhac ,
	tong_thu_nhap_hd ,
	tong_chi_phi_hoat_dong ,
	cp_nhanvienn ,
	cp_quanlyy ,
	cp_taisann , 
	cp_duphongg ,
	sl_nhansu , 
	cir , 
	margin , 
	hieusuat_von , 
	hieusuat_bq 
	from cte3 
	order by 
		case 
			when tenkhuvuc = 'Đông Bắc Bộ' then 1 
			when tenkhuvuc = 'Tây Bắc Bộ'  then 2
			when tenkhuvuc = 'Đồng Bằng Sông Hồng' then 3 
			when tenkhuvuc = 'Bắc Trung Bộ' then 4
			when tenkhuvuc = 'Nam Trung Bộ' then 5 
			when tenkhuvuc = 'Tây Nam Bộ' then 6 
			else 7 
		end ; 
	
	drop table if exists cp_cctgg ;
	drop table if exists cp_tt22 ;
	drop table if exists summary_report ;
end ;
$$ ;
~~~
and resutl after call procedure
~~~sql
call gen_final_report(2) ; 
select * from final_report;
~~~

| tenkhuvuc           | 1.Lợi nhuận trước thuế             | Thu nhập từ hoạt động thẻ            | Lãi trong hạn      | Lãi quá hạn       | Phí Bảo hiểm       | Phí tăng hạn mức    | Phí thanh toán chậm, thu từ ngoại bảng, khác…                                       | Chi phí thuần KDV | CP vốn TT 2     | CP vốn CCTG      | Chi phí thuần hoạt động khác | DT Kinh doanh        | CP hoa hồng   | CP thuần KD khác | Tổng thu nhập hoạt động               | Tổng chi phí hoạt động | CP nhân viên     | CP quản lý      | CP tài sản      | Chi phí dự phòng  | 2\. Số lượng nhân sự ( Sale Manager ) | CIR (%) | Margin (%) | Hiệu suất trên/vốn (%) | Hiệu suất BQ/ Nhân sự |
| ------------------- | ---------------------------------- | ------------------------------------ | ------------------ | ----------------- | ------------------ | ------------------- | ----------------------------------------------------------------------------------- | ----------------- | --------------- | ---------------- | ---------------------------- | -------------------- | ------------- | ---------------- | ------------------------------------- | ---------------------- | ---------------- | --------------- | --------------- | ----------------- | ------------------------------------- | ------- | ---------- | ---------------------- | --------------------- |
| Đông Bắc Bộ         |                      6.679.886.393 |                       77.783.523.764 |     71.210.401.716 |     64.191.654    |   2.248.781.647    |       3.625.425.756 |                                                                         634.722.991 | \-14.882.234.503  | \-1.049.734.026 | \-13.832.500.478 | \-5.181.917.673              |            5.445.450 | \-74.515.640  | \-5.112.847.483  |                        57.719.371.588 | \-10.423.556.244       | \-9.172.655.789  | \-356.162.358   | \-894.738.097   | \-40.615.928.951  | 9                                     | 0,18    | 0,09       | 0,45                   | 742209599,2           |
| Tây Bắc Bộ          |                      1.628.266.686 |                       43.498.186.792 |     40.355.250.939 |     55.514.989    |      574.178.105   |       2.113.532.018 |                                                                         399.710.741 | \-8.322.459.372   | \-587.033.404   | \-7.735.425.968  | \-3.283.570.980              |            3.400.433 | \-46.315.116  | \-3.240.656.297  |                        31.892.156.440 | \-6.944.913.994        | \-6.115.861.629  | \-240.733.533   | \-588.318.832   | \-23.318.975.760  | 7                                     | 0,22    | 0,04       | 0,2                    | 232609526,6           |
| Đồng Bằng Sông Hồng |                    17.924.707.036  |                     102.868.422.201  |     96.090.675.424 |     87.499.120    |      944.207.739   |       5.004.389.958 |                                                                         741.649.960 | \-19.681.700.032  | \-1.388.269.363 | \-18.293.430.670 | \-6.254.279.715              |            6.684.151 | \-91.043.735  | \-6.169.920.131  |                        76.932.442.454 | \-15.594.063.360       | \-13.799.752.480 | \-544.890.364   | \-1.249.420.516 | \-43.413.672.058  | 23                                    | 0,2     | 0,17       | 0,91                   | 779335088,5           |
| Bắc Trung Bộ        |                      4.960.550.205 |                       21.080.562.046 |     19.822.618.842 |           765.223 |         60.962.181 |       1.040.022.295 |                                                                         156.193.505 | \-4.033.320.331   | \-284.494.481   | \-3.748.825.850  | \-1.654.407.348              |            1.774.191 | \-25.259.007  | \-1.630.922.532  |                        15.392.834.367 | \-3.933.761.728        | \-3.477.629.404  | \-136.693.619   | \-319.438.705   | \-6.498.522.434   | 5                                     | 0,26    | 0,24       | 1,23                   | 992110041             |
| Nam Trung Bộ        |                    15.558.378.739  |                       58.847.327.983 |     54.408.780.960 |     53.184.939    |   1.325.958.386    |       2.789.252.822 |                                                                         270.150.876 | \-11.259.193.368  | \-794.179.018   | \-10.465.014.351 | \-3.229.115.518              |            3.333.393 | \-44.184.241  | \-3.188.264.670  |                        44.359.019.097 | \-6.492.521.569        | \-5.715.311.356  | \-222.914.654   | \-554.295.559   | \-22.308.118.789  | 5                                     | 0,15    | 0,26       | 1,38                   | 3111675748            |
| Tây Nam Bộ          |                    63.599.626.604  |                     272.049.134.149  |   252.252.873.483  |     63.407.212    |   3.895.335.021    |     12.950.572.262  |                                                                      2.886.946.171  | \-52.050.856.209  | \-3.671.461.756 | \-48.379.394.452 | \-16.705.224.033             |          17.351.807  | \-237.313.124 | \-16.485.262.716 |                     203.293.053.907   | \-30.513.800.136       | \-26.733.389.392 | \-1.026.441.571 | \-2.753.969.173 | \-109.179.627.167 | 16                                    | 0,15    | 0,23       | 1,22                   | 3974976663            |
| Đông Nam Bộ         | \-18.116.158.486                   |                     114.475.310.392  |   104.784.917.685  |   233.448.619     |   2.485.006.743    |       5.490.859.663 |                                                                      1.481.077.682  | \-21.902.432.953  | \-1.544.911.089 | \-20.357.521.864 | \-7.986.671.241              |            8.582.864 | \-116.750.438 | \-7.878.503.667  |                        84.586.206.198 | \-17.148.940.050       | \-15.110.980.529 | \-589.810.980   | \-1.448.148.541 | \-85.553.424.634  | 19                                    | 0,2     | \-0,16     | \-0,83                 | \-953482025,6         |

After generating the summary report, to resemble the image at the top, please use Excel to pivot the result table so that the columns display similarly to the image, and feel free to apply coloring to enhance its appearance. 😚😚

Thank you for watching until here !!!




















