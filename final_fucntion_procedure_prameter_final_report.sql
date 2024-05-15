-- Bản cuối cùng có truyền vào tham số để có thể tạo ra final report của từng tháng theo nhu cầu 


-- Tạo bảng tạm bằng function 
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

	----------------------------------
	-- Tinh Lai Qua han
	create temp table lai_qua_hann as 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month_raw_data_xlsx ftmrdx
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date ) <= month_pram
		and analysis_code like 'HEAD%' 
		and account_code in (702000030012, 702000030112) ) 
		
	-- Tinh rule1 
	,rule1_step1 as ( 
	select 
		* , substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month_raw_data_xlsx ftmrdx  
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date ) <= month_pram
		and analysis_code like 'DVML%' 
		and account_code in (702000030012, 702000030112) )
	, rule1_step2 as (
	select 
		ma_vung ,
		sum(amount) as total_rule1
	from rule1_step1 
	group by 1  ) 
	, rule2_step1 as (
	select 
		kpi_month , 
		mx.ma_khuvuc ,
		sum(outstanding_principal) as total_by_month
	from fact_kpi_month_raw_data_xlsx f
	inner join makhuvuc2_xlsx mx 
	on f.pos_city = mx.pos_city 
	where cast(right(cast(kpi_month as varchar),1) as int) <= month_pram 
		and max_bucket = 2
	group by 1,2 
	order by 2,1 ) 
	, rule2_step2 as (
	select 
		ma_khuvuc ,
		avg(total_by_month) as ave 
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
			((ty_trong * (select total_head from head )) + total_rule1) as lai_qua_han
	from rule2_step3 r2
	inner join rule1_step2 r1 
	on r2.ma_khuvuc = r1.ma_vung ;

	----- Tinh Phí Bảo Hiểm 
	create temp table phi_bao_hiemm as 
	with head as (
	select
		sum(amount) as total_head
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram 
		and analysis_code like 'HEAD%'
		and account_code = 716000000001 ) 
	,rule1_step1 as (
	select 
		* , substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram 
		and analysis_code like 'DVML%'
		and account_code = 716000000001 ) 
	, rule1_step2 as (
	select 
		ma_vung ,
		sum(amount) as total_rule1 
	from rule1_step1 
	group by 1 ) 
	, rule2_step1 as (
	select 
		kpi_month  ,
		ma_khuvuc , 
		count(psdn) as so_luong_psdn
	from fact_kpi_month_raw_data_xlsx f
	inner join makhuvuc2_xlsx mx 
	on f.pos_city = mx.pos_city 
		and cast(right(cast(kpi_month as varchar),1) as int) <= month_pram 
		and psdn = 1
	group by 1,2 
	order by 2,1 ) 
	, rule2_step2 as (
	select 
		ma_khuvuc , 
		sum(so_luong_psdn) as total_psdn_khuvuc
	from rule2_step1 
	group by 1 ) 
	, rule2_step3 as (
	select 
		ma_khuvuc , 
		total_psdn_khuvuc / (select sum(total_psdn_khuvuc) from rule2_step2 ) as ty_trong
	from rule2_step2 ) 
	
	select 
		distinct 
		r1.ma_vung ,
		((ty_trong * (select total_head from head) ) + total_rule1 )  as phi_bao_hiem
	from rule2_step3 as r2 
	inner join rule1_step2 as r1 
	on r2.ma_khuvuc = r1.ma_vung ; 

	
--- Phí Tăng Hạn Mức 
	create temp table phi_tang_han_mucc as 
	-- Tinh Phi Tang Han Muc
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where account_code in (719000030002)
		and extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%' )
	
	,rule1_step1 as (
	select 
		* ,
		substring(analysis_code,9,1) as ma_vung
	from fact_txn_month_raw_data_xlsx ftmrdx  
	where account_code in (719000030002)
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
		and cast(right(cast(kpi_month as varchar),1) as int) <= month_pram 
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
		(phanbo + total_rule1) as Phi_tang_han_muc
	from rule2_step4 as r2
	inner join rule1_step2 as r1
	on r2.ma_khuvuc = r1.ma_vung ; 

-- Phí thanh toán chậm 
	create temp table phi_thanh_toan_chamm as 
	-- Tính phí thanh toán chậm , thu từ ngoại bảng , ...
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where account_code in ( 719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
		and extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram 
		and analysis_code like 'HEAD%' )
	
	,rule1_step1 as (
	select 
		* ,
		substring(analysis_code,9,1) as ma_vung
	from fact_txn_month_raw_data_xlsx ftmrdx  
	where account_code in (719000030003,719000030103,790000030003,790000030103,790000030004,790000030104)
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
		and cast(right(cast(kpi_month as varchar),1) as int) <= month_pram 
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
		(phanbo + total_rule1) as Phi_thanh_toan_cham
	from rule2_step4 as r2
	inner join rule1_step2 as r1
	on r2.ma_khuvuc = r1.ma_vung ; 	

	-- Tinh chi phi 
	-- DT Kinh Doanh 
	create temp table dt_kinhdoanhh as 
	-- Tính DT Kinh Doanh
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram 
		and analysis_code like 'HEAD%'
		and account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101) ) 
	, rule1_step1 as (	
	select 
		* , 
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram 
		and analysis_code like 'DVML%'
		and account_code in (702000010001,702000010002,704000000001,705000000001,709000000001,714000000002,714000000003,714037000001,714000000004,714014000001,715000000001,715037000001,719000000001,709000000101,719000000101) )
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
		and cast(right(cast(kpi_month as varchar),1) as int) <= month_pram 
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
	((ty_trong * (select total_head from head)) + total_rule1 )  as DT_KinhDoanh
	from rule2_step3 as r2 
	inner join rule1_step2 as r1 
	on r2.ma_khuvuc = r1.ma_vung ;


	-- Cp Hoa hong 
	create temp table cp_hoahongg as 
	-- Tính Chi phí hoa hồng 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram 
		and analysis_code like 'HEAD%'
		and account_code in (816000000001,816000000002,816000000003) ) 
	, rule1_step1 as (	
	select 
		* , 
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date)<= month_pram 
		and analysis_code like 'DVML%'
		and account_code in (816000000001,816000000002,816000000003) )
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
		and cast(right(cast(kpi_month as varchar),1) as int) <= month_pram 
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
		((ty_trong * (select total_head from head)) + total_rule1 )  as CP_HoaHong
	from rule2_step3 as r2 
	inner join rule1_step2 as r1 
	on r2.ma_khuvuc = r1.ma_vung ; 

 	-- Chi phi thuan kinh doanh khac 
	create temp table cp_thuan_kd_khacc as 
	-- Tính Chi phí thuần kinh doanh khác
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date)<= month_pram 
		and analysis_code like 'HEAD%'
		and account_code in (809000000002,809000000001,811000000001,811000000102,811000000002,811014000001,811037000001,811039000001,811041000001,815000000001,819000000002,819000000003,819000000001,790000000003,790000050101,790000000101,790037000001,849000000001,899000000003,899000000002,811000000101,819000060001) ) 
	, rule1_step1 as (	
	select 
		* , 
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram 
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
		and cast(right(cast(kpi_month as varchar),1) as int) <= month_pram 
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
		((ty_trong * (select total_head from head)) + total_rule1 ) as CP_ThuanKinhDoanhKhac
	from rule2_step3 as r2 
	inner join rule1_step2 as r1 
	on r2.ma_khuvuc = r1.ma_vung ;


	-- Chi phi hoat dong 
	-- CP nhan vien 
	create temp table cp_nhanvienn as 
	-- Tính CP nhân viên 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%'
		and cast(account_code as varchar)  like '85%' ) 
	, rule1_step1 as (
	select 
		*  ,
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'DVML%'
		and cast(account_code as varchar)  like '85%' ) 
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
		((ty_trong *(select total_head from head)) + total_rule1) as CP_nhanvien
	from rule2_step2 as r2 
	inner join rule1_step2 as r1 
	on r2.ma_khuvuc = r1.ma_vung ;	

	-- Cp quan ly 
	create temp table cp_quanlyy as 
	-- Tính CP Quản lý
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%'
		and cast(account_code as varchar)  like '86%' ) 
	, rule1_step1 as (
	select 
		*  ,
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'DVML%'
		and cast(account_code as varchar)  like '86%' ) 
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
		((ty_trong *(select total_head from head)) + total_rule1) as CP_QuanLy
	from rule2_step2 as r2 
	inner join rule1_step2 as r1 
	on r2.ma_khuvuc = r1.ma_vung ;

	-- Cp tai san 
	create temp table cp_taisann as 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%'
		and cast(account_code as varchar)  like '87%' ) 
	, rule1_step1 as (
	select 
		*  ,
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'DVML%'
		and cast(account_code as varchar)  like '87%' ) 
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
		((ty_trong *(select total_head from head)) + total_rule1) as cp_taisan
	from rule2_step2 as r2 
	inner join rule1_step2 as r1 
	on r2.ma_khuvuc = r1.ma_vung ;

	-- Cp Du Phhong 
	create temp table cp_duphongg  as 
	-- Tính CP Dự phòng 
	with head as (
	select 
		sum(amount) as total_head
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
		and analysis_code like 'HEAD%'
		and account_code in (790000050001, 882200050001, 790000030001, 882200030001, 790000000001, 790000020101, 882200000001, 882200050101, 882200020101, 882200060001,790000050101 ,882200030101))
	, rule1_step1 as (
	select 
		*  ,
		substring(analysis_code,9,1) as ma_vung 
	from fact_txn_month_raw_data_xlsx ftmrdx 
	where extract(year from transaction_date) = 2023 
		and extract(month from transaction_date) <= month_pram
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
		((ty_trong *(select total_head from head)) + total_rule1) as CP_DuPhong
	from rule2_step2 as r2 
	inner join rule1_step2 as r1 
	on r2.ma_khuvuc = r1.ma_vung ;
end ;
$$ language plpgsql ;

-- tạo report được tổng hợp từ bảng tạm bằng procedure 
create or replace procedure generate_report(month_pram int ) 
language plpgsql 
as $$ 
begin 
	perform create_temp_table(month_pram) ; 
	
	drop table if exists report ; 
	create table report(
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

	insert into report
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
-- Khi gọi procedure ở đây cần truyền tham số tháng cho chúng : muốn có dữ liệu tháng nào thi điền tháng đó 
-- Do 2 chỉ số CP vốn CCGTG và CP vốn TT2 phụ thuộc vào 1 vài chỉ số đã được tạo ra ở report trên , do đó để 
-- tính toán 2 chỉ số này , chúng ta cần tiếp tục tạo ra 1 procedure và sử dụng bảng tạm trong fucntion  để có thể trích xuất dữ liệu từ report trên 
-- -> tạo ra 2 chỉ số -> ra được report final 

 -- tạo fucntion để dựng 2 bảng tạm cp_cctg và cp_tt2 từ report đã tạo ra ở trên 
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
	from report ) 
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
	from report ) 
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
	from report r 
	inner join cp_cctgg c 
	on r.tenkhuvuc = c.tenkhuvuc 
	inner join cp_tt22 t 
	on r.tenkhuvuc = t.tenkhuvuc ;
	
end ; 
$$ language plpgsql ; 


-- Tạo procedure để tính toán các chỉ số tông hợp và xuất ra thành 1 table baos cáo chuẩn 

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


-- Gọi lệnh : phải gọi cả 4 theo đúng thứ tự 
-- chỉ muốn tìm dữ liệu tháng nào thì truyền tháng đó vào , vd generate_report(1) 
-- Lưu ý : gen_final_report() cx cần trùng tham số với generate_report do để tạo ra final report cuối cùng 
-- bảng final_report sẽ là kết quả cuối cùng lấy ra 
call generate_report(3) ; 
call gen_final_report(3) ; 














