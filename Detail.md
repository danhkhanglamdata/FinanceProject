# Detailed Coding 
<div align="center">
  <img src="https://github.com/danhkhanglamdata/FinanceProject-SalesKPI/assets/153256289/ee48fbe1-b251-470d-a442-de8d185d041e" alt="anh6" width ="503px">
</div>

As illustrated in the diagram above, this is the process I will follow to produce the analysis results. Here, I will create a Summary Report using two datasets: `fact_kpi_month_dataraw` and `fact_kpi_asm_data`. By generating the Summary Report, we can load the data into Power BI to produce comprehensive dashboards on revenue and expenses.

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




