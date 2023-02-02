set mapred.job.name=job_ods_dms_uc_uc_dlr_sales_vio_settl;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_dlr_sales_vio_settl_tmp as (
select  id as id --主键
, seller_sales_violation_serial as seller_sales_violation_serial --买方销售违约编号
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, sales_contract_serial as sales_contract_serial --销售合同编号
, violation_amount as violation_amount --违约金额
, settl_status as settl_status --结算状态
, finish_payment_time as finish_payment_time --完成付款时间
, input_date as input_date --录入日期
, del_flag as del_flag --删除标识
, create_by as create_by --创建者
, create_time as create_time --创建时间
, update_by as update_by --更新者
, update_time as update_time --更新时间
, remark as remark --备注
, version as version --版本号
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from ods.ods_dms_uc_uc_dlr_sales_vio_settl
union all
select  trim(nvl(id, '')) as id  --主键
, nvl(seller_sales_violation_serial, 0) as seller_sales_violation_serial  --买方销售违约编号
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(sales_contract_serial, '')) as sales_contract_serial  --销售合同编号
, nvl(violation_amount, 0) as violation_amount  --违约金额
, trim(nvl(settl_status, '')) as settl_status  --结算状态
, trim(nvl(finish_payment_time, '')) as finish_payment_time  --完成付款时间
, trim(nvl(input_date, '')) as input_date  --录入日期
, nvl(del_flag, 0) as del_flag  --删除标识
, nvl(create_by, 0) as create_by  --创建者
, trim(nvl(create_time, '')) as create_time  --创建时间
, nvl(update_by, 0) as update_by  --更新者
, trim(nvl(update_time, '')) as update_time  --更新时间
, trim(nvl(remark, '')) as remark  --备注
, nvl(version, 0) as version  --版本号
, etl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from dl.tg_dms_uc_t_uc_dlr_sales_vio_settl
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_dlr_sales_vio_settl
select  id as id --主键
, seller_sales_violation_serial as seller_sales_violation_serial --买方销售违约编号
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, sales_contract_serial as sales_contract_serial --销售合同编号
, violation_amount as violation_amount --违约金额
, settl_status as settl_status --结算状态
, finish_payment_time as finish_payment_time --完成付款时间
, input_date as input_date --录入日期
, del_flag as del_flag --删除标识
, create_by as create_by --创建者
, create_time as create_time --创建时间
, update_by as update_by --更新者
, update_time as update_time --更新时间
, remark as remark --备注
, version as version --版本号
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, 'DMS' as source_system --来源系统
, substr(current_timestamp(), 1, 19) as insert_time --数据写入时间
, biz_date as biz_date  --分区字段
from (
select *,
row_number() over (partition by seller_sales_violation_serial
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_dlr_sales_vio_settl_tmp) a
where rn = 1;


