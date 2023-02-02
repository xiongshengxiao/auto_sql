set mapred.job.name=job_ods_dms_uc_uc_inventory_ledger_transfer;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_inventory_ledger_transfer_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, plan_transfer_date as plan_transfer_date --计划过户日期
, actual_transfer_date as actual_transfer_date --实际过户日期
, dealer_pay_transfer_cost as dealer_pay_transfer_cost --销售店承担过户费用
, dealer_new_car_owner_name as dealer_new_car_owner_name --销售店新车主姓名
, dealer_new_car_owner_id_card as dealer_new_car_owner_id_card --销售店新车主身份证号码
, dlr_new_car_owner_mobile_phone as dlr_new_car_owner_mobile_phone --销售店新车主手机号
, dealer_new_car_owner_address as dealer_new_car_owner_address --销售店新车主地址
, dlr_new_car_owner_lpn as dlr_new_car_owner_lpn --销售店新车主车牌号
, cancel_time as cancel_time --取消时间
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
from ods.ods_dms_uc_uc_inventory_ledger_transfer
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(plan_transfer_date, '')) as plan_transfer_date  --计划过户日期
, trim(nvl(actual_transfer_date, '')) as actual_transfer_date  --实际过户日期
, nvl(dealer_pay_transfer_cost, 0) as dealer_pay_transfer_cost  --销售店承担过户费用
, trim(nvl(dealer_new_car_owner_name, '')) as dealer_new_car_owner_name  --销售店新车主姓名
, trim(nvl(dealer_new_car_owner_id_card, '')) as dealer_new_car_owner_id_card  --销售店新车主身份证号码
, trim(nvl(dlr_new_car_owner_mobile_phone, '')) as dlr_new_car_owner_mobile_phone  --销售店新车主手机号
, trim(nvl(dealer_new_car_owner_address, '')) as dealer_new_car_owner_address  --销售店新车主地址
, trim(nvl(dlr_new_car_owner_lpn, '')) as dlr_new_car_owner_lpn  --销售店新车主车牌号
, trim(nvl(cancel_time, '')) as cancel_time  --取消时间
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
from dl.tg_dms_uc_t_uc_inventory_ledger_transfer
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_inventory_ledger_transfer
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, plan_transfer_date as plan_transfer_date --计划过户日期
, actual_transfer_date as actual_transfer_date --实际过户日期
, dealer_pay_transfer_cost as dealer_pay_transfer_cost --销售店承担过户费用
, dealer_new_car_owner_name as dealer_new_car_owner_name --销售店新车主姓名
, dealer_new_car_owner_id_card as dealer_new_car_owner_id_card --销售店新车主身份证号码
, dlr_new_car_owner_mobile_phone as dlr_new_car_owner_mobile_phone --销售店新车主手机号
, dealer_new_car_owner_address as dealer_new_car_owner_address --销售店新车主地址
, dlr_new_car_owner_lpn as dlr_new_car_owner_lpn --销售店新车主车牌号
, cancel_time as cancel_time --取消时间
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
row_number() over (partition by vehicle_manage_no,dealer_code
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_inventory_ledger_transfer_tmp) a
where rn = 1;


