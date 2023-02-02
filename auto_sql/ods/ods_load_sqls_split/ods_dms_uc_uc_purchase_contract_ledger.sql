set mapred.job.name=job_ods_dms_uc_uc_purchase_contract_ledger;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_purchase_contract_ledger_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, purchase_contract_id as purchase_contract_id --采购协议ID
, vinno as vinno --车架号
, car_owner_type as car_owner_type --车主类型
, car_owner_name as car_owner_name --车主名称
, car_owner_id_card as car_owner_id_card --车主身份证号码
, car_owner_mobile_phone as car_owner_mobile_phone --车主电话
, car_owner_tel as car_owner_tel --车主固定电话
, car_owner_zip as car_owner_zip --车主邮编
, car_owner_address as car_owner_address --车主住址
, car_owner_email as car_owner_email --车主邮箱
, license_plate_number as license_plate_number --车牌号
, purchase_sign_date as purchase_sign_date --采购签约日期
, purchase_cancel_date as purchase_cancel_date --采购取消日期
, cons_sign_date as cons_sign_date --寄售签约日期
, purchase_price as purchase_price --采购价格
, plan_sales_method as plan_sales_method --计划销售方式
, whsle_target_code as whsle_target_code --批售对象代码
, whsle_target_type as whsle_target_type --批售对象类型
, plan_storage_date as plan_storage_date --计划入库日期
, plan_transfer_date as plan_transfer_date --计划过户日期
, purchase_contract_status as purchase_contract_status --采购协议状态
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
from ods.ods_dms_uc_uc_purchase_contract_ledger
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(purchase_contract_id, '')) as purchase_contract_id  --采购协议ID
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(car_owner_type, '')) as car_owner_type  --车主类型
, trim(nvl(car_owner_name, '')) as car_owner_name  --车主名称
, trim(nvl(car_owner_id_card, '')) as car_owner_id_card  --车主身份证号码
, trim(nvl(car_owner_mobile_phone, '')) as car_owner_mobile_phone  --车主电话
, trim(nvl(car_owner_tel, '')) as car_owner_tel  --车主固定电话
, trim(nvl(car_owner_zip, '')) as car_owner_zip  --车主邮编
, trim(nvl(car_owner_address, '')) as car_owner_address  --车主住址
, trim(nvl(car_owner_email, '')) as car_owner_email  --车主邮箱
, trim(nvl(license_plate_number, '')) as license_plate_number  --车牌号
, trim(nvl(purchase_sign_date, '')) as purchase_sign_date  --采购签约日期
, trim(nvl(purchase_cancel_date, '')) as purchase_cancel_date  --采购取消日期
, trim(nvl(cons_sign_date, '')) as cons_sign_date  --寄售签约日期
, nvl(purchase_price, 0) as purchase_price  --采购价格
, trim(nvl(plan_sales_method, '')) as plan_sales_method  --计划销售方式
, trim(nvl(whsle_target_code, '')) as whsle_target_code  --批售对象代码
, trim(nvl(whsle_target_type, '')) as whsle_target_type  --批售对象类型
, trim(nvl(plan_storage_date, '')) as plan_storage_date  --计划入库日期
, trim(nvl(plan_transfer_date, '')) as plan_transfer_date  --计划过户日期
, nvl(purchase_contract_status, 0) as purchase_contract_status  --采购协议状态
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
from dl.tg_dms_uc_t_uc_purchase_contract_ledger
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_purchase_contract_ledger
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, purchase_contract_id as purchase_contract_id --采购协议ID
, vinno as vinno --车架号
, car_owner_type as car_owner_type --车主类型
, car_owner_name as car_owner_name --车主名称
, car_owner_id_card as car_owner_id_card --车主身份证号码
, car_owner_mobile_phone as car_owner_mobile_phone --车主电话
, car_owner_tel as car_owner_tel --车主固定电话
, car_owner_zip as car_owner_zip --车主邮编
, car_owner_address as car_owner_address --车主住址
, car_owner_email as car_owner_email --车主邮箱
, license_plate_number as license_plate_number --车牌号
, purchase_sign_date as purchase_sign_date --采购签约日期
, purchase_cancel_date as purchase_cancel_date --采购取消日期
, cons_sign_date as cons_sign_date --寄售签约日期
, purchase_price as purchase_price --采购价格
, plan_sales_method as plan_sales_method --计划销售方式
, whsle_target_code as whsle_target_code --批售对象代码
, whsle_target_type as whsle_target_type --批售对象类型
, plan_storage_date as plan_storage_date --计划入库日期
, plan_transfer_date as plan_transfer_date --计划过户日期
, purchase_contract_status as purchase_contract_status --采购协议状态
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
row_number() over (partition by dealer_code,vehicle_manage_no
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_purchase_contract_ledger_tmp) a
where rn = 1;


