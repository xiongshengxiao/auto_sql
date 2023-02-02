set mapred.job.name=job_ods_dms_uc_uc_sales_contract;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_sales_contract_tmp as (
select  id as id --主键
, sales_contract_serial as sales_contract_serial --销售合同编号
, old_nominee_name as old_nominee_name --旧名义人姓名
, old_nominee_telephone as old_nominee_telephone --旧名义人电话
, old_nominee_id_card as old_nominee_id_card --旧名义人身份证号码
, old_nominee_address as old_nominee_address --旧名义人住所
, old_license_plate_number as old_license_plate_number --旧车牌号
, sales_method as sales_method --销售方式
, retail_customer_type as retail_customer_type --零售客户类型
, retail_customer_name as retail_customer_name --零售客户姓名
, retail_customer_id_card as retail_customer_id_card --零售客户身份证号码
, retail_customer_mobile_phone as retail_customer_mobile_phone --零售客户移动电话
, retail_customer_telephone as retail_customer_telephone --零售客户电话
, retail_customer_postal_code as retail_customer_postal_code --零售客户邮编
, retail_customer_address as retail_customer_address --零售客户住所
, customer_id as customer_id --客户ID
, negotiation_id as negotiation_id --商谈ID
, whsle_target_type as whsle_target_type --批售对象类型
, whsle_car_trader_id as whsle_car_trader_id --批售车商ID
, whsle_dealer_code as whsle_dealer_code --批售销售店代码
, whsle_branch_code as whsle_branch_code --批售店铺代码
, whsle_coop_platform_code as whsle_coop_platform_code --批售合作平台代码
, whsle_target_name as whsle_target_name --批售对象名称
, whsle_contact_name as whsle_contact_name --批售联系人名称
, whsle_target_mobile_phone as whsle_target_mobile_phone --批售对象移动电话
, whsle_target_telephone as whsle_target_telephone --批售对象电话
, whsle_target_address as whsle_target_address --批售对象地址
, whsle_target_postal_code as whsle_target_postal_code --批售对象邮编
, whsle_target_email as whsle_target_email --批售对象E-MAIL
, vehicle_user_type as vehicle_user_type --车辆使用人类型
, vehicle_user_name as vehicle_user_name --车辆使用人姓名
, vehicle_user_id_card as vehicle_user_id_card --车辆使用人身份证号码
, vehicle_user_mobile as vehicle_user_mobile --车辆使用人手机
, vehicle_user_telephone as vehicle_user_telephone --车辆使用人电话
, vehicle_user_postal_code as vehicle_user_postal_code --车辆使用人邮编
, vehicle_user_address as vehicle_user_address --车辆使用人地址
, vehicle_body_price_income as vehicle_body_price_income --车辆本体价格收入
, vehicle_body_price_payment as vehicle_body_price_payment --车辆本体价格支出
, discount as discount --折扣
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
from ods.ods_dms_uc_uc_sales_contract
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(sales_contract_serial, '')) as sales_contract_serial  --销售合同编号
, trim(nvl(old_nominee_name, '')) as old_nominee_name  --旧名义人姓名
, trim(nvl(old_nominee_telephone, '')) as old_nominee_telephone  --旧名义人电话
, trim(nvl(old_nominee_id_card, '')) as old_nominee_id_card  --旧名义人身份证号码
, trim(nvl(old_nominee_address, '')) as old_nominee_address  --旧名义人住所
, trim(nvl(old_license_plate_number, '')) as old_license_plate_number  --旧车牌号
, trim(nvl(sales_method, '')) as sales_method  --销售方式
, trim(nvl(retail_customer_type, '')) as retail_customer_type  --零售客户类型
, trim(nvl(retail_customer_name, '')) as retail_customer_name  --零售客户姓名
, trim(nvl(retail_customer_id_card, '')) as retail_customer_id_card  --零售客户身份证号码
, trim(nvl(retail_customer_mobile_phone, '')) as retail_customer_mobile_phone  --零售客户移动电话
, trim(nvl(retail_customer_telephone, '')) as retail_customer_telephone  --零售客户电话
, trim(nvl(retail_customer_postal_code, '')) as retail_customer_postal_code  --零售客户邮编
, trim(nvl(retail_customer_address, '')) as retail_customer_address  --零售客户住所
, trim(nvl(customer_id, '')) as customer_id  --客户ID
, trim(nvl(negotiation_id, '')) as negotiation_id  --商谈ID
, trim(nvl(whsle_target_type, '')) as whsle_target_type  --批售对象类型
, trim(nvl(whsle_car_trader_id, '')) as whsle_car_trader_id  --批售车商ID
, trim(nvl(whsle_dealer_code, '')) as whsle_dealer_code  --批售销售店代码
, trim(nvl(whsle_branch_code, '')) as whsle_branch_code  --批售店铺代码
, trim(nvl(whsle_coop_platform_code, '')) as whsle_coop_platform_code  --批售合作平台代码
, trim(nvl(whsle_target_name, '')) as whsle_target_name  --批售对象名称
, trim(nvl(whsle_contact_name, '')) as whsle_contact_name  --批售联系人名称
, trim(nvl(whsle_target_mobile_phone, '')) as whsle_target_mobile_phone  --批售对象移动电话
, trim(nvl(whsle_target_telephone, '')) as whsle_target_telephone  --批售对象电话
, trim(nvl(whsle_target_address, '')) as whsle_target_address  --批售对象地址
, trim(nvl(whsle_target_postal_code, '')) as whsle_target_postal_code  --批售对象邮编
, trim(nvl(whsle_target_email, '')) as whsle_target_email  --批售对象E-MAIL
, trim(nvl(vehicle_user_type, '')) as vehicle_user_type  --车辆使用人类型
, trim(nvl(vehicle_user_name, '')) as vehicle_user_name  --车辆使用人姓名
, trim(nvl(vehicle_user_id_card, '')) as vehicle_user_id_card  --车辆使用人身份证号码
, trim(nvl(vehicle_user_mobile, '')) as vehicle_user_mobile  --车辆使用人手机
, trim(nvl(vehicle_user_telephone, '')) as vehicle_user_telephone  --车辆使用人电话
, trim(nvl(vehicle_user_postal_code, '')) as vehicle_user_postal_code  --车辆使用人邮编
, trim(nvl(vehicle_user_address, '')) as vehicle_user_address  --车辆使用人地址
, nvl(vehicle_body_price_income, 0) as vehicle_body_price_income  --车辆本体价格收入
, nvl(vehicle_body_price_payment, 0) as vehicle_body_price_payment  --车辆本体价格支出
, nvl(discount, 0) as discount  --折扣
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
from dl.tg_dms_uc_t_uc_sales_contract
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_sales_contract
select  id as id --主键
, sales_contract_serial as sales_contract_serial --销售合同编号
, old_nominee_name as old_nominee_name --旧名义人姓名
, old_nominee_telephone as old_nominee_telephone --旧名义人电话
, old_nominee_id_card as old_nominee_id_card --旧名义人身份证号码
, old_nominee_address as old_nominee_address --旧名义人住所
, old_license_plate_number as old_license_plate_number --旧车牌号
, sales_method as sales_method --销售方式
, retail_customer_type as retail_customer_type --零售客户类型
, retail_customer_name as retail_customer_name --零售客户姓名
, retail_customer_id_card as retail_customer_id_card --零售客户身份证号码
, retail_customer_mobile_phone as retail_customer_mobile_phone --零售客户移动电话
, retail_customer_telephone as retail_customer_telephone --零售客户电话
, retail_customer_postal_code as retail_customer_postal_code --零售客户邮编
, retail_customer_address as retail_customer_address --零售客户住所
, customer_id as customer_id --客户ID
, negotiation_id as negotiation_id --商谈ID
, whsle_target_type as whsle_target_type --批售对象类型
, whsle_car_trader_id as whsle_car_trader_id --批售车商ID
, whsle_dealer_code as whsle_dealer_code --批售销售店代码
, whsle_branch_code as whsle_branch_code --批售店铺代码
, whsle_coop_platform_code as whsle_coop_platform_code --批售合作平台代码
, whsle_target_name as whsle_target_name --批售对象名称
, whsle_contact_name as whsle_contact_name --批售联系人名称
, whsle_target_mobile_phone as whsle_target_mobile_phone --批售对象移动电话
, whsle_target_telephone as whsle_target_telephone --批售对象电话
, whsle_target_address as whsle_target_address --批售对象地址
, whsle_target_postal_code as whsle_target_postal_code --批售对象邮编
, whsle_target_email as whsle_target_email --批售对象E-MAIL
, vehicle_user_type as vehicle_user_type --车辆使用人类型
, vehicle_user_name as vehicle_user_name --车辆使用人姓名
, vehicle_user_id_card as vehicle_user_id_card --车辆使用人身份证号码
, vehicle_user_mobile as vehicle_user_mobile --车辆使用人手机
, vehicle_user_telephone as vehicle_user_telephone --车辆使用人电话
, vehicle_user_postal_code as vehicle_user_postal_code --车辆使用人邮编
, vehicle_user_address as vehicle_user_address --车辆使用人地址
, vehicle_body_price_income as vehicle_body_price_income --车辆本体价格收入
, vehicle_body_price_payment as vehicle_body_price_payment --车辆本体价格支出
, discount as discount --折扣
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
row_number() over (partition by sales_contract_serial
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_sales_contract_tmp) a
where rn = 1;


