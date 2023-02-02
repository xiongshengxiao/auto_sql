set mapred.job.name=job_ods_dms_uc_uc_new_vhcl_exchange_achv;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_new_vhcl_exchange_achv_tmp as (
select  id as id --主键
, new_vehicle_displace_achv_id as new_vehicle_displace_achv_id --新车置换业绩ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, license_plate_number as license_plate_number --车牌号
, achv_apply_status as achv_apply_status --业绩申请状态
, present_type as present_type --赠品类型
, displace_vehicle_related_id as displace_vehicle_related_id --置换车辆关联ID
, displace_vehicle_vinno as displace_vehicle_vinno --置换车辆车架号
, displace_vehicle_nominee_name as displace_vehicle_nominee_name --置换车辆名义人姓名
, displace_vehicle_nominee_tel as displace_vehicle_nominee_tel --置换车辆名义人电话
, displace_vehicle_sales_date as displace_vehicle_sales_date --置换车辆销售日期
, displace_vehicle_picture_url as displace_vehicle_picture_url --置换车辆图片路径
, displace_vehicle_config_grade as displace_vehicle_config_grade --置换车辆配置等级
, displace_vehicle_model_code as displace_vehicle_model_code --置换车型代码
, displace_vehicle_model_name as displace_vehicle_model_name --置换车型名字
, purchase_obtain_evidence_id as purchase_obtain_evidence_id --采购取证ID
, invoice_number as invoice_number --发票号
, invoice_amount as invoice_amount --发票金额
, actual_transfer_date as actual_transfer_date --实际过户日期
, seller_name as seller_name --卖方姓名
, seller_id_card as seller_id_card --卖方身份证
, buyer_name as buyer_name --买主名
, buyer_id_card as buyer_id_card --买方身份证
, apply_time as apply_time --申请时间
, apply_staff_account as apply_staff_account --申请担当账号
, apply_staff_name as apply_staff_name --申请担当姓名
, approve_time as approve_time --审批时间
, approve_staff_account as approve_staff_account --审核担当账号
, approve_staff_name as approve_staff_name --审核担当姓名
, approve_opinion as approve_opinion --审批意见
, cancel_time as cancel_time --取消时间
, cancel_staff_account as cancel_staff_account --取消担当账号
, cancel_staff_name as cancel_staff_name --取消担当姓名
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
from ods.ods_dms_uc_uc_new_vhcl_exchange_achv
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(new_vehicle_displace_achv_id, '')) as new_vehicle_displace_achv_id  --新车置换业绩ID
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(license_plate_number, '')) as license_plate_number  --车牌号
, trim(nvl(achv_apply_status, '')) as achv_apply_status  --业绩申请状态
, trim(nvl(present_type, '')) as present_type  --赠品类型
, trim(nvl(displace_vehicle_related_id, '')) as displace_vehicle_related_id  --置换车辆关联ID
, trim(nvl(displace_vehicle_vinno, '')) as displace_vehicle_vinno  --置换车辆车架号
, trim(nvl(displace_vehicle_nominee_name, '')) as displace_vehicle_nominee_name  --置换车辆名义人姓名
, trim(nvl(displace_vehicle_nominee_tel, '')) as displace_vehicle_nominee_tel  --置换车辆名义人电话
, trim(nvl(displace_vehicle_sales_date, '')) as displace_vehicle_sales_date  --置换车辆销售日期
, trim(nvl(displace_vehicle_picture_url, '')) as displace_vehicle_picture_url  --置换车辆图片路径
, trim(nvl(displace_vehicle_config_grade, '')) as displace_vehicle_config_grade  --置换车辆配置等级
, trim(nvl(displace_vehicle_model_code, '')) as displace_vehicle_model_code  --置换车型代码
, trim(nvl(displace_vehicle_model_name, '')) as displace_vehicle_model_name  --置换车型名字
, trim(nvl(purchase_obtain_evidence_id, '')) as purchase_obtain_evidence_id  --采购取证ID
, trim(nvl(invoice_number, '')) as invoice_number  --发票号
, nvl(invoice_amount, 0) as invoice_amount  --发票金额
, trim(nvl(actual_transfer_date, '')) as actual_transfer_date  --实际过户日期
, trim(nvl(seller_name, '')) as seller_name  --卖方姓名
, trim(nvl(seller_id_card, '')) as seller_id_card  --卖方身份证
, trim(nvl(buyer_name, '')) as buyer_name  --买主名
, trim(nvl(buyer_id_card, '')) as buyer_id_card  --买方身份证
, trim(nvl(apply_time, '')) as apply_time  --申请时间
, trim(nvl(apply_staff_account, '')) as apply_staff_account  --申请担当账号
, trim(nvl(apply_staff_name, '')) as apply_staff_name  --申请担当姓名
, trim(nvl(approve_time, '')) as approve_time  --审批时间
, trim(nvl(approve_staff_account, '')) as approve_staff_account  --审核担当账号
, trim(nvl(approve_staff_name, '')) as approve_staff_name  --审核担当姓名
, trim(nvl(approve_opinion, '')) as approve_opinion  --审批意见
, trim(nvl(cancel_time, '')) as cancel_time  --取消时间
, trim(nvl(cancel_staff_account, '')) as cancel_staff_account  --取消担当账号
, trim(nvl(cancel_staff_name, '')) as cancel_staff_name  --取消担当姓名
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
from dl.tg_dms_uc_t_uc_new_vhcl_exchange_achv
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_new_vhcl_exchange_achv
select  id as id --主键
, new_vehicle_displace_achv_id as new_vehicle_displace_achv_id --新车置换业绩ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, license_plate_number as license_plate_number --车牌号
, achv_apply_status as achv_apply_status --业绩申请状态
, present_type as present_type --赠品类型
, displace_vehicle_related_id as displace_vehicle_related_id --置换车辆关联ID
, displace_vehicle_vinno as displace_vehicle_vinno --置换车辆车架号
, displace_vehicle_nominee_name as displace_vehicle_nominee_name --置换车辆名义人姓名
, displace_vehicle_nominee_tel as displace_vehicle_nominee_tel --置换车辆名义人电话
, displace_vehicle_sales_date as displace_vehicle_sales_date --置换车辆销售日期
, displace_vehicle_picture_url as displace_vehicle_picture_url --置换车辆图片路径
, displace_vehicle_config_grade as displace_vehicle_config_grade --置换车辆配置等级
, displace_vehicle_model_code as displace_vehicle_model_code --置换车型代码
, displace_vehicle_model_name as displace_vehicle_model_name --置换车型名字
, purchase_obtain_evidence_id as purchase_obtain_evidence_id --采购取证ID
, invoice_number as invoice_number --发票号
, invoice_amount as invoice_amount --发票金额
, actual_transfer_date as actual_transfer_date --实际过户日期
, seller_name as seller_name --卖方姓名
, seller_id_card as seller_id_card --卖方身份证
, buyer_name as buyer_name --买主名
, buyer_id_card as buyer_id_card --买方身份证
, apply_time as apply_time --申请时间
, apply_staff_account as apply_staff_account --申请担当账号
, apply_staff_name as apply_staff_name --申请担当姓名
, approve_time as approve_time --审批时间
, approve_staff_account as approve_staff_account --审核担当账号
, approve_staff_name as approve_staff_name --审核担当姓名
, approve_opinion as approve_opinion --审批意见
, cancel_time as cancel_time --取消时间
, cancel_staff_account as cancel_staff_account --取消担当账号
, cancel_staff_name as cancel_staff_name --取消担当姓名
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
row_number() over (partition by new_vehicle_displace_achv_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_new_vhcl_exchange_achv_tmp) a
where rn = 1;


