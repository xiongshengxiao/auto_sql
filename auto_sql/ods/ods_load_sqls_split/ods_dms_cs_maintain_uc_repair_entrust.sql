set mapred.job.name=job_ods_dms_cs_maintain_uc_repair_entrust;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_cs_maintain_uc_repair_entrust_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, repair_status as repair_status --整备状态
, repair_type as repair_type --整备类型
, dealer_repair_start_time as dealer_repair_start_time --销售店整备开始时间
, dealer_repair_end_time as dealer_repair_end_time --销售店整备结束时间
, user_pay_repair_cost as user_pay_repair_cost --用户承担整备费用
, dealer_repair_cost as dealer_repair_cost --销售店整备成本
, repair_entrust_id as repair_entrust_id --整备委托ID
, repair_entrust_date as repair_entrust_date --整备委托日期
, repair_entrust_appraiser_code as repair_entrust_appraiser_code --整备委托评估师代码
, repair_entrust_memo as repair_entrust_memo --整备委托留言
, maintain_order_no as maintain_order_no --维修工单号
, maintain_order_accept_date as maintain_order_accept_date --维修工单受理日
, maintain_order_start_date as maintain_order_start_date --维修工单开始日
, maintain_order_complete_date as maintain_order_complete_date --维修工单完成日
, maintain_order_cancel_date as maintain_order_cancel_date --维修工单取消日期
, maintain_order_cancel_reason as maintain_order_cancel_reason --维修工单取消原因
, maintain_order_settl_date as maintain_order_settl_date --维修工单结算日
, maintain_order_settl_fee as maintain_order_settl_fee --维修工单结算费用
, service_staff_account as service_staff_account --服务担当账号
, nominee_type as nominee_type --名义人类型
, nominee_name as nominee_name --名义人姓名
, nominee_id_card as nominee_id_card --名义人身份证号码
, nominee_postal_code as nominee_postal_code --名义人邮编
, nominee_telephone as nominee_telephone --名义人电话
, nominee_email as nominee_email --名义人EMAIL
, nominee_mobile_phone as nominee_mobile_phone --名义人手机号
, uc_auth_flag as uc_auth_flag --二手车认证标识
, uc_auth_no as uc_auth_no --二手车认证编号
, nominee_address as nominee_address --名义人住所
, create_time as create_time --创建时间
, create_by as create_by --创建者
, update_by as update_by --更新者
, update_time as update_time --更新时间
, del_flag as del_flag --删除标识
, remark as remark --备注
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from ods.ods_dms_cs_maintain_uc_repair_entrust
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(repair_status, '')) as repair_status  --整备状态
, nvl(repair_type, 0) as repair_type  --整备类型
, trim(nvl(dealer_repair_start_time, '')) as dealer_repair_start_time  --销售店整备开始时间
, trim(nvl(dealer_repair_end_time, '')) as dealer_repair_end_time  --销售店整备结束时间
, nvl(user_pay_repair_cost, 0) as user_pay_repair_cost  --用户承担整备费用
, nvl(dealer_repair_cost, 0) as dealer_repair_cost  --销售店整备成本
, trim(nvl(repair_entrust_id, '')) as repair_entrust_id  --整备委托ID
, trim(nvl(repair_entrust_date, '')) as repair_entrust_date  --整备委托日期
, trim(nvl(repair_entrust_appraiser_code, '')) as repair_entrust_appraiser_code  --整备委托评估师代码
, trim(nvl(repair_entrust_memo, '')) as repair_entrust_memo  --整备委托留言
, trim(nvl(maintain_order_no, '')) as maintain_order_no  --维修工单号
, trim(nvl(maintain_order_accept_date, '')) as maintain_order_accept_date  --维修工单受理日
, trim(nvl(maintain_order_start_date, '')) as maintain_order_start_date  --维修工单开始日
, trim(nvl(maintain_order_complete_date, '')) as maintain_order_complete_date  --维修工单完成日
, trim(nvl(maintain_order_cancel_date, '')) as maintain_order_cancel_date  --维修工单取消日期
, trim(nvl(maintain_order_cancel_reason, '')) as maintain_order_cancel_reason  --维修工单取消原因
, trim(nvl(maintain_order_settl_date, '')) as maintain_order_settl_date  --维修工单结算日
, nvl(maintain_order_settl_fee, 0) as maintain_order_settl_fee  --维修工单结算费用
, trim(nvl(service_staff_account, '')) as service_staff_account  --服务担当账号
, trim(nvl(nominee_type, '')) as nominee_type  --名义人类型
, trim(nvl(nominee_name, '')) as nominee_name  --名义人姓名
, trim(nvl(nominee_id_card, '')) as nominee_id_card  --名义人身份证号码
, trim(nvl(nominee_postal_code, '')) as nominee_postal_code  --名义人邮编
, trim(nvl(nominee_telephone, '')) as nominee_telephone  --名义人电话
, trim(nvl(nominee_email, '')) as nominee_email  --名义人EMAIL
, trim(nvl(nominee_mobile_phone, '')) as nominee_mobile_phone  --名义人手机号
, trim(nvl(uc_auth_flag, '')) as uc_auth_flag  --二手车认证标识
, trim(nvl(uc_auth_no, '')) as uc_auth_no  --二手车认证编号
, trim(nvl(nominee_address, '')) as nominee_address  --名义人住所
, trim(nvl(create_time, '')) as create_time  --创建时间
, nvl(create_by, 0) as create_by  --创建者
, nvl(update_by, 0) as update_by  --更新者
, trim(nvl(update_time, '')) as update_time  --更新时间
, nvl(del_flag, 0) as del_flag  --删除标识
, trim(nvl(remark, '')) as remark  --备注
, etl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from dl.tg_dms_cs_t_maintain_uc_repair_entrust
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_cs_maintain_uc_repair_entrust
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, repair_status as repair_status --整备状态
, repair_type as repair_type --整备类型
, dealer_repair_start_time as dealer_repair_start_time --销售店整备开始时间
, dealer_repair_end_time as dealer_repair_end_time --销售店整备结束时间
, user_pay_repair_cost as user_pay_repair_cost --用户承担整备费用
, dealer_repair_cost as dealer_repair_cost --销售店整备成本
, repair_entrust_id as repair_entrust_id --整备委托ID
, repair_entrust_date as repair_entrust_date --整备委托日期
, repair_entrust_appraiser_code as repair_entrust_appraiser_code --整备委托评估师代码
, repair_entrust_memo as repair_entrust_memo --整备委托留言
, maintain_order_no as maintain_order_no --维修工单号
, maintain_order_accept_date as maintain_order_accept_date --维修工单受理日
, maintain_order_start_date as maintain_order_start_date --维修工单开始日
, maintain_order_complete_date as maintain_order_complete_date --维修工单完成日
, maintain_order_cancel_date as maintain_order_cancel_date --维修工单取消日期
, maintain_order_cancel_reason as maintain_order_cancel_reason --维修工单取消原因
, maintain_order_settl_date as maintain_order_settl_date --维修工单结算日
, maintain_order_settl_fee as maintain_order_settl_fee --维修工单结算费用
, service_staff_account as service_staff_account --服务担当账号
, nominee_type as nominee_type --名义人类型
, nominee_name as nominee_name --名义人姓名
, nominee_id_card as nominee_id_card --名义人身份证号码
, nominee_postal_code as nominee_postal_code --名义人邮编
, nominee_telephone as nominee_telephone --名义人电话
, nominee_email as nominee_email --名义人EMAIL
, nominee_mobile_phone as nominee_mobile_phone --名义人手机号
, uc_auth_flag as uc_auth_flag --二手车认证标识
, uc_auth_no as uc_auth_no --二手车认证编号
, nominee_address as nominee_address --名义人住所
, create_time as create_time --创建时间
, create_by as create_by --创建者
, update_by as update_by --更新者
, update_time as update_time --更新时间
, del_flag as del_flag --删除标识
, remark as remark --备注
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, 'DMS共通' as source_system --来源系统
, substr(current_timestamp(), 1, 19) as insert_time --数据写入时间
, biz_date as biz_date  --分区字段
from (
select *,
row_number() over (partition by vehicle_manage_no,maintain_order_no
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_cs_maintain_uc_repair_entrust_tmp) a
where rn = 1;


