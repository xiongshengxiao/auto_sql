set mapred.job.name=job_ods_dms_uc_uc_authen_apply;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_authen_apply_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, auth_apply_no as auth_apply_no --认证申请编号
, first_regist_date as first_regist_date --首次注册日期
, vinno as vinno --车架号
, uc_auth_no as uc_auth_no --二手车认证编号
, authen_type as authen_type --认证类型
, other_dealer_authen_flag as other_dealer_authen_flag --其他销售店认证标识
, authen_apply_type_code as authen_apply_type_code --认证申请类型CD
, auth_status as auth_status --认证状态
, cancel_reason_type as cancel_reason_type --取消原因类型
, auth_apply_time as auth_apply_time --认证申请时间
, authen_apply_staff_account as authen_apply_staff_account --认证申请担当账号
, authen_apply_staff_name as authen_apply_staff_name --认证申请担当姓名
, authen_operate_time as authen_operate_time --认证操作时间
, authen_operate_account as authen_operate_account --认证操作账号
, authen_operate_staff_name as authen_operate_staff_name --认证操作担当姓名
, authen_operate_opinion as authen_operate_opinion --认证操作意见
, uc_auth_time as uc_auth_time --二手车认证生效时间
, uc_auth_due_date as uc_auth_due_date --二手车认证到期日
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
from ods.ods_dms_uc_uc_authen_apply
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(auth_apply_no, '')) as auth_apply_no  --认证申请编号
, trim(nvl(first_regist_date, '')) as first_regist_date  --首次注册日期
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(uc_auth_no, '')) as uc_auth_no  --二手车认证编号
, trim(nvl(authen_type, '')) as authen_type  --认证类型
, trim(nvl(other_dealer_authen_flag, '')) as other_dealer_authen_flag  --其他销售店认证标识
, trim(nvl(authen_apply_type_code, '')) as authen_apply_type_code  --认证申请类型CD
, trim(nvl(auth_status, '')) as auth_status  --认证状态
, trim(nvl(cancel_reason_type, '')) as cancel_reason_type  --取消原因类型
, trim(nvl(auth_apply_time, '')) as auth_apply_time  --认证申请时间
, trim(nvl(authen_apply_staff_account, '')) as authen_apply_staff_account  --认证申请担当账号
, trim(nvl(authen_apply_staff_name, '')) as authen_apply_staff_name  --认证申请担当姓名
, trim(nvl(authen_operate_time, '')) as authen_operate_time  --认证操作时间
, trim(nvl(authen_operate_account, '')) as authen_operate_account  --认证操作账号
, trim(nvl(authen_operate_staff_name, '')) as authen_operate_staff_name  --认证操作担当姓名
, trim(nvl(authen_operate_opinion, '')) as authen_operate_opinion  --认证操作意见
, trim(nvl(uc_auth_time, '')) as uc_auth_time  --二手车认证生效时间
, trim(nvl(uc_auth_due_date, '')) as uc_auth_due_date  --二手车认证到期日
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
from dl.tg_dms_uc_t_uc_authen_apply
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_authen_apply
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, auth_apply_no as auth_apply_no --认证申请编号
, first_regist_date as first_regist_date --首次注册日期
, vinno as vinno --车架号
, uc_auth_no as uc_auth_no --二手车认证编号
, authen_type as authen_type --认证类型
, other_dealer_authen_flag as other_dealer_authen_flag --其他销售店认证标识
, authen_apply_type_code as authen_apply_type_code --认证申请类型CD
, auth_status as auth_status --认证状态
, cancel_reason_type as cancel_reason_type --取消原因类型
, auth_apply_time as auth_apply_time --认证申请时间
, authen_apply_staff_account as authen_apply_staff_account --认证申请担当账号
, authen_apply_staff_name as authen_apply_staff_name --认证申请担当姓名
, authen_operate_time as authen_operate_time --认证操作时间
, authen_operate_account as authen_operate_account --认证操作账号
, authen_operate_staff_name as authen_operate_staff_name --认证操作担当姓名
, authen_operate_opinion as authen_operate_opinion --认证操作意见
, uc_auth_time as uc_auth_time --二手车认证生效时间
, uc_auth_due_date as uc_auth_due_date --二手车认证到期日
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
row_number() over (partition by auth_apply_no
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_authen_apply_tmp) a
where rn = 1;


