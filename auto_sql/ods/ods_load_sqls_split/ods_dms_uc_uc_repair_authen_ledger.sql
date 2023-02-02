set mapred.job.name=job_ods_dms_uc_uc_repair_authen_ledger;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_repair_authen_ledger_tmp as (
select  id as id --主键
, repair_authen_id as repair_authen_id --整备认证ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, ledger_status as ledger_status --台账状态
, uc_auth_due_date as uc_auth_due_date --二手车认证到期日
, uc_auth_time as uc_auth_time --二手车认证生效时间
, authen_apply_type_code as authen_apply_type_code --认证申请类型CD
, authen_type as authen_type --认证类型
, auth_apply_time as auth_apply_time --认证申请时间
, auth_apply_by_code as auth_apply_by_code --认证申请人代码
, uc_auth_no as uc_auth_no --二手车认证编号
, gtmc_vehicle_flag as gtmc_vehicle_flag --广汽丰田车辆标识
, delivery_finish_date as delivery_finish_date --交车完成日期
, sales_method as sales_method --销售方式
, first_regist_date as first_regist_date --首次注册日期
, ass_date as ass_date --评估日期
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, odometer as odometer --行驶里程
, vehicle_type_code as vehicle_type_code --车辆类型代码
, vehicle_category_name as vehicle_category_name --车辆类型名称
, vehicle_grade as vehicle_grade --车辆等级
, collision_accident_diff as collision_accident_diff --碰撞事故区分
, water_accident_diff as water_accident_diff --水浸事故区分
, fire_accident_diff as fire_accident_diff --火烧事故区分
, excessive_modified_diff as excessive_modified_diff --过度改装区分
, wty_type as wty_type --三包类型
, instrument_tampering_flag as instrument_tampering_flag --仪表篡改标识
, elect_level as elect_level --机电级别
, accident_level as accident_level --事故级别
, exhaust as exhaust --排量
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
from ods.ods_dms_uc_uc_repair_authen_ledger
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(repair_authen_id, '')) as repair_authen_id  --整备认证ID
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(ledger_status, '')) as ledger_status  --台账状态
, trim(nvl(uc_auth_due_date, '')) as uc_auth_due_date  --二手车认证到期日
, trim(nvl(uc_auth_time, '')) as uc_auth_time  --二手车认证生效时间
, trim(nvl(authen_apply_type_code, '')) as authen_apply_type_code  --认证申请类型CD
, trim(nvl(authen_type, '')) as authen_type  --认证类型
, trim(nvl(auth_apply_time, '')) as auth_apply_time  --认证申请时间
, trim(nvl(auth_apply_by_code, '')) as auth_apply_by_code  --认证申请人代码
, trim(nvl(uc_auth_no, '')) as uc_auth_no  --二手车认证编号
, trim(nvl(gtmc_vehicle_flag, '')) as gtmc_vehicle_flag  --广汽丰田车辆标识
, trim(nvl(delivery_finish_date, '')) as delivery_finish_date  --交车完成日期
, trim(nvl(sales_method, '')) as sales_method  --销售方式
, trim(nvl(first_regist_date, '')) as first_regist_date  --首次注册日期
, trim(nvl(ass_date, '')) as ass_date  --评估日期
, trim(nvl(maker_code, '')) as maker_code  --厂商代码
, trim(nvl(maker_name, '')) as maker_name  --厂商名称
, nvl(odometer, 0) as odometer  --行驶里程
, trim(nvl(vehicle_type_code, '')) as vehicle_type_code  --车辆类型代码
, trim(nvl(vehicle_category_name, '')) as vehicle_category_name  --车辆类型名称
, trim(nvl(vehicle_grade, '')) as vehicle_grade  --车辆等级
, trim(nvl(collision_accident_diff, '')) as collision_accident_diff  --碰撞事故区分
, trim(nvl(water_accident_diff, '')) as water_accident_diff  --水浸事故区分
, trim(nvl(fire_accident_diff, '')) as fire_accident_diff  --火烧事故区分
, trim(nvl(excessive_modified_diff, '')) as excessive_modified_diff  --过度改装区分
, trim(nvl(wty_type, '')) as wty_type  --三包类型
, trim(nvl(instrument_tampering_flag, '')) as instrument_tampering_flag  --仪表篡改标识
, trim(nvl(elect_level, '')) as elect_level  --机电级别
, trim(nvl(accident_level, '')) as accident_level  --事故级别
, trim(nvl(exhaust, '')) as exhaust  --排量
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
from dl.tg_dms_uc_t_uc_repair_authen_ledger
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_repair_authen_ledger
select  id as id --主键
, repair_authen_id as repair_authen_id --整备认证ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, ledger_status as ledger_status --台账状态
, uc_auth_due_date as uc_auth_due_date --二手车认证到期日
, uc_auth_time as uc_auth_time --二手车认证生效时间
, authen_apply_type_code as authen_apply_type_code --认证申请类型CD
, authen_type as authen_type --认证类型
, auth_apply_time as auth_apply_time --认证申请时间
, auth_apply_by_code as auth_apply_by_code --认证申请人代码
, uc_auth_no as uc_auth_no --二手车认证编号
, gtmc_vehicle_flag as gtmc_vehicle_flag --广汽丰田车辆标识
, delivery_finish_date as delivery_finish_date --交车完成日期
, sales_method as sales_method --销售方式
, first_regist_date as first_regist_date --首次注册日期
, ass_date as ass_date --评估日期
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, odometer as odometer --行驶里程
, vehicle_type_code as vehicle_type_code --车辆类型代码
, vehicle_category_name as vehicle_category_name --车辆类型名称
, vehicle_grade as vehicle_grade --车辆等级
, collision_accident_diff as collision_accident_diff --碰撞事故区分
, water_accident_diff as water_accident_diff --水浸事故区分
, fire_accident_diff as fire_accident_diff --火烧事故区分
, excessive_modified_diff as excessive_modified_diff --过度改装区分
, wty_type as wty_type --三包类型
, instrument_tampering_flag as instrument_tampering_flag --仪表篡改标识
, elect_level as elect_level --机电级别
, accident_level as accident_level --事故级别
, exhaust as exhaust --排量
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
row_number() over (partition by repair_authen_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_repair_authen_ledger_tmp) a
where rn = 1;


