set mapred.job.name=job_ods_dms_uc_uc_assessment_vehicle_info_2_init;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
insert overwrite table ods.ods_dms_uc_uc_assessment_vehicle_info_2
select  id --主键
, ass_serial --评估编号
, dealer_code --销售店代码
, vehicle_manage_no --车辆管理编号
, branch_code --店铺代码
, vehicle_check_eff_period --车辆年审期限
, comp_ins_period --交强险期限
, vehicle_tax_period --车船税期限
, business_ins_eff_period --商业保险有效期限
, vehicle_tools --车载工具
, vehicle_use_manual --车辆使用手册
, vehicle_service_manual --车辆维修保养手册
, del_flag --删除标识
, create_by --创建者
, create_time --创建时间
, update_by --更新者
, update_time --更新时间
, remark --备注
, version --版本号
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, 'DMS' as source_system --来源系统
, substr(current_timestamp(), 1, 19) as insert_time --数据写入时间
, biz_date as biz_date  --分区字段
from (
select  trim(nvl(id, '')) as id  --主键
, nvl(ass_serial, 0) as ass_serial  --评估编号
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_check_eff_period, '')) as vehicle_check_eff_period  --车辆年审期限
, trim(nvl(comp_ins_period, '')) as comp_ins_period  --交强险期限
, trim(nvl(vehicle_tax_period, '')) as vehicle_tax_period  --车船税期限
, trim(nvl(business_ins_eff_period, '')) as business_ins_eff_period  --商业保险有效期限
, trim(nvl(vehicle_tools, '')) as vehicle_tools  --车载工具
, trim(nvl(vehicle_use_manual, '')) as vehicle_use_manual  --车辆使用手册
, trim(nvl(vehicle_service_manual, '')) as vehicle_service_manual  --车辆维修保养手册
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
, row_number() over(partition by ass_serial
order by update_time desc, load_time desc, create_time desc) rn
from dl.tg_dms_uc_t_uc_assessment_vehicle_info_2
where biz_date < '${TODAY}' )aa
where rn = 1;


