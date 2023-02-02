set mapred.job.name=job_ods_dms_uc_uc_repair_cost_init;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
insert overwrite table ods.ods_dms_uc_uc_repair_cost
select  id --主键
, dealer_code --销售店代码
, branch_code --店铺代码
, vehicle_manage_no --车辆管理编号
, vinno --车架号
, repair_id --整备ID
, repair_item_id --整备项ID
, project_source --项目来源
, repair_item_type --整备项类型
, repair_item_type_name --整备项类型名称
, repair_item_code --整备项代码
, repair_item_name --整备项名称
, repair_item_cost --整备项费用
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
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(repair_id, '')) as repair_id  --整备ID
, trim(nvl(repair_item_id, '')) as repair_item_id  --整备项ID
, nvl(project_source, 0) as project_source  --项目来源
, trim(nvl(repair_item_type, '')) as repair_item_type  --整备项类型
, trim(nvl(repair_item_type_name, '')) as repair_item_type_name  --整备项类型名称
, trim(nvl(repair_item_code, '')) as repair_item_code  --整备项代码
, trim(nvl(repair_item_name, '')) as repair_item_name  --整备项名称
, nvl(repair_item_cost, 0) as repair_item_cost  --整备项费用
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
, row_number() over(partition by repair_id
order by update_time desc, load_time desc, create_time desc) rn
from dl.tg_dms_uc_t_uc_repair_cost
where biz_date < '${TODAY}' )aa
where rn = 1;


