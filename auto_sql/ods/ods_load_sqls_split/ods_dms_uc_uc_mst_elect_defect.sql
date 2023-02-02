set mapred.job.name=job_ods_dms_uc_uc_mst_elect_defect;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_mst_elect_defect_tmp as (
select  id as id --主键
, detect_position_type as detect_position_type --检测部位类型
, detect_position_group_code as detect_position_group_code --检测部位组代码
, detect_position_code as detect_position_code --检测部位代码
, sort as sort --排序
, defect_item_name as defect_item_name --缺陷项名称
, defect_item_name_local as defect_item_name_local --缺陷项名称本地
, defect_item_name_2 as defect_item_name_2 --缺陷项名称2
, defect_item_name_2_local as defect_item_name_2_local --缺陷项名称2本地
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
from ods.ods_dms_uc_uc_mst_elect_defect
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(detect_position_type, '')) as detect_position_type  --检测部位类型
, trim(nvl(detect_position_group_code, '')) as detect_position_group_code  --检测部位组代码
, trim(nvl(detect_position_code, '')) as detect_position_code  --检测部位代码
, nvl(sort, 0) as sort  --排序
, trim(nvl(defect_item_name, '')) as defect_item_name  --缺陷项名称
, trim(nvl(defect_item_name_local, '')) as defect_item_name_local  --缺陷项名称本地
, trim(nvl(defect_item_name_2, '')) as defect_item_name_2  --缺陷项名称2
, trim(nvl(defect_item_name_2_local, '')) as defect_item_name_2_local  --缺陷项名称2本地
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
from dl.tg_dms_uc_t_uc_mst_elect_defect
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_mst_elect_defect
select  id as id --主键
, detect_position_type as detect_position_type --检测部位类型
, detect_position_group_code as detect_position_group_code --检测部位组代码
, detect_position_code as detect_position_code --检测部位代码
, sort as sort --排序
, defect_item_name as defect_item_name --缺陷项名称
, defect_item_name_local as defect_item_name_local --缺陷项名称本地
, defect_item_name_2 as defect_item_name_2 --缺陷项名称2
, defect_item_name_2_local as defect_item_name_2_local --缺陷项名称2本地
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
row_number() over (partition by detect_position_type,detect_position_group_code,detect_position_code
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_mst_elect_defect_tmp) a
where rn = 1;


