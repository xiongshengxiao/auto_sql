set mapred.job.name=job_ods_dms_cs_meta_second_level_source;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_cs_meta_second_level_source_tmp as (
select  id as id --主键
, source_first_level_code as source_first_level_code --一级来源代码
, source_code as source_code --来源代码
, source_name as source_name --来源名称
, create_by as create_by --创建者
, create_time as create_time --创建时间
, update_by as update_by --更新者
, update_time as update_time --更新时间
, del_flag as del_flag --删除标识
, remark as remark --备注
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from ods.ods_dms_cs_meta_second_level_source
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(source_first_level_code, '')) as source_first_level_code  --一级来源代码
, trim(nvl(source_code, '')) as source_code  --来源代码
, trim(nvl(source_name, '')) as source_name  --来源名称
, nvl(create_by, 0) as create_by  --创建者
, trim(nvl(create_time, '')) as create_time  --创建时间
, nvl(update_by, 0) as update_by  --更新者
, trim(nvl(update_time, '')) as update_time  --更新时间
, nvl(del_flag, 0) as del_flag  --删除标识
, trim(nvl(remark, '')) as remark  --备注
, etl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from dl.tg_dms_cs_t_meta_second_level_source
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_cs_meta_second_level_source
select  id as id --主键
, source_first_level_code as source_first_level_code --一级来源代码
, source_code as source_code --来源代码
, source_name as source_name --来源名称
, create_by as create_by --创建者
, create_time as create_time --创建时间
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
row_number() over (partition by source_code
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_cs_meta_second_level_source_tmp) a
where rn = 1;

