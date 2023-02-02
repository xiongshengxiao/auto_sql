set mapred.job.name=job_ods_dms_uc_uc_repair_item;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_repair_item_tmp as (
select  id as id --主键
, repair_item_id as repair_item_id --整备项ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, repair_item_code as repair_item_code --整备项代码
, repair_item_name as repair_item_name --整备项名称
, repair_item_type as repair_item_type --整备项类型
, repair_item_type_name as repair_item_type_name --整备项类型名称
, repair_item_cost as repair_item_cost --整备项费用
, retail_bench_cost as retail_bench_cost --零售基准费用
, default_show_flag as default_show_flag --默认展示标识
, brand_limit_diff as brand_limit_diff --品牌限定区分
, default_choose_flag as default_choose_flag --默认选择标识
, sq as sq --序列号
, repair_item_status as repair_item_status --整备项状态
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
from ods.ods_dms_uc_uc_repair_item
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(repair_item_id, '')) as repair_item_id  --整备项ID
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(repair_item_code, '')) as repair_item_code  --整备项代码
, trim(nvl(repair_item_name, '')) as repair_item_name  --整备项名称
, trim(nvl(repair_item_type, '')) as repair_item_type  --整备项类型
, trim(nvl(repair_item_type_name, '')) as repair_item_type_name  --整备项类型名称
, nvl(repair_item_cost, 0) as repair_item_cost  --整备项费用
, nvl(retail_bench_cost, 0) as retail_bench_cost  --零售基准费用
, nvl(default_show_flag, 0) as default_show_flag  --默认展示标识
, nvl(brand_limit_diff, 0) as brand_limit_diff  --品牌限定区分
, nvl(default_choose_flag, 0) as default_choose_flag  --默认选择标识
, nvl(sq, 0) as sq  --序列号
, trim(nvl(repair_item_status, '')) as repair_item_status  --整备项状态
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
from dl.tg_dms_uc_t_uc_repair_item
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_repair_item
select  id as id --主键
, repair_item_id as repair_item_id --整备项ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, repair_item_code as repair_item_code --整备项代码
, repair_item_name as repair_item_name --整备项名称
, repair_item_type as repair_item_type --整备项类型
, repair_item_type_name as repair_item_type_name --整备项类型名称
, repair_item_cost as repair_item_cost --整备项费用
, retail_bench_cost as retail_bench_cost --零售基准费用
, default_show_flag as default_show_flag --默认展示标识
, brand_limit_diff as brand_limit_diff --品牌限定区分
, default_choose_flag as default_choose_flag --默认选择标识
, sq as sq --序列号
, repair_item_status as repair_item_status --整备项状态
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
row_number() over (partition by repair_item_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_repair_item_tmp) a
where rn = 1;


