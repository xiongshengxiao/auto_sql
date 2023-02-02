set mapred.job.name=job_ods_dms_uc_uc_sales_contract_cost;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_sales_contract_cost_tmp as (
select  id as id --主键
, sales_contract_serial as sales_contract_serial --销售合同编号
, cost_item_code as cost_item_code --费用项代码
, cost_item_name as cost_item_name --费用项名称
, cost_type as cost_type --费用类型
, unit_price as unit_price --单价
, quantity as quantity --数量
, sales_price as sales_price --销售价格
, cost_item_cost_price as cost_item_cost_price --费用项成本价
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
from ods.ods_dms_uc_uc_sales_contract_cost
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(sales_contract_serial, '')) as sales_contract_serial  --销售合同编号
, trim(nvl(cost_item_code, '')) as cost_item_code  --费用项代码
, trim(nvl(cost_item_name, '')) as cost_item_name  --费用项名称
, trim(nvl(cost_type, '')) as cost_type  --费用类型
, nvl(unit_price, 0) as unit_price  --单价
, nvl(quantity, 0) as quantity  --数量
, nvl(sales_price, 0) as sales_price  --销售价格
, nvl(cost_item_cost_price, 0) as cost_item_cost_price  --费用项成本价
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
from dl.tg_dms_uc_t_uc_sales_contract_cost
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_sales_contract_cost
select  id as id --主键
, sales_contract_serial as sales_contract_serial --销售合同编号
, cost_item_code as cost_item_code --费用项代码
, cost_item_name as cost_item_name --费用项名称
, cost_type as cost_type --费用类型
, unit_price as unit_price --单价
, quantity as quantity --数量
, sales_price as sales_price --销售价格
, cost_item_cost_price as cost_item_cost_price --费用项成本价
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
row_number() over (partition by sales_contract_serial,cost_item_code
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_sales_contract_cost_tmp) a
where rn = 1;


