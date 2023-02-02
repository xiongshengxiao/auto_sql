set mapred.job.name=job_ods_dms_uc_uc_purchase_contract_other_cost;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_purchase_contract_other_cost_tmp as (
select  id as id --主键
, purchase_other_cost_id as purchase_other_cost_id --采购其他费用ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, purchase_contract_id as purchase_contract_id --采购协议ID
, cost_item_code as cost_item_code --费用项代码
, cost_item_name as cost_item_name --费用项名称
, cost_amount as cost_amount --费用金额
, cost_input_date as cost_input_date --费用录入日期
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
from ods.ods_dms_uc_uc_purchase_contract_other_cost
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(purchase_other_cost_id, '')) as purchase_other_cost_id  --采购其他费用ID
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(purchase_contract_id, '')) as purchase_contract_id  --采购协议ID
, trim(nvl(cost_item_code, '')) as cost_item_code  --费用项代码
, trim(nvl(cost_item_name, '')) as cost_item_name  --费用项名称
, nvl(cost_amount, 0) as cost_amount  --费用金额
, trim(nvl(cost_input_date, '')) as cost_input_date  --费用录入日期
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
from dl.tg_dms_uc_t_uc_purchase_contract_other_cost
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_purchase_contract_other_cost
select  id as id --主键
, purchase_other_cost_id as purchase_other_cost_id --采购其他费用ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, purchase_contract_id as purchase_contract_id --采购协议ID
, cost_item_code as cost_item_code --费用项代码
, cost_item_name as cost_item_name --费用项名称
, cost_amount as cost_amount --费用金额
, cost_input_date as cost_input_date --费用录入日期
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
row_number() over (partition by dealer_code,vehicle_manage_no,purchase_contract_id,cost_item_code
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_purchase_contract_other_cost_tmp) a
where rn = 1;


