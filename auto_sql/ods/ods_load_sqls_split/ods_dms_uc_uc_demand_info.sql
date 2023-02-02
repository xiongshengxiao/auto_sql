set mapred.job.name=job_ods_dms_uc_uc_demand_info;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_demand_info_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, uc_req_id as uc_req_id --二手车需求ID
, uc_clue_id as uc_clue_id --二手车线索ID
, uc_req_status as uc_req_status --二手车需求状态
, business_diff as business_diff --业务区分
, sold_vehicle_model_name as sold_vehicle_model_name --售出车型名称
, icm_sold_vehicle_info_json as icm_sold_vehicle_info_json --ICM售出车辆信息Json
, d2c_sold_vehicle_info_json as d2c_sold_vehicle_info_json --D2C售出车辆信息Json
, car_buy_intn_type as car_buy_intn_type --购车意向类型
, intn_new_vehicle_info_json as intn_new_vehicle_info_json --意向新车信息Json
, intn_uc_type as intn_uc_type --意向二手车类型
, intn_vehicle_manage_no as intn_vehicle_manage_no --意向车辆管理编号
, intn_uc_cond_json as intn_uc_cond_json --意向二手车条件Json
, purchase_type as purchase_type --购买类型
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
from ods.ods_dms_uc_uc_demand_info
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(uc_req_id, '')) as uc_req_id  --二手车需求ID
, trim(nvl(uc_clue_id, '')) as uc_clue_id  --二手车线索ID
, trim(nvl(uc_req_status, '')) as uc_req_status  --二手车需求状态
, trim(nvl(business_diff, '')) as business_diff  --业务区分
, trim(nvl(sold_vehicle_model_name, '')) as sold_vehicle_model_name  --售出车型名称
, trim(nvl(icm_sold_vehicle_info_json, '')) as icm_sold_vehicle_info_json  --ICM售出车辆信息Json
, trim(nvl(d2c_sold_vehicle_info_json, '')) as d2c_sold_vehicle_info_json  --D2C售出车辆信息Json
, trim(nvl(car_buy_intn_type, '')) as car_buy_intn_type  --购车意向类型
, trim(nvl(intn_new_vehicle_info_json, '')) as intn_new_vehicle_info_json  --意向新车信息Json
, trim(nvl(intn_uc_type, '')) as intn_uc_type  --意向二手车类型
, trim(nvl(intn_vehicle_manage_no, '')) as intn_vehicle_manage_no  --意向车辆管理编号
, trim(nvl(intn_uc_cond_json, '')) as intn_uc_cond_json  --意向二手车条件Json
, trim(nvl(purchase_type, '')) as purchase_type  --购买类型
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
from dl.tg_dms_uc_t_uc_demand_info
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_demand_info
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, uc_req_id as uc_req_id --二手车需求ID
, uc_clue_id as uc_clue_id --二手车线索ID
, uc_req_status as uc_req_status --二手车需求状态
, business_diff as business_diff --业务区分
, sold_vehicle_model_name as sold_vehicle_model_name --售出车型名称
, icm_sold_vehicle_info_json as icm_sold_vehicle_info_json --ICM售出车辆信息Json
, d2c_sold_vehicle_info_json as d2c_sold_vehicle_info_json --D2C售出车辆信息Json
, car_buy_intn_type as car_buy_intn_type --购车意向类型
, intn_new_vehicle_info_json as intn_new_vehicle_info_json --意向新车信息Json
, intn_uc_type as intn_uc_type --意向二手车类型
, intn_vehicle_manage_no as intn_vehicle_manage_no --意向车辆管理编号
, intn_uc_cond_json as intn_uc_cond_json --意向二手车条件Json
, purchase_type as purchase_type --购买类型
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
row_number() over (partition by dealer_code,uc_clue_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_demand_info_tmp) a
where rn = 1;


