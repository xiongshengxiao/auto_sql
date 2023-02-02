set mapred.job.name=job_ods_dms_uc_uc_cust_car_sale_follow;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_cust_car_sale_follow_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, negotiation_id as negotiation_id --商谈ID
, follow_time as follow_time --跟进时间
, car_sale_follow_status as car_sale_follow_status --卖车跟进状态
, follow_account as follow_account --跟进账号
, follow_staff_name as follow_staff_name --跟进担当姓名
, car_sale_follow_result as car_sale_follow_result --卖车跟进结果
, continue_reason_type as continue_reason_type --继续原因类型
, abandon_reason_type as abandon_reason_type --放弃原因类型
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, contact_diff as contact_diff --联系区分
, contact_content_code as contact_content_code --联系内容代码
, customer_expect_price as customer_expect_price --客户期望价格
, prompt_price as prompt_price --提示价格
, intn_level as intn_level --意向级别
, plan_follow_time as plan_follow_time --计划跟进时间
, plan_follow_content_diff as plan_follow_content_diff --计划跟进内容区分
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
from ods.ods_dms_uc_uc_cust_car_sale_follow
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(negotiation_id, '')) as negotiation_id  --商谈ID
, trim(nvl(follow_time, '')) as follow_time  --跟进时间
, trim(nvl(car_sale_follow_status, '')) as car_sale_follow_status  --卖车跟进状态
, trim(nvl(follow_account, '')) as follow_account  --跟进账号
, trim(nvl(follow_staff_name, '')) as follow_staff_name  --跟进担当姓名
, trim(nvl(car_sale_follow_result, '')) as car_sale_follow_result  --卖车跟进结果
, trim(nvl(continue_reason_type, '')) as continue_reason_type  --继续原因类型
, trim(nvl(abandon_reason_type, '')) as abandon_reason_type  --放弃原因类型
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(contact_diff, '')) as contact_diff  --联系区分
, trim(nvl(contact_content_code, '')) as contact_content_code  --联系内容代码
, nvl(customer_expect_price, 0) as customer_expect_price  --客户期望价格
, nvl(prompt_price, 0) as prompt_price  --提示价格
, trim(nvl(intn_level, '')) as intn_level  --意向级别
, trim(nvl(plan_follow_time, '')) as plan_follow_time  --计划跟进时间
, trim(nvl(plan_follow_content_diff, '')) as plan_follow_content_diff  --计划跟进内容区分
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
from dl.tg_dms_uc_t_uc_cust_car_sale_follow
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_cust_car_sale_follow
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, negotiation_id as negotiation_id --商谈ID
, follow_time as follow_time --跟进时间
, car_sale_follow_status as car_sale_follow_status --卖车跟进状态
, follow_account as follow_account --跟进账号
, follow_staff_name as follow_staff_name --跟进担当姓名
, car_sale_follow_result as car_sale_follow_result --卖车跟进结果
, continue_reason_type as continue_reason_type --继续原因类型
, abandon_reason_type as abandon_reason_type --放弃原因类型
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, contact_diff as contact_diff --联系区分
, contact_content_code as contact_content_code --联系内容代码
, customer_expect_price as customer_expect_price --客户期望价格
, prompt_price as prompt_price --提示价格
, intn_level as intn_level --意向级别
, plan_follow_time as plan_follow_time --计划跟进时间
, plan_follow_content_diff as plan_follow_content_diff --计划跟进内容区分
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
row_number() over (partition by negotiation_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_cust_car_sale_follow_tmp) a
where rn = 1;


