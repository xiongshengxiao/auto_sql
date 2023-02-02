set mapred.job.name=job_tg_ums_salesinfo_init;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
insert 
into 
table dl.tg_ums_salesinfo
select  nvl(a.dealer_code,'') as dlrcd  --销售店代码
, nvl(a.branch_code,'') as strcd  --店铺代码
, nvl(a.negotiation_id,'') as salesid  --商谈ID
, cast(a.dl_batch_id as int) as batch_id  --dl加载批次
, nvl(a.customer_id,'') as originalid  --客户ID
, nvl(a.source_code,0) as source_0_cd  --来源代码
, nvl(a.source_first_level_code,0) as source_1_cd  --一级来源代码
, nvl(a.source_second_level_code,0) as source_2_cd  --二级来源代码
, nvl(a.activity_manager_account,'') as staffcd  --活动管理者账号
, nvl(a.introducer_account,'') as introduce_staffcd  --介绍人账号
, nvl(a.introducer_dept_code,'') as introduce_type  --介绍人部门代码
, '' as hope_vehiclecd  --希望车型代码
, nvl(a.business_diff,'') as business_div  --业务区分
, nvl(a.intn_level,'') as expectation  --意向级别
, '' as status  --状态
, nvl(c.car_sale_follow_result/car_buy_follow_result,'') as active_result_1  --卖车跟进结果/买车跟进结果
, nvl(c.continue_reason_type/abandon_reason_type,'') as active_result_2  --继续原因类型/放弃原因类型
, nvl(a.follow_time,'') as active_result_date  --跟进时间
, '' as active_result_staffcd  --实际活动销售人员代码
, nvl(a.next_follow_time,'') as next_active_date  --下次跟进时间
, '' as next_active_content_div  --下次活动内容区分
, '' as giveup_reason  --放弃原因
, nvl(a.vehicle_model_name,'') as sell_vehiclename  --车型名称
, nvl(a.vehicle_manage_no,'') as sell_vehicleno  --车辆管理编号
, nvl(a.intn_type,'') as except_confidtion_div  --意向类型
, 0 as except_car_sum  --例外车辆数
, nvl(b.purchase_type,'') as purchase_div  --购买类型
, nvl(a.vehicle_manage_no,'') as except_vehicleno  --车辆管理编号
, '' as except_vehicle_dlrcd  --例外车辆经销店
, '' as makerpart  --制造商区分
, '' as makercd  --制造商代码
, '' as car_type  --车辆类型
, '' as vehiclecd  --车型代码
, '' as grade  --车辆配置等级
, 0 as expect_price_from  --期望起始价格
, 0 as expect_price_to  --期望最高价
, '' as modelyear_from  --车型年起始
, '' as modelyear_to  --车型年结束
, '' as mileage_from  --最小里程
, '' as mileage_to  --最大里程
, '' as color_exterior  --车身颜色代码
, '' as other_content  --其他内容
, nvl(a.uc_clue_id,'') as clue_id  --二手车线索ID
, nvl(a.clue_create_time,'') as clue_accept_date  --线索创建时间
, '' as icrop_req_id  --icrop用件序号
, 0 as icrop_vcl_seq  --icrop车辆序号
, nvl(a.del_flag,'') as delflg  --删除标识
, nvl(a.create_time,'') as createdate  --创建时间
, greatest(nvl(a.update_time, '00000000'),nvl(c.update_time, '00000000'),nvl(b.update_time, '00000000')) as updatedate  --更新时间
, nvl(a.follow_account,'') as updateaccount  --跟进账号
, if(a.del_flag=1 ,'D', 'U') as operate_type -- 数据操作类型
, substr(current_timestamp(), 1, 19) as load_time  --数据加载到表的时间
from ods.ods_dms_uc_uc_negotiation a 
 left join ods.ods_dms_uc_uc_demand_info b
 on a.uc_req_id=b.uc_req_id and a.dealer_code=b.dealer_code 
 left join ods.ods_dms_uc_uc_cust_car_sale_follow c
 on a.negotiation_id=c.negotiation_id and a.dealer_code=c.dealer_code 
 left join t_uc_cust_car_buy_follow d
 on a.negotiation_id=d.negotiation_id and a.dealer_code=d.dealer_code
--where biz_date < '${TODAY}'
;


