set mapred.job.name=job_ods_dms_uc_uc_negotiation_init;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
insert overwrite table ods.ods_dms_uc_uc_negotiation
select  id --主键
, dealer_code --销售店代码
, branch_code --店铺代码
, negotiation_id --商谈ID
, customer_id --客户ID
, customer_name --客户姓名
, negotiation_customer_address --商谈客户地址
, customer_mobile_phone --客户移动电话
, customer_fixed_tel --客户固定电话
, customer_postal_code --客户邮编
, activity_manager_account --活动管理者账号
, activity_manager_name --活动管理者姓名
, source_code --来源代码
, source_name --来源名称
, source_first_level_code --一级来源代码
, source_first_level_name --一级来源名称
, source_second_level_code --二级来源代码
, source_second_level_name --二级来源名称
, introducer_dept_code --介绍人部门代码
, introducer_dept_name --介绍人部门名称
, introducer_account --介绍人账号
, introducer_name --介绍人姓名
, intn_level --意向级别
, negotiation_status --商谈状态
, manager_dist_flag --管理者分配标识
, uc_req_id --二手车需求ID
, uc_clue_id --二手车线索ID
, clue_create_time --线索创建时间
, follow_account --跟进账号
, follow_staff_name --跟进担当姓名
, follow_time --跟进时间
, next_follow_time --下次跟进时间
, business_diff --业务区分
, intn_type --意向类型
, vehicle_manage_no --车辆管理编号
, cover_thumbnail_img_url --封面缩略图片URL
, brand_code --品牌CD
, brand_name --品牌名称
, maker_code --厂商代码
, maker_name --厂商名称
, vehicle_model_code --车型代码
, vehicle_model_name --车型名称
, vehicle_grade --车辆等级
, license_plate_number --车牌号
, vinno --车架号
, first_regist_date --首次注册日期
, ass_date --评估日期
, purchase_sign_date --采购签约日期
, cons_sign_date --寄售签约日期
, min_down_payment --最低首付
, max_down_payment --最高首付
, min_monthly_payment --最低月供
, max_monthly_payment --最高月供
, sales_sign_date --销售签约日期
, sales_negotiation_id --销售商谈ID
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
, trim(nvl(negotiation_id, '')) as negotiation_id  --商谈ID
, trim(nvl(customer_id, '')) as customer_id  --客户ID
, trim(nvl(customer_name, '')) as customer_name  --客户姓名
, trim(nvl(negotiation_customer_address, '')) as negotiation_customer_address  --商谈客户地址
, trim(nvl(customer_mobile_phone, '')) as customer_mobile_phone  --客户移动电话
, trim(nvl(customer_fixed_tel, '')) as customer_fixed_tel  --客户固定电话
, trim(nvl(customer_postal_code, '')) as customer_postal_code  --客户邮编
, trim(nvl(activity_manager_account, '')) as activity_manager_account  --活动管理者账号
, trim(nvl(activity_manager_name, '')) as activity_manager_name  --活动管理者姓名
, trim(nvl(source_code, '')) as source_code  --来源代码
, trim(nvl(source_name, '')) as source_name  --来源名称
, trim(nvl(source_first_level_code, '')) as source_first_level_code  --一级来源代码
, trim(nvl(source_first_level_name, '')) as source_first_level_name  --一级来源名称
, trim(nvl(source_second_level_code, '')) as source_second_level_code  --二级来源代码
, trim(nvl(source_second_level_name, '')) as source_second_level_name  --二级来源名称
, trim(nvl(introducer_dept_code, '')) as introducer_dept_code  --介绍人部门代码
, trim(nvl(introducer_dept_name, '')) as introducer_dept_name  --介绍人部门名称
, trim(nvl(introducer_account, '')) as introducer_account  --介绍人账号
, trim(nvl(introducer_name, '')) as introducer_name  --介绍人姓名
, trim(nvl(intn_level, '')) as intn_level  --意向级别
, trim(nvl(negotiation_status, '')) as negotiation_status  --商谈状态
, trim(nvl(manager_dist_flag, '')) as manager_dist_flag  --管理者分配标识
, trim(nvl(uc_req_id, '')) as uc_req_id  --二手车需求ID
, trim(nvl(uc_clue_id, '')) as uc_clue_id  --二手车线索ID
, trim(nvl(clue_create_time, '')) as clue_create_time  --线索创建时间
, trim(nvl(follow_account, '')) as follow_account  --跟进账号
, trim(nvl(follow_staff_name, '')) as follow_staff_name  --跟进担当姓名
, trim(nvl(follow_time, '')) as follow_time  --跟进时间
, trim(nvl(next_follow_time, '')) as next_follow_time  --下次跟进时间
, trim(nvl(business_diff, '')) as business_diff  --业务区分
, trim(nvl(intn_type, '')) as intn_type  --意向类型
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(cover_thumbnail_img_url, '')) as cover_thumbnail_img_url  --封面缩略图片URL
, trim(nvl(brand_code, '')) as brand_code  --品牌CD
, trim(nvl(brand_name, '')) as brand_name  --品牌名称
, trim(nvl(maker_code, '')) as maker_code  --厂商代码
, trim(nvl(maker_name, '')) as maker_name  --厂商名称
, trim(nvl(vehicle_model_code, '')) as vehicle_model_code  --车型代码
, trim(nvl(vehicle_model_name, '')) as vehicle_model_name  --车型名称
, trim(nvl(vehicle_grade, '')) as vehicle_grade  --车辆等级
, trim(nvl(license_plate_number, '')) as license_plate_number  --车牌号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(first_regist_date, '')) as first_regist_date  --首次注册日期
, trim(nvl(ass_date, '')) as ass_date  --评估日期
, trim(nvl(purchase_sign_date, '')) as purchase_sign_date  --采购签约日期
, trim(nvl(cons_sign_date, '')) as cons_sign_date  --寄售签约日期
, nvl(min_down_payment, 0) as min_down_payment  --最低首付
, nvl(max_down_payment, 0) as max_down_payment  --最高首付
, nvl(min_monthly_payment, 0) as min_monthly_payment  --最低月供
, nvl(max_monthly_payment, 0) as max_monthly_payment  --最高月供
, trim(nvl(sales_sign_date, '')) as sales_sign_date  --销售签约日期
, trim(nvl(sales_negotiation_id, '')) as sales_negotiation_id  --销售商谈ID
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
, row_number() over(partition by dealer_code,negotiation_id
order by update_time desc, load_time desc, create_time desc) rn
from dl.tg_dms_uc_t_uc_negotiation
where biz_date < '${TODAY}' )aa
where rn = 1;


