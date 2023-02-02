set mapred.job.name=job_dwd_sd_customer_appt_board;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with dwd_sd_customer_appt_board_tmp as (
select  id as id --主键
, pending_id as pending_id --待办ID
, follow_id as follow_id --跟进ID
, customer_id as customer_id --客户ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, sales_adviser_code as sales_adviser_code --销售顾问代码
, sales_adviser_name as sales_adviser_name --销售顾问姓名
, sales_adviser_org_id as sales_adviser_org_id --销售顾问组织ID
, prior_intn_vehicle_type_code as prior_intn_vehicle_type_code --优先意向车种代码
, prior_intn_vehicle_type_name as prior_intn_vehicle_type_name --优先意向车种名称
, prior_intn_vehicle_model_code as prior_intn_vehicle_model_code --优先意向车型代码
, prior_intn_vehicle_model_name as prior_intn_vehicle_model_name --优先意向车型名称
, prior_intn_vehicle_grade_code as prior_intn_vehicle_grade_code --优先意向车等级代码
, prior_intn_vehicle_grade_name as prior_intn_vehicle_grade_name --优先意向车等级名称
, prior_intn_vehicle_sfx_code as prior_intn_vehicle_sfx_code --优先意向车SFX代码
, prior_intn_vehicle_color_code as prior_intn_vehicle_color_code --优先意向车颜色代码
, prior_intn_vehicle_color_name as prior_intn_vehicle_color_name --优先意向车颜色名称
, prior_intn_interior_color_code as prior_intn_interior_color_code --优先意向车内饰颜色代码
, prior_intn_interior_color_name as prior_intn_interior_color_name --优先意向车内饰颜色名称
, prior_intn_vehicle_pic_url as prior_intn_vehicle_pic_url --优先意向车图片url
, customer_name as customer_name --客户姓名
, mobile_phone as mobile_phone --移动电话
, mobile_phone_reverse as mobile_phone_reverse --移动电话(反向)
, gender as gender --性别
, appt_flag as appt_flag --是否预约
, arrive_purpose_code as arrive_purpose_code --到店目的代码
, arrive_purpose_name as arrive_purpose_name --到店目的名称
, schedule_arrive_begin_time as schedule_arrive_begin_time --计划到店开始时间
, schedule_arrive_end_time as schedule_arrive_end_time --计划到店结束时间
, actual_arrive_begin_time as actual_arrive_begin_time --实绩到店开始时间
, actual_arrive_end_time as actual_arrive_end_time --实绩到店结束时间
, arrive_duration as arrive_duration --到店时长
, arrive_type as arrive_type --到店类型
, arrive_status as arrive_status --到店状态
, arrive_quantity as arrive_quantity --到店人数
, remark as remark --备注
, del_flag as del_flag --删除标识
, create_by as create_by --创建者
, create_time as create_time --创建时间
, update_by as update_by --更新者
, update_time as update_time --更新时间
, operate_type as operate_type --数据的修改类型:I--Insert,U--表示数据更改,D--表示数据删除,M--业务系统数据归档,A--初始全量加载
, dl_batch_id as dl_batch_id --dl加载批次
, extract_time as extract_time --抽取表时的系统时间（kafka-hbase）
, load_time as load_time --数据加载到DL的时间
, source_system as source_system --来源系统(dms共通,dms销售....)
, insert_time as insert_time --数据写入时间(跑批时间)
, biz_date as biz_date --分区字段
, dwd_del_flag as dwd_del_flag --dwd删除标识(operate_type,delete_flag,del_flag,deleteflag合并字段，0:未删除;1:删除)
from dwd.dwd_sd_customer_appt_board
union all
select  a.id as id  --主键
, a.pending_id as pending_id  --待办ID
, a.follow_id as follow_id  --跟进ID
, a.customer_id as customer_id  --客户ID
, a.dealer_code as dealer_code  --销售店代码
, a.branch_code as branch_code  --店铺代码
, a.sales_adviser_code as sales_adviser_code  --销售顾问代码
, a.sales_adviser_name as sales_adviser_name  --销售顾问姓名
, a.sales_adviser_org_id as sales_adviser_org_id  --销售顾问组织ID
, a.prior_intn_vehicle_type_code as prior_intn_vehicle_type_code  --优先意向车种代码
, a.prior_intn_vehicle_type_name as prior_intn_vehicle_type_name  --优先意向车种名称
, a.prior_intn_vehicle_model_code as prior_intn_vehicle_model_code  --优先意向车型代码
, a.prior_intn_vehicle_model_name as prior_intn_vehicle_model_name  --优先意向车型名称
, a.prior_intn_vehicle_grade_code as prior_intn_vehicle_grade_code  --优先意向车等级代码
, a.prior_intn_vehicle_grade_name as prior_intn_vehicle_grade_name  --优先意向车等级名称
, a.prior_intn_vehicle_sfx_code as prior_intn_vehicle_sfx_code  --优先意向车SFX代码
, a.prior_intn_vehicle_color_code as prior_intn_vehicle_color_code  --优先意向车颜色代码
, a.prior_intn_vehicle_color_name as prior_intn_vehicle_color_name  --优先意向车颜色名称
, a.prior_intn_interior_color_code as prior_intn_interior_color_code  --优先意向车内饰颜色代码
, a.prior_intn_interior_color_name as prior_intn_interior_color_name  --优先意向车内饰颜色名称
, a.prior_intn_vehicle_pic_url as prior_intn_vehicle_pic_url  --优先意向车图片url
, a.customer_name as customer_name  --客户姓名
, a.mobile_phone as mobile_phone  --移动电话
, a.mobile_phone_reverse as mobile_phone_reverse  --移动电话(反向)
, a.gender as gender  --性别
, a.appt_flag as appt_flag  --是否预约
, a.arrive_purpose_code as arrive_purpose_code  --到店目的代码
, a.arrive_purpose_name as arrive_purpose_name  --到店目的名称
, a.schedule_arrive_begin_time as schedule_arrive_begin_time  --计划到店开始时间
, a.schedule_arrive_end_time as schedule_arrive_end_time  --计划到店结束时间
, a.actual_arrive_begin_time as actual_arrive_begin_time  --实绩到店开始时间
, a.actual_arrive_end_time as actual_arrive_end_time  --实绩到店结束时间
, a.arrive_duration as arrive_duration  --到店时长
, a.arrive_type as arrive_type  --到店类型
, a.arrive_status as arrive_status  --到店状态
, a.arrive_quantity as arrive_quantity  --到店人数
, a.remark as remark  --备注
, a.del_flag as del_flag  --删除标识
, a.create_by as create_by  --创建者
, a.create_time as create_time  --创建时间
, a.update_by as update_by  --更新者
, a.update_time as update_time  --更新时间
, if(a.del_flag=1 ,'D', 'U') as operate_type -- 数据操作类型
, a.dl_batch_id as dl_batch_id  --dl加载批次
, a.extract_time as extract_time  --抽取表时的系统时间（HBase-Hive）
, a.load_time as load_time  --数据加载到hive的时间
, 'DMS' as source_system --来源系统
, substr(current_timestamp(), 1, 19) as insert_time --数据写入时间
, a.biz_date as biz_date  --分区字段
, a.del_flag as dwd_del_flag  --删除标识
from ods.ods_dms_sal_vhs_customer_appt_board a
where biz_date = '${YESTERDAY}'
)
insert overwrite table dwd.dwd_sd_customer_appt_board
select  id as id --主键
, pending_id as pending_id --待办ID
, follow_id as follow_id --跟进ID
, customer_id as customer_id --客户ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, sales_adviser_code as sales_adviser_code --销售顾问代码
, sales_adviser_name as sales_adviser_name --销售顾问姓名
, sales_adviser_org_id as sales_adviser_org_id --销售顾问组织ID
, prior_intn_vehicle_type_code as prior_intn_vehicle_type_code --优先意向车种代码
, prior_intn_vehicle_type_name as prior_intn_vehicle_type_name --优先意向车种名称
, prior_intn_vehicle_model_code as prior_intn_vehicle_model_code --优先意向车型代码
, prior_intn_vehicle_model_name as prior_intn_vehicle_model_name --优先意向车型名称
, prior_intn_vehicle_grade_code as prior_intn_vehicle_grade_code --优先意向车等级代码
, prior_intn_vehicle_grade_name as prior_intn_vehicle_grade_name --优先意向车等级名称
, prior_intn_vehicle_sfx_code as prior_intn_vehicle_sfx_code --优先意向车SFX代码
, prior_intn_vehicle_color_code as prior_intn_vehicle_color_code --优先意向车颜色代码
, prior_intn_vehicle_color_name as prior_intn_vehicle_color_name --优先意向车颜色名称
, prior_intn_interior_color_code as prior_intn_interior_color_code --优先意向车内饰颜色代码
, prior_intn_interior_color_name as prior_intn_interior_color_name --优先意向车内饰颜色名称
, prior_intn_vehicle_pic_url as prior_intn_vehicle_pic_url --优先意向车图片url
, customer_name as customer_name --客户姓名
, mobile_phone as mobile_phone --移动电话
, mobile_phone_reverse as mobile_phone_reverse --移动电话(反向)
, gender as gender --性别
, appt_flag as appt_flag --是否预约
, arrive_purpose_code as arrive_purpose_code --到店目的代码
, arrive_purpose_name as arrive_purpose_name --到店目的名称
, schedule_arrive_begin_time as schedule_arrive_begin_time --计划到店开始时间
, schedule_arrive_end_time as schedule_arrive_end_time --计划到店结束时间
, actual_arrive_begin_time as actual_arrive_begin_time --实绩到店开始时间
, actual_arrive_end_time as actual_arrive_end_time --实绩到店结束时间
, arrive_duration as arrive_duration --到店时长
, arrive_type as arrive_type --到店类型
, arrive_status as arrive_status --到店状态
, arrive_quantity as arrive_quantity --到店人数
, remark as remark --备注
, del_flag as del_flag --删除标识
, create_by as create_by --创建者
, create_time as create_time --创建时间
, update_by as update_by --更新者
, update_time as update_time --更新时间
, operate_type as operate_type --数据的修改类型:I--Insert,U--表示数据更改,D--表示数据删除,M--业务系统数据归档,A--初始全量加载
, dl_batch_id as dl_batch_id --dl加载批次
, extract_time as extract_time --抽取表时的系统时间（kafka-hbase）
, load_time as load_time --数据加载到DL的时间
, source_system as source_system --来源系统(dms共通,dms销售....)
, insert_time as insert_time --数据写入时间(跑批时间)
, biz_date as biz_date --分区字段
, dwd_del_flag as dwd_del_flag --dwd删除标识(operate_type,delete_flag,del_flag,deleteflag合并字段，0:未删除;1:删除)
from (
select *,
row_number() over (partition by id
order by update_time desc, load_time desc, create_time desc,biz_date desc) rn
from dwd_sd_customer_appt_board_tmp) a
where rn = 1;



set mapred.job.name=job_dwd_sd_clue_customer;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with dwd_sd_clue_customer_tmp as (
select  id as id --主键
, enquiry_customer_code as enquiry_customer_code --咨询客户代码
, dealer_code as dealer_code --销售店代码
, social_code as social_code --社会代码
, customer_type as customer_type --客户类型
, customer_name as customer_name --客户姓名
, address as address --地址
, postal_code as postal_code --邮编
, landline_number as landline_number --座机号码
, mobile_phone as mobile_phone --移动电话
, mobile_phone_reverse as mobile_phone_reverse --移动电话(反向)
, business_landline_number as business_landline_number --业务座机号码
, fax_number as fax_number --传真号码
, email1 as email1 --邮箱1
, email2 as email2 --邮箱2
, gender as gender --性别
, birthday as birthday --生日
, sms_flag as sms_flag --短信标识
, mail_flag as mail_flag --邮箱标识
, contact_dm_flag as contact_dm_flag --是否使用DM联系
, contact_home_flag as contact_home_flag --是否使用家庭电话联系
, contact_mobile_phone_flag as contact_mobile_phone_flag --是否使用手机联系
, contact_sms_flag as contact_sms_flag --是否使用短信联系
, contact_email_flag as contact_email_flag --是否使用邮箱联系
, wechat as wechat --微信
, qq as qq --QQ
, blog as blog --博客
, del_time as del_time --删除时间
, remark as remark --备注
, del_flag as del_flag --删除标识
, create_by as create_by --创建者
, create_time as create_time --创建时间
, update_by as update_by --更新者
, update_time as update_time --更新时间
, region as region --地区
, row_create_function as row_create_function --记录创建程序
, row_update_function as row_update_function --记录更新程序
, row_lock_version as row_lock_version --记录锁定版本
, strcdstaff as strcdstaff --店里的工作人员代码
, sacode as sacode --SA码
, employeename as employeename --担当者
, employeedepartment as employeedepartment --担当者部门
, employeeposition as employeeposition --担当者职位
, proopecd as proopecd --处理对象
, registflg as registflg --注册标志
, state_cd as state_cd --省代码
, district_cd as district_cd --区域代码
, city_cd as city_cd --城市代码
, location_cd as location_cd --区域代码
, nametitle_cd as nametitle_cd --尊敬的代码
, nametitle as nametitle --尊称
, operate_type as operate_type --数据的修改类型:I--Insert,U--表示数据更改,D--表示数据删除,M--业务系统数据归档,A--初始全量加载
, dl_batch_id as dl_batch_id --dl加载批次
, extract_time as extract_time --抽取表时的系统时间（kafka-hbase）
, load_time as load_time --数据加载到DL的时间
, source_system as source_system --来源系统(dms共通,dms销售....)
, insert_time as insert_time --数据写入时间(跑批时间)
, biz_date as biz_date --分区字段
, dwd_del_flag as dwd_del_flag --dwd删除标识(operate_type,delete_flag,del_flag,deleteflag合并字段，0:未删除;1:删除)
from dwd.dwd_sd_clue_customer
union all
select  a.id as id  --主键
, a.enquiry_customer_code as enquiry_customer_code  --咨询客户代码
, a.dealer_code as dealer_code  --销售店代码
, a.social_code as social_code  --社会代码
, a.customer_type as customer_type  --客户类型
, a.customer_name as customer_name  --客户姓名
, a.address as address  --地址
, a.postal_code as postal_code  --邮编
, a.landline_number as landline_number  --座机号码
, a.mobile_phone as mobile_phone  --移动电话
, a.mobile_phone_reverse as mobile_phone_reverse  --移动电话(反向)
, a.business_landline_number as business_landline_number  --业务座机号码
, a.fax_number as fax_number  --传真号码
, a.email1 as email1  --邮箱1
, a.email2 as email2  --邮箱2
, a.gender as gender  --性别
, a.birthday as birthday  --生日
, a.sms_flag as sms_flag  --短信标识
, a.mail_flag as mail_flag  --邮箱标识
, a.contact_dm_flag as contact_dm_flag  --是否使用DM联系
, a.contact_home_flag as contact_home_flag  --是否使用家庭电话联系
, a.contact_mobile_phone_flag as contact_mobile_phone_flag  --是否使用手机联系
, a.contact_sms_flag as contact_sms_flag  --是否使用短信联系
, a.contact_email_flag as contact_email_flag  --是否使用邮箱联系
, a.wechat as wechat  --微信
, a.qq as qq  --QQ
, a.blog as blog  --博客
, a.del_time as del_time  --删除时间
, a.remark as remark  --备注
, a.del_flag as del_flag  --删除标识
, a.create_by as create_by  --创建者
, a.create_time as create_time  --创建时间
, a.update_by as update_by  --更新者
, a.update_time as update_time  --更新时间
, '' as region  --地区
, '' as row_create_function  --记录创建程序
, '' as row_update_function  --记录更新程序
, '' as row_lock_version  --记录锁定版本
, '' as strcdstaff  --店里的工作人员代码
, '' as sacode  --SA码
, '' as employeename  --担当者
, '' as employeedepartment  --担当者部门
, '' as employeeposition  --担当者职位
, '' as proopecd  --处理对象
, '' as registflg  --注册标志
, '' as state_cd  --省代码
, '' as district_cd  --区域代码
, '' as city_cd  --城市代码
, '' as location_cd  --区域代码
, '' as nametitle_cd  --尊敬的代码
, '' as nametitle  --尊称
, if(a.del_flag=1 ,'D', 'U') as operate_type -- 数据操作类型
, a.dl_batch_id as dl_batch_id  --dl加载批次
, a.extract_time as extract_time  --抽取表时的系统时间（HBase-Hive）
, a.load_time as load_time  --数据加载到hive的时间
, 'DMS' as source_system --来源系统
, substr(current_timestamp(), 1, 19) as insert_time --数据写入时间
, a.biz_date as biz_date  --分区字段
, a.del_flag as dwd_del_flag  --删除标识
from ods.ods_dms_sal_vhs_clue_customer a
where biz_date = '${YESTERDAY}'
)
insert overwrite table dwd.dwd_sd_clue_customer
select  id as id --主键
, enquiry_customer_code as enquiry_customer_code --咨询客户代码
, dealer_code as dealer_code --销售店代码
, social_code as social_code --社会代码
, customer_type as customer_type --客户类型
, customer_name as customer_name --客户姓名
, address as address --地址
, postal_code as postal_code --邮编
, landline_number as landline_number --座机号码
, mobile_phone as mobile_phone --移动电话
, mobile_phone_reverse as mobile_phone_reverse --移动电话(反向)
, business_landline_number as business_landline_number --业务座机号码
, fax_number as fax_number --传真号码
, email1 as email1 --邮箱1
, email2 as email2 --邮箱2
, gender as gender --性别
, birthday as birthday --生日
, sms_flag as sms_flag --短信标识
, mail_flag as mail_flag --邮箱标识
, contact_dm_flag as contact_dm_flag --是否使用DM联系
, contact_home_flag as contact_home_flag --是否使用家庭电话联系
, contact_mobile_phone_flag as contact_mobile_phone_flag --是否使用手机联系
, contact_sms_flag as contact_sms_flag --是否使用短信联系
, contact_email_flag as contact_email_flag --是否使用邮箱联系
, wechat as wechat --微信
, qq as qq --QQ
, blog as blog --博客
, del_time as del_time --删除时间
, remark as remark --备注
, del_flag as del_flag --删除标识
, create_by as create_by --创建者
, create_time as create_time --创建时间
, update_by as update_by --更新者
, update_time as update_time --更新时间
, region as region --地区
, row_create_function as row_create_function --记录创建程序
, row_update_function as row_update_function --记录更新程序
, row_lock_version as row_lock_version --记录锁定版本
, strcdstaff as strcdstaff --店里的工作人员代码
, sacode as sacode --SA码
, employeename as employeename --担当者
, employeedepartment as employeedepartment --担当者部门
, employeeposition as employeeposition --担当者职位
, proopecd as proopecd --处理对象
, registflg as registflg --注册标志
, state_cd as state_cd --省代码
, district_cd as district_cd --区域代码
, city_cd as city_cd --城市代码
, location_cd as location_cd --区域代码
, nametitle_cd as nametitle_cd --尊敬的代码
, nametitle as nametitle --尊称
, operate_type as operate_type --数据的修改类型:I--Insert,U--表示数据更改,D--表示数据删除,M--业务系统数据归档,A--初始全量加载
, dl_batch_id as dl_batch_id --dl加载批次
, extract_time as extract_time --抽取表时的系统时间（kafka-hbase）
, load_time as load_time --数据加载到DL的时间
, source_system as source_system --来源系统(dms共通,dms销售....)
, insert_time as insert_time --数据写入时间(跑批时间)
, biz_date as biz_date --分区字段
, dwd_del_flag as dwd_del_flag --dwd删除标识(operate_type,delete_flag,del_flag,deleteflag合并字段，0:未删除;1:删除)
from (
select *,
row_number() over (partition by id
order by update_time desc, load_time desc, create_time desc,biz_date desc) rn
from dwd_sd_clue_customer_tmp) a
where rn = 1;


