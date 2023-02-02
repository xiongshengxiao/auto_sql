set mapred.job.name=job_tg_ums_customerinfo;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
insert 
into 
table dl.tg_ums_customerinfo
select  a.dealer_code as dlrcd  --销售店代码
, a.branch_code as strcd  --店铺代码
, a.customer_id as originalid  --客户ID
, cast(a.dl_batch_id as int) as batch_id  --dl加载批次
, a.customer_name as customer_name  --客户姓名
, '' as custype  --客户类型
, a.negotiation_customer_address as customer_address  --商谈客户地址
, a.customer_fixed_tel as customer_telno  --电话号码
, a.customer_mobile_phone as customer_mobile  --手机号
, a.customer_postal_code as customer_zipcode  --客户邮编
, '' as customer_email1  --客户email1
, '' as customer_email2  --客户email2
, '' as webflg  --D-Mail配信flag
, '' as telflg  --固话配信flag
, '' as mobileflg  --客户手机配信flag
, '' as smsflg  --短信配信flag
, '' as emailflg  --E-MAIL配信flag
, a.comm_activity_type as commercial_recv_type  --商业性活动类型
, a.del_flag as delflg  --删除标识
, a.create_time as createdate  --创建时间
, a.update_time as updatedate  --更新时间
, '' as updateaccount  --修改次数
, if(a.del_flag=1 ,'D', 'U') as operate_type -- 数据操作类型
, substr(current_timestamp(), 1, 19) as load_time  --数据加载到表的时间
from ods.ods_dms_uc_uc_customer_info a
where biz_date = '${YESTERDAY}'
;

