set mapred.job.name=job_ods_dms_uc_uc_sales_ledger_init;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
insert overwrite table ods.ods_dms_uc_uc_sales_ledger
select  id --主键
, sales_contract_serial --销售合同编号
, dealer_code --销售店代码
, branch_code --店铺代码
, vehicle_manage_no --车辆管理编号
, sales_status --销售状态
, sales_method --销售方式
, sales_sign_date --销售签约日期
, sales_sign_staff_account --销售签约担当账号
, temp_save_date --临时保存日期
, temp_save_staff_account --临时保存担当账号
, sales_total_income --销售总收入
, sales_total_payment --销售总支付
, payment_plan_date --付款计划日期
, payment_finish_date --付款完成日期
, invoice_number --发票号
, invoice_number_login_date --发票号登录日期
, authen_flag --认证标识
, uc_auth_no --二手车认证编号
, uc_auth_due_date --二手车认证到期日
, displace_flag --置换标识
, delivery_plan_date --交车计划日期
, delivery_finish_date --交车完成日期
, delivery_cancel_date --交车取消日期
, dealer_payment_transfer_cost --销售店支付过户费用
, delivery_license_plate_number --交车车牌号
, sales_cancel_date --销售取消日
, sales_cancel_staff_account --销售取消担当账号
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
, trim(nvl(sales_contract_serial, '')) as sales_contract_serial  --销售合同编号
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(sales_status, '')) as sales_status  --销售状态
, trim(nvl(sales_method, '')) as sales_method  --销售方式
, trim(nvl(sales_sign_date, '')) as sales_sign_date  --销售签约日期
, trim(nvl(sales_sign_staff_account, '')) as sales_sign_staff_account  --销售签约担当账号
, trim(nvl(temp_save_date, '')) as temp_save_date  --临时保存日期
, trim(nvl(temp_save_staff_account, '')) as temp_save_staff_account  --临时保存担当账号
, nvl(sales_total_income, 0) as sales_total_income  --销售总收入
, nvl(sales_total_payment, 0) as sales_total_payment  --销售总支付
, trim(nvl(payment_plan_date, '')) as payment_plan_date  --付款计划日期
, trim(nvl(payment_finish_date, '')) as payment_finish_date  --付款完成日期
, trim(nvl(invoice_number, '')) as invoice_number  --发票号
, trim(nvl(invoice_number_login_date, '')) as invoice_number_login_date  --发票号登录日期
, trim(nvl(authen_flag, '')) as authen_flag  --认证标识
, trim(nvl(uc_auth_no, '')) as uc_auth_no  --二手车认证编号
, trim(nvl(uc_auth_due_date, '')) as uc_auth_due_date  --二手车认证到期日
, trim(nvl(displace_flag, '')) as displace_flag  --置换标识
, trim(nvl(delivery_plan_date, '')) as delivery_plan_date  --交车计划日期
, trim(nvl(delivery_finish_date, '')) as delivery_finish_date  --交车完成日期
, trim(nvl(delivery_cancel_date, '')) as delivery_cancel_date  --交车取消日期
, nvl(dealer_payment_transfer_cost, 0) as dealer_payment_transfer_cost  --销售店支付过户费用
, trim(nvl(delivery_license_plate_number, '')) as delivery_license_plate_number  --交车车牌号
, trim(nvl(sales_cancel_date, '')) as sales_cancel_date  --销售取消日
, trim(nvl(sales_cancel_staff_account, '')) as sales_cancel_staff_account  --销售取消担当账号
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
, row_number() over(partition by sales_contract_serial
order by update_time desc, load_time desc, create_time desc) rn
from dl.tg_dms_uc_t_uc_sales_ledger
where biz_date < '${TODAY}' )aa
where rn = 1;


