set mapred.job.name=job_ods_dms_cs_salesed_vehicle_ledger_init;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
insert overwrite table ods.ods_dms_cs_salesed_vehicle_ledger
select  id --主键
, customer_order_number --客户订单号
, car_owner_cert_number --车主证件号码
, car_owner_type --车主类型
, car_owner_name --车主姓名
, sales_dealer_code --销售销售店代码
, sales_company_name --销售公司名称
, sales_company_taxpayer_number --销售公司统一信用代码
, delivery_dealer_code --交车销售店代码
, sales_adviser_code --销售顾问代码
, delivery_consultant_code --交车顾问代码
, after_sales_adviser_code --售后顾问代码
, vinno --车架号
, vehicle_flag --车辆区分
, vehicle_type --车辆类别
, vinno_d7 --车架号后7位
, urn --车辆生产识别码
, vehicle_name --车辆名称
, vehicle_name_code --车辆名称代码
, vehicle_model_name --车型名称
, vehicle_model --车型
, stand_vehicle_model --车型代码[标准]
, vehicle_model_subdivision --车型代码[细分]
, vehicle_sfx_code --车辆SFX代码
, grade_code --等级代码
, grade_name --等级名称（销售）
, invoice_grade --发票用车辆等级
, invoice_vehicle_name --发票用车型名称
, vehicle_color_code --车辆颜色代码
, vehicle_color_name --车辆颜色名称
, interior_color_code --内饰颜色代码
, interior_color_name --内饰颜色名称
, engine --发动机
, engine_number --发动机号(合格证)
, engine_type --发动机型号(合格证)
, exhaust --排量
, gearbox --变速箱
, delivery_odometer_value --交车时公里数
, vehicle_odometer_value --行驶公里数
, vehicle_use --车辆用途
, sales_diff --购买类型/销售区分
, factory_code --工厂代码
, id_line --生产线号
, production_date --制造日期
, sales_price --销售价格
, new_vehicle_retail_price --新车零售价格
, fuel_type --燃料种类
, fuel_type_name --燃料种类名称
, final_inspt_date --终检日
, outday_date --出门日
, match_date --匹配日
, sales_date --销售日
, pds_date --PDS日
, delivery_date --交车日
, warranty_status --三包凭证更新状态
, regist_number --车牌号
, vehicle_certificate_number --整车合格证编号
, invoice_number --发票编号（销售）
, del_flag --删除标识
, remark --备注
, create_time --创建时间
, create_by --创建者
, update_time --更新时间
, update_by --更新者
, vinno_after_6_digit --车架号（后六位）
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, 'DMS共通' as source_system --来源系统
, substr(current_timestamp(), 1, 19) as insert_time --数据写入时间
, biz_date as biz_date  --分区字段
from (
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(customer_order_number, '')) as customer_order_number  --客户订单号
, trim(nvl(car_owner_cert_number, '')) as car_owner_cert_number  --车主证件号码
, trim(nvl(car_owner_type, '')) as car_owner_type  --车主类型
, trim(nvl(car_owner_name, '')) as car_owner_name  --车主姓名
, trim(nvl(sales_dealer_code, '')) as sales_dealer_code  --销售销售店代码
, trim(nvl(sales_company_name, '')) as sales_company_name  --销售公司名称
, trim(nvl(sales_company_taxpayer_number, '')) as sales_company_taxpayer_number  --销售公司统一信用代码
, trim(nvl(delivery_dealer_code, '')) as delivery_dealer_code  --交车销售店代码
, trim(nvl(sales_adviser_code, '')) as sales_adviser_code  --销售顾问代码
, trim(nvl(delivery_consultant_code, '')) as delivery_consultant_code  --交车顾问代码
, trim(nvl(after_sales_adviser_code, '')) as after_sales_adviser_code  --售后顾问代码
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(vehicle_flag, '')) as vehicle_flag  --车辆区分
, trim(nvl(vehicle_type, '')) as vehicle_type  --车辆类别
, trim(nvl(vinno_d7, '')) as vinno_d7  --车架号后7位
, trim(nvl(urn, '')) as urn  --车辆生产识别码
, trim(nvl(vehicle_name, '')) as vehicle_name  --车辆名称
, trim(nvl(vehicle_name_code, '')) as vehicle_name_code  --车辆名称代码
, trim(nvl(vehicle_model_name, '')) as vehicle_model_name  --车型名称
, trim(nvl(vehicle_model, '')) as vehicle_model  --车型
, trim(nvl(stand_vehicle_model, '')) as stand_vehicle_model  --车型代码[标准]
, trim(nvl(vehicle_model_subdivision, '')) as vehicle_model_subdivision  --车型代码[细分]
, trim(nvl(vehicle_sfx_code, '')) as vehicle_sfx_code  --车辆SFX代码
, trim(nvl(grade_code, '')) as grade_code  --等级代码
, trim(nvl(grade_name, '')) as grade_name  --等级名称（销售）
, trim(nvl(invoice_grade, '')) as invoice_grade  --发票用车辆等级
, trim(nvl(invoice_vehicle_name, '')) as invoice_vehicle_name  --发票用车型名称
, trim(nvl(vehicle_color_code, '')) as vehicle_color_code  --车辆颜色代码
, trim(nvl(vehicle_color_name, '')) as vehicle_color_name  --车辆颜色名称
, trim(nvl(interior_color_code, '')) as interior_color_code  --内饰颜色代码
, trim(nvl(interior_color_name, '')) as interior_color_name  --内饰颜色名称
, trim(nvl(engine, '')) as engine  --发动机
, trim(nvl(engine_number, '')) as engine_number  --发动机号(合格证)
, trim(nvl(engine_type, '')) as engine_type  --发动机型号(合格证)
, trim(nvl(exhaust, '')) as exhaust  --排量
, trim(nvl(gearbox, '')) as gearbox  --变速箱
, nvl(delivery_odometer_value, 0) as delivery_odometer_value  --交车时公里数
, nvl(vehicle_odometer_value, 0) as vehicle_odometer_value  --行驶公里数
, trim(nvl(vehicle_use, '')) as vehicle_use  --车辆用途
, trim(nvl(sales_diff, '')) as sales_diff  --购买类型/销售区分
, trim(nvl(factory_code, '')) as factory_code  --工厂代码
, trim(nvl(id_line, '')) as id_line  --生产线号
, trim(nvl(production_date, '')) as production_date  --制造日期
, nvl(sales_price, 0) as sales_price  --销售价格
, nvl(new_vehicle_retail_price, 0) as new_vehicle_retail_price  --新车零售价格
, trim(nvl(fuel_type, '')) as fuel_type  --燃料种类
, trim(nvl(fuel_type_name, '')) as fuel_type_name  --燃料种类名称
, trim(nvl(final_inspt_date, '')) as final_inspt_date  --终检日
, trim(nvl(outday_date, '')) as outday_date  --出门日
, trim(nvl(match_date, '')) as match_date  --匹配日
, trim(nvl(sales_date, '')) as sales_date  --销售日
, trim(nvl(pds_date, '')) as pds_date  --PDS日
, trim(nvl(delivery_date, '')) as delivery_date  --交车日
, trim(nvl(warranty_status, '')) as warranty_status  --三包凭证更新状态
, trim(nvl(regist_number, '')) as regist_number  --车牌号
, trim(nvl(vehicle_certificate_number, '')) as vehicle_certificate_number  --整车合格证编号
, trim(nvl(invoice_number, '')) as invoice_number  --发票编号（销售）
, nvl(del_flag, 0) as del_flag  --删除标识
, trim(nvl(remark, '')) as remark  --备注
, trim(nvl(create_time, '')) as create_time  --创建时间
, nvl(create_by, 0) as create_by  --创建者
, trim(nvl(update_time, '')) as update_time  --更新时间
, nvl(update_by, 0) as update_by  --更新者
, trim(nvl(vinno_after_6_digit, '')) as vinno_after_6_digit  --车架号（后六位）
, etl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
, row_number() over(partition by customer_order_number
order by update_time desc, load_time desc, create_time desc) rn
from dl.tg_dms_cs_t_salesed_vehicle_ledger
where biz_date < '${TODAY}' )aa
where rn = 1;


