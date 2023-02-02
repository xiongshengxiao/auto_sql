set mapred.job.name=job_ods_dms_uc_uc_assessment_info;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_assessment_info_tmp as (
select  id as id --主键
, ass_serial as ass_serial --评估编号
, dealer_code as dealer_code --销售店代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, negotiation_id as negotiation_id --商谈ID
, sq as sq --序列号
, ass_status as ass_status --评估状态
, branch_code as branch_code --店铺代码
, vinno as vinno --车架号
, ass_date as ass_date --评估日期
, appraiser_memo as appraiser_memo --评估师寄语
, appraiser_account as appraiser_account --评估师账号
, appraiser_name as appraiser_name --评估师名称
, check_manage_no as check_manage_no --检查管理No
, vehicle_config_info_json as vehicle_config_info_json --车辆配置信息Json
, vehicle_check_remark_json as vehicle_check_remark_json --车辆检查备注Json
, vhcl_check_remark_label_json as vhcl_check_remark_label_json --车辆检查备注标签Json
, vehicle_addi_check_json as vehicle_addi_check_json --车辆额外检查Json
, vehicle_img_json as vehicle_img_json --车辆图片Json
, vehicle_defect_info_json as vehicle_defect_info_json --车辆缺陷信息Json
, imp_service_info_json as imp_service_info_json --重要维修保养信息Json
, update_account as update_account --更新账号
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
from ods.ods_dms_uc_uc_assessment_info
union all
select  trim(nvl(id, '')) as id  --主键
, nvl(ass_serial, 0) as ass_serial  --评估编号
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(negotiation_id, '')) as negotiation_id  --商谈ID
, nvl(sq, 0) as sq  --序列号
, trim(nvl(ass_status, '')) as ass_status  --评估状态
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(ass_date, '')) as ass_date  --评估日期
, trim(nvl(appraiser_memo, '')) as appraiser_memo  --评估师寄语
, trim(nvl(appraiser_account, '')) as appraiser_account  --评估师账号
, trim(nvl(appraiser_name, '')) as appraiser_name  --评估师名称
, trim(nvl(check_manage_no, '')) as check_manage_no  --检查管理No
, trim(nvl(vehicle_config_info_json, '')) as vehicle_config_info_json  --车辆配置信息Json
, trim(nvl(vehicle_check_remark_json, '')) as vehicle_check_remark_json  --车辆检查备注Json
, trim(nvl(vhcl_check_remark_label_json, '')) as vhcl_check_remark_label_json  --车辆检查备注标签Json
, trim(nvl(vehicle_addi_check_json, '')) as vehicle_addi_check_json  --车辆额外检查Json
, trim(nvl(vehicle_img_json, '')) as vehicle_img_json  --车辆图片Json
, trim(nvl(vehicle_defect_info_json, '')) as vehicle_defect_info_json  --车辆缺陷信息Json
, trim(nvl(imp_service_info_json, '')) as imp_service_info_json  --重要维修保养信息Json
, trim(nvl(update_account, '')) as update_account  --更新账号
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
from dl.tg_dms_uc_t_uc_assessment_info
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_assessment_info
select  id as id --主键
, ass_serial as ass_serial --评估编号
, dealer_code as dealer_code --销售店代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, negotiation_id as negotiation_id --商谈ID
, sq as sq --序列号
, ass_status as ass_status --评估状态
, branch_code as branch_code --店铺代码
, vinno as vinno --车架号
, ass_date as ass_date --评估日期
, appraiser_memo as appraiser_memo --评估师寄语
, appraiser_account as appraiser_account --评估师账号
, appraiser_name as appraiser_name --评估师名称
, check_manage_no as check_manage_no --检查管理No
, vehicle_config_info_json as vehicle_config_info_json --车辆配置信息Json
, vehicle_check_remark_json as vehicle_check_remark_json --车辆检查备注Json
, vhcl_check_remark_label_json as vhcl_check_remark_label_json --车辆检查备注标签Json
, vehicle_addi_check_json as vehicle_addi_check_json --车辆额外检查Json
, vehicle_img_json as vehicle_img_json --车辆图片Json
, vehicle_defect_info_json as vehicle_defect_info_json --车辆缺陷信息Json
, imp_service_info_json as imp_service_info_json --重要维修保养信息Json
, update_account as update_account --更新账号
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
row_number() over (partition by ass_serial
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_assessment_info_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_assessment_ledger;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_assessment_ledger_tmp as (
select  id as id --主键
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, dealer_name as dealer_name --销售店名称
, ass_status as ass_status --评估状态
, vinno as vinno --车架号
, engine_number as engine_number --发动机号
, license_plate_number as license_plate_number --车牌号
, negotiation_id as negotiation_id --商谈ID
, customer_id as customer_id --客户ID
, customer_name as customer_name --客户姓名
, mobile_phone as mobile_phone --手机号
, telephone_number as telephone_number --电话号码
, brand_code as brand_code --品牌CD
, brand_name as brand_name --品牌名称
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, vehicle_model_code as vehicle_model_code --车型代码
, vehicle_model_name as vehicle_model_name --车型名称
, vehicle_grade as vehicle_grade --车辆等级
, first_regist_date as first_regist_date --首次注册日期
, odometer as odometer --行驶里程
, exhaust as exhaust --排量
, fuel_type_code as fuel_type_code --燃料种类代码
, fuel_type_name as fuel_type_name --燃料种类名称
, vehicle_type_code as vehicle_type_code --车辆类型代码
, vehicle_category_name as vehicle_category_name --车辆类型名称
, gearbox_type as gearbox_type --变速箱类型
, gearbox_name as gearbox_name --变速箱名称
, gearbox_category as gearbox_category --变速箱分类
, gearbox_category_name as gearbox_category_name --变速箱分类名称
, exterior_color_code_1 as exterior_color_code_1 --外装色CD1
, exterior_color_1 as exterior_color_1 --外装色1
, exterior_color_code_2 as exterior_color_code_2 --外装色CD2
, exterior_color_2 as exterior_color_2 --外装色2
, cover_thumbnail_img_url as cover_thumbnail_img_url --封面缩略图片URL
, ass_serial as ass_serial --评估编号
, ass_date as ass_date --评估日期
, ass_count as ass_count --评估次数
, appraiser_memo as appraiser_memo --评估师寄语
, related_type as related_type --关联类型
, related_id as related_id --关联ID
, sales_decision_time as sales_decision_time --销售决策时间
, sales_decision_status as sales_decision_status --销售决策状态
, plan_sales_method as plan_sales_method --计划销售方式
, retail_ref_sales_price as retail_ref_sales_price --零售参考销售价格
, retail_plan_sales_price as retail_plan_sales_price --零售计划销售价格
, retail_ref_purchase_price as retail_ref_purchase_price --零售参考采购价格
, retail_plan_purchase_price as retail_plan_purchase_price --零售计划采购价格
, retail_bench_gross_profit as retail_bench_gross_profit --零售基准毛利
, retail_plan_gross_profit as retail_plan_gross_profit --零售计划毛利
, whsle_ref_sales_price as whsle_ref_sales_price --批售参考销售价格
, whsle_plan_sales_price as whsle_plan_sales_price --批售计划销售价格
, whsle_ref_purchase_price as whsle_ref_purchase_price --批售参考采购价格
, whsle_plan_purchase_price as whsle_plan_purchase_price --批售计划采购价格
, whsle_bench_gross_profit as whsle_bench_gross_profit --批售基准毛利
, whsle_plan_gross_profit as whsle_plan_gross_profit --批售计划毛利
, new_vehicle_guide_price as new_vehicle_guide_price --新车指导价格
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
from ods.ods_dms_uc_uc_assessment_ledger
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(dealer_name, '')) as dealer_name  --销售店名称
, trim(nvl(ass_status, '')) as ass_status  --评估状态
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(engine_number, '')) as engine_number  --发动机号
, trim(nvl(license_plate_number, '')) as license_plate_number  --车牌号
, trim(nvl(negotiation_id, '')) as negotiation_id  --商谈ID
, trim(nvl(customer_id, '')) as customer_id  --客户ID
, trim(nvl(customer_name, '')) as customer_name  --客户姓名
, trim(nvl(mobile_phone, '')) as mobile_phone  --手机号
, trim(nvl(telephone_number, '')) as telephone_number  --电话号码
, trim(nvl(brand_code, '')) as brand_code  --品牌CD
, trim(nvl(brand_name, '')) as brand_name  --品牌名称
, trim(nvl(maker_code, '')) as maker_code  --厂商代码
, trim(nvl(maker_name, '')) as maker_name  --厂商名称
, trim(nvl(vehicle_model_code, '')) as vehicle_model_code  --车型代码
, trim(nvl(vehicle_model_name, '')) as vehicle_model_name  --车型名称
, trim(nvl(vehicle_grade, '')) as vehicle_grade  --车辆等级
, trim(nvl(first_regist_date, '')) as first_regist_date  --首次注册日期
, nvl(odometer, 0) as odometer  --行驶里程
, trim(nvl(exhaust, '')) as exhaust  --排量
, trim(nvl(fuel_type_code, '')) as fuel_type_code  --燃料种类代码
, trim(nvl(fuel_type_name, '')) as fuel_type_name  --燃料种类名称
, trim(nvl(vehicle_type_code, '')) as vehicle_type_code  --车辆类型代码
, trim(nvl(vehicle_category_name, '')) as vehicle_category_name  --车辆类型名称
, trim(nvl(gearbox_type, '')) as gearbox_type  --变速箱类型
, trim(nvl(gearbox_name, '')) as gearbox_name  --变速箱名称
, trim(nvl(gearbox_category, '')) as gearbox_category  --变速箱分类
, trim(nvl(gearbox_category_name, '')) as gearbox_category_name  --变速箱分类名称
, trim(nvl(exterior_color_code_1, '')) as exterior_color_code_1  --外装色CD1
, trim(nvl(exterior_color_1, '')) as exterior_color_1  --外装色1
, trim(nvl(exterior_color_code_2, '')) as exterior_color_code_2  --外装色CD2
, trim(nvl(exterior_color_2, '')) as exterior_color_2  --外装色2
, trim(nvl(cover_thumbnail_img_url, '')) as cover_thumbnail_img_url  --封面缩略图片URL
, nvl(ass_serial, 0) as ass_serial  --评估编号
, trim(nvl(ass_date, '')) as ass_date  --评估日期
, nvl(ass_count, 0) as ass_count  --评估次数
, trim(nvl(appraiser_memo, '')) as appraiser_memo  --评估师寄语
, trim(nvl(related_type, '')) as related_type  --关联类型
, trim(nvl(related_id, '')) as related_id  --关联ID
, trim(nvl(sales_decision_time, '')) as sales_decision_time  --销售决策时间
, trim(nvl(sales_decision_status, '')) as sales_decision_status  --销售决策状态
, trim(nvl(plan_sales_method, '')) as plan_sales_method  --计划销售方式
, nvl(retail_ref_sales_price, 0) as retail_ref_sales_price  --零售参考销售价格
, nvl(retail_plan_sales_price, 0) as retail_plan_sales_price  --零售计划销售价格
, nvl(retail_ref_purchase_price, 0) as retail_ref_purchase_price  --零售参考采购价格
, nvl(retail_plan_purchase_price, 0) as retail_plan_purchase_price  --零售计划采购价格
, nvl(retail_bench_gross_profit, 0) as retail_bench_gross_profit  --零售基准毛利
, nvl(retail_plan_gross_profit, 0) as retail_plan_gross_profit  --零售计划毛利
, nvl(whsle_ref_sales_price, 0) as whsle_ref_sales_price  --批售参考销售价格
, nvl(whsle_plan_sales_price, 0) as whsle_plan_sales_price  --批售计划销售价格
, nvl(whsle_ref_purchase_price, 0) as whsle_ref_purchase_price  --批售参考采购价格
, nvl(whsle_plan_purchase_price, 0) as whsle_plan_purchase_price  --批售计划采购价格
, nvl(whsle_bench_gross_profit, 0) as whsle_bench_gross_profit  --批售基准毛利
, nvl(whsle_plan_gross_profit, 0) as whsle_plan_gross_profit  --批售计划毛利
, nvl(new_vehicle_guide_price, 0) as new_vehicle_guide_price  --新车指导价格
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
from dl.tg_dms_uc_t_uc_assessment_ledger
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_assessment_ledger
select  id as id --主键
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, dealer_name as dealer_name --销售店名称
, ass_status as ass_status --评估状态
, vinno as vinno --车架号
, engine_number as engine_number --发动机号
, license_plate_number as license_plate_number --车牌号
, negotiation_id as negotiation_id --商谈ID
, customer_id as customer_id --客户ID
, customer_name as customer_name --客户姓名
, mobile_phone as mobile_phone --手机号
, telephone_number as telephone_number --电话号码
, brand_code as brand_code --品牌CD
, brand_name as brand_name --品牌名称
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, vehicle_model_code as vehicle_model_code --车型代码
, vehicle_model_name as vehicle_model_name --车型名称
, vehicle_grade as vehicle_grade --车辆等级
, first_regist_date as first_regist_date --首次注册日期
, odometer as odometer --行驶里程
, exhaust as exhaust --排量
, fuel_type_code as fuel_type_code --燃料种类代码
, fuel_type_name as fuel_type_name --燃料种类名称
, vehicle_type_code as vehicle_type_code --车辆类型代码
, vehicle_category_name as vehicle_category_name --车辆类型名称
, gearbox_type as gearbox_type --变速箱类型
, gearbox_name as gearbox_name --变速箱名称
, gearbox_category as gearbox_category --变速箱分类
, gearbox_category_name as gearbox_category_name --变速箱分类名称
, exterior_color_code_1 as exterior_color_code_1 --外装色CD1
, exterior_color_1 as exterior_color_1 --外装色1
, exterior_color_code_2 as exterior_color_code_2 --外装色CD2
, exterior_color_2 as exterior_color_2 --外装色2
, cover_thumbnail_img_url as cover_thumbnail_img_url --封面缩略图片URL
, ass_serial as ass_serial --评估编号
, ass_date as ass_date --评估日期
, ass_count as ass_count --评估次数
, appraiser_memo as appraiser_memo --评估师寄语
, related_type as related_type --关联类型
, related_id as related_id --关联ID
, sales_decision_time as sales_decision_time --销售决策时间
, sales_decision_status as sales_decision_status --销售决策状态
, plan_sales_method as plan_sales_method --计划销售方式
, retail_ref_sales_price as retail_ref_sales_price --零售参考销售价格
, retail_plan_sales_price as retail_plan_sales_price --零售计划销售价格
, retail_ref_purchase_price as retail_ref_purchase_price --零售参考采购价格
, retail_plan_purchase_price as retail_plan_purchase_price --零售计划采购价格
, retail_bench_gross_profit as retail_bench_gross_profit --零售基准毛利
, retail_plan_gross_profit as retail_plan_gross_profit --零售计划毛利
, whsle_ref_sales_price as whsle_ref_sales_price --批售参考销售价格
, whsle_plan_sales_price as whsle_plan_sales_price --批售计划销售价格
, whsle_ref_purchase_price as whsle_ref_purchase_price --批售参考采购价格
, whsle_plan_purchase_price as whsle_plan_purchase_price --批售计划采购价格
, whsle_bench_gross_profit as whsle_bench_gross_profit --批售基准毛利
, whsle_plan_gross_profit as whsle_plan_gross_profit --批售计划毛利
, new_vehicle_guide_price as new_vehicle_guide_price --新车指导价格
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
row_number() over (partition by dealer_code,vehicle_manage_no
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_assessment_ledger_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_assessment_vehicle_info;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_assessment_vehicle_info_tmp as (
select  id as id --主键
, ass_serial as ass_serial --评估编号
, dealer_code as dealer_code --销售店代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, branch_code as branch_code --店铺代码
, brand_code as brand_code --品牌CD
, brand_name as brand_name --品牌名称
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, vehicle_model_code as vehicle_model_code --车型代码
, vehicle_model_name as vehicle_model_name --车型名称
, vehicle_grade as vehicle_grade --车辆等级
, vehicle_type_code as vehicle_type_code --车辆类型代码
, vehicle_category_name as vehicle_category_name --车辆类型名称
, national_standard_code as national_standard_code --国标代码
, vinno as vinno --车架号
, engine_number as engine_number --发动机号
, license_plate_number as license_plate_number --车牌号
, production_date as production_date --制造日期
, exterior_color_code_1 as exterior_color_code_1 --外装色CD1
, exterior_color_1 as exterior_color_1 --外装色1
, exterior_color_code_2 as exterior_color_code_2 --外装色CD2
, exterior_color_2 as exterior_color_2 --外装色2
, color_change_diff as color_change_diff --颜色变更区分
, interior_color_code_1 as interior_color_code_1 --内装色CD1
, interior_color_1 as interior_color_1 --内装色1
, interior_color_code_2 as interior_color_code_2 --内装色CD2
, interior_color_2 as interior_color_2 --内装色2
, odometer as odometer --行驶里程
, first_regist_date as first_regist_date --首次注册日期
, recent_regist_date as recent_regist_date --最近注册日期
, use_nature as use_nature --使用性质
, whole_quality_level as whole_quality_level --整体品质级别
, elect_level as elect_level --机电级别
, outside_rating as outside_rating --外观定级
, interior_level as interior_level --内饰级别
, accident_level as accident_level --事故级别
, collision_accident_diff as collision_accident_diff --碰撞事故区分
, water_accident_diff as water_accident_diff --水浸事故区分
, fire_accident_diff as fire_accident_diff --火烧事故区分
, excessive_modified_diff as excessive_modified_diff --过度改装区分
, wty_type as wty_type --三包类型
, instrument_tampering_flag as instrument_tampering_flag --仪表篡改标识
, vehicle_door_qty as vehicle_door_qty --车门数量
, rate_allow_people_qty as rate_allow_people_qty --额定载客人数
, whole_weight as whole_weight --整备质量
, car_length as car_length --车长
, car_width as car_width --车宽
, car_height as car_height --车高
, fuel_type_code as fuel_type_code --燃料种类代码
, fuel_type_name as fuel_type_name --燃料种类名称
, exhaust as exhaust --排量
, fuel_consumption as fuel_consumption --油耗
, discharge_standard as discharge_standard --排放标准
, gearbox_type as gearbox_type --变速箱类型
, gearbox_name as gearbox_name --变速箱名称
, drive_method_serial as drive_method_serial --驱动方式编号
, drive_method_name as drive_method_name --驱动方式名称
, cover_thumbnail_img_url as cover_thumbnail_img_url --封面缩略图片URL
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
from ods.ods_dms_uc_uc_assessment_vehicle_info
union all
select  trim(nvl(id, '')) as id  --主键
, nvl(ass_serial, 0) as ass_serial  --评估编号
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(brand_code, '')) as brand_code  --品牌CD
, trim(nvl(brand_name, '')) as brand_name  --品牌名称
, trim(nvl(maker_code, '')) as maker_code  --厂商代码
, trim(nvl(maker_name, '')) as maker_name  --厂商名称
, trim(nvl(vehicle_model_code, '')) as vehicle_model_code  --车型代码
, trim(nvl(vehicle_model_name, '')) as vehicle_model_name  --车型名称
, trim(nvl(vehicle_grade, '')) as vehicle_grade  --车辆等级
, trim(nvl(vehicle_type_code, '')) as vehicle_type_code  --车辆类型代码
, trim(nvl(vehicle_category_name, '')) as vehicle_category_name  --车辆类型名称
, trim(nvl(national_standard_code, '')) as national_standard_code  --国标代码
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(engine_number, '')) as engine_number  --发动机号
, trim(nvl(license_plate_number, '')) as license_plate_number  --车牌号
, trim(nvl(production_date, '')) as production_date  --制造日期
, trim(nvl(exterior_color_code_1, '')) as exterior_color_code_1  --外装色CD1
, trim(nvl(exterior_color_1, '')) as exterior_color_1  --外装色1
, trim(nvl(exterior_color_code_2, '')) as exterior_color_code_2  --外装色CD2
, trim(nvl(exterior_color_2, '')) as exterior_color_2  --外装色2
, trim(nvl(color_change_diff, '')) as color_change_diff  --颜色变更区分
, trim(nvl(interior_color_code_1, '')) as interior_color_code_1  --内装色CD1
, trim(nvl(interior_color_1, '')) as interior_color_1  --内装色1
, trim(nvl(interior_color_code_2, '')) as interior_color_code_2  --内装色CD2
, trim(nvl(interior_color_2, '')) as interior_color_2  --内装色2
, nvl(odometer, 0) as odometer  --行驶里程
, trim(nvl(first_regist_date, '')) as first_regist_date  --首次注册日期
, trim(nvl(recent_regist_date, '')) as recent_regist_date  --最近注册日期
, trim(nvl(use_nature, '')) as use_nature  --使用性质
, trim(nvl(whole_quality_level, '')) as whole_quality_level  --整体品质级别
, trim(nvl(elect_level, '')) as elect_level  --机电级别
, trim(nvl(outside_rating, '')) as outside_rating  --外观定级
, trim(nvl(interior_level, '')) as interior_level  --内饰级别
, trim(nvl(accident_level, '')) as accident_level  --事故级别
, trim(nvl(collision_accident_diff, '')) as collision_accident_diff  --碰撞事故区分
, trim(nvl(water_accident_diff, '')) as water_accident_diff  --水浸事故区分
, trim(nvl(fire_accident_diff, '')) as fire_accident_diff  --火烧事故区分
, trim(nvl(excessive_modified_diff, '')) as excessive_modified_diff  --过度改装区分
, trim(nvl(wty_type, '')) as wty_type  --三包类型
, trim(nvl(instrument_tampering_flag, '')) as instrument_tampering_flag  --仪表篡改标识
, nvl(vehicle_door_qty, 0) as vehicle_door_qty  --车门数量
, nvl(rate_allow_people_qty, 0) as rate_allow_people_qty  --额定载客人数
, nvl(whole_weight, 0) as whole_weight  --整备质量
, trim(nvl(car_length, '')) as car_length  --车长
, trim(nvl(car_width, '')) as car_width  --车宽
, trim(nvl(car_height, '')) as car_height  --车高
, trim(nvl(fuel_type_code, '')) as fuel_type_code  --燃料种类代码
, trim(nvl(fuel_type_name, '')) as fuel_type_name  --燃料种类名称
, trim(nvl(exhaust, '')) as exhaust  --排量
, trim(nvl(fuel_consumption, '')) as fuel_consumption  --油耗
, trim(nvl(discharge_standard, '')) as discharge_standard  --排放标准
, trim(nvl(gearbox_type, '')) as gearbox_type  --变速箱类型
, trim(nvl(gearbox_name, '')) as gearbox_name  --变速箱名称
, trim(nvl(drive_method_serial, '')) as drive_method_serial  --驱动方式编号
, trim(nvl(drive_method_name, '')) as drive_method_name  --驱动方式名称
, trim(nvl(cover_thumbnail_img_url, '')) as cover_thumbnail_img_url  --封面缩略图片URL
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
from dl.tg_dms_uc_t_uc_assessment_vehicle_info
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_assessment_vehicle_info
select  id as id --主键
, ass_serial as ass_serial --评估编号
, dealer_code as dealer_code --销售店代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, branch_code as branch_code --店铺代码
, brand_code as brand_code --品牌CD
, brand_name as brand_name --品牌名称
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, vehicle_model_code as vehicle_model_code --车型代码
, vehicle_model_name as vehicle_model_name --车型名称
, vehicle_grade as vehicle_grade --车辆等级
, vehicle_type_code as vehicle_type_code --车辆类型代码
, vehicle_category_name as vehicle_category_name --车辆类型名称
, national_standard_code as national_standard_code --国标代码
, vinno as vinno --车架号
, engine_number as engine_number --发动机号
, license_plate_number as license_plate_number --车牌号
, production_date as production_date --制造日期
, exterior_color_code_1 as exterior_color_code_1 --外装色CD1
, exterior_color_1 as exterior_color_1 --外装色1
, exterior_color_code_2 as exterior_color_code_2 --外装色CD2
, exterior_color_2 as exterior_color_2 --外装色2
, color_change_diff as color_change_diff --颜色变更区分
, interior_color_code_1 as interior_color_code_1 --内装色CD1
, interior_color_1 as interior_color_1 --内装色1
, interior_color_code_2 as interior_color_code_2 --内装色CD2
, interior_color_2 as interior_color_2 --内装色2
, odometer as odometer --行驶里程
, first_regist_date as first_regist_date --首次注册日期
, recent_regist_date as recent_regist_date --最近注册日期
, use_nature as use_nature --使用性质
, whole_quality_level as whole_quality_level --整体品质级别
, elect_level as elect_level --机电级别
, outside_rating as outside_rating --外观定级
, interior_level as interior_level --内饰级别
, accident_level as accident_level --事故级别
, collision_accident_diff as collision_accident_diff --碰撞事故区分
, water_accident_diff as water_accident_diff --水浸事故区分
, fire_accident_diff as fire_accident_diff --火烧事故区分
, excessive_modified_diff as excessive_modified_diff --过度改装区分
, wty_type as wty_type --三包类型
, instrument_tampering_flag as instrument_tampering_flag --仪表篡改标识
, vehicle_door_qty as vehicle_door_qty --车门数量
, rate_allow_people_qty as rate_allow_people_qty --额定载客人数
, whole_weight as whole_weight --整备质量
, car_length as car_length --车长
, car_width as car_width --车宽
, car_height as car_height --车高
, fuel_type_code as fuel_type_code --燃料种类代码
, fuel_type_name as fuel_type_name --燃料种类名称
, exhaust as exhaust --排量
, fuel_consumption as fuel_consumption --油耗
, discharge_standard as discharge_standard --排放标准
, gearbox_type as gearbox_type --变速箱类型
, gearbox_name as gearbox_name --变速箱名称
, drive_method_serial as drive_method_serial --驱动方式编号
, drive_method_name as drive_method_name --驱动方式名称
, cover_thumbnail_img_url as cover_thumbnail_img_url --封面缩略图片URL
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
row_number() over (partition by ass_serial
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_assessment_vehicle_info_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_assessment_vehicle_info_2;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_assessment_vehicle_info_2_tmp as (
select  id as id --主键
, ass_serial as ass_serial --评估编号
, dealer_code as dealer_code --销售店代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, branch_code as branch_code --店铺代码
, vehicle_check_eff_period as vehicle_check_eff_period --车辆年审期限
, comp_ins_period as comp_ins_period --交强险期限
, vehicle_tax_period as vehicle_tax_period --车船税期限
, business_ins_eff_period as business_ins_eff_period --商业保险有效期限
, vehicle_tools as vehicle_tools --车载工具
, vehicle_use_manual as vehicle_use_manual --车辆使用手册
, vehicle_service_manual as vehicle_service_manual --车辆维修保养手册
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
from ods.ods_dms_uc_uc_assessment_vehicle_info_2
union all
select  trim(nvl(id, '')) as id  --主键
, nvl(ass_serial, 0) as ass_serial  --评估编号
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_check_eff_period, '')) as vehicle_check_eff_period  --车辆年审期限
, trim(nvl(comp_ins_period, '')) as comp_ins_period  --交强险期限
, trim(nvl(vehicle_tax_period, '')) as vehicle_tax_period  --车船税期限
, trim(nvl(business_ins_eff_period, '')) as business_ins_eff_period  --商业保险有效期限
, trim(nvl(vehicle_tools, '')) as vehicle_tools  --车载工具
, trim(nvl(vehicle_use_manual, '')) as vehicle_use_manual  --车辆使用手册
, trim(nvl(vehicle_service_manual, '')) as vehicle_service_manual  --车辆维修保养手册
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
from dl.tg_dms_uc_t_uc_assessment_vehicle_info_2
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_assessment_vehicle_info_2
select  id as id --主键
, ass_serial as ass_serial --评估编号
, dealer_code as dealer_code --销售店代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, branch_code as branch_code --店铺代码
, vehicle_check_eff_period as vehicle_check_eff_period --车辆年审期限
, comp_ins_period as comp_ins_period --交强险期限
, vehicle_tax_period as vehicle_tax_period --车船税期限
, business_ins_eff_period as business_ins_eff_period --商业保险有效期限
, vehicle_tools as vehicle_tools --车载工具
, vehicle_use_manual as vehicle_use_manual --车辆使用手册
, vehicle_service_manual as vehicle_service_manual --车辆维修保养手册
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
row_number() over (partition by ass_serial
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_assessment_vehicle_info_2_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_assessment_vehicle_info_3;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_assessment_vehicle_info_3_tmp as (
select  id as id --主键
, ass_serial as ass_serial --评估编号
, dealer_code as dealer_code --销售店代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, branch_code as branch_code --店铺代码
, service_count as service_count --维修保养次数
, recent_service_date as recent_service_date --最近维修保养日期
, recent_service_mileage as recent_service_mileage --最近维修保养里程
, recent_service_dealer as recent_service_dealer --最近维修保养销售店
, service_history_flag as service_history_flag --维修保养履历标识
, maintain_suggest_result as maintain_suggest_result --维修建议与结果
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
from ods.ods_dms_uc_uc_assessment_vehicle_info_3
union all
select  trim(nvl(id, '')) as id  --主键
, nvl(ass_serial, 0) as ass_serial  --评估编号
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, nvl(service_count, 0) as service_count  --维修保养次数
, trim(nvl(recent_service_date, '')) as recent_service_date  --最近维修保养日期
, nvl(recent_service_mileage, 0) as recent_service_mileage  --最近维修保养里程
, trim(nvl(recent_service_dealer, '')) as recent_service_dealer  --最近维修保养销售店
, trim(nvl(service_history_flag, '')) as service_history_flag  --维修保养履历标识
, trim(nvl(maintain_suggest_result, '')) as maintain_suggest_result  --维修建议与结果
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
from dl.tg_dms_uc_t_uc_assessment_vehicle_info_3
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_assessment_vehicle_info_3
select  id as id --主键
, ass_serial as ass_serial --评估编号
, dealer_code as dealer_code --销售店代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, branch_code as branch_code --店铺代码
, service_count as service_count --维修保养次数
, recent_service_date as recent_service_date --最近维修保养日期
, recent_service_mileage as recent_service_mileage --最近维修保养里程
, recent_service_dealer as recent_service_dealer --最近维修保养销售店
, service_history_flag as service_history_flag --维修保养履历标识
, maintain_suggest_result as maintain_suggest_result --维修建议与结果
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
row_number() over (partition by ass_serial
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_assessment_vehicle_info_3_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_authen_apply;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_authen_apply_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, auth_apply_no as auth_apply_no --认证申请编号
, first_regist_date as first_regist_date --首次注册日期
, vinno as vinno --车架号
, uc_auth_no as uc_auth_no --二手车认证编号
, authen_type as authen_type --认证类型
, other_dealer_authen_flag as other_dealer_authen_flag --其他销售店认证标识
, authen_apply_type_code as authen_apply_type_code --认证申请类型CD
, auth_status as auth_status --认证状态
, cancel_reason_type as cancel_reason_type --取消原因类型
, auth_apply_time as auth_apply_time --认证申请时间
, authen_apply_staff_account as authen_apply_staff_account --认证申请担当账号
, authen_apply_staff_name as authen_apply_staff_name --认证申请担当姓名
, authen_operate_time as authen_operate_time --认证操作时间
, authen_operate_account as authen_operate_account --认证操作账号
, authen_operate_staff_name as authen_operate_staff_name --认证操作担当姓名
, authen_operate_opinion as authen_operate_opinion --认证操作意见
, uc_auth_time as uc_auth_time --二手车认证生效时间
, uc_auth_due_date as uc_auth_due_date --二手车认证到期日
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
from ods.ods_dms_uc_uc_authen_apply
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(auth_apply_no, '')) as auth_apply_no  --认证申请编号
, trim(nvl(first_regist_date, '')) as first_regist_date  --首次注册日期
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(uc_auth_no, '')) as uc_auth_no  --二手车认证编号
, trim(nvl(authen_type, '')) as authen_type  --认证类型
, trim(nvl(other_dealer_authen_flag, '')) as other_dealer_authen_flag  --其他销售店认证标识
, trim(nvl(authen_apply_type_code, '')) as authen_apply_type_code  --认证申请类型CD
, trim(nvl(auth_status, '')) as auth_status  --认证状态
, trim(nvl(cancel_reason_type, '')) as cancel_reason_type  --取消原因类型
, trim(nvl(auth_apply_time, '')) as auth_apply_time  --认证申请时间
, trim(nvl(authen_apply_staff_account, '')) as authen_apply_staff_account  --认证申请担当账号
, trim(nvl(authen_apply_staff_name, '')) as authen_apply_staff_name  --认证申请担当姓名
, trim(nvl(authen_operate_time, '')) as authen_operate_time  --认证操作时间
, trim(nvl(authen_operate_account, '')) as authen_operate_account  --认证操作账号
, trim(nvl(authen_operate_staff_name, '')) as authen_operate_staff_name  --认证操作担当姓名
, trim(nvl(authen_operate_opinion, '')) as authen_operate_opinion  --认证操作意见
, trim(nvl(uc_auth_time, '')) as uc_auth_time  --二手车认证生效时间
, trim(nvl(uc_auth_due_date, '')) as uc_auth_due_date  --二手车认证到期日
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
from dl.tg_dms_uc_t_uc_authen_apply
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_authen_apply
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, auth_apply_no as auth_apply_no --认证申请编号
, first_regist_date as first_regist_date --首次注册日期
, vinno as vinno --车架号
, uc_auth_no as uc_auth_no --二手车认证编号
, authen_type as authen_type --认证类型
, other_dealer_authen_flag as other_dealer_authen_flag --其他销售店认证标识
, authen_apply_type_code as authen_apply_type_code --认证申请类型CD
, auth_status as auth_status --认证状态
, cancel_reason_type as cancel_reason_type --取消原因类型
, auth_apply_time as auth_apply_time --认证申请时间
, authen_apply_staff_account as authen_apply_staff_account --认证申请担当账号
, authen_apply_staff_name as authen_apply_staff_name --认证申请担当姓名
, authen_operate_time as authen_operate_time --认证操作时间
, authen_operate_account as authen_operate_account --认证操作账号
, authen_operate_staff_name as authen_operate_staff_name --认证操作担当姓名
, authen_operate_opinion as authen_operate_opinion --认证操作意见
, uc_auth_time as uc_auth_time --二手车认证生效时间
, uc_auth_due_date as uc_auth_due_date --二手车认证到期日
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
row_number() over (partition by auth_apply_no
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_authen_apply_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_cust_car_buy_follow;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_cust_car_buy_follow_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, negotiation_id as negotiation_id --商谈ID
, follow_time as follow_time --跟进时间
, car_buy_follow_status as car_buy_follow_status --买车跟进状态
, follow_account as follow_account --跟进账号
, follow_staff_name as follow_staff_name --跟进担当姓名
, car_buy_follow_result as car_buy_follow_result --买车跟进结果
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
from ods.ods_dms_uc_uc_cust_car_buy_follow
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(negotiation_id, '')) as negotiation_id  --商谈ID
, trim(nvl(follow_time, '')) as follow_time  --跟进时间
, trim(nvl(car_buy_follow_status, '')) as car_buy_follow_status  --买车跟进状态
, trim(nvl(follow_account, '')) as follow_account  --跟进账号
, trim(nvl(follow_staff_name, '')) as follow_staff_name  --跟进担当姓名
, trim(nvl(car_buy_follow_result, '')) as car_buy_follow_result  --买车跟进结果
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
from dl.tg_dms_uc_t_uc_cust_car_buy_follow
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_cust_car_buy_follow
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, negotiation_id as negotiation_id --商谈ID
, follow_time as follow_time --跟进时间
, car_buy_follow_status as car_buy_follow_status --买车跟进状态
, follow_account as follow_account --跟进账号
, follow_staff_name as follow_staff_name --跟进担当姓名
, car_buy_follow_result as car_buy_follow_result --买车跟进结果
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
from ods_dms_uc_uc_cust_car_buy_follow_tmp) a
where rn = 1;



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



set mapred.job.name=job_ods_dms_uc_uc_cust_sales_vio_settl;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_cust_sales_vio_settl_tmp as (
select  id as id --主键
, buyer_sales_violation_serial as buyer_sales_violation_serial --买方销售违约编号
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, sales_contract_serial as sales_contract_serial --销售合同编号
, violation_amount as violation_amount --违约金额
, settl_status as settl_status --结算状态
, finish_payment_time as finish_payment_time --完成付款时间
, input_date as input_date --录入日期
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
from ods.ods_dms_uc_uc_cust_sales_vio_settl
union all
select  trim(nvl(id, '')) as id  --主键
, nvl(buyer_sales_violation_serial, 0) as buyer_sales_violation_serial  --买方销售违约编号
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(sales_contract_serial, '')) as sales_contract_serial  --销售合同编号
, nvl(violation_amount, 0) as violation_amount  --违约金额
, trim(nvl(settl_status, '')) as settl_status  --结算状态
, trim(nvl(finish_payment_time, '')) as finish_payment_time  --完成付款时间
, trim(nvl(input_date, '')) as input_date  --录入日期
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
from dl.tg_dms_uc_t_uc_cust_sales_vio_settl
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_cust_sales_vio_settl
select  id as id --主键
, buyer_sales_violation_serial as buyer_sales_violation_serial --买方销售违约编号
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, sales_contract_serial as sales_contract_serial --销售合同编号
, violation_amount as violation_amount --违约金额
, settl_status as settl_status --结算状态
, finish_payment_time as finish_payment_time --完成付款时间
, input_date as input_date --录入日期
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
row_number() over (partition by buyer_sales_violation_serial
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_cust_sales_vio_settl_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_customer_info;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_customer_info_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, customer_id as customer_id --客户ID
, customer_name as customer_name --客户姓名
, negotiation_customer_address as negotiation_customer_address --商谈客户地址
, customer_mobile_phone as customer_mobile_phone --客户移动电话
, customer_fixed_tel as customer_fixed_tel --客户固定电话
, customer_postal_code as customer_postal_code --客户邮编
, customer_email as customer_email --客户电子邮箱
, comm_activity_type as comm_activity_type --商业性活动类型
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
from ods.ods_dms_uc_uc_customer_info
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(customer_id, '')) as customer_id  --客户ID
, trim(nvl(customer_name, '')) as customer_name  --客户姓名
, trim(nvl(negotiation_customer_address, '')) as negotiation_customer_address  --商谈客户地址
, trim(nvl(customer_mobile_phone, '')) as customer_mobile_phone  --客户移动电话
, trim(nvl(customer_fixed_tel, '')) as customer_fixed_tel  --客户固定电话
, trim(nvl(customer_postal_code, '')) as customer_postal_code  --客户邮编
, trim(nvl(customer_email, '')) as customer_email  --客户电子邮箱
, trim(nvl(comm_activity_type, '')) as comm_activity_type  --商业性活动类型
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
from dl.tg_dms_uc_t_uc_customer_info
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_customer_info
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, customer_id as customer_id --客户ID
, customer_name as customer_name --客户姓名
, negotiation_customer_address as negotiation_customer_address --商谈客户地址
, customer_mobile_phone as customer_mobile_phone --客户移动电话
, customer_fixed_tel as customer_fixed_tel --客户固定电话
, customer_postal_code as customer_postal_code --客户邮编
, customer_email as customer_email --客户电子邮箱
, comm_activity_type as comm_activity_type --商业性活动类型
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
row_number() over (partition by dealer_code,customer_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_customer_info_tmp) a
where rn = 1;



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



set mapred.job.name=job_ods_dms_uc_uc_dlr_sales_vio_settl;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_dlr_sales_vio_settl_tmp as (
select  id as id --主键
, seller_sales_violation_serial as seller_sales_violation_serial --买方销售违约编号
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, sales_contract_serial as sales_contract_serial --销售合同编号
, violation_amount as violation_amount --违约金额
, settl_status as settl_status --结算状态
, finish_payment_time as finish_payment_time --完成付款时间
, input_date as input_date --录入日期
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
from ods.ods_dms_uc_uc_dlr_sales_vio_settl
union all
select  trim(nvl(id, '')) as id  --主键
, nvl(seller_sales_violation_serial, 0) as seller_sales_violation_serial  --买方销售违约编号
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(sales_contract_serial, '')) as sales_contract_serial  --销售合同编号
, nvl(violation_amount, 0) as violation_amount  --违约金额
, trim(nvl(settl_status, '')) as settl_status  --结算状态
, trim(nvl(finish_payment_time, '')) as finish_payment_time  --完成付款时间
, trim(nvl(input_date, '')) as input_date  --录入日期
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
from dl.tg_dms_uc_t_uc_dlr_sales_vio_settl
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_dlr_sales_vio_settl
select  id as id --主键
, seller_sales_violation_serial as seller_sales_violation_serial --买方销售违约编号
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, sales_contract_serial as sales_contract_serial --销售合同编号
, violation_amount as violation_amount --违约金额
, settl_status as settl_status --结算状态
, finish_payment_time as finish_payment_time --完成付款时间
, input_date as input_date --录入日期
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
row_number() over (partition by seller_sales_violation_serial
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_dlr_sales_vio_settl_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_exchange_vehicle_relation;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_exchange_vehicle_relation_tmp as (
select  id as id --主键
, displace_vehicle_related_id as displace_vehicle_related_id --置换车辆关联ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, car_owner_name as car_owner_name --车主名称
, displace_vehicle_manage_no as displace_vehicle_manage_no --置换车辆管理编号
, displace_vinno as displace_vinno --置换车架号
, displace_vehicle_picture_url as displace_vehicle_picture_url --置换车辆图片路径
, displace_vehicle_config_grade as displace_vehicle_config_grade --置换车辆配置等级
, displace_vehicle_model_code as displace_vehicle_model_code --置换车型代码
, displace_vehicle_model_name as displace_vehicle_model_name --置换车型名字
, displace_vehicle_nominee_name as displace_vehicle_nominee_name --置换车辆名义人姓名
, displace_vehicle_nominee_tel as displace_vehicle_nominee_tel --置换车辆名义人电话
, displace_vehicle_sales_date as displace_vehicle_sales_date --置换车辆销售日期
, displace_related_status as displace_related_status --置换关联状态
, displace_related_time as displace_related_time --置换关联时间
, displace_type as displace_type --置换类型
, displace_vehicle_related_type as displace_vehicle_related_type --置换车辆关联类型
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
from ods.ods_dms_uc_uc_exchange_vehicle_relation
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(displace_vehicle_related_id, '')) as displace_vehicle_related_id  --置换车辆关联ID
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(car_owner_name, '')) as car_owner_name  --车主名称
, trim(nvl(displace_vehicle_manage_no, '')) as displace_vehicle_manage_no  --置换车辆管理编号
, trim(nvl(displace_vinno, '')) as displace_vinno  --置换车架号
, trim(nvl(displace_vehicle_picture_url, '')) as displace_vehicle_picture_url  --置换车辆图片路径
, trim(nvl(displace_vehicle_config_grade, '')) as displace_vehicle_config_grade  --置换车辆配置等级
, trim(nvl(displace_vehicle_model_code, '')) as displace_vehicle_model_code  --置换车型代码
, trim(nvl(displace_vehicle_model_name, '')) as displace_vehicle_model_name  --置换车型名字
, trim(nvl(displace_vehicle_nominee_name, '')) as displace_vehicle_nominee_name  --置换车辆名义人姓名
, trim(nvl(displace_vehicle_nominee_tel, '')) as displace_vehicle_nominee_tel  --置换车辆名义人电话
, trim(nvl(displace_vehicle_sales_date, '')) as displace_vehicle_sales_date  --置换车辆销售日期
, nvl(displace_related_status, 0) as displace_related_status  --置换关联状态
, trim(nvl(displace_related_time, '')) as displace_related_time  --置换关联时间
, trim(nvl(displace_type, '')) as displace_type  --置换类型
, trim(nvl(displace_vehicle_related_type, '')) as displace_vehicle_related_type  --置换车辆关联类型
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
from dl.tg_dms_uc_t_uc_exchange_vehicle_relation
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_exchange_vehicle_relation
select  id as id --主键
, displace_vehicle_related_id as displace_vehicle_related_id --置换车辆关联ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, car_owner_name as car_owner_name --车主名称
, displace_vehicle_manage_no as displace_vehicle_manage_no --置换车辆管理编号
, displace_vinno as displace_vinno --置换车架号
, displace_vehicle_picture_url as displace_vehicle_picture_url --置换车辆图片路径
, displace_vehicle_config_grade as displace_vehicle_config_grade --置换车辆配置等级
, displace_vehicle_model_code as displace_vehicle_model_code --置换车型代码
, displace_vehicle_model_name as displace_vehicle_model_name --置换车型名字
, displace_vehicle_nominee_name as displace_vehicle_nominee_name --置换车辆名义人姓名
, displace_vehicle_nominee_tel as displace_vehicle_nominee_tel --置换车辆名义人电话
, displace_vehicle_sales_date as displace_vehicle_sales_date --置换车辆销售日期
, displace_related_status as displace_related_status --置换关联状态
, displace_related_time as displace_related_time --置换关联时间
, displace_type as displace_type --置换类型
, displace_vehicle_related_type as displace_vehicle_related_type --置换车辆关联类型
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
row_number() over (partition by displace_vehicle_related_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_exchange_vehicle_relation_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_inventory_ledger;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_inventory_ledger_tmp as (
select  id as id --主键
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, dealer_name as dealer_name --销售店名称
, uc_stock_status as uc_stock_status --二手车库存状态
, vinno as vinno --车架号
, engine_number as engine_number --发动机号
, license_plate_number as license_plate_number --车牌号
, mobile_phone as mobile_phone --手机号
, telephone_number as telephone_number --电话号码
, customer_name as customer_name --客户姓名
, brand_code as brand_code --品牌CD
, brand_name as brand_name --品牌名称
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, vehicle_model_code as vehicle_model_code --车型代码
, vehicle_model_name as vehicle_model_name --车型名称
, config_grade as config_grade --配置等级
, first_regist_date as first_regist_date --首次注册日期
, production_date as production_date --制造日期
, odometer as odometer --行驶里程
, exhaust as exhaust --排量
, fuel_type_code as fuel_type_code --燃料种类代码
, fuel_type_name as fuel_type_name --燃料种类名称
, vehicle_type_code as vehicle_type_code --车辆类型代码
, vehicle_category_name as vehicle_category_name --车辆类型名称
, gearbox_type as gearbox_type --变速箱类型
, gearbox_name as gearbox_name --变速箱名称
, gearbox_category as gearbox_category --变速箱分类
, gearbox_category_name as gearbox_category_name --变速箱分类名称
, exterior_color_code_1 as exterior_color_code_1 --外装色CD1
, exterior_color_1 as exterior_color_1 --外装色1
, exterior_color_code_2 as exterior_color_code_2 --外装色CD2
, exterior_color_2 as exterior_color_2 --外装色2
, ass_date as ass_date --评估日期
, detect_work_order_finish_date as detect_work_order_finish_date --检测工单完成日期
, sales_decision_time as sales_decision_time --销售决策时间
, ref_sales_price as ref_sales_price --参考销售价格
, plan_sales_price as plan_sales_price --计划销售价格
, ref_purchase_price as ref_purchase_price --参考采购价格
, plan_purchase_price as plan_purchase_price --计划采购价格
, sign_type as sign_type --签约类型
, sign_time as sign_time --签约时间
, cons_contract_serial as cons_contract_serial --寄售协议编号
, cons_contract_status as cons_contract_status --寄售协议状态
, cons_start_date as cons_start_date --寄售开始日期
, cons_end_date as cons_end_date --寄售结束日期
, cons_expect_price as cons_expect_price --寄售期望价格
, cons_type_code as cons_type_code --寄售类型代码
, cons_service_cost as cons_service_cost --寄售服务费用
, purchase_contract_id as purchase_contract_id --采购协议ID
, purchase_contract_status as purchase_contract_status --采购协议状态
, purchase_sign_date as purchase_sign_date --采购签约日期
, purchase_cancel_date as purchase_cancel_date --采购取消日期
, purchase_price as purchase_price --采购价格
, car_owner_name as car_owner_name --车主名称
, car_owner_id_card as car_owner_id_card --车主身份证号码
, car_owner_mobile_phone as car_owner_mobile_phone --车主电话
, car_owner_fixed_tel as car_owner_fixed_tel --车主固定电话
, sales_method as sales_method --销售方式
, storage_method as storage_method --入库方式
, storage_type as storage_type --入库类型
, elect_defect_flag as elect_defect_flag --机电缺陷标识
, vehicle_place_address as vehicle_place_address --车辆放置地址
, related_type as related_type --关联类型
, related_id as related_id --关联ID
, plan_storage_date as plan_storage_date --计划入库日期
, actual_storage_date as actual_storage_date --实际入库日期
, transfer_flag as transfer_flag --过户标识
, plan_transfer_date as plan_transfer_date --计划过户日期
, actual_transfer_date as actual_transfer_date --实际过户日期
, delivery_finish_date as delivery_finish_date --交车完成日期
, dealer_pay_transfer_cost as dealer_pay_transfer_cost --销售店承担过户费用
, dealer_new_car_owner_name as dealer_new_car_owner_name --销售店新车主姓名
, dealer_new_car_owner_id_card as dealer_new_car_owner_id_card --销售店新车主身份证号码
, dlr_new_car_owner_mobile_phone as dlr_new_car_owner_mobile_phone --销售店新车主手机号
, dealer_new_car_owner_address as dealer_new_car_owner_address --销售店新车主地址
, dlr_new_car_owner_lpn as dlr_new_car_owner_lpn --销售店新车主车牌号
, displace_related_operate_flag as displace_related_operate_flag --置换关联操作标识
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
from ods.ods_dms_uc_uc_inventory_ledger
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(dealer_name, '')) as dealer_name  --销售店名称
, trim(nvl(uc_stock_status, '')) as uc_stock_status  --二手车库存状态
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(engine_number, '')) as engine_number  --发动机号
, trim(nvl(license_plate_number, '')) as license_plate_number  --车牌号
, trim(nvl(mobile_phone, '')) as mobile_phone  --手机号
, trim(nvl(telephone_number, '')) as telephone_number  --电话号码
, trim(nvl(customer_name, '')) as customer_name  --客户姓名
, trim(nvl(brand_code, '')) as brand_code  --品牌CD
, trim(nvl(brand_name, '')) as brand_name  --品牌名称
, trim(nvl(maker_code, '')) as maker_code  --厂商代码
, trim(nvl(maker_name, '')) as maker_name  --厂商名称
, trim(nvl(vehicle_model_code, '')) as vehicle_model_code  --车型代码
, trim(nvl(vehicle_model_name, '')) as vehicle_model_name  --车型名称
, trim(nvl(config_grade, '')) as config_grade  --配置等级
, trim(nvl(first_regist_date, '')) as first_regist_date  --首次注册日期
, trim(nvl(production_date, '')) as production_date  --制造日期
, nvl(odometer, 0) as odometer  --行驶里程
, trim(nvl(exhaust, '')) as exhaust  --排量
, trim(nvl(fuel_type_code, '')) as fuel_type_code  --燃料种类代码
, trim(nvl(fuel_type_name, '')) as fuel_type_name  --燃料种类名称
, trim(nvl(vehicle_type_code, '')) as vehicle_type_code  --车辆类型代码
, trim(nvl(vehicle_category_name, '')) as vehicle_category_name  --车辆类型名称
, trim(nvl(gearbox_type, '')) as gearbox_type  --变速箱类型
, trim(nvl(gearbox_name, '')) as gearbox_name  --变速箱名称
, trim(nvl(gearbox_category, '')) as gearbox_category  --变速箱分类
, trim(nvl(gearbox_category_name, '')) as gearbox_category_name  --变速箱分类名称
, trim(nvl(exterior_color_code_1, '')) as exterior_color_code_1  --外装色CD1
, trim(nvl(exterior_color_1, '')) as exterior_color_1  --外装色1
, trim(nvl(exterior_color_code_2, '')) as exterior_color_code_2  --外装色CD2
, trim(nvl(exterior_color_2, '')) as exterior_color_2  --外装色2
, trim(nvl(ass_date, '')) as ass_date  --评估日期
, trim(nvl(detect_work_order_finish_date, '')) as detect_work_order_finish_date  --检测工单完成日期
, trim(nvl(sales_decision_time, '')) as sales_decision_time  --销售决策时间
, nvl(ref_sales_price, 0) as ref_sales_price  --参考销售价格
, nvl(plan_sales_price, 0) as plan_sales_price  --计划销售价格
, nvl(ref_purchase_price, 0) as ref_purchase_price  --参考采购价格
, nvl(plan_purchase_price, 0) as plan_purchase_price  --计划采购价格
, nvl(sign_type, 0) as sign_type  --签约类型
, trim(nvl(sign_time, '')) as sign_time  --签约时间
, trim(nvl(cons_contract_serial, '')) as cons_contract_serial  --寄售协议编号
, trim(nvl(cons_contract_status, '')) as cons_contract_status  --寄售协议状态
, trim(nvl(cons_start_date, '')) as cons_start_date  --寄售开始日期
, trim(nvl(cons_end_date, '')) as cons_end_date  --寄售结束日期
, nvl(cons_expect_price, 0) as cons_expect_price  --寄售期望价格
, trim(nvl(cons_type_code, '')) as cons_type_code  --寄售类型代码
, nvl(cons_service_cost, 0) as cons_service_cost  --寄售服务费用
, trim(nvl(purchase_contract_id, '')) as purchase_contract_id  --采购协议ID
, nvl(purchase_contract_status, 0) as purchase_contract_status  --采购协议状态
, trim(nvl(purchase_sign_date, '')) as purchase_sign_date  --采购签约日期
, trim(nvl(purchase_cancel_date, '')) as purchase_cancel_date  --采购取消日期
, nvl(purchase_price, 0) as purchase_price  --采购价格
, trim(nvl(car_owner_name, '')) as car_owner_name  --车主名称
, trim(nvl(car_owner_id_card, '')) as car_owner_id_card  --车主身份证号码
, trim(nvl(car_owner_mobile_phone, '')) as car_owner_mobile_phone  --车主电话
, trim(nvl(car_owner_fixed_tel, '')) as car_owner_fixed_tel  --车主固定电话
, trim(nvl(sales_method, '')) as sales_method  --销售方式
, nvl(storage_method, 0) as storage_method  --入库方式
, trim(nvl(storage_type, '')) as storage_type  --入库类型
, trim(nvl(elect_defect_flag, '')) as elect_defect_flag  --机电缺陷标识
, trim(nvl(vehicle_place_address, '')) as vehicle_place_address  --车辆放置地址
, trim(nvl(related_type, '')) as related_type  --关联类型
, trim(nvl(related_id, '')) as related_id  --关联ID
, trim(nvl(plan_storage_date, '')) as plan_storage_date  --计划入库日期
, trim(nvl(actual_storage_date, '')) as actual_storage_date  --实际入库日期
, trim(nvl(transfer_flag, '')) as transfer_flag  --过户标识
, trim(nvl(plan_transfer_date, '')) as plan_transfer_date  --计划过户日期
, trim(nvl(actual_transfer_date, '')) as actual_transfer_date  --实际过户日期
, trim(nvl(delivery_finish_date, '')) as delivery_finish_date  --交车完成日期
, nvl(dealer_pay_transfer_cost, 0) as dealer_pay_transfer_cost  --销售店承担过户费用
, trim(nvl(dealer_new_car_owner_name, '')) as dealer_new_car_owner_name  --销售店新车主姓名
, trim(nvl(dealer_new_car_owner_id_card, '')) as dealer_new_car_owner_id_card  --销售店新车主身份证号码
, trim(nvl(dlr_new_car_owner_mobile_phone, '')) as dlr_new_car_owner_mobile_phone  --销售店新车主手机号
, trim(nvl(dealer_new_car_owner_address, '')) as dealer_new_car_owner_address  --销售店新车主地址
, trim(nvl(dlr_new_car_owner_lpn, '')) as dlr_new_car_owner_lpn  --销售店新车主车牌号
, nvl(displace_related_operate_flag, 0) as displace_related_operate_flag  --置换关联操作标识
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
from dl.tg_dms_uc_t_uc_inventory_ledger
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_inventory_ledger
select  id as id --主键
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, dealer_name as dealer_name --销售店名称
, uc_stock_status as uc_stock_status --二手车库存状态
, vinno as vinno --车架号
, engine_number as engine_number --发动机号
, license_plate_number as license_plate_number --车牌号
, mobile_phone as mobile_phone --手机号
, telephone_number as telephone_number --电话号码
, customer_name as customer_name --客户姓名
, brand_code as brand_code --品牌CD
, brand_name as brand_name --品牌名称
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, vehicle_model_code as vehicle_model_code --车型代码
, vehicle_model_name as vehicle_model_name --车型名称
, config_grade as config_grade --配置等级
, first_regist_date as first_regist_date --首次注册日期
, production_date as production_date --制造日期
, odometer as odometer --行驶里程
, exhaust as exhaust --排量
, fuel_type_code as fuel_type_code --燃料种类代码
, fuel_type_name as fuel_type_name --燃料种类名称
, vehicle_type_code as vehicle_type_code --车辆类型代码
, vehicle_category_name as vehicle_category_name --车辆类型名称
, gearbox_type as gearbox_type --变速箱类型
, gearbox_name as gearbox_name --变速箱名称
, gearbox_category as gearbox_category --变速箱分类
, gearbox_category_name as gearbox_category_name --变速箱分类名称
, exterior_color_code_1 as exterior_color_code_1 --外装色CD1
, exterior_color_1 as exterior_color_1 --外装色1
, exterior_color_code_2 as exterior_color_code_2 --外装色CD2
, exterior_color_2 as exterior_color_2 --外装色2
, ass_date as ass_date --评估日期
, detect_work_order_finish_date as detect_work_order_finish_date --检测工单完成日期
, sales_decision_time as sales_decision_time --销售决策时间
, ref_sales_price as ref_sales_price --参考销售价格
, plan_sales_price as plan_sales_price --计划销售价格
, ref_purchase_price as ref_purchase_price --参考采购价格
, plan_purchase_price as plan_purchase_price --计划采购价格
, sign_type as sign_type --签约类型
, sign_time as sign_time --签约时间
, cons_contract_serial as cons_contract_serial --寄售协议编号
, cons_contract_status as cons_contract_status --寄售协议状态
, cons_start_date as cons_start_date --寄售开始日期
, cons_end_date as cons_end_date --寄售结束日期
, cons_expect_price as cons_expect_price --寄售期望价格
, cons_type_code as cons_type_code --寄售类型代码
, cons_service_cost as cons_service_cost --寄售服务费用
, purchase_contract_id as purchase_contract_id --采购协议ID
, purchase_contract_status as purchase_contract_status --采购协议状态
, purchase_sign_date as purchase_sign_date --采购签约日期
, purchase_cancel_date as purchase_cancel_date --采购取消日期
, purchase_price as purchase_price --采购价格
, car_owner_name as car_owner_name --车主名称
, car_owner_id_card as car_owner_id_card --车主身份证号码
, car_owner_mobile_phone as car_owner_mobile_phone --车主电话
, car_owner_fixed_tel as car_owner_fixed_tel --车主固定电话
, sales_method as sales_method --销售方式
, storage_method as storage_method --入库方式
, storage_type as storage_type --入库类型
, elect_defect_flag as elect_defect_flag --机电缺陷标识
, vehicle_place_address as vehicle_place_address --车辆放置地址
, related_type as related_type --关联类型
, related_id as related_id --关联ID
, plan_storage_date as plan_storage_date --计划入库日期
, actual_storage_date as actual_storage_date --实际入库日期
, transfer_flag as transfer_flag --过户标识
, plan_transfer_date as plan_transfer_date --计划过户日期
, actual_transfer_date as actual_transfer_date --实际过户日期
, delivery_finish_date as delivery_finish_date --交车完成日期
, dealer_pay_transfer_cost as dealer_pay_transfer_cost --销售店承担过户费用
, dealer_new_car_owner_name as dealer_new_car_owner_name --销售店新车主姓名
, dealer_new_car_owner_id_card as dealer_new_car_owner_id_card --销售店新车主身份证号码
, dlr_new_car_owner_mobile_phone as dlr_new_car_owner_mobile_phone --销售店新车主手机号
, dealer_new_car_owner_address as dealer_new_car_owner_address --销售店新车主地址
, dlr_new_car_owner_lpn as dlr_new_car_owner_lpn --销售店新车主车牌号
, displace_related_operate_flag as displace_related_operate_flag --置换关联操作标识
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
row_number() over (partition by dealer_code,vehicle_manage_no
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_inventory_ledger_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_inventory_ledger_transfer;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_inventory_ledger_transfer_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, plan_transfer_date as plan_transfer_date --计划过户日期
, actual_transfer_date as actual_transfer_date --实际过户日期
, dealer_pay_transfer_cost as dealer_pay_transfer_cost --销售店承担过户费用
, dealer_new_car_owner_name as dealer_new_car_owner_name --销售店新车主姓名
, dealer_new_car_owner_id_card as dealer_new_car_owner_id_card --销售店新车主身份证号码
, dlr_new_car_owner_mobile_phone as dlr_new_car_owner_mobile_phone --销售店新车主手机号
, dealer_new_car_owner_address as dealer_new_car_owner_address --销售店新车主地址
, dlr_new_car_owner_lpn as dlr_new_car_owner_lpn --销售店新车主车牌号
, cancel_time as cancel_time --取消时间
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
from ods.ods_dms_uc_uc_inventory_ledger_transfer
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(plan_transfer_date, '')) as plan_transfer_date  --计划过户日期
, trim(nvl(actual_transfer_date, '')) as actual_transfer_date  --实际过户日期
, nvl(dealer_pay_transfer_cost, 0) as dealer_pay_transfer_cost  --销售店承担过户费用
, trim(nvl(dealer_new_car_owner_name, '')) as dealer_new_car_owner_name  --销售店新车主姓名
, trim(nvl(dealer_new_car_owner_id_card, '')) as dealer_new_car_owner_id_card  --销售店新车主身份证号码
, trim(nvl(dlr_new_car_owner_mobile_phone, '')) as dlr_new_car_owner_mobile_phone  --销售店新车主手机号
, trim(nvl(dealer_new_car_owner_address, '')) as dealer_new_car_owner_address  --销售店新车主地址
, trim(nvl(dlr_new_car_owner_lpn, '')) as dlr_new_car_owner_lpn  --销售店新车主车牌号
, trim(nvl(cancel_time, '')) as cancel_time  --取消时间
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
from dl.tg_dms_uc_t_uc_inventory_ledger_transfer
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_inventory_ledger_transfer
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, plan_transfer_date as plan_transfer_date --计划过户日期
, actual_transfer_date as actual_transfer_date --实际过户日期
, dealer_pay_transfer_cost as dealer_pay_transfer_cost --销售店承担过户费用
, dealer_new_car_owner_name as dealer_new_car_owner_name --销售店新车主姓名
, dealer_new_car_owner_id_card as dealer_new_car_owner_id_card --销售店新车主身份证号码
, dlr_new_car_owner_mobile_phone as dlr_new_car_owner_mobile_phone --销售店新车主手机号
, dealer_new_car_owner_address as dealer_new_car_owner_address --销售店新车主地址
, dlr_new_car_owner_lpn as dlr_new_car_owner_lpn --销售店新车主车牌号
, cancel_time as cancel_time --取消时间
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
row_number() over (partition by vehicle_manage_no,dealer_code
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_inventory_ledger_transfer_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_mst_dealer;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_mst_dealer_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, dealer_name as dealer_name --销售店名称
, dealer_region as dealer_region --销售店区域
, dealer_address as dealer_address --销售店地址
, dealer_contact_tel as dealer_contact_tel --销售店联系电话
, dealer_website as dealer_website --销售店主页
, dealer_picture_url as dealer_picture_url --销售店图片
, dealer_fax as dealer_fax --销售店传真
, uc_dept_staff_telephone as uc_dept_staff_telephone --二手车部担当电话
, business_start_time as business_start_time --营业开始时间
, business_end_time as business_end_time --营业结束时间
, dealer_theme_slogan as dealer_theme_slogan --店的主题口号
, map_img as map_img --地图图片
, region_code as region_code --区域代码
, region_name as region_name --区域名称
, province_code as province_code --省代码
, province_name as province_name --省份名称
, city_code as city_code --市代码
, city_name as city_name --市名称
, ass_hotline as ass_hotline --评估热线
, sales_hotline as sales_hotline --销售热线
, dlr_image_display as dlr_image_display --销售店显示图片
, dealer_thumbnail_img as dealer_thumbnail_img --销售店缩略图片
, map_display_img as map_display_img --地图显示图片
, map_thumbnail_img as map_thumbnail_img --地图缩略图片
, contacts_account as contacts_account --联系人账号
, dealer_prefix as dealer_prefix --销售店前缀
, shop_enter_flag as shop_enter_flag --商城入驻标识
, displace_cert_flag as displace_cert_flag --置换资格标识
, displace_cert_effective_date as displace_cert_effective_date --置换资格生效日期
, displace_cert_effective_flag as displace_cert_effective_flag --置换资格生效标识
, retail_cert_flag as retail_cert_flag --零售资格标识
, retail_cert_effective_date as retail_cert_effective_date --零售资格生效日期
, retail_cert_effective_flag as retail_cert_effective_flag --零售资格生效标识
, after_sales_service_flag as after_sales_service_flag --售后服务标识
, authen_service_flag as authen_service_flag --认证服务标识
, authen_service_contact_account as authen_service_contact_account --认证服务联系账号
, authen_service_contact_name as authen_service_contact_name --认证服务联系姓名
, authen_service_contact_telephone as authen_service_contact_telephone --认证服务联系电话
, authen_service_cost as authen_service_cost --认证服务费用
, maker_suggest_service_cost as maker_suggest_service_cost --厂商建议服务费用
, authen_service_remark as authen_service_remark --认证服务备注
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
from ods.ods_dms_uc_uc_mst_dealer
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(dealer_name, '')) as dealer_name  --销售店名称
, trim(nvl(dealer_region, '')) as dealer_region  --销售店区域
, trim(nvl(dealer_address, '')) as dealer_address  --销售店地址
, trim(nvl(dealer_contact_tel, '')) as dealer_contact_tel  --销售店联系电话
, trim(nvl(dealer_website, '')) as dealer_website  --销售店主页
, trim(nvl(dealer_picture_url, '')) as dealer_picture_url  --销售店图片
, trim(nvl(dealer_fax, '')) as dealer_fax  --销售店传真
, trim(nvl(uc_dept_staff_telephone, '')) as uc_dept_staff_telephone  --二手车部担当电话
, trim(nvl(business_start_time, '')) as business_start_time  --营业开始时间
, trim(nvl(business_end_time, '')) as business_end_time  --营业结束时间
, trim(nvl(dealer_theme_slogan, '')) as dealer_theme_slogan  --店的主题口号
, trim(nvl(map_img, '')) as map_img  --地图图片
, trim(nvl(region_code, '')) as region_code  --区域代码
, trim(nvl(region_name, '')) as region_name  --区域名称
, trim(nvl(province_code, '')) as province_code  --省代码
, trim(nvl(province_name, '')) as province_name  --省份名称
, trim(nvl(city_code, '')) as city_code  --市代码
, trim(nvl(city_name, '')) as city_name  --市名称
, trim(nvl(ass_hotline, '')) as ass_hotline  --评估热线
, trim(nvl(sales_hotline, '')) as sales_hotline  --销售热线
, trim(nvl(dlr_image_display, '')) as dlr_image_display  --销售店显示图片
, trim(nvl(dealer_thumbnail_img, '')) as dealer_thumbnail_img  --销售店缩略图片
, trim(nvl(map_display_img, '')) as map_display_img  --地图显示图片
, trim(nvl(map_thumbnail_img, '')) as map_thumbnail_img  --地图缩略图片
, trim(nvl(contacts_account, '')) as contacts_account  --联系人账号
, trim(nvl(dealer_prefix, '')) as dealer_prefix  --销售店前缀
, trim(nvl(shop_enter_flag, '')) as shop_enter_flag  --商城入驻标识
, trim(nvl(displace_cert_flag, '')) as displace_cert_flag  --置换资格标识
, trim(nvl(displace_cert_effective_date, '')) as displace_cert_effective_date  --置换资格生效日期
, trim(nvl(displace_cert_effective_flag, '')) as displace_cert_effective_flag  --置换资格生效标识
, trim(nvl(retail_cert_flag, '')) as retail_cert_flag  --零售资格标识
, trim(nvl(retail_cert_effective_date, '')) as retail_cert_effective_date  --零售资格生效日期
, trim(nvl(retail_cert_effective_flag, '')) as retail_cert_effective_flag  --零售资格生效标识
, trim(nvl(after_sales_service_flag, '')) as after_sales_service_flag  --售后服务标识
, trim(nvl(authen_service_flag, '')) as authen_service_flag  --认证服务标识
, trim(nvl(authen_service_contact_account, '')) as authen_service_contact_account  --认证服务联系账号
, trim(nvl(authen_service_contact_name, '')) as authen_service_contact_name  --认证服务联系姓名
, trim(nvl(authen_service_contact_telephone, '')) as authen_service_contact_telephone  --认证服务联系电话
, nvl(authen_service_cost, 0) as authen_service_cost  --认证服务费用
, nvl(maker_suggest_service_cost, 0) as maker_suggest_service_cost  --厂商建议服务费用
, trim(nvl(authen_service_remark, '')) as authen_service_remark  --认证服务备注
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
from dl.tg_dms_uc_t_uc_mst_dealer
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_mst_dealer
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, dealer_name as dealer_name --销售店名称
, dealer_region as dealer_region --销售店区域
, dealer_address as dealer_address --销售店地址
, dealer_contact_tel as dealer_contact_tel --销售店联系电话
, dealer_website as dealer_website --销售店主页
, dealer_picture_url as dealer_picture_url --销售店图片
, dealer_fax as dealer_fax --销售店传真
, uc_dept_staff_telephone as uc_dept_staff_telephone --二手车部担当电话
, business_start_time as business_start_time --营业开始时间
, business_end_time as business_end_time --营业结束时间
, dealer_theme_slogan as dealer_theme_slogan --店的主题口号
, map_img as map_img --地图图片
, region_code as region_code --区域代码
, region_name as region_name --区域名称
, province_code as province_code --省代码
, province_name as province_name --省份名称
, city_code as city_code --市代码
, city_name as city_name --市名称
, ass_hotline as ass_hotline --评估热线
, sales_hotline as sales_hotline --销售热线
, dlr_image_display as dlr_image_display --销售店显示图片
, dealer_thumbnail_img as dealer_thumbnail_img --销售店缩略图片
, map_display_img as map_display_img --地图显示图片
, map_thumbnail_img as map_thumbnail_img --地图缩略图片
, contacts_account as contacts_account --联系人账号
, dealer_prefix as dealer_prefix --销售店前缀
, shop_enter_flag as shop_enter_flag --商城入驻标识
, displace_cert_flag as displace_cert_flag --置换资格标识
, displace_cert_effective_date as displace_cert_effective_date --置换资格生效日期
, displace_cert_effective_flag as displace_cert_effective_flag --置换资格生效标识
, retail_cert_flag as retail_cert_flag --零售资格标识
, retail_cert_effective_date as retail_cert_effective_date --零售资格生效日期
, retail_cert_effective_flag as retail_cert_effective_flag --零售资格生效标识
, after_sales_service_flag as after_sales_service_flag --售后服务标识
, authen_service_flag as authen_service_flag --认证服务标识
, authen_service_contact_account as authen_service_contact_account --认证服务联系账号
, authen_service_contact_name as authen_service_contact_name --认证服务联系姓名
, authen_service_contact_telephone as authen_service_contact_telephone --认证服务联系电话
, authen_service_cost as authen_service_cost --认证服务费用
, maker_suggest_service_cost as maker_suggest_service_cost --厂商建议服务费用
, authen_service_remark as authen_service_remark --认证服务备注
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
row_number() over (partition by dealer_code
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_mst_dealer_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_mst_elect_defect;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_mst_elect_defect_tmp as (
select  id as id --主键
, detect_position_type as detect_position_type --检测部位类型
, detect_position_group_code as detect_position_group_code --检测部位组代码
, detect_position_code as detect_position_code --检测部位代码
, sort as sort --排序
, defect_item_name as defect_item_name --缺陷项名称
, defect_item_name_local as defect_item_name_local --缺陷项名称本地
, defect_item_name_2 as defect_item_name_2 --缺陷项名称2
, defect_item_name_2_local as defect_item_name_2_local --缺陷项名称2本地
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
from ods.ods_dms_uc_uc_mst_elect_defect
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(detect_position_type, '')) as detect_position_type  --检测部位类型
, trim(nvl(detect_position_group_code, '')) as detect_position_group_code  --检测部位组代码
, trim(nvl(detect_position_code, '')) as detect_position_code  --检测部位代码
, nvl(sort, 0) as sort  --排序
, trim(nvl(defect_item_name, '')) as defect_item_name  --缺陷项名称
, trim(nvl(defect_item_name_local, '')) as defect_item_name_local  --缺陷项名称本地
, trim(nvl(defect_item_name_2, '')) as defect_item_name_2  --缺陷项名称2
, trim(nvl(defect_item_name_2_local, '')) as defect_item_name_2_local  --缺陷项名称2本地
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
from dl.tg_dms_uc_t_uc_mst_elect_defect
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_mst_elect_defect
select  id as id --主键
, detect_position_type as detect_position_type --检测部位类型
, detect_position_group_code as detect_position_group_code --检测部位组代码
, detect_position_code as detect_position_code --检测部位代码
, sort as sort --排序
, defect_item_name as defect_item_name --缺陷项名称
, defect_item_name_local as defect_item_name_local --缺陷项名称本地
, defect_item_name_2 as defect_item_name_2 --缺陷项名称2
, defect_item_name_2_local as defect_item_name_2_local --缺陷项名称2本地
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
row_number() over (partition by detect_position_type,detect_position_group_code,detect_position_code
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_mst_elect_defect_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_negotiation;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_negotiation_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, negotiation_id as negotiation_id --商谈ID
, customer_id as customer_id --客户ID
, customer_name as customer_name --客户姓名
, negotiation_customer_address as negotiation_customer_address --商谈客户地址
, customer_mobile_phone as customer_mobile_phone --客户移动电话
, customer_fixed_tel as customer_fixed_tel --客户固定电话
, customer_postal_code as customer_postal_code --客户邮编
, activity_manager_account as activity_manager_account --活动管理者账号
, activity_manager_name as activity_manager_name --活动管理者姓名
, source_code as source_code --来源代码
, source_name as source_name --来源名称
, source_first_level_code as source_first_level_code --一级来源代码
, source_first_level_name as source_first_level_name --一级来源名称
, source_second_level_code as source_second_level_code --二级来源代码
, source_second_level_name as source_second_level_name --二级来源名称
, introducer_dept_code as introducer_dept_code --介绍人部门代码
, introducer_dept_name as introducer_dept_name --介绍人部门名称
, introducer_account as introducer_account --介绍人账号
, introducer_name as introducer_name --介绍人姓名
, intn_level as intn_level --意向级别
, negotiation_status as negotiation_status --商谈状态
, manager_dist_flag as manager_dist_flag --管理者分配标识
, uc_req_id as uc_req_id --二手车需求ID
, uc_clue_id as uc_clue_id --二手车线索ID
, clue_create_time as clue_create_time --线索创建时间
, follow_account as follow_account --跟进账号
, follow_staff_name as follow_staff_name --跟进担当姓名
, follow_time as follow_time --跟进时间
, next_follow_time as next_follow_time --下次跟进时间
, business_diff as business_diff --业务区分
, intn_type as intn_type --意向类型
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, cover_thumbnail_img_url as cover_thumbnail_img_url --封面缩略图片URL
, brand_code as brand_code --品牌CD
, brand_name as brand_name --品牌名称
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, vehicle_model_code as vehicle_model_code --车型代码
, vehicle_model_name as vehicle_model_name --车型名称
, vehicle_grade as vehicle_grade --车辆等级
, license_plate_number as license_plate_number --车牌号
, vinno as vinno --车架号
, first_regist_date as first_regist_date --首次注册日期
, ass_date as ass_date --评估日期
, purchase_sign_date as purchase_sign_date --采购签约日期
, cons_sign_date as cons_sign_date --寄售签约日期
, min_down_payment as min_down_payment --最低首付
, max_down_payment as max_down_payment --最高首付
, min_monthly_payment as min_monthly_payment --最低月供
, max_monthly_payment as max_monthly_payment --最高月供
, sales_sign_date as sales_sign_date --销售签约日期
, sales_negotiation_id as sales_negotiation_id --销售商谈ID
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
from ods.ods_dms_uc_uc_negotiation
union all
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
from dl.tg_dms_uc_t_uc_negotiation
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_negotiation
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, negotiation_id as negotiation_id --商谈ID
, customer_id as customer_id --客户ID
, customer_name as customer_name --客户姓名
, negotiation_customer_address as negotiation_customer_address --商谈客户地址
, customer_mobile_phone as customer_mobile_phone --客户移动电话
, customer_fixed_tel as customer_fixed_tel --客户固定电话
, customer_postal_code as customer_postal_code --客户邮编
, activity_manager_account as activity_manager_account --活动管理者账号
, activity_manager_name as activity_manager_name --活动管理者姓名
, source_code as source_code --来源代码
, source_name as source_name --来源名称
, source_first_level_code as source_first_level_code --一级来源代码
, source_first_level_name as source_first_level_name --一级来源名称
, source_second_level_code as source_second_level_code --二级来源代码
, source_second_level_name as source_second_level_name --二级来源名称
, introducer_dept_code as introducer_dept_code --介绍人部门代码
, introducer_dept_name as introducer_dept_name --介绍人部门名称
, introducer_account as introducer_account --介绍人账号
, introducer_name as introducer_name --介绍人姓名
, intn_level as intn_level --意向级别
, negotiation_status as negotiation_status --商谈状态
, manager_dist_flag as manager_dist_flag --管理者分配标识
, uc_req_id as uc_req_id --二手车需求ID
, uc_clue_id as uc_clue_id --二手车线索ID
, clue_create_time as clue_create_time --线索创建时间
, follow_account as follow_account --跟进账号
, follow_staff_name as follow_staff_name --跟进担当姓名
, follow_time as follow_time --跟进时间
, next_follow_time as next_follow_time --下次跟进时间
, business_diff as business_diff --业务区分
, intn_type as intn_type --意向类型
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, cover_thumbnail_img_url as cover_thumbnail_img_url --封面缩略图片URL
, brand_code as brand_code --品牌CD
, brand_name as brand_name --品牌名称
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, vehicle_model_code as vehicle_model_code --车型代码
, vehicle_model_name as vehicle_model_name --车型名称
, vehicle_grade as vehicle_grade --车辆等级
, license_plate_number as license_plate_number --车牌号
, vinno as vinno --车架号
, first_regist_date as first_regist_date --首次注册日期
, ass_date as ass_date --评估日期
, purchase_sign_date as purchase_sign_date --采购签约日期
, cons_sign_date as cons_sign_date --寄售签约日期
, min_down_payment as min_down_payment --最低首付
, max_down_payment as max_down_payment --最高首付
, min_monthly_payment as min_monthly_payment --最低月供
, max_monthly_payment as max_monthly_payment --最高月供
, sales_sign_date as sales_sign_date --销售签约日期
, sales_negotiation_id as sales_negotiation_id --销售商谈ID
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
row_number() over (partition by dealer_code,negotiation_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_negotiation_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_new_vhcl_exchange_achv;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_new_vhcl_exchange_achv_tmp as (
select  id as id --主键
, new_vehicle_displace_achv_id as new_vehicle_displace_achv_id --新车置换业绩ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, license_plate_number as license_plate_number --车牌号
, achv_apply_status as achv_apply_status --业绩申请状态
, present_type as present_type --赠品类型
, displace_vehicle_related_id as displace_vehicle_related_id --置换车辆关联ID
, displace_vehicle_vinno as displace_vehicle_vinno --置换车辆车架号
, displace_vehicle_nominee_name as displace_vehicle_nominee_name --置换车辆名义人姓名
, displace_vehicle_nominee_tel as displace_vehicle_nominee_tel --置换车辆名义人电话
, displace_vehicle_sales_date as displace_vehicle_sales_date --置换车辆销售日期
, displace_vehicle_picture_url as displace_vehicle_picture_url --置换车辆图片路径
, displace_vehicle_config_grade as displace_vehicle_config_grade --置换车辆配置等级
, displace_vehicle_model_code as displace_vehicle_model_code --置换车型代码
, displace_vehicle_model_name as displace_vehicle_model_name --置换车型名字
, purchase_obtain_evidence_id as purchase_obtain_evidence_id --采购取证ID
, invoice_number as invoice_number --发票号
, invoice_amount as invoice_amount --发票金额
, actual_transfer_date as actual_transfer_date --实际过户日期
, seller_name as seller_name --卖方姓名
, seller_id_card as seller_id_card --卖方身份证
, buyer_name as buyer_name --买主名
, buyer_id_card as buyer_id_card --买方身份证
, apply_time as apply_time --申请时间
, apply_staff_account as apply_staff_account --申请担当账号
, apply_staff_name as apply_staff_name --申请担当姓名
, approve_time as approve_time --审批时间
, approve_staff_account as approve_staff_account --审核担当账号
, approve_staff_name as approve_staff_name --审核担当姓名
, approve_opinion as approve_opinion --审批意见
, cancel_time as cancel_time --取消时间
, cancel_staff_account as cancel_staff_account --取消担当账号
, cancel_staff_name as cancel_staff_name --取消担当姓名
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
from ods.ods_dms_uc_uc_new_vhcl_exchange_achv
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(new_vehicle_displace_achv_id, '')) as new_vehicle_displace_achv_id  --新车置换业绩ID
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(license_plate_number, '')) as license_plate_number  --车牌号
, trim(nvl(achv_apply_status, '')) as achv_apply_status  --业绩申请状态
, trim(nvl(present_type, '')) as present_type  --赠品类型
, trim(nvl(displace_vehicle_related_id, '')) as displace_vehicle_related_id  --置换车辆关联ID
, trim(nvl(displace_vehicle_vinno, '')) as displace_vehicle_vinno  --置换车辆车架号
, trim(nvl(displace_vehicle_nominee_name, '')) as displace_vehicle_nominee_name  --置换车辆名义人姓名
, trim(nvl(displace_vehicle_nominee_tel, '')) as displace_vehicle_nominee_tel  --置换车辆名义人电话
, trim(nvl(displace_vehicle_sales_date, '')) as displace_vehicle_sales_date  --置换车辆销售日期
, trim(nvl(displace_vehicle_picture_url, '')) as displace_vehicle_picture_url  --置换车辆图片路径
, trim(nvl(displace_vehicle_config_grade, '')) as displace_vehicle_config_grade  --置换车辆配置等级
, trim(nvl(displace_vehicle_model_code, '')) as displace_vehicle_model_code  --置换车型代码
, trim(nvl(displace_vehicle_model_name, '')) as displace_vehicle_model_name  --置换车型名字
, trim(nvl(purchase_obtain_evidence_id, '')) as purchase_obtain_evidence_id  --采购取证ID
, trim(nvl(invoice_number, '')) as invoice_number  --发票号
, nvl(invoice_amount, 0) as invoice_amount  --发票金额
, trim(nvl(actual_transfer_date, '')) as actual_transfer_date  --实际过户日期
, trim(nvl(seller_name, '')) as seller_name  --卖方姓名
, trim(nvl(seller_id_card, '')) as seller_id_card  --卖方身份证
, trim(nvl(buyer_name, '')) as buyer_name  --买主名
, trim(nvl(buyer_id_card, '')) as buyer_id_card  --买方身份证
, trim(nvl(apply_time, '')) as apply_time  --申请时间
, trim(nvl(apply_staff_account, '')) as apply_staff_account  --申请担当账号
, trim(nvl(apply_staff_name, '')) as apply_staff_name  --申请担当姓名
, trim(nvl(approve_time, '')) as approve_time  --审批时间
, trim(nvl(approve_staff_account, '')) as approve_staff_account  --审核担当账号
, trim(nvl(approve_staff_name, '')) as approve_staff_name  --审核担当姓名
, trim(nvl(approve_opinion, '')) as approve_opinion  --审批意见
, trim(nvl(cancel_time, '')) as cancel_time  --取消时间
, trim(nvl(cancel_staff_account, '')) as cancel_staff_account  --取消担当账号
, trim(nvl(cancel_staff_name, '')) as cancel_staff_name  --取消担当姓名
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
from dl.tg_dms_uc_t_uc_new_vhcl_exchange_achv
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_new_vhcl_exchange_achv
select  id as id --主键
, new_vehicle_displace_achv_id as new_vehicle_displace_achv_id --新车置换业绩ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, license_plate_number as license_plate_number --车牌号
, achv_apply_status as achv_apply_status --业绩申请状态
, present_type as present_type --赠品类型
, displace_vehicle_related_id as displace_vehicle_related_id --置换车辆关联ID
, displace_vehicle_vinno as displace_vehicle_vinno --置换车辆车架号
, displace_vehicle_nominee_name as displace_vehicle_nominee_name --置换车辆名义人姓名
, displace_vehicle_nominee_tel as displace_vehicle_nominee_tel --置换车辆名义人电话
, displace_vehicle_sales_date as displace_vehicle_sales_date --置换车辆销售日期
, displace_vehicle_picture_url as displace_vehicle_picture_url --置换车辆图片路径
, displace_vehicle_config_grade as displace_vehicle_config_grade --置换车辆配置等级
, displace_vehicle_model_code as displace_vehicle_model_code --置换车型代码
, displace_vehicle_model_name as displace_vehicle_model_name --置换车型名字
, purchase_obtain_evidence_id as purchase_obtain_evidence_id --采购取证ID
, invoice_number as invoice_number --发票号
, invoice_amount as invoice_amount --发票金额
, actual_transfer_date as actual_transfer_date --实际过户日期
, seller_name as seller_name --卖方姓名
, seller_id_card as seller_id_card --卖方身份证
, buyer_name as buyer_name --买主名
, buyer_id_card as buyer_id_card --买方身份证
, apply_time as apply_time --申请时间
, apply_staff_account as apply_staff_account --申请担当账号
, apply_staff_name as apply_staff_name --申请担当姓名
, approve_time as approve_time --审批时间
, approve_staff_account as approve_staff_account --审核担当账号
, approve_staff_name as approve_staff_name --审核担当姓名
, approve_opinion as approve_opinion --审批意见
, cancel_time as cancel_time --取消时间
, cancel_staff_account as cancel_staff_account --取消担当账号
, cancel_staff_name as cancel_staff_name --取消担当姓名
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
row_number() over (partition by new_vehicle_displace_achv_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_new_vhcl_exchange_achv_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_purchase_contract_ledger;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_purchase_contract_ledger_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, purchase_contract_id as purchase_contract_id --采购协议ID
, vinno as vinno --车架号
, car_owner_type as car_owner_type --车主类型
, car_owner_name as car_owner_name --车主名称
, car_owner_id_card as car_owner_id_card --车主身份证号码
, car_owner_mobile_phone as car_owner_mobile_phone --车主电话
, car_owner_tel as car_owner_tel --车主固定电话
, car_owner_zip as car_owner_zip --车主邮编
, car_owner_address as car_owner_address --车主住址
, car_owner_email as car_owner_email --车主邮箱
, license_plate_number as license_plate_number --车牌号
, purchase_sign_date as purchase_sign_date --采购签约日期
, purchase_cancel_date as purchase_cancel_date --采购取消日期
, cons_sign_date as cons_sign_date --寄售签约日期
, purchase_price as purchase_price --采购价格
, plan_sales_method as plan_sales_method --计划销售方式
, whsle_target_code as whsle_target_code --批售对象代码
, whsle_target_type as whsle_target_type --批售对象类型
, plan_storage_date as plan_storage_date --计划入库日期
, plan_transfer_date as plan_transfer_date --计划过户日期
, purchase_contract_status as purchase_contract_status --采购协议状态
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
from ods.ods_dms_uc_uc_purchase_contract_ledger
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(purchase_contract_id, '')) as purchase_contract_id  --采购协议ID
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(car_owner_type, '')) as car_owner_type  --车主类型
, trim(nvl(car_owner_name, '')) as car_owner_name  --车主名称
, trim(nvl(car_owner_id_card, '')) as car_owner_id_card  --车主身份证号码
, trim(nvl(car_owner_mobile_phone, '')) as car_owner_mobile_phone  --车主电话
, trim(nvl(car_owner_tel, '')) as car_owner_tel  --车主固定电话
, trim(nvl(car_owner_zip, '')) as car_owner_zip  --车主邮编
, trim(nvl(car_owner_address, '')) as car_owner_address  --车主住址
, trim(nvl(car_owner_email, '')) as car_owner_email  --车主邮箱
, trim(nvl(license_plate_number, '')) as license_plate_number  --车牌号
, trim(nvl(purchase_sign_date, '')) as purchase_sign_date  --采购签约日期
, trim(nvl(purchase_cancel_date, '')) as purchase_cancel_date  --采购取消日期
, trim(nvl(cons_sign_date, '')) as cons_sign_date  --寄售签约日期
, nvl(purchase_price, 0) as purchase_price  --采购价格
, trim(nvl(plan_sales_method, '')) as plan_sales_method  --计划销售方式
, trim(nvl(whsle_target_code, '')) as whsle_target_code  --批售对象代码
, trim(nvl(whsle_target_type, '')) as whsle_target_type  --批售对象类型
, trim(nvl(plan_storage_date, '')) as plan_storage_date  --计划入库日期
, trim(nvl(plan_transfer_date, '')) as plan_transfer_date  --计划过户日期
, nvl(purchase_contract_status, 0) as purchase_contract_status  --采购协议状态
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
from dl.tg_dms_uc_t_uc_purchase_contract_ledger
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_purchase_contract_ledger
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, purchase_contract_id as purchase_contract_id --采购协议ID
, vinno as vinno --车架号
, car_owner_type as car_owner_type --车主类型
, car_owner_name as car_owner_name --车主名称
, car_owner_id_card as car_owner_id_card --车主身份证号码
, car_owner_mobile_phone as car_owner_mobile_phone --车主电话
, car_owner_tel as car_owner_tel --车主固定电话
, car_owner_zip as car_owner_zip --车主邮编
, car_owner_address as car_owner_address --车主住址
, car_owner_email as car_owner_email --车主邮箱
, license_plate_number as license_plate_number --车牌号
, purchase_sign_date as purchase_sign_date --采购签约日期
, purchase_cancel_date as purchase_cancel_date --采购取消日期
, cons_sign_date as cons_sign_date --寄售签约日期
, purchase_price as purchase_price --采购价格
, plan_sales_method as plan_sales_method --计划销售方式
, whsle_target_code as whsle_target_code --批售对象代码
, whsle_target_type as whsle_target_type --批售对象类型
, plan_storage_date as plan_storage_date --计划入库日期
, plan_transfer_date as plan_transfer_date --计划过户日期
, purchase_contract_status as purchase_contract_status --采购协议状态
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
row_number() over (partition by dealer_code,vehicle_manage_no
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_purchase_contract_ledger_tmp) a
where rn = 1;



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



set mapred.job.name=job_ods_dms_uc_uc_repair_authen_ledger;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_repair_authen_ledger_tmp as (
select  id as id --主键
, repair_authen_id as repair_authen_id --整备认证ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, ledger_status as ledger_status --台账状态
, uc_auth_due_date as uc_auth_due_date --二手车认证到期日
, uc_auth_time as uc_auth_time --二手车认证生效时间
, authen_apply_type_code as authen_apply_type_code --认证申请类型CD
, authen_type as authen_type --认证类型
, auth_apply_time as auth_apply_time --认证申请时间
, auth_apply_by_code as auth_apply_by_code --认证申请人代码
, uc_auth_no as uc_auth_no --二手车认证编号
, gtmc_vehicle_flag as gtmc_vehicle_flag --广汽丰田车辆标识
, delivery_finish_date as delivery_finish_date --交车完成日期
, sales_method as sales_method --销售方式
, first_regist_date as first_regist_date --首次注册日期
, ass_date as ass_date --评估日期
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, odometer as odometer --行驶里程
, vehicle_type_code as vehicle_type_code --车辆类型代码
, vehicle_category_name as vehicle_category_name --车辆类型名称
, vehicle_grade as vehicle_grade --车辆等级
, collision_accident_diff as collision_accident_diff --碰撞事故区分
, water_accident_diff as water_accident_diff --水浸事故区分
, fire_accident_diff as fire_accident_diff --火烧事故区分
, excessive_modified_diff as excessive_modified_diff --过度改装区分
, wty_type as wty_type --三包类型
, instrument_tampering_flag as instrument_tampering_flag --仪表篡改标识
, elect_level as elect_level --机电级别
, accident_level as accident_level --事故级别
, exhaust as exhaust --排量
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
from ods.ods_dms_uc_uc_repair_authen_ledger
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(repair_authen_id, '')) as repair_authen_id  --整备认证ID
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(ledger_status, '')) as ledger_status  --台账状态
, trim(nvl(uc_auth_due_date, '')) as uc_auth_due_date  --二手车认证到期日
, trim(nvl(uc_auth_time, '')) as uc_auth_time  --二手车认证生效时间
, trim(nvl(authen_apply_type_code, '')) as authen_apply_type_code  --认证申请类型CD
, trim(nvl(authen_type, '')) as authen_type  --认证类型
, trim(nvl(auth_apply_time, '')) as auth_apply_time  --认证申请时间
, trim(nvl(auth_apply_by_code, '')) as auth_apply_by_code  --认证申请人代码
, trim(nvl(uc_auth_no, '')) as uc_auth_no  --二手车认证编号
, trim(nvl(gtmc_vehicle_flag, '')) as gtmc_vehicle_flag  --广汽丰田车辆标识
, trim(nvl(delivery_finish_date, '')) as delivery_finish_date  --交车完成日期
, trim(nvl(sales_method, '')) as sales_method  --销售方式
, trim(nvl(first_regist_date, '')) as first_regist_date  --首次注册日期
, trim(nvl(ass_date, '')) as ass_date  --评估日期
, trim(nvl(maker_code, '')) as maker_code  --厂商代码
, trim(nvl(maker_name, '')) as maker_name  --厂商名称
, nvl(odometer, 0) as odometer  --行驶里程
, trim(nvl(vehicle_type_code, '')) as vehicle_type_code  --车辆类型代码
, trim(nvl(vehicle_category_name, '')) as vehicle_category_name  --车辆类型名称
, trim(nvl(vehicle_grade, '')) as vehicle_grade  --车辆等级
, trim(nvl(collision_accident_diff, '')) as collision_accident_diff  --碰撞事故区分
, trim(nvl(water_accident_diff, '')) as water_accident_diff  --水浸事故区分
, trim(nvl(fire_accident_diff, '')) as fire_accident_diff  --火烧事故区分
, trim(nvl(excessive_modified_diff, '')) as excessive_modified_diff  --过度改装区分
, trim(nvl(wty_type, '')) as wty_type  --三包类型
, trim(nvl(instrument_tampering_flag, '')) as instrument_tampering_flag  --仪表篡改标识
, trim(nvl(elect_level, '')) as elect_level  --机电级别
, trim(nvl(accident_level, '')) as accident_level  --事故级别
, trim(nvl(exhaust, '')) as exhaust  --排量
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
from dl.tg_dms_uc_t_uc_repair_authen_ledger
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_repair_authen_ledger
select  id as id --主键
, repair_authen_id as repair_authen_id --整备认证ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, ledger_status as ledger_status --台账状态
, uc_auth_due_date as uc_auth_due_date --二手车认证到期日
, uc_auth_time as uc_auth_time --二手车认证生效时间
, authen_apply_type_code as authen_apply_type_code --认证申请类型CD
, authen_type as authen_type --认证类型
, auth_apply_time as auth_apply_time --认证申请时间
, auth_apply_by_code as auth_apply_by_code --认证申请人代码
, uc_auth_no as uc_auth_no --二手车认证编号
, gtmc_vehicle_flag as gtmc_vehicle_flag --广汽丰田车辆标识
, delivery_finish_date as delivery_finish_date --交车完成日期
, sales_method as sales_method --销售方式
, first_regist_date as first_regist_date --首次注册日期
, ass_date as ass_date --评估日期
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, odometer as odometer --行驶里程
, vehicle_type_code as vehicle_type_code --车辆类型代码
, vehicle_category_name as vehicle_category_name --车辆类型名称
, vehicle_grade as vehicle_grade --车辆等级
, collision_accident_diff as collision_accident_diff --碰撞事故区分
, water_accident_diff as water_accident_diff --水浸事故区分
, fire_accident_diff as fire_accident_diff --火烧事故区分
, excessive_modified_diff as excessive_modified_diff --过度改装区分
, wty_type as wty_type --三包类型
, instrument_tampering_flag as instrument_tampering_flag --仪表篡改标识
, elect_level as elect_level --机电级别
, accident_level as accident_level --事故级别
, exhaust as exhaust --排量
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
row_number() over (partition by repair_authen_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_repair_authen_ledger_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_repair_cost;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_repair_cost_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, repair_id as repair_id --整备ID
, repair_item_id as repair_item_id --整备项ID
, project_source as project_source --项目来源
, repair_item_type as repair_item_type --整备项类型
, repair_item_type_name as repair_item_type_name --整备项类型名称
, repair_item_code as repair_item_code --整备项代码
, repair_item_name as repair_item_name --整备项名称
, repair_item_cost as repair_item_cost --整备项费用
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
from ods.ods_dms_uc_uc_repair_cost
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(repair_id, '')) as repair_id  --整备ID
, trim(nvl(repair_item_id, '')) as repair_item_id  --整备项ID
, nvl(project_source, 0) as project_source  --项目来源
, trim(nvl(repair_item_type, '')) as repair_item_type  --整备项类型
, trim(nvl(repair_item_type_name, '')) as repair_item_type_name  --整备项类型名称
, trim(nvl(repair_item_code, '')) as repair_item_code  --整备项代码
, trim(nvl(repair_item_name, '')) as repair_item_name  --整备项名称
, nvl(repair_item_cost, 0) as repair_item_cost  --整备项费用
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
from dl.tg_dms_uc_t_uc_repair_cost
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_repair_cost
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, repair_id as repair_id --整备ID
, repair_item_id as repair_item_id --整备项ID
, project_source as project_source --项目来源
, repair_item_type as repair_item_type --整备项类型
, repair_item_type_name as repair_item_type_name --整备项类型名称
, repair_item_code as repair_item_code --整备项代码
, repair_item_name as repair_item_name --整备项名称
, repair_item_cost as repair_item_cost --整备项费用
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
row_number() over (partition by repair_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_repair_cost_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_repair_item;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_repair_item_tmp as (
select  id as id --主键
, repair_item_id as repair_item_id --整备项ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, repair_item_code as repair_item_code --整备项代码
, repair_item_name as repair_item_name --整备项名称
, repair_item_type as repair_item_type --整备项类型
, repair_item_type_name as repair_item_type_name --整备项类型名称
, repair_item_cost as repair_item_cost --整备项费用
, retail_bench_cost as retail_bench_cost --零售基准费用
, default_show_flag as default_show_flag --默认展示标识
, brand_limit_diff as brand_limit_diff --品牌限定区分
, default_choose_flag as default_choose_flag --默认选择标识
, sq as sq --序列号
, repair_item_status as repair_item_status --整备项状态
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
from ods.ods_dms_uc_uc_repair_item
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(repair_item_id, '')) as repair_item_id  --整备项ID
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(repair_item_code, '')) as repair_item_code  --整备项代码
, trim(nvl(repair_item_name, '')) as repair_item_name  --整备项名称
, trim(nvl(repair_item_type, '')) as repair_item_type  --整备项类型
, trim(nvl(repair_item_type_name, '')) as repair_item_type_name  --整备项类型名称
, nvl(repair_item_cost, 0) as repair_item_cost  --整备项费用
, nvl(retail_bench_cost, 0) as retail_bench_cost  --零售基准费用
, nvl(default_show_flag, 0) as default_show_flag  --默认展示标识
, nvl(brand_limit_diff, 0) as brand_limit_diff  --品牌限定区分
, nvl(default_choose_flag, 0) as default_choose_flag  --默认选择标识
, nvl(sq, 0) as sq  --序列号
, trim(nvl(repair_item_status, '')) as repair_item_status  --整备项状态
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
from dl.tg_dms_uc_t_uc_repair_item
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_repair_item
select  id as id --主键
, repair_item_id as repair_item_id --整备项ID
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, repair_item_code as repair_item_code --整备项代码
, repair_item_name as repair_item_name --整备项名称
, repair_item_type as repair_item_type --整备项类型
, repair_item_type_name as repair_item_type_name --整备项类型名称
, repair_item_cost as repair_item_cost --整备项费用
, retail_bench_cost as retail_bench_cost --零售基准费用
, default_show_flag as default_show_flag --默认展示标识
, brand_limit_diff as brand_limit_diff --品牌限定区分
, default_choose_flag as default_choose_flag --默认选择标识
, sq as sq --序列号
, repair_item_status as repair_item_status --整备项状态
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
row_number() over (partition by repair_item_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_repair_item_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_uc_uc_sales_contract;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_sales_contract_tmp as (
select  id as id --主键
, sales_contract_serial as sales_contract_serial --销售合同编号
, old_nominee_name as old_nominee_name --旧名义人姓名
, old_nominee_telephone as old_nominee_telephone --旧名义人电话
, old_nominee_id_card as old_nominee_id_card --旧名义人身份证号码
, old_nominee_address as old_nominee_address --旧名义人住所
, old_license_plate_number as old_license_plate_number --旧车牌号
, sales_method as sales_method --销售方式
, retail_customer_type as retail_customer_type --零售客户类型
, retail_customer_name as retail_customer_name --零售客户姓名
, retail_customer_id_card as retail_customer_id_card --零售客户身份证号码
, retail_customer_mobile_phone as retail_customer_mobile_phone --零售客户移动电话
, retail_customer_telephone as retail_customer_telephone --零售客户电话
, retail_customer_postal_code as retail_customer_postal_code --零售客户邮编
, retail_customer_address as retail_customer_address --零售客户住所
, customer_id as customer_id --客户ID
, negotiation_id as negotiation_id --商谈ID
, whsle_target_type as whsle_target_type --批售对象类型
, whsle_car_trader_id as whsle_car_trader_id --批售车商ID
, whsle_dealer_code as whsle_dealer_code --批售销售店代码
, whsle_branch_code as whsle_branch_code --批售店铺代码
, whsle_coop_platform_code as whsle_coop_platform_code --批售合作平台代码
, whsle_target_name as whsle_target_name --批售对象名称
, whsle_contact_name as whsle_contact_name --批售联系人名称
, whsle_target_mobile_phone as whsle_target_mobile_phone --批售对象移动电话
, whsle_target_telephone as whsle_target_telephone --批售对象电话
, whsle_target_address as whsle_target_address --批售对象地址
, whsle_target_postal_code as whsle_target_postal_code --批售对象邮编
, whsle_target_email as whsle_target_email --批售对象E-MAIL
, vehicle_user_type as vehicle_user_type --车辆使用人类型
, vehicle_user_name as vehicle_user_name --车辆使用人姓名
, vehicle_user_id_card as vehicle_user_id_card --车辆使用人身份证号码
, vehicle_user_mobile as vehicle_user_mobile --车辆使用人手机
, vehicle_user_telephone as vehicle_user_telephone --车辆使用人电话
, vehicle_user_postal_code as vehicle_user_postal_code --车辆使用人邮编
, vehicle_user_address as vehicle_user_address --车辆使用人地址
, vehicle_body_price_income as vehicle_body_price_income --车辆本体价格收入
, vehicle_body_price_payment as vehicle_body_price_payment --车辆本体价格支出
, discount as discount --折扣
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
from ods.ods_dms_uc_uc_sales_contract
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(sales_contract_serial, '')) as sales_contract_serial  --销售合同编号
, trim(nvl(old_nominee_name, '')) as old_nominee_name  --旧名义人姓名
, trim(nvl(old_nominee_telephone, '')) as old_nominee_telephone  --旧名义人电话
, trim(nvl(old_nominee_id_card, '')) as old_nominee_id_card  --旧名义人身份证号码
, trim(nvl(old_nominee_address, '')) as old_nominee_address  --旧名义人住所
, trim(nvl(old_license_plate_number, '')) as old_license_plate_number  --旧车牌号
, trim(nvl(sales_method, '')) as sales_method  --销售方式
, trim(nvl(retail_customer_type, '')) as retail_customer_type  --零售客户类型
, trim(nvl(retail_customer_name, '')) as retail_customer_name  --零售客户姓名
, trim(nvl(retail_customer_id_card, '')) as retail_customer_id_card  --零售客户身份证号码
, trim(nvl(retail_customer_mobile_phone, '')) as retail_customer_mobile_phone  --零售客户移动电话
, trim(nvl(retail_customer_telephone, '')) as retail_customer_telephone  --零售客户电话
, trim(nvl(retail_customer_postal_code, '')) as retail_customer_postal_code  --零售客户邮编
, trim(nvl(retail_customer_address, '')) as retail_customer_address  --零售客户住所
, trim(nvl(customer_id, '')) as customer_id  --客户ID
, trim(nvl(negotiation_id, '')) as negotiation_id  --商谈ID
, trim(nvl(whsle_target_type, '')) as whsle_target_type  --批售对象类型
, trim(nvl(whsle_car_trader_id, '')) as whsle_car_trader_id  --批售车商ID
, trim(nvl(whsle_dealer_code, '')) as whsle_dealer_code  --批售销售店代码
, trim(nvl(whsle_branch_code, '')) as whsle_branch_code  --批售店铺代码
, trim(nvl(whsle_coop_platform_code, '')) as whsle_coop_platform_code  --批售合作平台代码
, trim(nvl(whsle_target_name, '')) as whsle_target_name  --批售对象名称
, trim(nvl(whsle_contact_name, '')) as whsle_contact_name  --批售联系人名称
, trim(nvl(whsle_target_mobile_phone, '')) as whsle_target_mobile_phone  --批售对象移动电话
, trim(nvl(whsle_target_telephone, '')) as whsle_target_telephone  --批售对象电话
, trim(nvl(whsle_target_address, '')) as whsle_target_address  --批售对象地址
, trim(nvl(whsle_target_postal_code, '')) as whsle_target_postal_code  --批售对象邮编
, trim(nvl(whsle_target_email, '')) as whsle_target_email  --批售对象E-MAIL
, trim(nvl(vehicle_user_type, '')) as vehicle_user_type  --车辆使用人类型
, trim(nvl(vehicle_user_name, '')) as vehicle_user_name  --车辆使用人姓名
, trim(nvl(vehicle_user_id_card, '')) as vehicle_user_id_card  --车辆使用人身份证号码
, trim(nvl(vehicle_user_mobile, '')) as vehicle_user_mobile  --车辆使用人手机
, trim(nvl(vehicle_user_telephone, '')) as vehicle_user_telephone  --车辆使用人电话
, trim(nvl(vehicle_user_postal_code, '')) as vehicle_user_postal_code  --车辆使用人邮编
, trim(nvl(vehicle_user_address, '')) as vehicle_user_address  --车辆使用人地址
, nvl(vehicle_body_price_income, 0) as vehicle_body_price_income  --车辆本体价格收入
, nvl(vehicle_body_price_payment, 0) as vehicle_body_price_payment  --车辆本体价格支出
, nvl(discount, 0) as discount  --折扣
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
from dl.tg_dms_uc_t_uc_sales_contract
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_sales_contract
select  id as id --主键
, sales_contract_serial as sales_contract_serial --销售合同编号
, old_nominee_name as old_nominee_name --旧名义人姓名
, old_nominee_telephone as old_nominee_telephone --旧名义人电话
, old_nominee_id_card as old_nominee_id_card --旧名义人身份证号码
, old_nominee_address as old_nominee_address --旧名义人住所
, old_license_plate_number as old_license_plate_number --旧车牌号
, sales_method as sales_method --销售方式
, retail_customer_type as retail_customer_type --零售客户类型
, retail_customer_name as retail_customer_name --零售客户姓名
, retail_customer_id_card as retail_customer_id_card --零售客户身份证号码
, retail_customer_mobile_phone as retail_customer_mobile_phone --零售客户移动电话
, retail_customer_telephone as retail_customer_telephone --零售客户电话
, retail_customer_postal_code as retail_customer_postal_code --零售客户邮编
, retail_customer_address as retail_customer_address --零售客户住所
, customer_id as customer_id --客户ID
, negotiation_id as negotiation_id --商谈ID
, whsle_target_type as whsle_target_type --批售对象类型
, whsle_car_trader_id as whsle_car_trader_id --批售车商ID
, whsle_dealer_code as whsle_dealer_code --批售销售店代码
, whsle_branch_code as whsle_branch_code --批售店铺代码
, whsle_coop_platform_code as whsle_coop_platform_code --批售合作平台代码
, whsle_target_name as whsle_target_name --批售对象名称
, whsle_contact_name as whsle_contact_name --批售联系人名称
, whsle_target_mobile_phone as whsle_target_mobile_phone --批售对象移动电话
, whsle_target_telephone as whsle_target_telephone --批售对象电话
, whsle_target_address as whsle_target_address --批售对象地址
, whsle_target_postal_code as whsle_target_postal_code --批售对象邮编
, whsle_target_email as whsle_target_email --批售对象E-MAIL
, vehicle_user_type as vehicle_user_type --车辆使用人类型
, vehicle_user_name as vehicle_user_name --车辆使用人姓名
, vehicle_user_id_card as vehicle_user_id_card --车辆使用人身份证号码
, vehicle_user_mobile as vehicle_user_mobile --车辆使用人手机
, vehicle_user_telephone as vehicle_user_telephone --车辆使用人电话
, vehicle_user_postal_code as vehicle_user_postal_code --车辆使用人邮编
, vehicle_user_address as vehicle_user_address --车辆使用人地址
, vehicle_body_price_income as vehicle_body_price_income --车辆本体价格收入
, vehicle_body_price_payment as vehicle_body_price_payment --车辆本体价格支出
, discount as discount --折扣
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
row_number() over (partition by sales_contract_serial
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_sales_contract_tmp) a
where rn = 1;



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



set mapred.job.name=job_ods_dms_uc_uc_sales_ledger;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_uc_uc_sales_ledger_tmp as (
select  id as id --主键
, sales_contract_serial as sales_contract_serial --销售合同编号
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, sales_status as sales_status --销售状态
, sales_method as sales_method --销售方式
, sales_sign_date as sales_sign_date --销售签约日期
, sales_sign_staff_account as sales_sign_staff_account --销售签约担当账号
, temp_save_date as temp_save_date --临时保存日期
, temp_save_staff_account as temp_save_staff_account --临时保存担当账号
, sales_total_income as sales_total_income --销售总收入
, sales_total_payment as sales_total_payment --销售总支付
, payment_plan_date as payment_plan_date --付款计划日期
, payment_finish_date as payment_finish_date --付款完成日期
, invoice_number as invoice_number --发票号
, invoice_number_login_date as invoice_number_login_date --发票号登录日期
, authen_flag as authen_flag --认证标识
, uc_auth_no as uc_auth_no --二手车认证编号
, uc_auth_due_date as uc_auth_due_date --二手车认证到期日
, displace_flag as displace_flag --置换标识
, delivery_plan_date as delivery_plan_date --交车计划日期
, delivery_finish_date as delivery_finish_date --交车完成日期
, delivery_cancel_date as delivery_cancel_date --交车取消日期
, dealer_payment_transfer_cost as dealer_payment_transfer_cost --销售店支付过户费用
, delivery_license_plate_number as delivery_license_plate_number --交车车牌号
, sales_cancel_date as sales_cancel_date --销售取消日
, sales_cancel_staff_account as sales_cancel_staff_account --销售取消担当账号
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
from ods.ods_dms_uc_uc_sales_ledger
union all
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
from dl.tg_dms_uc_t_uc_sales_ledger
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_uc_uc_sales_ledger
select  id as id --主键
, sales_contract_serial as sales_contract_serial --销售合同编号
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, sales_status as sales_status --销售状态
, sales_method as sales_method --销售方式
, sales_sign_date as sales_sign_date --销售签约日期
, sales_sign_staff_account as sales_sign_staff_account --销售签约担当账号
, temp_save_date as temp_save_date --临时保存日期
, temp_save_staff_account as temp_save_staff_account --临时保存担当账号
, sales_total_income as sales_total_income --销售总收入
, sales_total_payment as sales_total_payment --销售总支付
, payment_plan_date as payment_plan_date --付款计划日期
, payment_finish_date as payment_finish_date --付款完成日期
, invoice_number as invoice_number --发票号
, invoice_number_login_date as invoice_number_login_date --发票号登录日期
, authen_flag as authen_flag --认证标识
, uc_auth_no as uc_auth_no --二手车认证编号
, uc_auth_due_date as uc_auth_due_date --二手车认证到期日
, displace_flag as displace_flag --置换标识
, delivery_plan_date as delivery_plan_date --交车计划日期
, delivery_finish_date as delivery_finish_date --交车完成日期
, delivery_cancel_date as delivery_cancel_date --交车取消日期
, dealer_payment_transfer_cost as dealer_payment_transfer_cost --销售店支付过户费用
, delivery_license_plate_number as delivery_license_plate_number --交车车牌号
, sales_cancel_date as sales_cancel_date --销售取消日
, sales_cancel_staff_account as sales_cancel_staff_account --销售取消担当账号
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
row_number() over (partition by sales_contract_serial
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_uc_uc_sales_ledger_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_cs_vhcl_common_model_grade_config;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_cs_vhcl_common_model_grade_config_tmp as (
select  id as id --主键
, uc_franchise_code as uc_franchise_code --二手车品牌编号
, uc_franchise_name as uc_franchise_name --二手车品牌名称
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, vehicle_model as vehicle_model --车型
, vehicle_model_name as vehicle_model_name --车型名称
, grade_name as grade_name --等级名称
, vehicle_detail_config_id as vehicle_detail_config_id --车辆详细配置ID
, vehicle_detail_config_json as vehicle_detail_config_json --车辆详细配置Json
, sales_start_date as sales_start_date --销售开始日期
, sales_end_date as sales_end_date --销售结束日期
, vehicle_type_code as vehicle_type_code --车辆类型代码
, vehicle_category_name as vehicle_category_name --车辆类型名称
, new_vehicle_price as new_vehicle_price --新车价格
, exhaust as exhaust --排量
, national_standard_model as national_standard_model --国标型号
, leave_factory_time as leave_factory_time --出厂时间
, vehicle_model_publish_year as vehicle_model_publish_year --车型发布年份
, country_release_code as country_release_code --国家公布代码
, production_status as production_status --生产状态
, production_time as production_time --生产时间
, stop_production_time as stop_production_time --停产时间
, vehicle_model_info as vehicle_model_info --车型信息
, vehicle_model_category as vehicle_model_category --车型分类
, gear_type as gear_type --档位类型
, car_length as car_length --车长
, car_width as car_width --车宽
, car_height as car_height --车高
, vehicle_door_number_serial as vehicle_door_number_serial --车门数编号
, rate_allow_people_serial as rate_allow_people_serial --额定载客编号
, discharge_standard_serial as discharge_standard_serial --排放标准编号
, gearbox_type as gearbox_type --变速箱类型
, drive_method_serial as drive_method_serial --驱动方式编号
, energy_type_code as energy_type_code --能源类型代码
, fuel_consumption as fuel_consumption --油耗
, whole_weight as whole_weight --整备质量
, gearbox_category as gearbox_category --变速箱分类
, gearbox_category_name as gearbox_category_name --变速箱分类名称
, version as version --版本号
, create_time as create_time --创建时间
, update_by as update_by --更新者
, create_by as create_by --创建者
, update_time as update_time --更新时间
, del_flag as del_flag --删除标识
, remark as remark --备注
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from ods.ods_dms_cs_vhcl_common_model_grade_config
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(uc_franchise_code, '')) as uc_franchise_code  --二手车品牌编号
, trim(nvl(uc_franchise_name, '')) as uc_franchise_name  --二手车品牌名称
, trim(nvl(maker_code, '')) as maker_code  --厂商代码
, trim(nvl(maker_name, '')) as maker_name  --厂商名称
, trim(nvl(vehicle_model, '')) as vehicle_model  --车型
, trim(nvl(vehicle_model_name, '')) as vehicle_model_name  --车型名称
, trim(nvl(grade_name, '')) as grade_name  --等级名称
, trim(nvl(vehicle_detail_config_id, '')) as vehicle_detail_config_id  --车辆详细配置ID
, trim(nvl(vehicle_detail_config_json, '')) as vehicle_detail_config_json  --车辆详细配置Json
, trim(nvl(sales_start_date, '')) as sales_start_date  --销售开始日期
, trim(nvl(sales_end_date, '')) as sales_end_date  --销售结束日期
, trim(nvl(vehicle_type_code, '')) as vehicle_type_code  --车辆类型代码
, trim(nvl(vehicle_category_name, '')) as vehicle_category_name  --车辆类型名称
, nvl(new_vehicle_price, 0) as new_vehicle_price  --新车价格
, trim(nvl(exhaust, '')) as exhaust  --排量
, trim(nvl(national_standard_model, '')) as national_standard_model  --国标型号
, trim(nvl(leave_factory_time, '')) as leave_factory_time  --出厂时间
, trim(nvl(vehicle_model_publish_year, '')) as vehicle_model_publish_year  --车型发布年份
, trim(nvl(country_release_code, '')) as country_release_code  --国家公布代码
, trim(nvl(production_status, '')) as production_status  --生产状态
, trim(nvl(production_time, '')) as production_time  --生产时间
, trim(nvl(stop_production_time, '')) as stop_production_time  --停产时间
, trim(nvl(vehicle_model_info, '')) as vehicle_model_info  --车型信息
, trim(nvl(vehicle_model_category, '')) as vehicle_model_category  --车型分类
, trim(nvl(gear_type, '')) as gear_type  --档位类型
, trim(nvl(car_length, '')) as car_length  --车长
, trim(nvl(car_width, '')) as car_width  --车宽
, trim(nvl(car_height, '')) as car_height  --车高
, trim(nvl(vehicle_door_number_serial, '')) as vehicle_door_number_serial  --车门数编号
, trim(nvl(rate_allow_people_serial, '')) as rate_allow_people_serial  --额定载客编号
, trim(nvl(discharge_standard_serial, '')) as discharge_standard_serial  --排放标准编号
, trim(nvl(gearbox_type, '')) as gearbox_type  --变速箱类型
, trim(nvl(drive_method_serial, '')) as drive_method_serial  --驱动方式编号
, trim(nvl(energy_type_code, '')) as energy_type_code  --能源类型代码
, trim(nvl(fuel_consumption, '')) as fuel_consumption  --油耗
, nvl(whole_weight, 0) as whole_weight  --整备质量
, trim(nvl(gearbox_category, '')) as gearbox_category  --变速箱分类
, trim(nvl(gearbox_category_name, '')) as gearbox_category_name  --变速箱分类名称
, nvl(version, 0) as version  --版本号
, trim(nvl(create_time, '')) as create_time  --创建时间
, nvl(update_by, 0) as update_by  --更新者
, nvl(create_by, 0) as create_by  --创建者
, trim(nvl(update_time, '')) as update_time  --更新时间
, nvl(del_flag, 0) as del_flag  --删除标识
, trim(nvl(remark, '')) as remark  --备注
, etl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from dl.tg_dms_cs_t_vhcl_common_model_grade_config
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_cs_vhcl_common_model_grade_config
select  id as id --主键
, uc_franchise_code as uc_franchise_code --二手车品牌编号
, uc_franchise_name as uc_franchise_name --二手车品牌名称
, maker_code as maker_code --厂商代码
, maker_name as maker_name --厂商名称
, vehicle_model as vehicle_model --车型
, vehicle_model_name as vehicle_model_name --车型名称
, grade_name as grade_name --等级名称
, vehicle_detail_config_id as vehicle_detail_config_id --车辆详细配置ID
, vehicle_detail_config_json as vehicle_detail_config_json --车辆详细配置Json
, sales_start_date as sales_start_date --销售开始日期
, sales_end_date as sales_end_date --销售结束日期
, vehicle_type_code as vehicle_type_code --车辆类型代码
, vehicle_category_name as vehicle_category_name --车辆类型名称
, new_vehicle_price as new_vehicle_price --新车价格
, exhaust as exhaust --排量
, national_standard_model as national_standard_model --国标型号
, leave_factory_time as leave_factory_time --出厂时间
, vehicle_model_publish_year as vehicle_model_publish_year --车型发布年份
, country_release_code as country_release_code --国家公布代码
, production_status as production_status --生产状态
, production_time as production_time --生产时间
, stop_production_time as stop_production_time --停产时间
, vehicle_model_info as vehicle_model_info --车型信息
, vehicle_model_category as vehicle_model_category --车型分类
, gear_type as gear_type --档位类型
, car_length as car_length --车长
, car_width as car_width --车宽
, car_height as car_height --车高
, vehicle_door_number_serial as vehicle_door_number_serial --车门数编号
, rate_allow_people_serial as rate_allow_people_serial --额定载客编号
, discharge_standard_serial as discharge_standard_serial --排放标准编号
, gearbox_type as gearbox_type --变速箱类型
, drive_method_serial as drive_method_serial --驱动方式编号
, energy_type_code as energy_type_code --能源类型代码
, fuel_consumption as fuel_consumption --油耗
, whole_weight as whole_weight --整备质量
, gearbox_category as gearbox_category --变速箱分类
, gearbox_category_name as gearbox_category_name --变速箱分类名称
, version as version --版本号
, create_time as create_time --创建时间
, update_by as update_by --更新者
, create_by as create_by --创建者
, update_time as update_time --更新时间
, del_flag as del_flag --删除标识
, remark as remark --备注
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, 'DMS共通' as source_system --来源系统
, substr(current_timestamp(), 1, 19) as insert_time --数据写入时间
, biz_date as biz_date  --分区字段
from (
select *,
row_number() over (partition by vehicle_detail_config_id
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_cs_vhcl_common_model_grade_config_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_cs_salesed_vehicle_ledger;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_cs_salesed_vehicle_ledger_tmp as (
select  id as id --主键
, customer_order_number as customer_order_number --客户订单号
, car_owner_cert_number as car_owner_cert_number --车主证件号码
, car_owner_type as car_owner_type --车主类型
, car_owner_name as car_owner_name --车主姓名
, sales_dealer_code as sales_dealer_code --销售销售店代码
, sales_company_name as sales_company_name --销售公司名称
, sales_company_taxpayer_number as sales_company_taxpayer_number --销售公司统一信用代码
, delivery_dealer_code as delivery_dealer_code --交车销售店代码
, sales_adviser_code as sales_adviser_code --销售顾问代码
, delivery_consultant_code as delivery_consultant_code --交车顾问代码
, after_sales_adviser_code as after_sales_adviser_code --售后顾问代码
, vinno as vinno --车架号
, vehicle_flag as vehicle_flag --车辆区分
, vehicle_type as vehicle_type --车辆类别
, vinno_d7 as vinno_d7 --车架号后7位
, urn as urn --车辆生产识别码
, vehicle_name as vehicle_name --车辆名称
, vehicle_name_code as vehicle_name_code --车辆名称代码
, vehicle_model_name as vehicle_model_name --车型名称
, vehicle_model as vehicle_model --车型
, stand_vehicle_model as stand_vehicle_model --车型代码[标准]
, vehicle_model_subdivision as vehicle_model_subdivision --车型代码[细分]
, vehicle_sfx_code as vehicle_sfx_code --车辆SFX代码
, grade_code as grade_code --等级代码
, grade_name as grade_name --等级名称（销售）
, invoice_grade as invoice_grade --发票用车辆等级
, invoice_vehicle_name as invoice_vehicle_name --发票用车型名称
, vehicle_color_code as vehicle_color_code --车辆颜色代码
, vehicle_color_name as vehicle_color_name --车辆颜色名称
, interior_color_code as interior_color_code --内饰颜色代码
, interior_color_name as interior_color_name --内饰颜色名称
, engine as engine --发动机
, engine_number as engine_number --发动机号(合格证)
, engine_type as engine_type --发动机型号(合格证)
, exhaust as exhaust --排量
, gearbox as gearbox --变速箱
, delivery_odometer_value as delivery_odometer_value --交车时公里数
, vehicle_odometer_value as vehicle_odometer_value --行驶公里数
, vehicle_use as vehicle_use --车辆用途
, sales_diff as sales_diff --购买类型/销售区分
, factory_code as factory_code --工厂代码
, id_line as id_line --生产线号
, production_date as production_date --制造日期
, sales_price as sales_price --销售价格
, new_vehicle_retail_price as new_vehicle_retail_price --新车零售价格
, fuel_type as fuel_type --燃料种类
, fuel_type_name as fuel_type_name --燃料种类名称
, final_inspt_date as final_inspt_date --终检日
, outday_date as outday_date --出门日
, match_date as match_date --匹配日
, sales_date as sales_date --销售日
, pds_date as pds_date --PDS日
, delivery_date as delivery_date --交车日
, warranty_status as warranty_status --三包凭证更新状态
, regist_number as regist_number --车牌号
, vehicle_certificate_number as vehicle_certificate_number --整车合格证编号
, invoice_number as invoice_number --发票编号（销售）
, del_flag as del_flag --删除标识
, remark as remark --备注
, create_time as create_time --创建时间
, create_by as create_by --创建者
, update_time as update_time --更新时间
, update_by as update_by --更新者
, vinno_after_6_digit as vinno_after_6_digit --车架号（后六位）
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from ods.ods_dms_cs_salesed_vehicle_ledger
union all
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
from dl.tg_dms_cs_t_salesed_vehicle_ledger
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_cs_salesed_vehicle_ledger
select  id as id --主键
, customer_order_number as customer_order_number --客户订单号
, car_owner_cert_number as car_owner_cert_number --车主证件号码
, car_owner_type as car_owner_type --车主类型
, car_owner_name as car_owner_name --车主姓名
, sales_dealer_code as sales_dealer_code --销售销售店代码
, sales_company_name as sales_company_name --销售公司名称
, sales_company_taxpayer_number as sales_company_taxpayer_number --销售公司统一信用代码
, delivery_dealer_code as delivery_dealer_code --交车销售店代码
, sales_adviser_code as sales_adviser_code --销售顾问代码
, delivery_consultant_code as delivery_consultant_code --交车顾问代码
, after_sales_adviser_code as after_sales_adviser_code --售后顾问代码
, vinno as vinno --车架号
, vehicle_flag as vehicle_flag --车辆区分
, vehicle_type as vehicle_type --车辆类别
, vinno_d7 as vinno_d7 --车架号后7位
, urn as urn --车辆生产识别码
, vehicle_name as vehicle_name --车辆名称
, vehicle_name_code as vehicle_name_code --车辆名称代码
, vehicle_model_name as vehicle_model_name --车型名称
, vehicle_model as vehicle_model --车型
, stand_vehicle_model as stand_vehicle_model --车型代码[标准]
, vehicle_model_subdivision as vehicle_model_subdivision --车型代码[细分]
, vehicle_sfx_code as vehicle_sfx_code --车辆SFX代码
, grade_code as grade_code --等级代码
, grade_name as grade_name --等级名称（销售）
, invoice_grade as invoice_grade --发票用车辆等级
, invoice_vehicle_name as invoice_vehicle_name --发票用车型名称
, vehicle_color_code as vehicle_color_code --车辆颜色代码
, vehicle_color_name as vehicle_color_name --车辆颜色名称
, interior_color_code as interior_color_code --内饰颜色代码
, interior_color_name as interior_color_name --内饰颜色名称
, engine as engine --发动机
, engine_number as engine_number --发动机号(合格证)
, engine_type as engine_type --发动机型号(合格证)
, exhaust as exhaust --排量
, gearbox as gearbox --变速箱
, delivery_odometer_value as delivery_odometer_value --交车时公里数
, vehicle_odometer_value as vehicle_odometer_value --行驶公里数
, vehicle_use as vehicle_use --车辆用途
, sales_diff as sales_diff --购买类型/销售区分
, factory_code as factory_code --工厂代码
, id_line as id_line --生产线号
, production_date as production_date --制造日期
, sales_price as sales_price --销售价格
, new_vehicle_retail_price as new_vehicle_retail_price --新车零售价格
, fuel_type as fuel_type --燃料种类
, fuel_type_name as fuel_type_name --燃料种类名称
, final_inspt_date as final_inspt_date --终检日
, outday_date as outday_date --出门日
, match_date as match_date --匹配日
, sales_date as sales_date --销售日
, pds_date as pds_date --PDS日
, delivery_date as delivery_date --交车日
, warranty_status as warranty_status --三包凭证更新状态
, regist_number as regist_number --车牌号
, vehicle_certificate_number as vehicle_certificate_number --整车合格证编号
, invoice_number as invoice_number --发票编号（销售）
, del_flag as del_flag --删除标识
, remark as remark --备注
, create_time as create_time --创建时间
, create_by as create_by --创建者
, update_time as update_time --更新时间
, update_by as update_by --更新者
, vinno_after_6_digit as vinno_after_6_digit --车架号（后六位）
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, 'DMS共通' as source_system --来源系统
, substr(current_timestamp(), 1, 19) as insert_time --数据写入时间
, biz_date as biz_date  --分区字段
from (
select *,
row_number() over (partition by customer_order_number
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_cs_salesed_vehicle_ledger_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_cs_maintain_uc_repair_entrust;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_cs_maintain_uc_repair_entrust_tmp as (
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, repair_status as repair_status --整备状态
, repair_type as repair_type --整备类型
, dealer_repair_start_time as dealer_repair_start_time --销售店整备开始时间
, dealer_repair_end_time as dealer_repair_end_time --销售店整备结束时间
, user_pay_repair_cost as user_pay_repair_cost --用户承担整备费用
, dealer_repair_cost as dealer_repair_cost --销售店整备成本
, repair_entrust_id as repair_entrust_id --整备委托ID
, repair_entrust_date as repair_entrust_date --整备委托日期
, repair_entrust_appraiser_code as repair_entrust_appraiser_code --整备委托评估师代码
, repair_entrust_memo as repair_entrust_memo --整备委托留言
, maintain_order_no as maintain_order_no --维修工单号
, maintain_order_accept_date as maintain_order_accept_date --维修工单受理日
, maintain_order_start_date as maintain_order_start_date --维修工单开始日
, maintain_order_complete_date as maintain_order_complete_date --维修工单完成日
, maintain_order_cancel_date as maintain_order_cancel_date --维修工单取消日期
, maintain_order_cancel_reason as maintain_order_cancel_reason --维修工单取消原因
, maintain_order_settl_date as maintain_order_settl_date --维修工单结算日
, maintain_order_settl_fee as maintain_order_settl_fee --维修工单结算费用
, service_staff_account as service_staff_account --服务担当账号
, nominee_type as nominee_type --名义人类型
, nominee_name as nominee_name --名义人姓名
, nominee_id_card as nominee_id_card --名义人身份证号码
, nominee_postal_code as nominee_postal_code --名义人邮编
, nominee_telephone as nominee_telephone --名义人电话
, nominee_email as nominee_email --名义人EMAIL
, nominee_mobile_phone as nominee_mobile_phone --名义人手机号
, uc_auth_flag as uc_auth_flag --二手车认证标识
, uc_auth_no as uc_auth_no --二手车认证编号
, nominee_address as nominee_address --名义人住所
, create_time as create_time --创建时间
, create_by as create_by --创建者
, update_by as update_by --更新者
, update_time as update_time --更新时间
, del_flag as del_flag --删除标识
, remark as remark --备注
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from ods.ods_dms_cs_maintain_uc_repair_entrust
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(dealer_code, '')) as dealer_code  --销售店代码
, trim(nvl(branch_code, '')) as branch_code  --店铺代码
, trim(nvl(vehicle_manage_no, '')) as vehicle_manage_no  --车辆管理编号
, trim(nvl(vinno, '')) as vinno  --车架号
, trim(nvl(repair_status, '')) as repair_status  --整备状态
, nvl(repair_type, 0) as repair_type  --整备类型
, trim(nvl(dealer_repair_start_time, '')) as dealer_repair_start_time  --销售店整备开始时间
, trim(nvl(dealer_repair_end_time, '')) as dealer_repair_end_time  --销售店整备结束时间
, nvl(user_pay_repair_cost, 0) as user_pay_repair_cost  --用户承担整备费用
, nvl(dealer_repair_cost, 0) as dealer_repair_cost  --销售店整备成本
, trim(nvl(repair_entrust_id, '')) as repair_entrust_id  --整备委托ID
, trim(nvl(repair_entrust_date, '')) as repair_entrust_date  --整备委托日期
, trim(nvl(repair_entrust_appraiser_code, '')) as repair_entrust_appraiser_code  --整备委托评估师代码
, trim(nvl(repair_entrust_memo, '')) as repair_entrust_memo  --整备委托留言
, trim(nvl(maintain_order_no, '')) as maintain_order_no  --维修工单号
, trim(nvl(maintain_order_accept_date, '')) as maintain_order_accept_date  --维修工单受理日
, trim(nvl(maintain_order_start_date, '')) as maintain_order_start_date  --维修工单开始日
, trim(nvl(maintain_order_complete_date, '')) as maintain_order_complete_date  --维修工单完成日
, trim(nvl(maintain_order_cancel_date, '')) as maintain_order_cancel_date  --维修工单取消日期
, trim(nvl(maintain_order_cancel_reason, '')) as maintain_order_cancel_reason  --维修工单取消原因
, trim(nvl(maintain_order_settl_date, '')) as maintain_order_settl_date  --维修工单结算日
, nvl(maintain_order_settl_fee, 0) as maintain_order_settl_fee  --维修工单结算费用
, trim(nvl(service_staff_account, '')) as service_staff_account  --服务担当账号
, trim(nvl(nominee_type, '')) as nominee_type  --名义人类型
, trim(nvl(nominee_name, '')) as nominee_name  --名义人姓名
, trim(nvl(nominee_id_card, '')) as nominee_id_card  --名义人身份证号码
, trim(nvl(nominee_postal_code, '')) as nominee_postal_code  --名义人邮编
, trim(nvl(nominee_telephone, '')) as nominee_telephone  --名义人电话
, trim(nvl(nominee_email, '')) as nominee_email  --名义人EMAIL
, trim(nvl(nominee_mobile_phone, '')) as nominee_mobile_phone  --名义人手机号
, trim(nvl(uc_auth_flag, '')) as uc_auth_flag  --二手车认证标识
, trim(nvl(uc_auth_no, '')) as uc_auth_no  --二手车认证编号
, trim(nvl(nominee_address, '')) as nominee_address  --名义人住所
, trim(nvl(create_time, '')) as create_time  --创建时间
, nvl(create_by, 0) as create_by  --创建者
, nvl(update_by, 0) as update_by  --更新者
, trim(nvl(update_time, '')) as update_time  --更新时间
, nvl(del_flag, 0) as del_flag  --删除标识
, trim(nvl(remark, '')) as remark  --备注
, etl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from dl.tg_dms_cs_t_maintain_uc_repair_entrust
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_cs_maintain_uc_repair_entrust
select  id as id --主键
, dealer_code as dealer_code --销售店代码
, branch_code as branch_code --店铺代码
, vehicle_manage_no as vehicle_manage_no --车辆管理编号
, vinno as vinno --车架号
, repair_status as repair_status --整备状态
, repair_type as repair_type --整备类型
, dealer_repair_start_time as dealer_repair_start_time --销售店整备开始时间
, dealer_repair_end_time as dealer_repair_end_time --销售店整备结束时间
, user_pay_repair_cost as user_pay_repair_cost --用户承担整备费用
, dealer_repair_cost as dealer_repair_cost --销售店整备成本
, repair_entrust_id as repair_entrust_id --整备委托ID
, repair_entrust_date as repair_entrust_date --整备委托日期
, repair_entrust_appraiser_code as repair_entrust_appraiser_code --整备委托评估师代码
, repair_entrust_memo as repair_entrust_memo --整备委托留言
, maintain_order_no as maintain_order_no --维修工单号
, maintain_order_accept_date as maintain_order_accept_date --维修工单受理日
, maintain_order_start_date as maintain_order_start_date --维修工单开始日
, maintain_order_complete_date as maintain_order_complete_date --维修工单完成日
, maintain_order_cancel_date as maintain_order_cancel_date --维修工单取消日期
, maintain_order_cancel_reason as maintain_order_cancel_reason --维修工单取消原因
, maintain_order_settl_date as maintain_order_settl_date --维修工单结算日
, maintain_order_settl_fee as maintain_order_settl_fee --维修工单结算费用
, service_staff_account as service_staff_account --服务担当账号
, nominee_type as nominee_type --名义人类型
, nominee_name as nominee_name --名义人姓名
, nominee_id_card as nominee_id_card --名义人身份证号码
, nominee_postal_code as nominee_postal_code --名义人邮编
, nominee_telephone as nominee_telephone --名义人电话
, nominee_email as nominee_email --名义人EMAIL
, nominee_mobile_phone as nominee_mobile_phone --名义人手机号
, uc_auth_flag as uc_auth_flag --二手车认证标识
, uc_auth_no as uc_auth_no --二手车认证编号
, nominee_address as nominee_address --名义人住所
, create_time as create_time --创建时间
, create_by as create_by --创建者
, update_by as update_by --更新者
, update_time as update_time --更新时间
, del_flag as del_flag --删除标识
, remark as remark --备注
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, 'DMS共通' as source_system --来源系统
, substr(current_timestamp(), 1, 19) as insert_time --数据写入时间
, biz_date as biz_date  --分区字段
from (
select *,
row_number() over (partition by vehicle_manage_no,maintain_order_no
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_cs_maintain_uc_repair_entrust_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_cs_meta_first_level_source;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_cs_meta_first_level_source_tmp as (
select  id as id --主键
, source_code as source_code --来源代码
, source_name as source_name --来源名称
, create_by as create_by --创建者
, create_time as create_time --创建时间
, update_by as update_by --更新者
, update_time as update_time --更新时间
, del_flag as del_flag --删除标识
, remark as remark --备注
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from ods.ods_dms_cs_meta_first_level_source
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(source_code, '')) as source_code  --来源代码
, trim(nvl(source_name, '')) as source_name  --来源名称
, nvl(create_by, 0) as create_by  --创建者
, trim(nvl(create_time, '')) as create_time  --创建时间
, nvl(update_by, 0) as update_by  --更新者
, trim(nvl(update_time, '')) as update_time  --更新时间
, nvl(del_flag, 0) as del_flag  --删除标识
, trim(nvl(remark, '')) as remark  --备注
, etl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from dl.tg_dms_cs_t_meta_first_level_source
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_cs_meta_first_level_source
select  id as id --主键
, source_code as source_code --来源代码
, source_name as source_name --来源名称
, create_by as create_by --创建者
, create_time as create_time --创建时间
, update_by as update_by --更新者
, update_time as update_time --更新时间
, del_flag as del_flag --删除标识
, remark as remark --备注
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, 'DMS共通' as source_system --来源系统
, substr(current_timestamp(), 1, 19) as insert_time --数据写入时间
, biz_date as biz_date  --分区字段
from (
select *,
row_number() over (partition by source_code
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_cs_meta_first_level_source_tmp) a
where rn = 1;



set mapred.job.name=job_ods_dms_cs_meta_second_level_source;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
with ods_dms_cs_meta_second_level_source_tmp as (
select  id as id --主键
, source_first_level_code as source_first_level_code --一级来源代码
, source_code as source_code --来源代码
, source_name as source_name --来源名称
, create_by as create_by --创建者
, create_time as create_time --创建时间
, update_by as update_by --更新者
, update_time as update_time --更新时间
, del_flag as del_flag --删除标识
, remark as remark --备注
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from ods.ods_dms_cs_meta_second_level_source
union all
select  trim(nvl(id, '')) as id  --主键
, trim(nvl(source_first_level_code, '')) as source_first_level_code  --一级来源代码
, trim(nvl(source_code, '')) as source_code  --来源代码
, trim(nvl(source_name, '')) as source_name  --来源名称
, nvl(create_by, 0) as create_by  --创建者
, trim(nvl(create_time, '')) as create_time  --创建时间
, nvl(update_by, 0) as update_by  --更新者
, trim(nvl(update_time, '')) as update_time  --更新时间
, nvl(del_flag, 0) as del_flag  --删除标识
, trim(nvl(remark, '')) as remark  --备注
, etl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, biz_date as biz_date  --分区字段
from dl.tg_dms_cs_t_meta_second_level_source
where biz_date = '${YESTERDAY}'
)
insert overwrite table ods.ods_dms_cs_meta_second_level_source
select  id as id --主键
, source_first_level_code as source_first_level_code --一级来源代码
, source_code as source_code --来源代码
, source_name as source_name --来源名称
, create_by as create_by --创建者
, create_time as create_time --创建时间
, update_by as update_by --更新者
, update_time as update_time --更新时间
, del_flag as del_flag --删除标识
, remark as remark --备注
, dl_batch_id as dl_batch_id  --dl加载批次
, extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）
, load_time as load_time  --数据加载到hive的时间
, 'DMS共通' as source_system --来源系统
, substr(current_timestamp(), 1, 19) as insert_time --数据写入时间
, biz_date as biz_date  --分区字段
from (
select *,
row_number() over (partition by source_code
order by update_time desc, load_time desc, create_time desc)rn
from ods_dms_cs_meta_second_level_source_tmp) a
where rn = 1;


