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


