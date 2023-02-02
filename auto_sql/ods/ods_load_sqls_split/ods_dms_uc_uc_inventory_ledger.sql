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


