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


