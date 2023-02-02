set mapred.job.name=job_ods_dms_uc_uc_assessment_vehicle_info_init;
--set hive.exec.parallel=true;
--set hive.map.aggr=true;
--set hive.groupby.skewindata=true;
insert overwrite table ods.ods_dms_uc_uc_assessment_vehicle_info
select  id --主键
, ass_serial --评估编号
, dealer_code --销售店代码
, vehicle_manage_no --车辆管理编号
, branch_code --店铺代码
, brand_code --品牌CD
, brand_name --品牌名称
, maker_code --厂商代码
, maker_name --厂商名称
, vehicle_model_code --车型代码
, vehicle_model_name --车型名称
, vehicle_grade --车辆等级
, vehicle_type_code --车辆类型代码
, vehicle_category_name --车辆类型名称
, national_standard_code --国标代码
, vinno --车架号
, engine_number --发动机号
, license_plate_number --车牌号
, production_date --制造日期
, exterior_color_code_1 --外装色CD1
, exterior_color_1 --外装色1
, exterior_color_code_2 --外装色CD2
, exterior_color_2 --外装色2
, color_change_diff --颜色变更区分
, interior_color_code_1 --内装色CD1
, interior_color_1 --内装色1
, interior_color_code_2 --内装色CD2
, interior_color_2 --内装色2
, odometer --行驶里程
, first_regist_date --首次注册日期
, recent_regist_date --最近注册日期
, use_nature --使用性质
, whole_quality_level --整体品质级别
, elect_level --机电级别
, outside_rating --外观定级
, interior_level --内饰级别
, accident_level --事故级别
, collision_accident_diff --碰撞事故区分
, water_accident_diff --水浸事故区分
, fire_accident_diff --火烧事故区分
, excessive_modified_diff --过度改装区分
, wty_type --三包类型
, instrument_tampering_flag --仪表篡改标识
, vehicle_door_qty --车门数量
, rate_allow_people_qty --额定载客人数
, whole_weight --整备质量
, car_length --车长
, car_width --车宽
, car_height --车高
, fuel_type_code --燃料种类代码
, fuel_type_name --燃料种类名称
, exhaust --排量
, fuel_consumption --油耗
, discharge_standard --排放标准
, gearbox_type --变速箱类型
, gearbox_name --变速箱名称
, drive_method_serial --驱动方式编号
, drive_method_name --驱动方式名称
, cover_thumbnail_img_url --封面缩略图片URL
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
, row_number() over(partition by ass_serial
order by update_time desc, load_time desc, create_time desc) rn
from dl.tg_dms_uc_t_uc_assessment_vehicle_info
where biz_date < '${TODAY}' )aa
where rn = 1;


