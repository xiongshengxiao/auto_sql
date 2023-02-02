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


