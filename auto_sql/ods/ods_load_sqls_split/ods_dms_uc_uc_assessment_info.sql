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


