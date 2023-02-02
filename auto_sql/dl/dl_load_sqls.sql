tg_dms_uc_t_uc_assessment_info = select \
NVL(get_json_object(data, '$.id'),'') id \
,cast(NVL(get_json_object(data, '$.ass_serial'),'0') as bigint) ass_serial \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.negotiation_id'),'') negotiation_id \
,cast(NVL(get_json_object(data, '$.sq'),'0') as bigint) sq \
,NVL(get_json_object(data, '$.ass_status'),'') ass_status \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.ass_date'),'') ass_date \
,NVL(get_json_object(data, '$.appraiser_memo'),'') appraiser_memo \
,NVL(get_json_object(data, '$.appraiser_account'),'') appraiser_account \
,NVL(get_json_object(data, '$.appraiser_name'),'') appraiser_name \
,NVL(get_json_object(data, '$.check_manage_no'),'') check_manage_no \
,NVL(get_json_object(data, '$.vehicle_config_info_json'),'') vehicle_config_info_json \
,NVL(get_json_object(data, '$.vehicle_check_remark_json'),'') vehicle_check_remark_json \
,NVL(get_json_object(data, '$.vhcl_check_remark_label_json'),'') vhcl_check_remark_label_json \
,NVL(get_json_object(data, '$.vehicle_addi_check_json'),'') vehicle_addi_check_json \
,NVL(get_json_object(data, '$.vehicle_img_json'),'') vehicle_img_json \
,NVL(get_json_object(data, '$.vehicle_defect_info_json'),'') vehicle_defect_info_json \
,NVL(get_json_object(data, '$.imp_service_info_json'),'') imp_service_info_json \
,NVL(get_json_object(data, '$.update_account'),'') update_account \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_assessment_info' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_assessment_ledger = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.dealer_name'),'') dealer_name \
,NVL(get_json_object(data, '$.ass_status'),'') ass_status \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.engine_number'),'') engine_number \
,NVL(get_json_object(data, '$.license_plate_number'),'') license_plate_number \
,NVL(get_json_object(data, '$.negotiation_id'),'') negotiation_id \
,NVL(get_json_object(data, '$.customer_id'),'') customer_id \
,NVL(get_json_object(data, '$.customer_name'),'') customer_name \
,NVL(get_json_object(data, '$.mobile_phone'),'') mobile_phone \
,NVL(get_json_object(data, '$.telephone_number'),'') telephone_number \
,NVL(get_json_object(data, '$.brand_code'),'') brand_code \
,NVL(get_json_object(data, '$.brand_name'),'') brand_name \
,NVL(get_json_object(data, '$.maker_code'),'') maker_code \
,NVL(get_json_object(data, '$.maker_name'),'') maker_name \
,NVL(get_json_object(data, '$.vehicle_model_code'),'') vehicle_model_code \
,NVL(get_json_object(data, '$.vehicle_model_name'),'') vehicle_model_name \
,NVL(get_json_object(data, '$.vehicle_grade'),'') vehicle_grade \
,NVL(get_json_object(data, '$.first_regist_date'),'') first_regist_date \
,cast(NVL(get_json_object(data, '$.odometer'),'0') as bigint) odometer \
,NVL(get_json_object(data, '$.exhaust'),'') exhaust \
,NVL(get_json_object(data, '$.fuel_type_code'),'') fuel_type_code \
,NVL(get_json_object(data, '$.fuel_type_name'),'') fuel_type_name \
,NVL(get_json_object(data, '$.vehicle_type_code'),'') vehicle_type_code \
,NVL(get_json_object(data, '$.vehicle_category_name'),'') vehicle_category_name \
,NVL(get_json_object(data, '$.gearbox_type'),'') gearbox_type \
,NVL(get_json_object(data, '$.gearbox_name'),'') gearbox_name \
,NVL(get_json_object(data, '$.gearbox_category'),'') gearbox_category \
,NVL(get_json_object(data, '$.gearbox_category_name'),'') gearbox_category_name \
,NVL(get_json_object(data, '$.exterior_color_code_1'),'') exterior_color_code_1 \
,NVL(get_json_object(data, '$.exterior_color_1'),'') exterior_color_1 \
,NVL(get_json_object(data, '$.exterior_color_code_2'),'') exterior_color_code_2 \
,NVL(get_json_object(data, '$.exterior_color_2'),'') exterior_color_2 \
,NVL(get_json_object(data, '$.cover_thumbnail_img_url'),'') cover_thumbnail_img_url \
,cast(NVL(get_json_object(data, '$.ass_serial'),'0') as bigint) ass_serial \
,NVL(get_json_object(data, '$.ass_date'),'') ass_date \
,cast(NVL(get_json_object(data, '$.ass_count'),'0') as bigint) ass_count \
,NVL(get_json_object(data, '$.appraiser_memo'),'') appraiser_memo \
,NVL(get_json_object(data, '$.related_type'),'') related_type \
,NVL(get_json_object(data, '$.related_id'),'') related_id \
,NVL(get_json_object(data, '$.sales_decision_time'),'') sales_decision_time \
,NVL(get_json_object(data, '$.sales_decision_status'),'') sales_decision_status \
,NVL(get_json_object(data, '$.plan_sales_method'),'') plan_sales_method \
,cast(NVL(get_json_object(data, '$.retail_ref_sales_price'),'0') as decimal(12,2)) retail_ref_sales_price \
,cast(NVL(get_json_object(data, '$.retail_plan_sales_price'),'0') as decimal(12,2)) retail_plan_sales_price \
,cast(NVL(get_json_object(data, '$.retail_ref_purchase_price'),'0') as decimal(12,2)) retail_ref_purchase_price \
,cast(NVL(get_json_object(data, '$.retail_plan_purchase_price'),'0') as decimal(12,2)) retail_plan_purchase_price \
,cast(NVL(get_json_object(data, '$.retail_bench_gross_profit'),'0') as decimal(12,2)) retail_bench_gross_profit \
,cast(NVL(get_json_object(data, '$.retail_plan_gross_profit'),'0') as decimal(12,2)) retail_plan_gross_profit \
,cast(NVL(get_json_object(data, '$.whsle_ref_sales_price'),'0') as decimal(12,2)) whsle_ref_sales_price \
,cast(NVL(get_json_object(data, '$.whsle_plan_sales_price'),'0') as decimal(12,2)) whsle_plan_sales_price \
,cast(NVL(get_json_object(data, '$.whsle_ref_purchase_price'),'0') as decimal(12,2)) whsle_ref_purchase_price \
,cast(NVL(get_json_object(data, '$.whsle_plan_purchase_price'),'0') as decimal(12,2)) whsle_plan_purchase_price \
,cast(NVL(get_json_object(data, '$.whsle_bench_gross_profit'),'0') as decimal(12,2)) whsle_bench_gross_profit \
,cast(NVL(get_json_object(data, '$.whsle_plan_gross_profit'),'0') as decimal(12,2)) whsle_plan_gross_profit \
,cast(NVL(get_json_object(data, '$.new_vehicle_guide_price'),'0') as decimal(12,2)) new_vehicle_guide_price \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_assessment_ledger' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_assessment_vehicle_info = select \
NVL(get_json_object(data, '$.id'),'') id \
,cast(NVL(get_json_object(data, '$.ass_serial'),'0') as bigint) ass_serial \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.brand_code'),'') brand_code \
,NVL(get_json_object(data, '$.brand_name'),'') brand_name \
,NVL(get_json_object(data, '$.maker_code'),'') maker_code \
,NVL(get_json_object(data, '$.maker_name'),'') maker_name \
,NVL(get_json_object(data, '$.vehicle_model_code'),'') vehicle_model_code \
,NVL(get_json_object(data, '$.vehicle_model_name'),'') vehicle_model_name \
,NVL(get_json_object(data, '$.vehicle_grade'),'') vehicle_grade \
,NVL(get_json_object(data, '$.vehicle_type_code'),'') vehicle_type_code \
,NVL(get_json_object(data, '$.vehicle_category_name'),'') vehicle_category_name \
,NVL(get_json_object(data, '$.national_standard_code'),'') national_standard_code \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.engine_number'),'') engine_number \
,NVL(get_json_object(data, '$.license_plate_number'),'') license_plate_number \
,NVL(get_json_object(data, '$.production_date'),'') production_date \
,NVL(get_json_object(data, '$.exterior_color_code_1'),'') exterior_color_code_1 \
,NVL(get_json_object(data, '$.exterior_color_1'),'') exterior_color_1 \
,NVL(get_json_object(data, '$.exterior_color_code_2'),'') exterior_color_code_2 \
,NVL(get_json_object(data, '$.exterior_color_2'),'') exterior_color_2 \
,NVL(get_json_object(data, '$.color_change_diff'),'') color_change_diff \
,NVL(get_json_object(data, '$.interior_color_code_1'),'') interior_color_code_1 \
,NVL(get_json_object(data, '$.interior_color_1'),'') interior_color_1 \
,NVL(get_json_object(data, '$.interior_color_code_2'),'') interior_color_code_2 \
,NVL(get_json_object(data, '$.interior_color_2'),'') interior_color_2 \
,cast(NVL(get_json_object(data, '$.odometer'),'0') as bigint) odometer \
,NVL(get_json_object(data, '$.first_regist_date'),'') first_regist_date \
,NVL(get_json_object(data, '$.recent_regist_date'),'') recent_regist_date \
,NVL(get_json_object(data, '$.use_nature'),'') use_nature \
,NVL(get_json_object(data, '$.whole_quality_level'),'') whole_quality_level \
,NVL(get_json_object(data, '$.elect_level'),'') elect_level \
,NVL(get_json_object(data, '$.outside_rating'),'') outside_rating \
,NVL(get_json_object(data, '$.interior_level'),'') interior_level \
,NVL(get_json_object(data, '$.accident_level'),'') accident_level \
,NVL(get_json_object(data, '$.collision_accident_diff'),'') collision_accident_diff \
,NVL(get_json_object(data, '$.water_accident_diff'),'') water_accident_diff \
,NVL(get_json_object(data, '$.fire_accident_diff'),'') fire_accident_diff \
,NVL(get_json_object(data, '$.excessive_modified_diff'),'') excessive_modified_diff \
,NVL(get_json_object(data, '$.wty_type'),'') wty_type \
,NVL(get_json_object(data, '$.instrument_tampering_flag'),'') instrument_tampering_flag \
,cast(NVL(get_json_object(data, '$.vehicle_door_qty'),'0') as bigint) vehicle_door_qty \
,cast(NVL(get_json_object(data, '$.rate_allow_people_qty'),'0') as bigint) rate_allow_people_qty \
,cast(NVL(get_json_object(data, '$.whole_weight'),'0') as decimal(9,3)) whole_weight \
,NVL(get_json_object(data, '$.car_length'),'') car_length \
,NVL(get_json_object(data, '$.car_width'),'') car_width \
,NVL(get_json_object(data, '$.car_height'),'') car_height \
,NVL(get_json_object(data, '$.fuel_type_code'),'') fuel_type_code \
,NVL(get_json_object(data, '$.fuel_type_name'),'') fuel_type_name \
,NVL(get_json_object(data, '$.exhaust'),'') exhaust \
,NVL(get_json_object(data, '$.fuel_consumption'),'') fuel_consumption \
,NVL(get_json_object(data, '$.discharge_standard'),'') discharge_standard \
,NVL(get_json_object(data, '$.gearbox_type'),'') gearbox_type \
,NVL(get_json_object(data, '$.gearbox_name'),'') gearbox_name \
,NVL(get_json_object(data, '$.drive_method_serial'),'') drive_method_serial \
,NVL(get_json_object(data, '$.drive_method_name'),'') drive_method_name \
,NVL(get_json_object(data, '$.cover_thumbnail_img_url'),'') cover_thumbnail_img_url \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_assessment_vehicle_info' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_assessment_vehicle_info_2 = select \
NVL(get_json_object(data, '$.id'),'') id \
,cast(NVL(get_json_object(data, '$.ass_serial'),'0') as bigint) ass_serial \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_check_eff_period'),'') vehicle_check_eff_period \
,NVL(get_json_object(data, '$.comp_ins_period'),'') comp_ins_period \
,NVL(get_json_object(data, '$.vehicle_tax_period'),'') vehicle_tax_period \
,NVL(get_json_object(data, '$.business_ins_eff_period'),'') business_ins_eff_period \
,NVL(get_json_object(data, '$.vehicle_tools'),'') vehicle_tools \
,NVL(get_json_object(data, '$.vehicle_use_manual'),'') vehicle_use_manual \
,NVL(get_json_object(data, '$.vehicle_service_manual'),'') vehicle_service_manual \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_assessment_vehicle_info_2' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_assessment_vehicle_info_3 = select \
NVL(get_json_object(data, '$.id'),'') id \
,cast(NVL(get_json_object(data, '$.ass_serial'),'0') as bigint) ass_serial \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,cast(NVL(get_json_object(data, '$.service_count'),'0') as bigint) service_count \
,NVL(get_json_object(data, '$.recent_service_date'),'') recent_service_date \
,cast(NVL(get_json_object(data, '$.recent_service_mileage'),'0') as bigint) recent_service_mileage \
,NVL(get_json_object(data, '$.recent_service_dealer'),'') recent_service_dealer \
,NVL(get_json_object(data, '$.service_history_flag'),'') service_history_flag \
,NVL(get_json_object(data, '$.maintain_suggest_result'),'') maintain_suggest_result \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_assessment_vehicle_info_3' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_authen_apply = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.auth_apply_no'),'') auth_apply_no \
,NVL(get_json_object(data, '$.first_regist_date'),'') first_regist_date \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.uc_auth_no'),'') uc_auth_no \
,NVL(get_json_object(data, '$.authen_type'),'') authen_type \
,NVL(get_json_object(data, '$.other_dealer_authen_flag'),'') other_dealer_authen_flag \
,NVL(get_json_object(data, '$.authen_apply_type_code'),'') authen_apply_type_code \
,NVL(get_json_object(data, '$.auth_status'),'') auth_status \
,NVL(get_json_object(data, '$.cancel_reason_type'),'') cancel_reason_type \
,NVL(get_json_object(data, '$.auth_apply_time'),'') auth_apply_time \
,NVL(get_json_object(data, '$.authen_apply_staff_account'),'') authen_apply_staff_account \
,NVL(get_json_object(data, '$.authen_apply_staff_name'),'') authen_apply_staff_name \
,NVL(get_json_object(data, '$.authen_operate_time'),'') authen_operate_time \
,NVL(get_json_object(data, '$.authen_operate_account'),'') authen_operate_account \
,NVL(get_json_object(data, '$.authen_operate_staff_name'),'') authen_operate_staff_name \
,NVL(get_json_object(data, '$.authen_operate_opinion'),'') authen_operate_opinion \
,NVL(get_json_object(data, '$.uc_auth_time'),'') uc_auth_time \
,NVL(get_json_object(data, '$.uc_auth_due_date'),'') uc_auth_due_date \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_authen_apply' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_cust_car_buy_follow = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.negotiation_id'),'') negotiation_id \
,NVL(get_json_object(data, '$.follow_time'),'') follow_time \
,NVL(get_json_object(data, '$.car_buy_follow_status'),'') car_buy_follow_status \
,NVL(get_json_object(data, '$.follow_account'),'') follow_account \
,NVL(get_json_object(data, '$.follow_staff_name'),'') follow_staff_name \
,NVL(get_json_object(data, '$.car_buy_follow_result'),'') car_buy_follow_result \
,NVL(get_json_object(data, '$.continue_reason_type'),'') continue_reason_type \
,NVL(get_json_object(data, '$.abandon_reason_type'),'') abandon_reason_type \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.contact_diff'),'') contact_diff \
,NVL(get_json_object(data, '$.contact_content_code'),'') contact_content_code \
,cast(NVL(get_json_object(data, '$.customer_expect_price'),'0') as decimal(12,2)) customer_expect_price \
,cast(NVL(get_json_object(data, '$.prompt_price'),'0') as decimal(12,2)) prompt_price \
,NVL(get_json_object(data, '$.intn_level'),'') intn_level \
,NVL(get_json_object(data, '$.plan_follow_time'),'') plan_follow_time \
,NVL(get_json_object(data, '$.plan_follow_content_diff'),'') plan_follow_content_diff \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_cust_car_buy_follow' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_cust_car_sale_follow = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.negotiation_id'),'') negotiation_id \
,NVL(get_json_object(data, '$.follow_time'),'') follow_time \
,NVL(get_json_object(data, '$.car_sale_follow_status'),'') car_sale_follow_status \
,NVL(get_json_object(data, '$.follow_account'),'') follow_account \
,NVL(get_json_object(data, '$.follow_staff_name'),'') follow_staff_name \
,NVL(get_json_object(data, '$.car_sale_follow_result'),'') car_sale_follow_result \
,NVL(get_json_object(data, '$.continue_reason_type'),'') continue_reason_type \
,NVL(get_json_object(data, '$.abandon_reason_type'),'') abandon_reason_type \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.contact_diff'),'') contact_diff \
,NVL(get_json_object(data, '$.contact_content_code'),'') contact_content_code \
,cast(NVL(get_json_object(data, '$.customer_expect_price'),'0') as decimal(12,2)) customer_expect_price \
,cast(NVL(get_json_object(data, '$.prompt_price'),'0') as decimal(12,2)) prompt_price \
,NVL(get_json_object(data, '$.intn_level'),'') intn_level \
,NVL(get_json_object(data, '$.plan_follow_time'),'') plan_follow_time \
,NVL(get_json_object(data, '$.plan_follow_content_diff'),'') plan_follow_content_diff \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_cust_car_sale_follow' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_cust_sales_vio_settl = select \
NVL(get_json_object(data, '$.id'),'') id \
,cast(NVL(get_json_object(data, '$.buyer_sales_violation_serial'),'0') as bigint) buyer_sales_violation_serial \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.sales_contract_serial'),'') sales_contract_serial \
,cast(NVL(get_json_object(data, '$.violation_amount'),'0') as decimal(12,2)) violation_amount \
,NVL(get_json_object(data, '$.settl_status'),'') settl_status \
,NVL(get_json_object(data, '$.finish_payment_time'),'') finish_payment_time \
,NVL(get_json_object(data, '$.input_date'),'') input_date \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_cust_sales_vio_settl' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_customer_info = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.customer_id'),'') customer_id \
,NVL(get_json_object(data, '$.customer_name'),'') customer_name \
,NVL(get_json_object(data, '$.negotiation_customer_address'),'') negotiation_customer_address \
,NVL(get_json_object(data, '$.customer_mobile_phone'),'') customer_mobile_phone \
,NVL(get_json_object(data, '$.customer_fixed_tel'),'') customer_fixed_tel \
,NVL(get_json_object(data, '$.customer_postal_code'),'') customer_postal_code \
,NVL(get_json_object(data, '$.customer_email'),'') customer_email \
,NVL(get_json_object(data, '$.comm_activity_type'),'') comm_activity_type \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_customer_info' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_demand_info = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.uc_req_id'),'') uc_req_id \
,NVL(get_json_object(data, '$.uc_clue_id'),'') uc_clue_id \
,NVL(get_json_object(data, '$.uc_req_status'),'') uc_req_status \
,NVL(get_json_object(data, '$.business_diff'),'') business_diff \
,NVL(get_json_object(data, '$.sold_vehicle_model_name'),'') sold_vehicle_model_name \
,NVL(get_json_object(data, '$.icm_sold_vehicle_info_json'),'') icm_sold_vehicle_info_json \
,NVL(get_json_object(data, '$.d2c_sold_vehicle_info_json'),'') d2c_sold_vehicle_info_json \
,NVL(get_json_object(data, '$.car_buy_intn_type'),'') car_buy_intn_type \
,NVL(get_json_object(data, '$.intn_new_vehicle_info_json'),'') intn_new_vehicle_info_json \
,NVL(get_json_object(data, '$.intn_uc_type'),'') intn_uc_type \
,NVL(get_json_object(data, '$.intn_vehicle_manage_no'),'') intn_vehicle_manage_no \
,NVL(get_json_object(data, '$.intn_uc_cond_json'),'') intn_uc_cond_json \
,NVL(get_json_object(data, '$.purchase_type'),'') purchase_type \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_demand_info' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_dlr_sales_vio_settl = select \
NVL(get_json_object(data, '$.id'),'') id \
,cast(NVL(get_json_object(data, '$.seller_sales_violation_serial'),'0') as bigint) seller_sales_violation_serial \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.sales_contract_serial'),'') sales_contract_serial \
,cast(NVL(get_json_object(data, '$.violation_amount'),'0') as decimal(12,2)) violation_amount \
,NVL(get_json_object(data, '$.settl_status'),'') settl_status \
,NVL(get_json_object(data, '$.finish_payment_time'),'') finish_payment_time \
,NVL(get_json_object(data, '$.input_date'),'') input_date \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_dlr_sales_vio_settl' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_exchange_vehicle_relation = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.displace_vehicle_related_id'),'') displace_vehicle_related_id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.car_owner_name'),'') car_owner_name \
,NVL(get_json_object(data, '$.displace_vehicle_manage_no'),'') displace_vehicle_manage_no \
,NVL(get_json_object(data, '$.displace_vinno'),'') displace_vinno \
,NVL(get_json_object(data, '$.displace_vehicle_picture_url'),'') displace_vehicle_picture_url \
,NVL(get_json_object(data, '$.displace_vehicle_config_grade'),'') displace_vehicle_config_grade \
,NVL(get_json_object(data, '$.displace_vehicle_model_code'),'') displace_vehicle_model_code \
,NVL(get_json_object(data, '$.displace_vehicle_model_name'),'') displace_vehicle_model_name \
,NVL(get_json_object(data, '$.displace_vehicle_nominee_name'),'') displace_vehicle_nominee_name \
,NVL(get_json_object(data, '$.displace_vehicle_nominee_tel'),'') displace_vehicle_nominee_tel \
,NVL(get_json_object(data, '$.displace_vehicle_sales_date'),'') displace_vehicle_sales_date \
,cast(NVL(get_json_object(data, '$.displace_related_status'),'0') as bigint) displace_related_status \
,NVL(get_json_object(data, '$.displace_related_time'),'') displace_related_time \
,NVL(get_json_object(data, '$.displace_type'),'') displace_type \
,NVL(get_json_object(data, '$.displace_vehicle_related_type'),'') displace_vehicle_related_type \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_exchange_vehicle_relation' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_inventory_ledger = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.dealer_name'),'') dealer_name \
,NVL(get_json_object(data, '$.uc_stock_status'),'') uc_stock_status \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.engine_number'),'') engine_number \
,NVL(get_json_object(data, '$.license_plate_number'),'') license_plate_number \
,NVL(get_json_object(data, '$.mobile_phone'),'') mobile_phone \
,NVL(get_json_object(data, '$.telephone_number'),'') telephone_number \
,NVL(get_json_object(data, '$.customer_name'),'') customer_name \
,NVL(get_json_object(data, '$.brand_code'),'') brand_code \
,NVL(get_json_object(data, '$.brand_name'),'') brand_name \
,NVL(get_json_object(data, '$.maker_code'),'') maker_code \
,NVL(get_json_object(data, '$.maker_name'),'') maker_name \
,NVL(get_json_object(data, '$.vehicle_model_code'),'') vehicle_model_code \
,NVL(get_json_object(data, '$.vehicle_model_name'),'') vehicle_model_name \
,NVL(get_json_object(data, '$.config_grade'),'') config_grade \
,NVL(get_json_object(data, '$.first_regist_date'),'') first_regist_date \
,NVL(get_json_object(data, '$.production_date'),'') production_date \
,cast(NVL(get_json_object(data, '$.odometer'),'0') as bigint) odometer \
,NVL(get_json_object(data, '$.exhaust'),'') exhaust \
,NVL(get_json_object(data, '$.fuel_type_code'),'') fuel_type_code \
,NVL(get_json_object(data, '$.fuel_type_name'),'') fuel_type_name \
,NVL(get_json_object(data, '$.vehicle_type_code'),'') vehicle_type_code \
,NVL(get_json_object(data, '$.vehicle_category_name'),'') vehicle_category_name \
,NVL(get_json_object(data, '$.gearbox_type'),'') gearbox_type \
,NVL(get_json_object(data, '$.gearbox_name'),'') gearbox_name \
,NVL(get_json_object(data, '$.gearbox_category'),'') gearbox_category \
,NVL(get_json_object(data, '$.gearbox_category_name'),'') gearbox_category_name \
,NVL(get_json_object(data, '$.exterior_color_code_1'),'') exterior_color_code_1 \
,NVL(get_json_object(data, '$.exterior_color_1'),'') exterior_color_1 \
,NVL(get_json_object(data, '$.exterior_color_code_2'),'') exterior_color_code_2 \
,NVL(get_json_object(data, '$.exterior_color_2'),'') exterior_color_2 \
,NVL(get_json_object(data, '$.ass_date'),'') ass_date \
,NVL(get_json_object(data, '$.detect_work_order_finish_date'),'') detect_work_order_finish_date \
,NVL(get_json_object(data, '$.sales_decision_time'),'') sales_decision_time \
,cast(NVL(get_json_object(data, '$.ref_sales_price'),'0') as decimal(12,2)) ref_sales_price \
,cast(NVL(get_json_object(data, '$.plan_sales_price'),'0') as decimal(12,2)) plan_sales_price \
,cast(NVL(get_json_object(data, '$.ref_purchase_price'),'0') as decimal(12,2)) ref_purchase_price \
,cast(NVL(get_json_object(data, '$.plan_purchase_price'),'0') as decimal(12,2)) plan_purchase_price \
,cast(NVL(get_json_object(data, '$.sign_type'),'0') as bigint) sign_type \
,NVL(get_json_object(data, '$.sign_time'),'') sign_time \
,NVL(get_json_object(data, '$.cons_contract_serial'),'') cons_contract_serial \
,NVL(get_json_object(data, '$.cons_contract_status'),'') cons_contract_status \
,NVL(get_json_object(data, '$.cons_start_date'),'') cons_start_date \
,NVL(get_json_object(data, '$.cons_end_date'),'') cons_end_date \
,cast(NVL(get_json_object(data, '$.cons_expect_price'),'0') as decimal(12,0)) cons_expect_price \
,NVL(get_json_object(data, '$.cons_type_code'),'') cons_type_code \
,cast(NVL(get_json_object(data, '$.cons_service_cost'),'0') as decimal(12,0)) cons_service_cost \
,NVL(get_json_object(data, '$.purchase_contract_id'),'') purchase_contract_id \
,cast(NVL(get_json_object(data, '$.purchase_contract_status'),'0') as bigint) purchase_contract_status \
,NVL(get_json_object(data, '$.purchase_sign_date'),'') purchase_sign_date \
,NVL(get_json_object(data, '$.purchase_cancel_date'),'') purchase_cancel_date \
,cast(NVL(get_json_object(data, '$.purchase_price'),'0') as decimal(12,2)) purchase_price \
,NVL(get_json_object(data, '$.car_owner_name'),'') car_owner_name \
,NVL(get_json_object(data, '$.car_owner_id_card'),'') car_owner_id_card \
,NVL(get_json_object(data, '$.car_owner_mobile_phone'),'') car_owner_mobile_phone \
,NVL(get_json_object(data, '$.car_owner_fixed_tel'),'') car_owner_fixed_tel \
,NVL(get_json_object(data, '$.sales_method'),'') sales_method \
,cast(NVL(get_json_object(data, '$.storage_method'),'0') as bigint) storage_method \
,NVL(get_json_object(data, '$.storage_type'),'') storage_type \
,NVL(get_json_object(data, '$.elect_defect_flag'),'') elect_defect_flag \
,NVL(get_json_object(data, '$.vehicle_place_address'),'') vehicle_place_address \
,NVL(get_json_object(data, '$.related_type'),'') related_type \
,NVL(get_json_object(data, '$.related_id'),'') related_id \
,NVL(get_json_object(data, '$.plan_storage_date'),'') plan_storage_date \
,NVL(get_json_object(data, '$.actual_storage_date'),'') actual_storage_date \
,NVL(get_json_object(data, '$.transfer_flag'),'') transfer_flag \
,NVL(get_json_object(data, '$.plan_transfer_date'),'') plan_transfer_date \
,NVL(get_json_object(data, '$.actual_transfer_date'),'') actual_transfer_date \
,NVL(get_json_object(data, '$.delivery_finish_date'),'') delivery_finish_date \
,cast(NVL(get_json_object(data, '$.dealer_pay_transfer_cost'),'0') as decimal(12,2)) dealer_pay_transfer_cost \
,NVL(get_json_object(data, '$.dealer_new_car_owner_name'),'') dealer_new_car_owner_name \
,NVL(get_json_object(data, '$.dealer_new_car_owner_id_card'),'') dealer_new_car_owner_id_card \
,NVL(get_json_object(data, '$.dlr_new_car_owner_mobile_phone'),'') dlr_new_car_owner_mobile_phone \
,NVL(get_json_object(data, '$.dealer_new_car_owner_address'),'') dealer_new_car_owner_address \
,NVL(get_json_object(data, '$.dlr_new_car_owner_lpn'),'') dlr_new_car_owner_lpn \
,cast(NVL(get_json_object(data, '$.displace_related_operate_flag'),'0') as bigint) displace_related_operate_flag \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_inventory_ledger' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_inventory_ledger_transfer = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.plan_transfer_date'),'') plan_transfer_date \
,NVL(get_json_object(data, '$.actual_transfer_date'),'') actual_transfer_date \
,cast(NVL(get_json_object(data, '$.dealer_pay_transfer_cost'),'0') as decimal(12,2)) dealer_pay_transfer_cost \
,NVL(get_json_object(data, '$.dealer_new_car_owner_name'),'') dealer_new_car_owner_name \
,NVL(get_json_object(data, '$.dealer_new_car_owner_id_card'),'') dealer_new_car_owner_id_card \
,NVL(get_json_object(data, '$.dlr_new_car_owner_mobile_phone'),'') dlr_new_car_owner_mobile_phone \
,NVL(get_json_object(data, '$.dealer_new_car_owner_address'),'') dealer_new_car_owner_address \
,NVL(get_json_object(data, '$.dlr_new_car_owner_lpn'),'') dlr_new_car_owner_lpn \
,NVL(get_json_object(data, '$.cancel_time'),'') cancel_time \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_inventory_ledger_transfer' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_mst_dealer = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.dealer_name'),'') dealer_name \
,NVL(get_json_object(data, '$.dealer_region'),'') dealer_region \
,NVL(get_json_object(data, '$.dealer_address'),'') dealer_address \
,NVL(get_json_object(data, '$.dealer_contact_tel'),'') dealer_contact_tel \
,NVL(get_json_object(data, '$.dealer_website'),'') dealer_website \
,NVL(get_json_object(data, '$.dealer_picture_url'),'') dealer_picture_url \
,NVL(get_json_object(data, '$.dealer_fax'),'') dealer_fax \
,NVL(get_json_object(data, '$.uc_dept_staff_telephone'),'') uc_dept_staff_telephone \
,NVL(get_json_object(data, '$.business_start_time'),'') business_start_time \
,NVL(get_json_object(data, '$.business_end_time'),'') business_end_time \
,NVL(get_json_object(data, '$.dealer_theme_slogan'),'') dealer_theme_slogan \
,NVL(get_json_object(data, '$.map_img'),'') map_img \
,NVL(get_json_object(data, '$.region_code'),'') region_code \
,NVL(get_json_object(data, '$.region_name'),'') region_name \
,NVL(get_json_object(data, '$.province_code'),'') province_code \
,NVL(get_json_object(data, '$.province_name'),'') province_name \
,NVL(get_json_object(data, '$.city_code'),'') city_code \
,NVL(get_json_object(data, '$.city_name'),'') city_name \
,NVL(get_json_object(data, '$.ass_hotline'),'') ass_hotline \
,NVL(get_json_object(data, '$.sales_hotline'),'') sales_hotline \
,NVL(get_json_object(data, '$.dlr_image_display'),'') dlr_image_display \
,NVL(get_json_object(data, '$.dealer_thumbnail_img'),'') dealer_thumbnail_img \
,NVL(get_json_object(data, '$.map_display_img'),'') map_display_img \
,NVL(get_json_object(data, '$.map_thumbnail_img'),'') map_thumbnail_img \
,NVL(get_json_object(data, '$.contacts_account'),'') contacts_account \
,NVL(get_json_object(data, '$.dealer_prefix'),'') dealer_prefix \
,NVL(get_json_object(data, '$.shop_enter_flag'),'') shop_enter_flag \
,NVL(get_json_object(data, '$.displace_cert_flag'),'') displace_cert_flag \
,NVL(get_json_object(data, '$.displace_cert_effective_date'),'') displace_cert_effective_date \
,NVL(get_json_object(data, '$.displace_cert_effective_flag'),'') displace_cert_effective_flag \
,NVL(get_json_object(data, '$.retail_cert_flag'),'') retail_cert_flag \
,NVL(get_json_object(data, '$.retail_cert_effective_date'),'') retail_cert_effective_date \
,NVL(get_json_object(data, '$.retail_cert_effective_flag'),'') retail_cert_effective_flag \
,NVL(get_json_object(data, '$.after_sales_service_flag'),'') after_sales_service_flag \
,NVL(get_json_object(data, '$.authen_service_flag'),'') authen_service_flag \
,NVL(get_json_object(data, '$.authen_service_contact_account'),'') authen_service_contact_account \
,NVL(get_json_object(data, '$.authen_service_contact_name'),'') authen_service_contact_name \
,NVL(get_json_object(data, '$.authen_service_contact_telephone'),'') authen_service_contact_telephone \
,cast(NVL(get_json_object(data, '$.authen_service_cost'),'0') as decimal(12,2)) authen_service_cost \
,cast(NVL(get_json_object(data, '$.maker_suggest_service_cost'),'0') as decimal(12,2)) maker_suggest_service_cost \
,NVL(get_json_object(data, '$.authen_service_remark'),'') authen_service_remark \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_mst_dealer' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_mst_elect_defect = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.detect_position_type'),'') detect_position_type \
,NVL(get_json_object(data, '$.detect_position_group_code'),'') detect_position_group_code \
,NVL(get_json_object(data, '$.detect_position_code'),'') detect_position_code \
,cast(NVL(get_json_object(data, '$.sort'),'0') as bigint) sort \
,NVL(get_json_object(data, '$.defect_item_name'),'') defect_item_name \
,NVL(get_json_object(data, '$.defect_item_name_local'),'') defect_item_name_local \
,NVL(get_json_object(data, '$.defect_item_name_2'),'') defect_item_name_2 \
,NVL(get_json_object(data, '$.defect_item_name_2_local'),'') defect_item_name_2_local \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_mst_elect_defect' and topic = 'sale_dms_supply_prd' 



tg_dms_uc_t_uc_negotiation = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.negotiation_id'),'') negotiation_id \
,NVL(get_json_object(data, '$.customer_id'),'') customer_id \
,NVL(get_json_object(data, '$.customer_name'),'') customer_name \
,NVL(get_json_object(data, '$.negotiation_customer_address'),'') negotiation_customer_address \
,NVL(get_json_object(data, '$.customer_mobile_phone'),'') customer_mobile_phone \
,NVL(get_json_object(data, '$.customer_fixed_tel'),'') customer_fixed_tel \
,NVL(get_json_object(data, '$.customer_postal_code'),'') customer_postal_code \
,NVL(get_json_object(data, '$.activity_manager_account'),'') activity_manager_account \
,NVL(get_json_object(data, '$.activity_manager_name'),'') activity_manager_name \
,NVL(get_json_object(data, '$.source_code'),'') source_code \
,NVL(get_json_object(data, '$.source_name'),'') source_name \
,NVL(get_json_object(data, '$.source_first_level_code'),'') source_first_level_code \
,NVL(get_json_object(data, '$.source_first_level_name'),'') source_first_level_name \
,NVL(get_json_object(data, '$.source_second_level_code'),'') source_second_level_code \
,NVL(get_json_object(data, '$.source_second_level_name'),'') source_second_level_name \
,NVL(get_json_object(data, '$.introducer_dept_code'),'') introducer_dept_code \
,NVL(get_json_object(data, '$.introducer_dept_name'),'') introducer_dept_name \
,NVL(get_json_object(data, '$.introducer_account'),'') introducer_account \
,NVL(get_json_object(data, '$.introducer_name'),'') introducer_name \
,NVL(get_json_object(data, '$.intn_level'),'') intn_level \
,NVL(get_json_object(data, '$.negotiation_status'),'') negotiation_status \
,NVL(get_json_object(data, '$.manager_dist_flag'),'') manager_dist_flag \
,NVL(get_json_object(data, '$.uc_req_id'),'') uc_req_id \
,NVL(get_json_object(data, '$.uc_clue_id'),'') uc_clue_id \
,NVL(get_json_object(data, '$.clue_create_time'),'') clue_create_time \
,NVL(get_json_object(data, '$.follow_account'),'') follow_account \
,NVL(get_json_object(data, '$.follow_staff_name'),'') follow_staff_name \
,NVL(get_json_object(data, '$.follow_time'),'') follow_time \
,NVL(get_json_object(data, '$.next_follow_time'),'') next_follow_time \
,NVL(get_json_object(data, '$.business_diff'),'') business_diff \
,NVL(get_json_object(data, '$.intn_type'),'') intn_type \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.cover_thumbnail_img_url'),'') cover_thumbnail_img_url \
,NVL(get_json_object(data, '$.brand_code'),'') brand_code \
,NVL(get_json_object(data, '$.brand_name'),'') brand_name \
,NVL(get_json_object(data, '$.maker_code'),'') maker_code \
,NVL(get_json_object(data, '$.maker_name'),'') maker_name \
,NVL(get_json_object(data, '$.vehicle_model_code'),'') vehicle_model_code \
,NVL(get_json_object(data, '$.vehicle_model_name'),'') vehicle_model_name \
,NVL(get_json_object(data, '$.vehicle_grade'),'') vehicle_grade \
,NVL(get_json_object(data, '$.license_plate_number'),'') license_plate_number \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.first_regist_date'),'') first_regist_date \
,NVL(get_json_object(data, '$.ass_date'),'') ass_date \
,NVL(get_json_object(data, '$.purchase_sign_date'),'') purchase_sign_date \
,NVL(get_json_object(data, '$.cons_sign_date'),'') cons_sign_date \
,cast(NVL(get_json_object(data, '$.min_down_payment'),'0') as decimal(12,2)) min_down_payment \
,cast(NVL(get_json_object(data, '$.max_down_payment'),'0') as decimal(12,2)) max_down_payment \
,cast(NVL(get_json_object(data, '$.min_monthly_payment'),'0') as decimal(12,2)) min_monthly_payment \
,cast(NVL(get_json_object(data, '$.max_monthly_payment'),'0') as decimal(12,2)) max_monthly_payment \
,NVL(get_json_object(data, '$.sales_sign_date'),'') sales_sign_date \
,NVL(get_json_object(data, '$.sales_negotiation_id'),'') sales_negotiation_id \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_negotiation' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_new_vhcl_exchange_achv = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.new_vehicle_displace_achv_id'),'') new_vehicle_displace_achv_id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.license_plate_number'),'') license_plate_number \
,NVL(get_json_object(data, '$.achv_apply_status'),'') achv_apply_status \
,NVL(get_json_object(data, '$.present_type'),'') present_type \
,NVL(get_json_object(data, '$.displace_vehicle_related_id'),'') displace_vehicle_related_id \
,NVL(get_json_object(data, '$.displace_vehicle_vinno'),'') displace_vehicle_vinno \
,NVL(get_json_object(data, '$.displace_vehicle_nominee_name'),'') displace_vehicle_nominee_name \
,NVL(get_json_object(data, '$.displace_vehicle_nominee_tel'),'') displace_vehicle_nominee_tel \
,NVL(get_json_object(data, '$.displace_vehicle_sales_date'),'') displace_vehicle_sales_date \
,NVL(get_json_object(data, '$.displace_vehicle_picture_url'),'') displace_vehicle_picture_url \
,NVL(get_json_object(data, '$.displace_vehicle_config_grade'),'') displace_vehicle_config_grade \
,NVL(get_json_object(data, '$.displace_vehicle_model_code'),'') displace_vehicle_model_code \
,NVL(get_json_object(data, '$.displace_vehicle_model_name'),'') displace_vehicle_model_name \
,NVL(get_json_object(data, '$.purchase_obtain_evidence_id'),'') purchase_obtain_evidence_id \
,NVL(get_json_object(data, '$.invoice_number'),'') invoice_number \
,cast(NVL(get_json_object(data, '$.invoice_amount'),'0') as decimal(10,2)) invoice_amount \
,NVL(get_json_object(data, '$.actual_transfer_date'),'') actual_transfer_date \
,NVL(get_json_object(data, '$.seller_name'),'') seller_name \
,NVL(get_json_object(data, '$.seller_id_card'),'') seller_id_card \
,NVL(get_json_object(data, '$.buyer_name'),'') buyer_name \
,NVL(get_json_object(data, '$.buyer_id_card'),'') buyer_id_card \
,NVL(get_json_object(data, '$.apply_time'),'') apply_time \
,NVL(get_json_object(data, '$.apply_staff_account'),'') apply_staff_account \
,NVL(get_json_object(data, '$.apply_staff_name'),'') apply_staff_name \
,NVL(get_json_object(data, '$.approve_time'),'') approve_time \
,NVL(get_json_object(data, '$.approve_staff_account'),'') approve_staff_account \
,NVL(get_json_object(data, '$.approve_staff_name'),'') approve_staff_name \
,NVL(get_json_object(data, '$.approve_opinion'),'') approve_opinion \
,NVL(get_json_object(data, '$.cancel_time'),'') cancel_time \
,NVL(get_json_object(data, '$.cancel_staff_account'),'') cancel_staff_account \
,NVL(get_json_object(data, '$.cancel_staff_name'),'') cancel_staff_name \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_new_vhcl_exchange_achv' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_purchase_contract_ledger = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.purchase_contract_id'),'') purchase_contract_id \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.car_owner_type'),'') car_owner_type \
,NVL(get_json_object(data, '$.car_owner_name'),'') car_owner_name \
,NVL(get_json_object(data, '$.car_owner_id_card'),'') car_owner_id_card \
,NVL(get_json_object(data, '$.car_owner_mobile_phone'),'') car_owner_mobile_phone \
,NVL(get_json_object(data, '$.car_owner_tel'),'') car_owner_tel \
,NVL(get_json_object(data, '$.car_owner_zip'),'') car_owner_zip \
,NVL(get_json_object(data, '$.car_owner_address'),'') car_owner_address \
,NVL(get_json_object(data, '$.car_owner_email'),'') car_owner_email \
,NVL(get_json_object(data, '$.license_plate_number'),'') license_plate_number \
,NVL(get_json_object(data, '$.purchase_sign_date'),'') purchase_sign_date \
,NVL(get_json_object(data, '$.purchase_cancel_date'),'') purchase_cancel_date \
,NVL(get_json_object(data, '$.cons_sign_date'),'') cons_sign_date \
,cast(NVL(get_json_object(data, '$.purchase_price'),'0') as decimal(12,2)) purchase_price \
,NVL(get_json_object(data, '$.plan_sales_method'),'') plan_sales_method \
,NVL(get_json_object(data, '$.whsle_target_code'),'') whsle_target_code \
,NVL(get_json_object(data, '$.whsle_target_type'),'') whsle_target_type \
,NVL(get_json_object(data, '$.plan_storage_date'),'') plan_storage_date \
,NVL(get_json_object(data, '$.plan_transfer_date'),'') plan_transfer_date \
,cast(NVL(get_json_object(data, '$.purchase_contract_status'),'0') as bigint) purchase_contract_status \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_purchase_contract_ledger' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_purchase_contract_other_cost = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.purchase_other_cost_id'),'') purchase_other_cost_id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.purchase_contract_id'),'') purchase_contract_id \
,NVL(get_json_object(data, '$.cost_item_code'),'') cost_item_code \
,NVL(get_json_object(data, '$.cost_item_name'),'') cost_item_name \
,cast(NVL(get_json_object(data, '$.cost_amount'),'0') as decimal(12,2)) cost_amount \
,NVL(get_json_object(data, '$.cost_input_date'),'') cost_input_date \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_purchase_contract_other_cost' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_repair_authen_ledger = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.repair_authen_id'),'') repair_authen_id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.ledger_status'),'') ledger_status \
,NVL(get_json_object(data, '$.uc_auth_due_date'),'') uc_auth_due_date \
,NVL(get_json_object(data, '$.uc_auth_time'),'') uc_auth_time \
,NVL(get_json_object(data, '$.authen_apply_type_code'),'') authen_apply_type_code \
,NVL(get_json_object(data, '$.authen_type'),'') authen_type \
,NVL(get_json_object(data, '$.auth_apply_time'),'') auth_apply_time \
,NVL(get_json_object(data, '$.auth_apply_by_code'),'') auth_apply_by_code \
,NVL(get_json_object(data, '$.uc_auth_no'),'') uc_auth_no \
,NVL(get_json_object(data, '$.gtmc_vehicle_flag'),'') gtmc_vehicle_flag \
,NVL(get_json_object(data, '$.delivery_finish_date'),'') delivery_finish_date \
,NVL(get_json_object(data, '$.sales_method'),'') sales_method \
,NVL(get_json_object(data, '$.first_regist_date'),'') first_regist_date \
,NVL(get_json_object(data, '$.ass_date'),'') ass_date \
,NVL(get_json_object(data, '$.maker_code'),'') maker_code \
,NVL(get_json_object(data, '$.maker_name'),'') maker_name \
,cast(NVL(get_json_object(data, '$.odometer'),'0') as bigint) odometer \
,NVL(get_json_object(data, '$.vehicle_type_code'),'') vehicle_type_code \
,NVL(get_json_object(data, '$.vehicle_category_name'),'') vehicle_category_name \
,NVL(get_json_object(data, '$.vehicle_grade'),'') vehicle_grade \
,NVL(get_json_object(data, '$.collision_accident_diff'),'') collision_accident_diff \
,NVL(get_json_object(data, '$.water_accident_diff'),'') water_accident_diff \
,NVL(get_json_object(data, '$.fire_accident_diff'),'') fire_accident_diff \
,NVL(get_json_object(data, '$.excessive_modified_diff'),'') excessive_modified_diff \
,NVL(get_json_object(data, '$.wty_type'),'') wty_type \
,NVL(get_json_object(data, '$.instrument_tampering_flag'),'') instrument_tampering_flag \
,NVL(get_json_object(data, '$.elect_level'),'') elect_level \
,NVL(get_json_object(data, '$.accident_level'),'') accident_level \
,NVL(get_json_object(data, '$.exhaust'),'') exhaust \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_repair_authen_ledger' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_repair_cost = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.repair_id'),'') repair_id \
,NVL(get_json_object(data, '$.repair_item_id'),'') repair_item_id \
,cast(NVL(get_json_object(data, '$.project_source'),'0') as bigint) project_source \
,NVL(get_json_object(data, '$.repair_item_type'),'') repair_item_type \
,NVL(get_json_object(data, '$.repair_item_type_name'),'') repair_item_type_name \
,NVL(get_json_object(data, '$.repair_item_code'),'') repair_item_code \
,NVL(get_json_object(data, '$.repair_item_name'),'') repair_item_name \
,cast(NVL(get_json_object(data, '$.repair_item_cost'),'0') as decimal(12,2)) repair_item_cost \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_repair_cost' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_repair_item = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.repair_item_id'),'') repair_item_id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.repair_item_code'),'') repair_item_code \
,NVL(get_json_object(data, '$.repair_item_name'),'') repair_item_name \
,NVL(get_json_object(data, '$.repair_item_type'),'') repair_item_type \
,NVL(get_json_object(data, '$.repair_item_type_name'),'') repair_item_type_name \
,cast(NVL(get_json_object(data, '$.repair_item_cost'),'0') as decimal(12,2)) repair_item_cost \
,cast(NVL(get_json_object(data, '$.retail_bench_cost'),'0') as decimal(12,2)) retail_bench_cost \
,cast(NVL(get_json_object(data, '$.default_show_flag'),'0') as bigint) default_show_flag \
,cast(NVL(get_json_object(data, '$.brand_limit_diff'),'0') as bigint) brand_limit_diff \
,cast(NVL(get_json_object(data, '$.default_choose_flag'),'0') as bigint) default_choose_flag \
,cast(NVL(get_json_object(data, '$.sq'),'0') as bigint) sq \
,NVL(get_json_object(data, '$.repair_item_status'),'') repair_item_status \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_repair_item' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_uc_t_uc_sales_contract = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.sales_contract_serial'),'') sales_contract_serial \
,NVL(get_json_object(data, '$.old_nominee_name'),'') old_nominee_name \
,NVL(get_json_object(data, '$.old_nominee_telephone'),'') old_nominee_telephone \
,NVL(get_json_object(data, '$.old_nominee_id_card'),'') old_nominee_id_card \
,NVL(get_json_object(data, '$.old_nominee_address'),'') old_nominee_address \
,NVL(get_json_object(data, '$.old_license_plate_number'),'') old_license_plate_number \
,NVL(get_json_object(data, '$.sales_method'),'') sales_method \
,NVL(get_json_object(data, '$.retail_customer_type'),'') retail_customer_type \
,NVL(get_json_object(data, '$.retail_customer_name'),'') retail_customer_name \
,NVL(get_json_object(data, '$.retail_customer_id_card'),'') retail_customer_id_card \
,NVL(get_json_object(data, '$.retail_customer_mobile_phone'),'') retail_customer_mobile_phone \
,NVL(get_json_object(data, '$.retail_customer_telephone'),'') retail_customer_telephone \
,NVL(get_json_object(data, '$.retail_customer_postal_code'),'') retail_customer_postal_code \
,NVL(get_json_object(data, '$.retail_customer_address'),'') retail_customer_address \
,NVL(get_json_object(data, '$.customer_id'),'') customer_id \
,NVL(get_json_object(data, '$.negotiation_id'),'') negotiation_id \
,NVL(get_json_object(data, '$.whsle_target_type'),'') whsle_target_type \
,NVL(get_json_object(data, '$.whsle_car_trader_id'),'') whsle_car_trader_id \
,NVL(get_json_object(data, '$.whsle_dealer_code'),'') whsle_dealer_code \
,NVL(get_json_object(data, '$.whsle_branch_code'),'') whsle_branch_code \
,NVL(get_json_object(data, '$.whsle_coop_platform_code'),'') whsle_coop_platform_code \
,NVL(get_json_object(data, '$.whsle_target_name'),'') whsle_target_name \
,NVL(get_json_object(data, '$.whsle_contact_name'),'') whsle_contact_name \
,NVL(get_json_object(data, '$.whsle_target_mobile_phone'),'') whsle_target_mobile_phone \
,NVL(get_json_object(data, '$.whsle_target_telephone'),'') whsle_target_telephone \
,NVL(get_json_object(data, '$.whsle_target_address'),'') whsle_target_address \
,NVL(get_json_object(data, '$.whsle_target_postal_code'),'') whsle_target_postal_code \
,NVL(get_json_object(data, '$.whsle_target_email'),'') whsle_target_email \
,NVL(get_json_object(data, '$.vehicle_user_type'),'') vehicle_user_type \
,NVL(get_json_object(data, '$.vehicle_user_name'),'') vehicle_user_name \
,NVL(get_json_object(data, '$.vehicle_user_id_card'),'') vehicle_user_id_card \
,NVL(get_json_object(data, '$.vehicle_user_mobile'),'') vehicle_user_mobile \
,NVL(get_json_object(data, '$.vehicle_user_telephone'),'') vehicle_user_telephone \
,NVL(get_json_object(data, '$.vehicle_user_postal_code'),'') vehicle_user_postal_code \
,NVL(get_json_object(data, '$.vehicle_user_address'),'') vehicle_user_address \
,cast(NVL(get_json_object(data, '$.vehicle_body_price_income'),'0') as decimal(12,2)) vehicle_body_price_income \
,cast(NVL(get_json_object(data, '$.vehicle_body_price_payment'),'0') as decimal(12,2)) vehicle_body_price_payment \
,cast(NVL(get_json_object(data, '$.discount'),'0') as decimal(12,2)) discount \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_sales_contract' and topic = 'sale_dms_supply_prd' 



tg_dms_uc_t_uc_sales_contract_cost = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.sales_contract_serial'),'') sales_contract_serial \
,NVL(get_json_object(data, '$.cost_item_code'),'') cost_item_code \
,NVL(get_json_object(data, '$.cost_item_name'),'') cost_item_name \
,NVL(get_json_object(data, '$.cost_type'),'') cost_type \
,cast(NVL(get_json_object(data, '$.unit_price'),'0') as decimal(10,2)) unit_price \
,cast(NVL(get_json_object(data, '$.quantity'),'0') as bigint) quantity \
,cast(NVL(get_json_object(data, '$.sales_price'),'0') as decimal(12,2)) sales_price \
,cast(NVL(get_json_object(data, '$.cost_item_cost_price'),'0') as decimal(12,2)) cost_item_cost_price \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_sales_contract_cost' and topic = 'sale_dms_supply_prd' 



tg_dms_uc_t_uc_sales_ledger = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.sales_contract_serial'),'') sales_contract_serial \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.sales_status'),'') sales_status \
,NVL(get_json_object(data, '$.sales_method'),'') sales_method \
,NVL(get_json_object(data, '$.sales_sign_date'),'') sales_sign_date \
,NVL(get_json_object(data, '$.sales_sign_staff_account'),'') sales_sign_staff_account \
,NVL(get_json_object(data, '$.temp_save_date'),'') temp_save_date \
,NVL(get_json_object(data, '$.temp_save_staff_account'),'') temp_save_staff_account \
,cast(NVL(get_json_object(data, '$.sales_total_income'),'0') as decimal(12,2)) sales_total_income \
,cast(NVL(get_json_object(data, '$.sales_total_payment'),'0') as decimal(12,2)) sales_total_payment \
,NVL(get_json_object(data, '$.payment_plan_date'),'') payment_plan_date \
,NVL(get_json_object(data, '$.payment_finish_date'),'') payment_finish_date \
,NVL(get_json_object(data, '$.invoice_number'),'') invoice_number \
,NVL(get_json_object(data, '$.invoice_number_login_date'),'') invoice_number_login_date \
,NVL(get_json_object(data, '$.authen_flag'),'') authen_flag \
,NVL(get_json_object(data, '$.uc_auth_no'),'') uc_auth_no \
,NVL(get_json_object(data, '$.uc_auth_due_date'),'') uc_auth_due_date \
,NVL(get_json_object(data, '$.displace_flag'),'') displace_flag \
,NVL(get_json_object(data, '$.delivery_plan_date'),'') delivery_plan_date \
,NVL(get_json_object(data, '$.delivery_finish_date'),'') delivery_finish_date \
,NVL(get_json_object(data, '$.delivery_cancel_date'),'') delivery_cancel_date \
,cast(NVL(get_json_object(data, '$.dealer_payment_transfer_cost'),'0') as decimal(12,2)) dealer_payment_transfer_cost \
,NVL(get_json_object(data, '$.delivery_license_plate_number'),'') delivery_license_plate_number \
,NVL(get_json_object(data, '$.sales_cancel_date'),'') sales_cancel_date \
,NVL(get_json_object(data, '$.sales_cancel_staff_account'),'') sales_cancel_staff_account \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,NVL(get_json_object(data, '$.remark'),'') remark \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_uc_sales_ledger' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_cs_t_vhcl_common_model_grade_config = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.uc_franchise_code'),'') uc_franchise_code \
,NVL(get_json_object(data, '$.uc_franchise_name'),'') uc_franchise_name \
,NVL(get_json_object(data, '$.maker_code'),'') maker_code \
,NVL(get_json_object(data, '$.maker_name'),'') maker_name \
,NVL(get_json_object(data, '$.vehicle_model'),'') vehicle_model \
,NVL(get_json_object(data, '$.vehicle_model_name'),'') vehicle_model_name \
,NVL(get_json_object(data, '$.grade_name'),'') grade_name \
,NVL(get_json_object(data, '$.vehicle_detail_config_id'),'') vehicle_detail_config_id \
,NVL(get_json_object(data, '$.vehicle_detail_config_json'),'') vehicle_detail_config_json \
,NVL(get_json_object(data, '$.sales_start_date'),'') sales_start_date \
,NVL(get_json_object(data, '$.sales_end_date'),'') sales_end_date \
,NVL(get_json_object(data, '$.vehicle_type_code'),'') vehicle_type_code \
,NVL(get_json_object(data, '$.vehicle_category_name'),'') vehicle_category_name \
,cast(NVL(get_json_object(data, '$.new_vehicle_price'),'0') as decimal(12,2)) new_vehicle_price \
,NVL(get_json_object(data, '$.exhaust'),'') exhaust \
,NVL(get_json_object(data, '$.national_standard_model'),'') national_standard_model \
,NVL(get_json_object(data, '$.leave_factory_time'),'') leave_factory_time \
,NVL(get_json_object(data, '$.vehicle_model_publish_year'),'') vehicle_model_publish_year \
,NVL(get_json_object(data, '$.country_release_code'),'') country_release_code \
,NVL(get_json_object(data, '$.production_status'),'') production_status \
,NVL(get_json_object(data, '$.production_time'),'') production_time \
,NVL(get_json_object(data, '$.stop_production_time'),'') stop_production_time \
,NVL(get_json_object(data, '$.vehicle_model_info'),'') vehicle_model_info \
,NVL(get_json_object(data, '$.vehicle_model_category'),'') vehicle_model_category \
,NVL(get_json_object(data, '$.gear_type'),'') gear_type \
,NVL(get_json_object(data, '$.car_length'),'') car_length \
,NVL(get_json_object(data, '$.car_width'),'') car_width \
,NVL(get_json_object(data, '$.car_height'),'') car_height \
,NVL(get_json_object(data, '$.vehicle_door_number_serial'),'') vehicle_door_number_serial \
,NVL(get_json_object(data, '$.rate_allow_people_serial'),'') rate_allow_people_serial \
,NVL(get_json_object(data, '$.discharge_standard_serial'),'') discharge_standard_serial \
,NVL(get_json_object(data, '$.gearbox_type'),'') gearbox_type \
,NVL(get_json_object(data, '$.drive_method_serial'),'') drive_method_serial \
,NVL(get_json_object(data, '$.energy_type_code'),'') energy_type_code \
,NVL(get_json_object(data, '$.fuel_consumption'),'') fuel_consumption \
,cast(NVL(get_json_object(data, '$.whole_weight'),'0') as decimal(9,3)) whole_weight \
,NVL(get_json_object(data, '$.gearbox_category'),'') gearbox_category \
,NVL(get_json_object(data, '$.gearbox_category_name'),'') gearbox_category_name \
,cast(NVL(get_json_object(data, '$.version'),'0') as bigint) version \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,NVL(get_json_object(data, '$.remark'),'') remark \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_vhcl_common_model_grade_config' and topic = 'sale_dms_supply_prd' 



tg_dms_cs_t_salesed_vehicle_ledger = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.customer_order_number'),'') customer_order_number \
,NVL(get_json_object(data, '$.car_owner_cert_number'),'') car_owner_cert_number \
,NVL(get_json_object(data, '$.car_owner_type'),'') car_owner_type \
,NVL(get_json_object(data, '$.car_owner_name'),'') car_owner_name \
,NVL(get_json_object(data, '$.sales_dealer_code'),'') sales_dealer_code \
,NVL(get_json_object(data, '$.sales_company_name'),'') sales_company_name \
,NVL(get_json_object(data, '$.sales_company_taxpayer_number'),'') sales_company_taxpayer_number \
,NVL(get_json_object(data, '$.delivery_dealer_code'),'') delivery_dealer_code \
,NVL(get_json_object(data, '$.sales_adviser_code'),'') sales_adviser_code \
,NVL(get_json_object(data, '$.delivery_consultant_code'),'') delivery_consultant_code \
,NVL(get_json_object(data, '$.after_sales_adviser_code'),'') after_sales_adviser_code \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.vehicle_flag'),'') vehicle_flag \
,NVL(get_json_object(data, '$.vehicle_type'),'') vehicle_type \
,NVL(get_json_object(data, '$.vinno_d7'),'') vinno_d7 \
,NVL(get_json_object(data, '$.urn'),'') urn \
,NVL(get_json_object(data, '$.vehicle_name'),'') vehicle_name \
,NVL(get_json_object(data, '$.vehicle_name_code'),'') vehicle_name_code \
,NVL(get_json_object(data, '$.vehicle_model_name'),'') vehicle_model_name \
,NVL(get_json_object(data, '$.vehicle_model'),'') vehicle_model \
,NVL(get_json_object(data, '$.stand_vehicle_model'),'') stand_vehicle_model \
,NVL(get_json_object(data, '$.vehicle_model_subdivision'),'') vehicle_model_subdivision \
,NVL(get_json_object(data, '$.vehicle_sfx_code'),'') vehicle_sfx_code \
,NVL(get_json_object(data, '$.grade_code'),'') grade_code \
,NVL(get_json_object(data, '$.grade_name'),'') grade_name \
,NVL(get_json_object(data, '$.invoice_grade'),'') invoice_grade \
,NVL(get_json_object(data, '$.invoice_vehicle_name'),'') invoice_vehicle_name \
,NVL(get_json_object(data, '$.vehicle_color_code'),'') vehicle_color_code \
,NVL(get_json_object(data, '$.vehicle_color_name'),'') vehicle_color_name \
,NVL(get_json_object(data, '$.interior_color_code'),'') interior_color_code \
,NVL(get_json_object(data, '$.interior_color_name'),'') interior_color_name \
,NVL(get_json_object(data, '$.engine'),'') engine \
,NVL(get_json_object(data, '$.engine_number'),'') engine_number \
,NVL(get_json_object(data, '$.engine_type'),'') engine_type \
,NVL(get_json_object(data, '$.exhaust'),'') exhaust \
,NVL(get_json_object(data, '$.gearbox'),'') gearbox \
,cast(NVL(get_json_object(data, '$.delivery_odometer_value'),'0') as decimal(10,2)) delivery_odometer_value \
,cast(NVL(get_json_object(data, '$.vehicle_odometer_value'),'0') as decimal(10,2)) vehicle_odometer_value \
,NVL(get_json_object(data, '$.vehicle_use'),'') vehicle_use \
,NVL(get_json_object(data, '$.sales_diff'),'') sales_diff \
,NVL(get_json_object(data, '$.factory_code'),'') factory_code \
,NVL(get_json_object(data, '$.id_line'),'') id_line \
,NVL(get_json_object(data, '$.production_date'),'') production_date \
,cast(NVL(get_json_object(data, '$.sales_price'),'0') as decimal(12,2)) sales_price \
,cast(NVL(get_json_object(data, '$.new_vehicle_retail_price'),'0') as decimal(12,2)) new_vehicle_retail_price \
,NVL(get_json_object(data, '$.fuel_type'),'') fuel_type \
,NVL(get_json_object(data, '$.fuel_type_name'),'') fuel_type_name \
,NVL(get_json_object(data, '$.final_inspt_date'),'') final_inspt_date \
,NVL(get_json_object(data, '$.outday_date'),'') outday_date \
,NVL(get_json_object(data, '$.match_date'),'') match_date \
,NVL(get_json_object(data, '$.sales_date'),'') sales_date \
,NVL(get_json_object(data, '$.pds_date'),'') pds_date \
,NVL(get_json_object(data, '$.delivery_date'),'') delivery_date \
,NVL(get_json_object(data, '$.warranty_status'),'') warranty_status \
,NVL(get_json_object(data, '$.regist_number'),'') regist_number \
,NVL(get_json_object(data, '$.vehicle_certificate_number'),'') vehicle_certificate_number \
,NVL(get_json_object(data, '$.invoice_number'),'') invoice_number \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,NVL(get_json_object(data, '$.remark'),'') remark \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.vinno_after_6_digit'),'') vinno_after_6_digit \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_salesed_vehicle_ledger' and topic = 'sale_dms_supply_prd' 



tg_dms_cs_t_maintain_uc_repair_entrust = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.dealer_code'),'') dealer_code \
,NVL(get_json_object(data, '$.branch_code'),'') branch_code \
,NVL(get_json_object(data, '$.vehicle_manage_no'),'') vehicle_manage_no \
,NVL(get_json_object(data, '$.vinno'),'') vinno \
,NVL(get_json_object(data, '$.repair_status'),'') repair_status \
,cast(NVL(get_json_object(data, '$.repair_type'),'0') as bigint) repair_type \
,NVL(get_json_object(data, '$.dealer_repair_start_time'),'') dealer_repair_start_time \
,NVL(get_json_object(data, '$.dealer_repair_end_time'),'') dealer_repair_end_time \
,cast(NVL(get_json_object(data, '$.user_pay_repair_cost'),'0') as decimal(12,2)) user_pay_repair_cost \
,cast(NVL(get_json_object(data, '$.dealer_repair_cost'),'0') as decimal(12,2)) dealer_repair_cost \
,NVL(get_json_object(data, '$.repair_entrust_id'),'') repair_entrust_id \
,NVL(get_json_object(data, '$.repair_entrust_date'),'') repair_entrust_date \
,NVL(get_json_object(data, '$.repair_entrust_appraiser_code'),'') repair_entrust_appraiser_code \
,NVL(get_json_object(data, '$.repair_entrust_memo'),'') repair_entrust_memo \
,NVL(get_json_object(data, '$.maintain_order_no'),'') maintain_order_no \
,NVL(get_json_object(data, '$.maintain_order_accept_date'),'') maintain_order_accept_date \
,NVL(get_json_object(data, '$.maintain_order_start_date'),'') maintain_order_start_date \
,NVL(get_json_object(data, '$.maintain_order_complete_date'),'') maintain_order_complete_date \
,NVL(get_json_object(data, '$.maintain_order_cancel_date'),'') maintain_order_cancel_date \
,NVL(get_json_object(data, '$.maintain_order_cancel_reason'),'') maintain_order_cancel_reason \
,NVL(get_json_object(data, '$.maintain_order_settl_date'),'') maintain_order_settl_date \
,cast(NVL(get_json_object(data, '$.maintain_order_settl_fee'),'0') as decimal(12,2)) maintain_order_settl_fee \
,NVL(get_json_object(data, '$.service_staff_account'),'') service_staff_account \
,NVL(get_json_object(data, '$.nominee_type'),'') nominee_type \
,NVL(get_json_object(data, '$.nominee_name'),'') nominee_name \
,NVL(get_json_object(data, '$.nominee_id_card'),'') nominee_id_card \
,NVL(get_json_object(data, '$.nominee_postal_code'),'') nominee_postal_code \
,NVL(get_json_object(data, '$.nominee_telephone'),'') nominee_telephone \
,NVL(get_json_object(data, '$.nominee_email'),'') nominee_email \
,NVL(get_json_object(data, '$.nominee_mobile_phone'),'') nominee_mobile_phone \
,NVL(get_json_object(data, '$.uc_auth_flag'),'') uc_auth_flag \
,NVL(get_json_object(data, '$.uc_auth_no'),'') uc_auth_no \
,NVL(get_json_object(data, '$.nominee_address'),'') nominee_address \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,NVL(get_json_object(data, '$.remark'),'') remark \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_maintain_uc_repair_entrust' and topic = 'sale_dms_supply_prd' \
and NVL(get_json_object(data, '$.dealer_code'),'') <> '10112' 



tg_dms_cs_t_meta_first_level_source = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.source_code'),'') source_code \
,NVL(get_json_object(data, '$.source_name'),'') source_name \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,NVL(get_json_object(data, '$.remark'),'') remark \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_meta_first_level_source' and topic = 'sale_dms_supply_prd' 



tg_dms_cs_t_meta_second_level_source = select \
NVL(get_json_object(data, '$.id'),'') id \
,NVL(get_json_object(data, '$.source_first_level_code'),'') source_first_level_code \
,NVL(get_json_object(data, '$.source_code'),'') source_code \
,NVL(get_json_object(data, '$.source_name'),'') source_name \
,cast(NVL(get_json_object(data, '$.create_by'),'0') as bigint) create_by \
,NVL(get_json_object(data, '$.create_time'),'') create_time \
,cast(NVL(get_json_object(data, '$.update_by'),'0') as bigint) update_by \
,NVL(get_json_object(data, '$.update_time'),'') update_time \
,cast(NVL(get_json_object(data, '$.del_flag'),'0') as bigint) del_flag \
,NVL(get_json_object(data, '$.remark'),'') remark \
, substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \
, send_time  extract_time \
, load_time \
, date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \
from tg_gtsp_dms_rt where \
table_name = 't_meta_second_level_source' and topic = 'sale_dms_supply_prd' 


