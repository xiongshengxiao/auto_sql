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


