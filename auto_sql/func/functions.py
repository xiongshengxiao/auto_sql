import xlrd
import os
import shutil

try:
    from loguru import logger
except:
    import logging as logger

    logger.basicConfig(level=logger.DEBUG,
                       format='%(asctime)s [%(name)s:%(lineno)d] [%(module)s:%(funcName)s] -%(levelname)s:%(message)s')


def dms_table_from(table_from):
    # 该函数将存储dms模块简称及其对应的模块中文全称
    mapping = {
        "cs": "DMS共通",
        "sal": "DMS销售",
        "parts": "DMS售后零件",
        "wty": "DMS售后保修",
        "srv": "DMS售后服务",
        "ts": "DMS售后技术"
    }
    return mapping.get(table_from, 'DMS')


class TypeMap:
    def __init__(self):
        self.mapping = {
            "tinyint": "bigint",
            "smallint": "bigint",
            "mediumint": "bigint",
            "integer": "bigint",
            "bigint": "bigint",
            "int": "bigint",
            "timestamp": "bigint",
            "bigint unsigned": "bigint",
            "float": "decimal",
            "double": "decimal",
            "decimal": "decimal",
            "longtext": "string",
            "text": "string",
            "varchar": "string",
            "char": "string",
            "date": "string",
            "datetime": "string",
            "boolean": "string",
            "string": "string",
            "json": "string",
            "blob": "string"
        }

    def map_target(self, source_type):
        source_type = source_type.lower().strip()
        mapping = self.mapping
        if len(source_type.split('(')) == 2:
            field_type = source_type.split('(')[0]
            field_len = '(' + source_type.split('(')[1]
            if field_type in mapping:
                main_type = mapping.get(field_type)
                if main_type in ('bigint', 'string'):
                    target_type = main_type
                else:
                    target_type = mapping.get(field_type) + field_len
            else:
                target_type = 'string'
        else:
            field_type = source_type
            target_type = mapping.get(field_type, 'string')
        return target_type

    def dl_map_target(self, source_type):
        # self.mapping['char'] = 'char'
        return self.map_target(source_type)

    def ods_map_target(self, source_type):
        # self.mapping['char'] = 'varchar'
        return self.map_target(source_type)


def remove_files(path):
    """
    @删除目录下的文件
    """
    file_list = os.listdir(path)
    for file in file_list:
        file_path = os.path.join(path, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
        if os.path.isdir(file_path):
            shutil.rmtree(file_path, True)
    logger.warning("清空目录: " + path)


# 从解析dms表结构
def analysis_page(table):
    """
    @该方法从excel获取dms表结构
    @path:excel文件路径
    @sheet_name:excel的sheet页
    """
    sheet_info = {}
    sheet_rows = table.nrows  # 数据行数
    if sheet_rows < 4:
        return sheet_info
    sheet_info['table_comment'] = table.cell(2, 1).value  # 表注释
    dms_table_name = table.cell(2, 2).value  # dms表名称
    sheet_info['dms_table_name'] = dms_table_name
    table_from = table.cell(2, 0).value  # 表来源模块
    sheet_info['table_from'] = table_from
    sheet_info['p_key'] = table.cell(2, 3).value  # 表去重主键
    sheet_info['dl_table_name'] = 'tg_dms_' + table_from + '_' + dms_table_name  # 拼接dl表名
    sheet_info['ods_table_name'] = 'ods_dms_' + table_from + dms_table_name[1:]  # 拼接ods表名
    sheet_info['field_mapping'] = []
    for rownum in range(4, sheet_rows):
        row = table.row_values(rownum)
        field_name = row[1].strip()
        field_id = row[2].strip().lower()
        field_type = row[3].strip().lower()
        # 业务主键获取进行转换
        if table_from == "cs":  # 共通
            tuple_date = []
        elif table_from == "sal":  # 销售
            tuple_date = ['id', 'pending_id', 'clue_id', 'clue_business_id', 'enquiry_customer_code', 'customer_id',
                          'follow_id', 'vehicle_type_code', 'vehicle_sfx_code', 'dealer_code', 'source_code',
                          'first_source_code', 'dist_rule_id', 'hobby_code', 'vehicle_model', 'concern_type_code',
                          'call_id']
        elif table_from == "parts":  # 售后零部件
            tuple_date = ["part_no", "part_type", "dealer_code", "mfr_type", "supplier_code", "receive_no", "source_no",
                          "adjust_apply_no", "effective_start_date", "effective_end_date", "maintain_type",
                          "maintain_order_no", "vinno", "batch_no"]
        elif table_from == "wty":  # 售后保修
            tuple_date = []
        elif table_from == "srv":  # 售后服务
            tuple_date = ['maintain_type_code', 'dealer_code', 'maintain_code', 'vehicle_model',
                          'vehicle_use_purpose_diff', 'vinno', 'license_plate_number', 'customer_code',
                          'srvvehicle_vehicle_id', 'srvvehicle_dealer_customer_id', 'appt_id', 'branch_code',
                          'customer_id', 'maintain_order_no', 'coupon_no', 'domestic_sales_no', 'export_sales_no',
                          'part_sales_export_settl_no', 'export_sales_order_id', 'part_no', 'mall_order_no',
                          'sales_export_order_id', 'work_order_no', 'hrCode', 'set_meal_code']
        else:  # 售后技术
            tuple_date = []
        if field_id in tuple_date or field_id.endswith('id'):
            field_type = "string"
        row_arr = [field_name, field_id, field_type]
        sheet_info['field_mapping'].append(row_arr)
    return sheet_info


# 从dms表结构创建dl表
def dl_table_create_by_dms(sheet_info):
    """
    @该方法从excel获取dms表结构，拼接创建dl表ddl
    @path:excel文件路径
    @sheet_name:excel的sheet页
    """
    table_comment = sheet_info['table_comment']  # 表注释
    dms_table_name = sheet_info['dms_table_name']  # dms表名称
    dl_table_name = sheet_info['dl_table_name']
    field_mapping = sheet_info['field_mapping']
    sql = []
    sql.append('drop table if exists dl.' + dl_table_name + ';')
    sql.append('create table if not exists dl.' + dl_table_name + '(')
    for row in field_mapping:
        field_name, field_id, field_type = row
        field_type = TypeMap().dl_map_target(field_type)
        sql.append(',' + field_id + ' ' + field_type + " comment '" + field_name + "'")
    # 拼接dl添加字段
    sql.append(",etl_batch_id string comment '加载批次'")
    sql.append(",extract_time string comment '抽取表时的系统时间（以北京时间为标准时区时间）'")
    sql.append(",load_time string comment '数据加载到hive的时间'")
    sql.append(") comment '" + table_comment + "'")  # 拼接表注释
    sql.append("partitioned by(biz_date string comment '分区字段')")
    sql.append("stored as parquet")  # 拼接表格式
    sql.append('tblproperties("parquet.compress"="snappy");')
    sql[2] = sql[2][1:]  # 去除第一行拼接的多余逗号
    sql.append("\n\n")  # 添加空行
    return sql


# 从dms表结构创建ods表
def ods_table_create_by_dms(sheet_info):
    """
    @该方法从excel获取dms表结构，拼接创建ods表ddl
    @path:excel文件路径
    @sheet_name:excel的sheet页
    """
    table_comment = sheet_info['table_comment']  # 表注释
    dms_table_name = sheet_info['dms_table_name']  # dms表名称
    dl_table_name = sheet_info['dl_table_name']
    field_mapping = sheet_info['field_mapping']
    ods_table_name = sheet_info['ods_table_name']
    sql = []
    sql.append('drop table if exists ods.' + ods_table_name + ';')
    sql.append('create table if not exists ods.' + ods_table_name + '(')
    for row in field_mapping:
        field_name, field_id, field_type = row
        field_type = TypeMap().ods_map_target(field_type)
        sql.append(',' + field_id + ' ' + field_type + " comment '" + field_name + "'")
    # 拼接ods添加字段
    sql.append(",dl_batch_id string comment 'dl加载批次'")
    sql.append(",extract_time string comment '抽取表时的系统时间（以北京时间为标准时区时间）'")
    sql.append(",load_time string comment '数据加载到hive的时间'")
    sql.append(",source_system string comment '来源系统'")
    sql.append(",insert_time string comment '数据写入时间'")
    sql.append(",biz_date string comment '分区字段'")
    sql.append(") comment '" + table_comment + "'")  # 拼接表注释
    sql.append("stored as parquet")  # 拼接表格式
    sql.append('tblproperties("parquet.compress"="snappy");')
    sql[2] = sql[2][1:]  # 去除第一行拼接的多余逗号
    sql.append("\n\n")  # 添加空行
    return sql


# 根据dms表结构创建hbase到hive表逻辑sql
def hbse_to_hive_create_by_dms(sheet_info, topic):
    """
    @该方法从excel获取dms表结构，拼接创建ods表ddl
    @path:excel文件路径
    @sheet_name:excel的sheet页
    """
    table_comment = sheet_info['table_comment']  # 表注释
    dms_table_name = sheet_info['dms_table_name']  # dms表名称
    dl_table_name = sheet_info['dl_table_name']
    field_mapping = sheet_info['field_mapping']
    ods_table_name = sheet_info['ods_table_name']
    dealer_fields = []
    sql = []
    sql.append(dl_table_name + ' = select \\')
    for row in field_mapping:
        field_name, field_id, field_type = row
        if 'dealer_code' == field_id:
            dealer_fields.append(field_id)
        main_type = field_type.split('(')[0]
        field_type = TypeMap().dl_map_target(field_type)
        if main_type in (
                'bigint', 'tinyint', 'smallint', 'mediumint', 'integer', 'int', 'timestamp', 'bigint unsigned',
                'decimal','float', 'double',):
            row_i = ",cast(NVL(get_json_object(data, '$." + field_id + "'),'0') as " \
                    + field_type + ") " + field_id + " \\"
        else:
            row_i = ",NVL(get_json_object(data, '$." + field_id + "'),'') " + field_id + " \\"
        sql.append(row_i)
    # 拼接bhase_to_hive添加字段
    sql.append(", substr(date_format(load_time, 'yyyyMMddHHmm'), 3, 9) etl_batch_id \\")
    sql.append(", send_time  extract_time \\")
    sql.append(", load_time \\")
    sql.append(", date_format(NVL(get_json_object(data, '$.update_time'), load_time), 'yyyyMMdd')  biz_date \\")
    sql.append("from tg_gtsp_dms_rt where \\")
    sql.append("table_name = '" + dms_table_name + "' and topic = '" + topic + "' \\")
    for dealer_id in dealer_fields:
        sql.append("and NVL(get_json_object(data, '$." + dealer_id + "'),'') <> '10112' \\")
    sql[1] = sql[1][1:]  # 去除第一行拼接的多余逗号
    sql[-1] = sql[-1][:-1]  # 去除最后一行的反斜杆
    sql.append("\n\n")  # 添加空行
    return sql


def ods_sql_create_by_dms(sheet_info):
    """
    @该方法从excel获取dms表结构，根据dms表结构拼接ods加载数据脚本
    @path:excel文件路径
    @sheet_name:excel的sheet页
    """
    table_comment = sheet_info['table_comment']  # 表注释
    dms_table_name = sheet_info['dms_table_name']  # dms表名称
    dl_table_name = sheet_info['dl_table_name']
    field_mapping = sheet_info['field_mapping']
    table_from = sheet_info['table_from']
    ods_table_name = sheet_info['ods_table_name']
    ods_table_tmp = ods_table_name + '_tmp'
    p_key = sheet_info['p_key']
    sql0 = []
    sql1 = []
    sql2 = []
    sql3 = []
    sql0.append('set mapred.job.name=job_' + ods_table_name + ';')
    sql0.append('--set hive.exec.parallel=true;')
    sql0.append('--set hive.map.aggr=true;')
    sql0.append('--set hive.groupby.skewindata=true;')
    sql0.append('with ' + ods_table_tmp + ' as (')
    for row in field_mapping:
        field_name, field_id, field_type = row
        # sql1:拼接从ods取数sql
        sql1.append(', ' + field_id + " as " + field_id + " --" + field_name)
        # sql2:拼接从dl增量取数sql,并对字符串类型做去空格，去null处理
        field_type = TypeMap().ods_map_target(field_type)
        main_type = field_type.split('(')[0]
        if main_type in ('varchar', 'string'):
            row_i = ", trim(nvl(" + field_id + ", '')) as " + field_id + "  --" + field_name
        else:
            row_i = ", nvl(" + field_id + ", 0) as " + field_id + "  --" + field_name
        sql2.append(row_i)

    # sql1拼接ods添加字段
    sql1[0] = 'select ' + sql1[0][1:]  # 去除第一行拼接的多余逗号
    sql1.append(", dl_batch_id as dl_batch_id  --dl加载批次")
    sql1.append(", extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）")
    sql1.append(", load_time as load_time  --数据加载到hive的时间")

    # sql3拼接insert段sql，在未添加额外字段前先将sql1复制一份给sql3
    sql3.append("insert overwrite table ods." + ods_table_name)
    sql3 += sql1.copy()  # 在未添加额外字段前先将sql1复制一份给sql3

    # 继续拼接sql1查询的ods表
    sql1.append(", biz_date as biz_date  --分区字段")
    sql1.append("from ods." + ods_table_name)
    sql1.append("union all")  # 拼接union all串

    # sql2拼接dl添加字段
    sql2[0] = 'select ' + sql2[0][1:]  # 去除第一行拼接的多余逗号
    sql2.append(", etl_batch_id as dl_batch_id  --dl加载批次")
    sql2.append(", extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）")
    sql2.append(", load_time as load_time  --数据加载到hive的时间")
    sql2.append(", biz_date as biz_date  --分区字段")
    sql2.append("from dl." + dl_table_name)  # 拼接查询的dl表
    sql2.append("where biz_date = '${YESTERDAY}'")  # 拼接查询的dl表分区条件，初始化时只需注释该行即可
    sql2.append(")")  # 拼接括号

    # 继续拼接sql3
    t_from = dms_table_from(table_from)  # 将dms来源系统简称转化为中文全称
    sql3.append(", '" + t_from + "' as source_system --来源系统")
    sql3.append(", substr(current_timestamp(), 1, 19) as insert_time --数据写入时间")
    sql3.append(", biz_date as biz_date  --分区字段")  # 最后添加biz_date
    sql3.append("from (")
    sql3.append("select *,")
    sql3.append("row_number() over (partition by " + p_key)
    sql3.append("order by update_time desc, load_time desc, create_time desc)rn")
    sql3.append("from " + ods_table_tmp + ") a")
    sql3.append("where rn = 1;")
    complete_sql = sql0 + sql1 + sql2 + sql3
    complete_sql.append("\n\n")  # 添加空行
    return complete_sql


def ods_init_sql_create_by_dms(sheet_info):
    """
    @该方法从excel获取dms表结构，根据dms表结构拼接ods初始化数据脚本
    @path:excel文件路径
    @sheet_name:excel的sheet页
    """
    dl_table_name = sheet_info['dl_table_name']
    field_mapping = sheet_info['field_mapping']
    table_from = sheet_info['table_from']
    ods_table_name = sheet_info['ods_table_name']
    p_key = sheet_info['p_key']
    sql0 = []
    sql1 = []
    sql2 = []
    sql0.append('set mapred.job.name=job_' + ods_table_name + '_init;')
    sql0.append('--set hive.exec.parallel=true;')
    sql0.append('--set hive.map.aggr=true;')
    sql0.append('--set hive.groupby.skewindata=true;')
    sql0.append('insert overwrite table ods.' + ods_table_name)
    for row in field_mapping:
        field_name, field_id, field_type = row
        # sql1:拼接从外层sql
        sql1.append(', ' + field_id + " --" + field_name)
        # sql2:拼接从dl取数内层sql,并对字符串类型做去空格，去null处理
        field_type = TypeMap().ods_map_target(field_type)
        main_type = field_type.split('(')[0]
        if main_type in ('varchar', 'string'):
            row_i = ", trim(nvl(" + field_id + ", '')) as " + field_id + "  --" + field_name
        else:
            row_i = ", nvl(" + field_id + ", 0) as " + field_id + "  --" + field_name
        sql2.append(row_i)

    # sql1拼接ods添加字段
    sql1[0] = 'select ' + sql1[0][1:]  # 去除第一行拼接的多余逗号
    sql1.append(", dl_batch_id as dl_batch_id  --dl加载批次")
    sql1.append(", extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）")
    sql1.append(", load_time as load_time  --数据加载到hive的时间")
    t_from = dms_table_from(table_from)  # 将dms来源系统简称转化为中文全称
    sql1.append(", '" + t_from + "' as source_system --来源系统")
    sql1.append(", substr(current_timestamp(), 1, 19) as insert_time --数据写入时间")
    sql1.append(", biz_date as biz_date  --分区字段")
    sql1.append("from (")

    # sql2拼接dl添加字段
    sql2[0] = 'select ' + sql2[0][1:]  # 去除第一行拼接的多余逗号
    sql2.append(", etl_batch_id as dl_batch_id  --dl加载批次")
    sql2.append(", extract_time as extract_time  --抽取表时的系统时间（以北京时间为标准时区时间）")
    sql2.append(", load_time as load_time  --数据加载到hive的时间")
    sql2.append(", biz_date as biz_date  --分区字段")
    sql2.append(", row_number() over(partition by " + p_key)
    sql2.append("order by update_time desc, load_time desc, create_time desc) rn")
    sql2.append("from dl." + dl_table_name)  # 拼接查询的dl表
    sql2.append("where biz_date < '${TODAY}' )aa")  # 拼接查询的dl表初始化分区条件
    sql2.append("where rn = 1;")  # 选出排序第一条

    complete_sql = sql0 + sql1 + sql2
    complete_sql.append("\n\n")  # 添加空行
    return complete_sql


def ods_sqls_split(split_type, path):
    """
    该函数读取ods数据的脚本，并拆分为单个表的文件
    """

    abspath = os.path.abspath(os.path.dirname(path))
    if split_type == "ods加载":
        ods_split_path = abspath + "\\ods_load_sqls_split"
        location_name = "ods拆分加载语句位置: "
    elif split_type == "ods初始化":
        ods_split_path = abspath + "\\ods_init_sqls_split"
        location_name = "ods拆分初始化语句位置: "
    else:
        return 0
    isExists = os.path.exists(ods_split_path)
    if not isExists:
        os.makedirs(ods_split_path)
    else:
        remove_files(ods_split_path)
    with open(path, 'r', encoding='utf-8') as f:
        files = {}
        file_name = ''
        for line in f:
            line = line.strip('\n')
            if 'set mapred.job.name=job_' in line:
                file_name = line.split('=job_')[1].split(';')[0] + ".sql"
                files[file_name] = []
            files[file_name].append(line)
    for file in files:
        load_file = (ods_split_path + '/' + file).replace("\\", "/")
        with open(load_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(files[file]))
    logger.info(location_name + os.path.abspath(ods_split_path))
    return 0


def create_all(pro):
    # 该函数执行dl与ods的建表及加载脚本，并将结果写到对应路径
    path = pro.get("excel_path", r"模板/dms表结构.xlsx")
    topic = pro.get("kafka_topic", "sale_dms_supply_prd")
    paths = {
        "dl建表": pro.get("target_dl_table_ddls", "dl/dl_table_ddls.sql"),
        "dl加载": pro.get("target_dl_load_sqls", "dl/dl_load_sqls.sql"),
        "ods建表": pro.get("target_ods_table_ddls", "ods/ods_table_ddls.sql"),
        "ods初始化": pro.get("target_ods_init_sqls", "ods/ods_init_sqls.sql"),
        "ods加载": pro.get("target_ods_load_sqls", "ods/ods_load_sqls.sql")
    }
    data = xlrd.open_workbook(path)
    sheet_names = data.sheet_names()
    all = {
        "dl建表": [],
        "dl加载": [],
        "ods建表": [],
        "ods初始化": [],
        "ods加载": []
    }
    for sheet_name in sheet_names:
        table = data.sheet_by_name(sheet_name)
        sheet_info = analysis_page(table)
        if len(sheet_info) == 0:
            logger.error("sheet页: {} 不符合输入要求，文件没有内容！！！".format(sheet_name))
            continue
        all["dl建表"] += dl_table_create_by_dms(sheet_info)  # dl创建表
        all["dl加载"] += hbse_to_hive_create_by_dms(sheet_info, topic)  # dl加载数据
        all["ods建表"] += ods_table_create_by_dms(sheet_info)  # ODS创建表
        all["ods初始化"] += ods_init_sql_create_by_dms(sheet_info)  # ODS初始化
        all["ods加载"] += ods_sql_create_by_dms(sheet_info)  # ODS加载
        logger.info("新dl,ods-建表ddl&加载sql-完成sheet页: {}".format(sheet_name))

    for name in all:
        path = paths[name]
        with open(path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(all[name]))
            logger.info(name + "语句位置: " + os.path.abspath(path))
            if name in ("ods初始化", "ods加载"):
                ods_sqls_split(name, path)


if __name__ == "__main__":
    path = r'C:\Users\dj\Desktop\dms表结构.xlsx'
    sheet_name = '客户订单表'
    # sql = dl_table_create_by_dms(path, sheet_name)  # 测试dl创建表
    # sql = ods_table_create_by_dms(path, sheet_name)  # 测试ODS创建表
    # sql = sql_create_by_dms(path, sheet_name)  # 测试生成调度脚本
    sql = hbse_to_hive_create_by_dms(path, sheet_name)
    for row in sql:
        print(row)
