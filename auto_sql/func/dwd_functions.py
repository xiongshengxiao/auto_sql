import xlrd
import os

from auto_sql.func import functions

try:
    from loguru import logger
except:
    import logging as logger

    logger.basicConfig(level=logger.DEBUG,
                       format='%(asctime)s [%(name)s:%(lineno)d] [%(module)s:%(funcName)s] -%(levelname)s:%(message)s')


def dwd_type_mapping(source_type):
    """
    该对dwd字段类型进行格式化
    """
    return functions.TypeMap().ods_map_target(source_type)


def analysis_page(table, dms_ods_mapping):
    """
    @sheet_info
    """
    sheet_info = {}
    sheet_rows = table.nrows  # 数据行数
    if sheet_rows < 6:
        sheet_info['flag'] = 0
        return sheet_info
    sheet_info['field_mapping'] = []
    sheet_info['odl_table_short'] = {}
    for rownum in range(6, sheet_rows):
        row = table.row_values(rownum)
        # print(row)
        dwd_field_name = row[1].strip()  # DWD字段注释
        dwd_field_id = row[2].strip().lower()  # DWD字段名
        dwd_field_type = row[4].strip().lower()  # DWD字段类型
        dms_table_id = row[5].strip().lower()  # DMS表名
        dms_field_name = row[6].strip()  # DMS字段注释
        dms_field_id = row[7].strip().lower()  # DMS字段名
        dms_field_type = row[9].strip().lower()  # DMS字段类型
        odl_table_id = row[15].strip().lower()  # 旧DL表名
        odl_field_name = row[16].strip()  # 旧DL字段注释
        odl_field_id = row[17].strip().lower()  # 旧DL字段名
        odl_field_type = row[19].strip().lower()  # 旧DL字段类型
        row_arr = [dwd_field_name, dwd_field_id, dwd_field_type,
                   dms_table_id, dms_field_name, dms_field_id, dms_field_type,
                   odl_table_id, odl_field_name, odl_field_id, odl_field_type]
        if dwd_field_id == '':
            continue
        if odl_table_id != '' and odl_field_id != '':
            sheet_info['odl_table_short'][odl_table_id] = ''
        sheet_info['field_mapping'].append(row_arr)
    dwd_table_name = table.cell(4, 2).value  # dwd表名称
    dwd_table_tmp = dwd_table_name + '_tmp'
    sheet_info['sheet_rows'] = sheet_rows
    sheet_info['table_comment'] = table.cell(3, 2).value  # 表注释
    sheet_info['dwd_table_name'] = dwd_table_name
    sheet_info['dwd_table_tmp'] = dwd_table_tmp
    sheet_info['load_join_arr'] = []
    sheet_info['table_short'] = {}
    sheet_info['dms_ods'] = {}
    dms_tables = table.cell(4, 8).value.split(',')  # dms表名称
    # 解析ods拼接规则
    if len(dms_tables) == 1:
        dms_table = dms_tables[0].strip()
        ods_table_name = dms_ods_mapping.get(dms_table)
        sheet_info['load_join_arr'].append("from ods." + ods_table_name + ' a')  # 拼接查询的ods表
        sheet_info['load_join_arr'].append("where biz_date = '${YESTERDAY}'")  # 拼接查询的ods表逻辑分区条件
        sheet_info['table_short'][dms_table] = "a"
        sheet_info['dms_ods'][dms_table] = ods_table_name
        sheet_info['flag'] = 1
    else:
        text = str(table.cell(6, 14)).lower().replace('\\n', ' \\n')
        if "text:" in text:
            text = text[6:-1]
        text_arr = []
        for a in text.split(" "):
            a = a.replace('\\n', '').strip()
            if a != '':
                text_arr.append(a)
            if a in dms_ods_mapping:
                dms_table = a
                ods_table_name = dms_ods_mapping.get(dms_table)
                sheet_info['dms_ods'][dms_table] = ods_table_name
                sheet_info['table_short'][dms_table] = dms_table
        target_join = " ".join(text_arr)
        for dms_table in sheet_info['table_short']:
            ods_table_name = sheet_info['dms_ods'][dms_table]
            sheet_info['table_short'][dms_table] = text_arr[text_arr.index(dms_table) + 1]  # 找到dms表对应简称
            target_join = target_join.replace(dms_table + ' ', "ods." + ods_table_name + ' ')
        if len(sheet_info['table_short']) == 1:
            sheet_info['load_join_arr'].append("from ods." + ods_table_name + ' a')  # 拼接查询的ods表
            sheet_info['load_join_arr'].append("where biz_date = '${YESTERDAY}'")  # 拼接查询的ods表逻辑分区条件
            sheet_info['flag'] = 1
        else:
            target_join = "left join " + target_join[5:]
            sheet_info['load_join_arr'].append("from ods_increment inc ")
            sheet_info['load_join_arr'] += target_join.split('\\n')
            sheet_info['flag'] = 2
    # 解析旧dl拼接规则
    sheet_info['odl_join_arr'] = []
    odl_num = len(sheet_info['odl_table_short'])
    if odl_num == 1:
        sheet_info['odl_join_arr'] = [f'from dl.{odl_table_id} a'.format(odl_table_id=odl_table_id)]
        sheet_info['odl_table_short'][list(sheet_info['odl_table_short'].keys())[0]] = 'a'
    elif odl_num > 1:
        text = str(table.cell(6, 26)).lower().replace('\\n', ' \\n')
        if "text:" in text:
            text = text[6:-1]
        text_arr = []
        for a in text.split(" "):
            a = a.replace('\\n', '').strip()
            if a != '':
                text_arr.append(a)
        # 获取join规则的内容
        target_join = " ".join(text_arr)
        for odl_table in sheet_info['odl_table_short']:
            try:
                sheet_info['odl_table_short'][odl_table] = text_arr[text_arr.index(odl_table) + 1]  # 找到旧dl表对应简称
            except ValueError as e:
                logger.error("dwd旧dl映射dwd[" + dwd_table_name + "]-join段未列出旧dl表名,错误: " + str(e))
            target_join = target_join.replace(odl_table + ' ', odl_table + '_tmp ')
            sheet_info['odl_join_arr'] = target_join.split('\\n')
    return sheet_info


def get_ods_increment(dms_names, dms_ods_mapping):
    ods_increment_arr = ["with ods_increment as(", "select id", "from ("]
    for dms_name in dms_names:
        ods_name = "ods." + dms_ods_mapping.get(dms_name)
        ods_increment_arr.append("select id as id")
        ods_increment_arr.append("from " + ods_name)
        ods_increment_arr.append("where biz_date = '${YESTERDAY}'")
        ods_increment_arr.append("union all")
    ods_increment_arr[-1] = ") a"
    ods_increment_arr.append("group by id")
    ods_increment_arr.append("),")
    return ods_increment_arr


def get_old_dl_distinct(old_dl_tables):
    old_dl_distinct = ""
    str_format = '''
                {dl_table}_tmp as (select * from ( select *,
                row_number() over (partition by id 
                order by updatetime desc,load_time desc,createtime desc)rn 
                from dl.{dl_table}) where rn=1),'''
    for dl_table in old_dl_tables:
        old_dl_distinct += str_format.format(dl_table=dl_table)
    old_dl_distinct = old_dl_distinct.replace(' ' * 16, '')
    return ('with ' + old_dl_distinct).split('\n')


def dwd_table_create(sheet_info):
    """
    @该方法从excel获取dwd表结构，拼接创建dwd表ddl
    @path:excel文件路径
    @sheet_name:excel的sheet页
    """
    table_comment = sheet_info['table_comment']  # 表注释
    field_mapping = sheet_info['field_mapping']
    dwd_table_name = sheet_info['dwd_table_name']  # dwd表名称
    sql = []
    sql.append('drop table if exists dwd.' + dwd_table_name + ';')
    sql.append('create table if not exists dwd.' + dwd_table_name + '(')
    for row in field_mapping:
        field_name, field_id, field_type = row[:3]
        field_type = dwd_type_mapping(field_type)
        sql.append(',' + field_id + ' ' + field_type + " comment '" + field_name + "'")
    sql.append(") comment '" + table_comment + "'")  # 拼接表注释
    sql.append("stored as parquet")  # 拼接表格式
    sql.append('tblproperties("parquet.compress"="snappy");')
    sql[2] = sql[2][1:]  # 去除第一行拼接的多余逗号
    sql.append("\n\n")  # 添加空行
    return sql


class FixedField:
    def __init__(self):
        self.mapping = {
            "operate_type": ", if(a.del_flag=1 ,'D', 'U') as operate_type -- 数据操作类型",
            "dl_batch_id": ", a.dl_batch_id as dl_batch_id  --dl加载批次",
            "extract_time": ", a.extract_time as extract_time  --抽取表时的系统时间（HBase-Hive）",
            "load_time": ", a.load_time as load_time  --数据加载到hive的时间",
            "source_system": ", 'DMS' as source_system --来源系统",
            "insert_time": ", substr(current_timestamp(), 1, 19) as insert_time --数据写入时间",
            "biz_date": ", a.biz_date as biz_date  --分区字段"
        }

    def get_field_row(self, field):
        if field in self.mapping:
            return self.mapping.get(field)
        else:
            return "not_fixed_field"


def dwd_sql_create(sheet_name, sheet_info):
    """
    @该方法根据excel dwd要件拼接dwd加载数据sql
    @path:excel文件路径
    @sheet_name:excel的sheet页
    """

    flag = sheet_info['flag']
    if flag == 0:
        logger.error("dwd要件格式不符合要求: {}".format(sheet_name))
        return []
    table_comment = sheet_info['table_comment']  # 表注释
    field_mapping = sheet_info['field_mapping']
    dwd_table_name = sheet_info['dwd_table_name']  # dwd表名称
    dwd_table_tmp = sheet_info['dwd_table_tmp']  # dwd临时表名称
    load_join_arr = sheet_info['load_join_arr']  # dwd增量取数join段 []
    table_short = sheet_info['table_short']  # dms表或ods表对应简称 {}
    dms_ods = sheet_info['dms_ods']  # dms表与ods表映射 {}

    p_key = 'id'
    sql0 = []  # sql0拼接设置sql段
    sql1 = []  # sql1拼接从dwd取数sql段
    sql2 = []  # sql2拼接从ods取数sql段
    sql3 = []  # sql3拼接插入dwd表sql段
    sql0.append('set mapred.job.name=job_' + dwd_table_name + ';')
    sql0.append('--set hive.exec.parallel=true;')
    sql0.append('--set hive.map.aggr=true;')
    sql0.append('--set hive.groupby.skewindata=true;')
    if flag == 1:
        sql1.append('with ' + dwd_table_tmp + ' as (')
        ods_increment_sql = []
    else:
        logger.warning("sheet页: {} ---表【{}】为多表join，请注意调整代码---".format(sheet_name, dwd_table_name))
        sql1.append(dwd_table_tmp + ' as (')
        ods_increment_sql = get_ods_increment(table_short, dms_ods)
    logs = []
    for row in field_mapping:
        dwd_field_name, dwd_field_id, dwd_field_type, dms_table_name, ods_field_name, ods_field_id = row[:6]
        # sql1:直接拼接从dwd取数sql
        sql1.append(', ' + dwd_field_id + " as " + dwd_field_id + " --" + dwd_field_name)
        # sql3:从sql1与sql2的合并数据中取数
        sql3.append(', ' + dwd_field_id + " as " + dwd_field_id + " --" + dwd_field_name)
        # sql2:从ods取数sql需做判断
        fixed_field_row = FixedField().get_field_row(dwd_field_id)
        if fixed_field_row == "not_fixed_field":
            # fixed_field_row == "not_fixed_field"，则该字段不是固定字段
            field_type = dwd_type_mapping(dwd_field_type)
            main_type = field_type.split('(')[0]
            if ods_field_id == '':
                # 不是固定字段且ods_field_id为空，则该字段应是补充的字段，默认写空串或0
                if main_type in ('varchar', 'string'):
                    row_i = ", '' as " + dwd_field_id + "  --" + dwd_field_name
                else:
                    row_i = ", 0 as " + dwd_field_id + "  --" + dwd_field_name
            else:
                # 如果不是固定字段，且ods_field_id不为空，则该字段与ods有字段映射，正常取数
                # sql2:拼接从ods增量取数sql
                dms_table_names = dms_table_name.split(',')
                if len(dms_table_names) > 1:
                    logs.append("sheet页: {} ---字段【{}】规则需手动修改---" \
                                .format(sheet_name, dwd_field_name))
                try:
                    table_name_short = table_short[dms_table_names[0]]
                except:
                    logs.append("sheet页: {} ---表【{}】未在join段或dms表处列出---" \
                                .format(sheet_name, dms_table_names[0]))
                    table_name_short = '此处需修改'
                if flag == 1:
                    row_i = ", " + table_name_short + '.' + ods_field_id + " as " \
                            + dwd_field_id + "  --" + ods_field_name
                elif dwd_field_id == 'update_time':
                    arr = [", greatest("]
                    for t in table_short:
                        short = table_short[t]
                        str = "nvl(" + short + ".update_time, '0000-00-00 00:00:00'),"
                        arr.append(str)
                    row_i = ''.join(arr)[:-1] + ") as update_time  --更新时间"
                elif main_type in ('varchar', 'string'):
                    row_i = ", nvl(" + table_name_short + '.' + ods_field_id + ",'') as " \
                            + dwd_field_id + "  --" + ods_field_name
                else:
                    row_i = ", nvl(" + table_name_short + '.' + ods_field_id + ",0) as " \
                            + dwd_field_id + "  --" + ods_field_name
        else:
            # fixed_field_row 不等于 "not_fixed_field"，则该字段是固定字段

            if flag == 2 and dwd_field_id == 'biz_date':
                # 如果多表join则biz_date取最大
                arr = [", greatest("]
                for t in table_short:
                    short = table_short[t]
                    str = "nvl(" + short + ".biz_date, '00000000'),"
                    arr.append(str)
                row_i = ''.join(arr)[:-1] + ") as biz_date  --分区字段"
            else:
                # sql2拼接固定字段字符串
                row_i = fixed_field_row
        sql2.append(row_i)
    for log in logs[:2]:
        logger.warning(log)
    sql1[1] = "select " + sql1[1][1:]  # 去除第一行拼接的多余逗号
    sql1.append("from dwd." + dwd_table_name)
    sql1.append("union all")  # 拼接union all串
    sql2[0] = "select " + sql2[0][1:]  # 去除第一行拼接的多余逗号
    sql2 += load_join_arr
    sql2.append(")")  # 拼接括号
    sql2.append("insert overwrite table dwd." + dwd_table_name)  # 拼接插入dwd表
    sql3[0] = "select " + sql3[0][1:]  # 去除第一行拼接的多余逗号
    sql3.append("from (")
    sql3.append("select *,")
    sql3.append("row_number() over (partition by " + p_key)
    sql3.append("order by update_time desc, load_time desc, create_time desc,biz_date desc) rn")
    sql3.append("from " + dwd_table_tmp + ") a")
    sql3.append("where rn = 1;")
    complete_sql = sql0 + ods_increment_sql + sql1 + sql2 + sql3
    complete_sql.append("\n\n")  # 添加空行
    return complete_sql


def dwd_old_sql_create(sheet_info):
    """
    @该方法根据excel dwd要件拼接dwd加载数据sql
    @path:excel文件路径
    @sheet_name:excel的sheet页
    """
    odl_table_short = sheet_info['odl_table_short']
    field_mapping = sheet_info['field_mapping']
    dwd_table_name = sheet_info['dwd_table_name']  # dwd表名称
    dwd_table_tmp = sheet_info['dwd_table_tmp']  # dwd临时表名称
    join_arr = sheet_info['odl_join_arr']  # dwd初始化旧表join段
    p_key = 'id'
    sql0 = []  # sql0拼接设置sql段
    sql1 = []  # sql1拼接从dwd取数sql段
    sql2 = []  # sql2拼接从旧dl取数sql段
    sql3 = []  # sql3拼接插入dwd表sql段
    sql0.append('set mapred.job.name=job_' + dwd_table_name + '_old_dl_init;')
    sql0.append('--set hive.exec.parallel=true;')
    sql0.append('--set hive.map.aggr=true;')
    sql0.append('--set hive.groupby.skewindata=true;')
    old_dl_distinct = get_old_dl_distinct(odl_table_short)
    sql1.append(dwd_table_tmp + ' as (')
    logs = []
    for row in field_mapping:
        dwd_field_name, dwd_field_id, dwd_field_type = row[:3]
        odl_table_id, odl_field_name, odl_field_id, odl_field_type = row[7:]
        # sql1:直接拼接从dwd取数sql
        sql1.append(', ' + dwd_field_id + " as " + dwd_field_id + " --" + dwd_field_name)
        # sql3:从sql1与sql2的合并数据中取数
        sql3.append(', ' + dwd_field_id + " as " + dwd_field_id + " --" + dwd_field_name)
        # sql2:从旧取数sql需做判断
        field_type = dwd_type_mapping(dwd_field_type)
        main_type = field_type.split('(')[0]
        odl_field_main_type = odl_field_type.split('(')[0]
        if dwd_field_id == 'source_system':
            row_i = ", 'OLD_DMS' as " + dwd_field_id + "  --" + dwd_field_name
        elif dwd_field_id == 'biz_date' and odl_field_id != 'biz_date':
            row_i = ", date_format(if(trim(nvl(updatetime,''))='', " \
                    "load_time, updatetime), 'yyyyMMdd') as biz_date --默认解析更新时间或加载时间"
        elif odl_field_id == '' or odl_table_id == '':
            # 不是固定字段且ods_field_id为空，则该字段应是补充的字段，默认写空串或0
            if main_type in ('varchar', 'string'):
                row_i = ", '' as " + dwd_field_id + "  --" + dwd_field_name
            else:
                row_i = ", 0 as " + dwd_field_id + "  --" + dwd_field_name
        else:
            # 如果不是固定字段，且odl_field_id不为空，则该字段与旧dl有字段映射，正常取数
            # sql2:拼接从旧dl取数sql
            table_name_short = odl_table_short[odl_table_id]
            if main_type in ('varchar', 'string') and odl_field_main_type in ('varchar', 'string'):
                row_i = ", nvl(" + table_name_short + '.' + odl_field_id + ",'') as " \
                        + dwd_field_id + "  --" + odl_field_name
            elif main_type in ('varchar', 'string') and odl_field_main_type not in ('varchar', 'string'):
                row_i = ", nvl(cast(" + table_name_short + '.' + odl_field_id + " as " + field_type + "),'') as " \
                        + dwd_field_id + "  --" + odl_field_name
            elif main_type not in ('varchar', 'string') and odl_field_main_type in ('varchar', 'string'):
                row_i = ", nvl(cast(" + table_name_short + '.' + odl_field_id + " as " + field_type + "),0) as " \
                        + dwd_field_id + "  --" + odl_field_name
            else:
                row_i = ", nvl(" + table_name_short + '.' + odl_field_id + ",0) as " \
                        + dwd_field_id + "  --" + odl_field_name
        sql2.append(row_i)
    sql1[1] = "select " + sql1[1][1:]  # 去除第一行拼接的多余逗号
    sql1.append("from dwd." + dwd_table_name)
    sql1.append("union all")  # 拼接union all串
    sql2[0] = "select " + sql2[0][1:]  # 去除第一行拼接的多余逗号
    sql2 += join_arr
    sql2.append(")")  # 拼接括号
    sql2.append("insert overwrite table dwd." + dwd_table_name)  # 拼接插入dwd表
    sql3[0] = "select " + sql3[0][1:]  # 去除第一行拼接的多余逗号
    sql3.append("from (")
    sql3.append("select *,")
    sql3.append("row_number() over (partition by " + p_key)
    sql3.append("order by update_time desc, load_time desc, create_time desc,biz_date desc) rn")
    sql3.append("from " + dwd_table_tmp + ") a")
    sql3.append("where rn = 1;")
    complete_sql = sql0 + old_dl_distinct + sql1 + sql2 + sql3
    complete_sql.append("\n\n")  # 添加空行
    return complete_sql


def dwd_sqls_split(path):
    """
    # 该函数读取dwd加载数据的脚本，并拆分为单个表的文件
    """
    abspath = os.path.abspath(os.path.dirname(path))
    dwd_split_path = abspath + "\\dwd_load_sqls_split"
    dwd_init_path = abspath + "\\dwd_init_sqls_split"
    for p in (dwd_split_path, dwd_init_path):
        isExists = os.path.exists(p)
        if not isExists:
            os.makedirs(p)
        else:
            functions.remove_files(p)
    with open(path, 'r', encoding='utf-8') as f:
        load_files = {}
        init_files = {}
        load_file_name = ''
        init_file_name = ''
        file_num = 0
        for line in f:
            line = line.strip('\n')
            init_line = line
            load_line = line
            if 'set mapred.job.name=job_' in line:
                file_num += 1
                init_file_name = line.split('=job_')[1].split(';')[0] + "_init.sql"
                init_line = line[:-1] + "_init;"
                init_files[init_file_name] = []
                load_file_name = line.split('=job_')[1].split(';')[0] + ".sql"
                load_files[load_file_name] = []
            if "where biz_date = '${YESTERDAY}'" in line:
                init_line = "--where biz_date = '${YESTERDAY}'"
            load_files[load_file_name].append(load_line)
            init_files[init_file_name].append(init_line)
    for file in load_files:
        load_file = (dwd_split_path + '/' + file).replace("\\", "/")
        with open(load_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(load_files[file]))
    for file in init_files:
        init_file = (dwd_init_path + '/' + file).replace("\\", "/")
        with open(init_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(init_files[file]))
    logger.info("dwd拆分初始化/加载文件各完成 {} 个".format(file_num))
    logger.info("dwd拆分加载语句位置: " + os.path.abspath(dwd_split_path))
    logger.info("dwd拆分初始语句位置: " + os.path.abspath(dwd_init_path))
    return 0


def dwd_old_dl_sqls_split(path):
    """
    # 该函数读取dwd加载数据的脚本，并拆分为单个表的文件
    """
    abspath = os.path.abspath(os.path.dirname(path))
    split_path = abspath + "\\dwd_old_dl_init_sqls_split"
    isExists = os.path.exists(split_path)
    if not isExists:
        os.makedirs(split_path)
    else:
        functions.remove_files(split_path)
    with open(path, 'r', encoding='utf-8') as f:
        files = {}
        file_name = ''
        file_num = 0
        for line in f:
            line = line.strip('\n')
            if 'set mapred.job.name=job_' in line:
                file_num += 1
                file_name = line.split('=job_')[1].split(';')[0] + ".sql"
                files[file_name] = []
            files[file_name].append(line)
    for file in files:
        dwd_old_dl_file = (split_path + '/' + file).replace("\\", "/")
        with open(dwd_old_dl_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(files[file]))
    logger.info("dwd拆分旧dl写dwd脚本完成 {} 个".format(file_num))
    logger.info("dwd拆分旧dl写dwd脚本位置: " + os.path.abspath(split_path))
    return 0


def get_ods_tables(data):
    table = data.sheet_by_name('目录')  # 通过名称获取sheet内容
    nrows = table.nrows  # 数据行数
    mapping = {}
    if nrows < 6:
        logger.error('要件输入不符合要求！！！！！')
        return []
    for rownum in range(8, nrows):
        row = table.row_values(rownum)
        ods, dms = row[11:13]
        ods = ods.strip()
        dms = dms.strip()
        if ods == '' or dms == '':
            logger.error('要件输入不符合要求,ods-dms表映射有空 ！！！！！')
            return
        mapping[dms] = ods
    return mapping


def create_all(pro):
    """
    该函数执行dl与ods的建表及加载脚本，并将结果写到对应路径
    """
    path = pro.get("excel_path", r"模板/dms表结构.xlsx")
    paths = {
        "dwd建表": pro.get("target_dwd_table_ddls", "dwd/dwd_table_ddls.sql"),
        "dwd加载": pro.get("target_dwd_load_sqls", "dwd/dwd_load_sqls.sql"),
        "dwd初始化旧DL": pro.get("target_dwd_old_dl_init_sqls", "dwd/dwd_old_dl_init_sqls.sql"),
    }
    data = xlrd.open_workbook(path)
    dms_ods_mapping = get_ods_tables(data)
    # 获取表从第几个开始
    sheet_names = data.sheet_names()[3:]
    all = {}
    all["dwd建表"] = []
    all["dwd加载"] = []
    all["dwd初始化旧DL"] = []
    for sheet_name in sheet_names:
        if sheet_name != '附 DMS表清单':
            table = data.sheet_by_name(sheet_name)
            sheet_info = analysis_page(table, dms_ods_mapping)
            all["dwd建表"] += dwd_table_create(sheet_info)  # dwd创建表
            all["dwd加载"] += dwd_sql_create(sheet_name, sheet_info)  # dwd加载数据
            logger.info("dwd建表ddl&加载sql-完成sheet页: {}".format(sheet_name))
            if len(sheet_info['odl_table_short']) != 0:
                all["dwd初始化旧DL"] += dwd_old_sql_create(sheet_info)
                logger.info("dwd初始化旧DL表sql-完成sheet页: {}".format(sheet_name))

    for name in all:
        path = paths[name]
        with open(path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(all[name]))
            logger.info("{}语句位置: {}".format(name, os.path.abspath(path)))
            if name == "dwd加载":
                dwd_sqls_split(path)
            if name == "dwd初始化旧DL":
                dwd_old_dl_sqls_split(path)


if __name__ == "__main__":
    pro = {
        "excel_path": r'C:\Users\dj\Desktop\my_job\DMS\02要件设计\各模块要件中间版本【持续添加】' \
                      r'\5.1-DWD\要件\销售线索\新DMS入湖变更及整合DWD要件(线索1.3)-20220507.xlsx',
        "target_dwd_table_ddls": "dwd/dwd_table_ddls.sql",
        "target_dwd_load_sqls": "dwd/dwd_load_sqls.sql"
    }
    # create_all(pro)
    print(get_old_dl_distinct({'aa': '0'}))
    # print(join_relation)
