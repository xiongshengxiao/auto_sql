import xlrd
import os

from auto_sql.func import functions

try:
    from loguru import logger
except:
    import logging as logger

    logger.basicConfig(level=logger.DEBUG,
                       format='%(asctime)s [%(name)s:%(lineno)d] [%(module)s:%(funcName)s] -%(levelname)s:%(message)s')


def odl_type_mapping(source_type):
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
    if sheet_rows < 7:
        sheet_info['flag'] = 0
        return sheet_info
    sheet_info['field_mapping'] = []
    sheet_info['table_short'] = {}
    sheet_info['dms_ods'] = {}
    sheet_info['is_part'] = False
    for rownum in range(7, sheet_rows):
        row = table.row_values(rownum)
        odl_field_name = row[1].strip()  # 旧dl字段注释
        odl_field_id = row[2].strip().lower()  # 旧dl字段名
        odl_field_type = row[4].strip().lower()  # 旧dl字段属性
        dms_table_id = row[5].strip().lower()  # DMS表名
        dms_field_name = row[6].strip()  # DMS字段注释
        dms_field_id = row[7].strip().lower()  # DMS字段名
        dms_field_type = row[9].strip().lower()  # DMS字段类型
        # conversion_code= row[14].strip().lower() # ods-旧dl的转换代码
        if odl_field_id == '':
            continue
        if odl_field_id == 'biz_date':
            sheet_info['is_part'] = True
        li = [odl_field_id[0], odl_field_id[-1]]
        if '_' in li:
            odl_field_id = '`' + odl_field_id + '`'
        row_arr = [odl_field_name, odl_field_id, odl_field_type,
                   dms_table_id, dms_field_name, dms_field_id, dms_field_type
                   ]
        if dms_table_id != '' and len(dms_table_id) > 3 and dms_table_id not in sheet_info['dms_ods']:
            sheet_info['table_short'][dms_table_id] = ''
            sheet_info['dms_ods'][dms_table_id] = dms_ods_mapping.get(dms_table_id)
        sheet_info['field_mapping'].append(row_arr)
    odl_table_name = table.cell(5, 2).value  # 旧DL表名称
    sheet_info['sheet_rows'] = sheet_rows
    sheet_info['table_comment'] = table.cell(4, 2).value  # 表注释
    sheet_info['odl_table_name'] = odl_table_name
    sheet_info['load_join_arr'] = []
    sheet_info['init_join_arr'] = []
    # dms_tables = table.cell(4, 7).value.lower().replace('、', ',').split(',')  # dms表名称
    # 解析ods拼接规则
    text = str(table.cell(7, 17)).lower().replace('\\n', ' \\n')
    if "text:" in text:
        text = text[6:-1]
    text_arr = []
    for a in text.split(" "):
        a = a.replace('\\n', '').strip()
        if a != '':
            text_arr.append(a)
    target_join = " ".join(text_arr)
    dms_tables = list(sheet_info['dms_ods'].keys())
    for dms_table in dms_tables:
        ods_table_name = sheet_info['dms_ods'].get(dms_table)
        sheet_info['table_short'][dms_table] = text_arr[text_arr.index(dms_table) + 1]  # 找到dms表对应简称
        # print(dms_table, target_join)
        target_join = target_join.replace(dms_table + ' ', "ods." + ods_table_name + ' ')

    if 'from' not in target_join:
        logger.error("注意调整代码和要件-join段未写'from': {}".format(odl_table_name))
    if ' t_' in target_join:
        logger.error("注意调整代码和要件-join段表比实际用到表多: {}".format(odl_table_name))

    target_join = target_join.replace('left join', '\\n left join').replace(' on ', '\\n on ')

    sheet_info['init_join_arr'] = target_join.split('\\n')
    if len(dms_tables) == 1:
        sheet_info['load_join_arr'] = target_join.split('\\n')  # 拼接查询的ods表
        sheet_info['load_join_arr'].append("where biz_date = '${YESTERDAY}'")  # 拼接查询的ods表逻辑分区条件
        sheet_info['flag'] = 1
    else:
        target_join = "left join " + target_join[5:]
        sheet_info['load_join_arr'].append("from ods_increment inc ")
        sheet_info['load_join_arr'] += target_join.split('\\n')
        sheet_info['flag'] = 2
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
    ods_increment_arr.append(")")
    return ods_increment_arr


def old_dl_sql_create(sheet_name, sheet_info):
    """
    @该方法根据excel 新旧映射要件拼接写旧dl sql
    @path:excel文件路径
    @sheet_name:excel的sheet页
    """

    flag = sheet_info['flag']
    if flag == 0:
        logger.error("要件格式不符合要求: {}".format(sheet_name))
        return []
    field_mapping = sheet_info['field_mapping']
    odl_table_name = sheet_info['odl_table_name']  # 旧DL表名称
    init_join_arr = sheet_info['init_join_arr']  # 增量取数join段 []
    load_join_arr = sheet_info['load_join_arr']  # 增量取数join段 []
    table_short = sheet_info['table_short']  # dms表或ods表对应简称 {}
    dms_ods = sheet_info['dms_ods']  # dms表与ods表映射 {}
    is_part = sheet_info['is_part']
    p_key = 'id'
    sql0 = []  # sql0拼接设置sql段
    sql1 = []  # sql1拼接从ods取数sql段
    sql2 = []  # sql2拼接insert段
    init_sql0 = []  # sql0拼接设置sql段
    init_sql1 = []  # sql1拼接从ods取数sql段
    init_sql2 = []  # sql2拼接insert段
    logs = []
    for row in field_mapping:
        odl_field_name, odl_field_id, odl_field_type, dms_table_name, ods_field_name, ods_field_id = row[:6]

        # sql1:从ods取数sql需做判断
        dms_table_name = dms_table_name.split(',')[0]
        if dms_table_name != '':
            try:
                short = table_short.get(dms_table_name)
            except:
                logs.append("sheet页: {} ---表【{}】未在join段或dms表处列出---" \
                            .format(sheet_name, dms_table_names[0]))
                short = '此处需修改'
        else:
            short = table_short.get(list(table_short.keys())[0])
        if odl_field_id == "operate_type":
            row_i = ", if(" + short + ".del_flag=1 ,'D', 'U') as operate_type -- 数据操作类型"
        elif odl_field_id == "batch_id":
            row_i = ", cast(" + short + ".dl_batch_id as int) as batch_id  --dl加载批次"
        elif odl_field_id == "extract_time":
            row_i = ", " + short + ".extract_time as extract_time  --抽取表时的系统时间（HBase-Hive）"
        elif odl_field_id == "load_time":
            row_i = ", substr(current_timestamp(), 1, 19) as load_time  --数据加载到表的时间"
        elif odl_field_id == "biz_date":
            if flag == 1:
                row_i = ", from_unixtime(unix_timestamp(" + short + ".biz_date,'yyyyMMdd')+86400," \
                                                                    "'yyyyMMdd') as biz_date  --分区字段"
            else:
                # 如果多表join则biz_date取最大
                arr = ["greatest("]
                for t in table_short:
                    short = table_short[t]
                    str = "nvl(" + short + ".biz_date, '00000000'),"
                    arr.append(str)
                row_i = ", from_unixtime(unix_timestamp(" \
                        + ''.join(arr)[:-1] + "),'yyyyMMdd')+86400,'yyyyMMdd') as biz_date  --分区字段"
        elif ods_field_id == 'update_time':
            if flag == 1:
                row_i = ", " + short + '.' + ods_field_id + " as " \
                        + odl_field_id + "  --" + ods_field_name
            else:
                # 如果多表join则biz_date取最大
                arr = [", greatest("]
                for t in table_short:
                    short = table_short[t]
                    str = "nvl(" + short + ".update_time, '00000000'),"
                    arr.append(str)
                row_i = ''.join(arr)[:-1] + ") as " + odl_field_id + "  --更新时间"
        else:
            # 该字段不是固定字段
            field_type = odl_type_mapping(odl_field_type)
            main_type = field_type.split('(')[0]
            if ods_field_id == '':
                # 不是固定字段且ods_field_id为空，则该字段应是补充的字段，默认写空串或0
                if main_type in ('varchar', 'string'):
                    row_i = ", '' as " + odl_field_id + "  --" + odl_field_name
                else:
                    row_i = ", 0 as " + odl_field_id + "  --" + odl_field_name
            else:
                # 如果不是固定字段，且ods_field_id不为空，则该字段与ods有字段映射，正常取数
                # sql2:拼接从ods增量取数sql
                dms_table_names = dms_table_name.split(',')
                if len(dms_table_names) > 1:
                    logs.append("sheet页: {} ---字段【{}】规则需手动修改---" \
                                .format(sheet_name, odl_field_name))
                if flag == 1:
                    row_i = ", " + short + '.' + ods_field_id + " as " \
                            + odl_field_id + "  --" + ods_field_name
                elif main_type in ('varchar', 'string'):
                    row_i = ", nvl(" + short + '.' + ods_field_id + ",'') as " \
                            + odl_field_id + "  --" + ods_field_name
                else:
                    row_i = ", nvl(" + short + '.' + ods_field_id + ",0) as " \
                            + odl_field_id + "  --" + ods_field_name
        sql1.append(row_i)
    for log in logs[:2]:
        logger.warning(log)
    sql1[0] = "select " + sql1[0][1:]  # 去除第一行拼接的多余逗号
    init_sql1 = sql1.copy()
    sql0.append('set mapred.job.name=job_' + odl_table_name + ';')
    sql0.append('--set hive.exec.parallel=true;')
    sql0.append('--set hive.map.aggr=true;')
    sql0.append('--set hive.groupby.skewindata=true;')
    init_sql0.append('set mapred.job.name=job_' + odl_table_name + '_init;')
    init_sql0.append('--set hive.exec.parallel=true;')
    init_sql0.append('--set hive.map.aggr=true;')
    init_sql0.append('--set hive.groupby.skewindata=true;')

    if is_part:
        init_sql0.append('set hive.exec.dynamic.partition=true;')
        init_sql0.append('set hive.exec.dynamic.partition.mode=nonstrict;')
        init_sql0.append('set hive.exec.max.dynamic.partitions.pernode=1000;')
        init_sql0.append('set hive.exec.max.dynamic.partitions=5000;')
        sql1 = sql1[:-1]
        sql2.append("insert ")
        sql2.append("overwrite ")
        sql2.append("table dl." + odl_table_name + " partition (biz_date = '${TODAY}')")
        init_sql2.append("insert ")
        init_sql2.append("into ")
        init_sql2.append("table dl." + odl_table_name + " partition (biz_date)")
    else:
        sql2.append("insert ")
        sql2.append("into ")
        sql2.append("table dl." + odl_table_name)
        init_sql2 = sql2.copy()

    sql1 += load_join_arr
    sql1.append(";")

    init_sql1 += init_join_arr
    if not is_part:
        if flag == 1:
            init_sql1.append("where biz_date < '${TODAY}'")
        else:
            init_sql1.append("--where biz_date < '${TODAY}'")
    init_sql1.append(";")

    ods_increment_sql = []
    if flag > 1:
        ods_increment_sql = get_ods_increment(table_short, dms_ods)

    complete_sql = sql0 + ods_increment_sql + sql2 + sql1
    complete_sql.append("\n\n")  # 添加空行

    init_complete_sql = init_sql0 + init_sql2 + init_sql1
    init_complete_sql.append("\n\n")  # 添加空行

    return complete_sql, init_complete_sql


def odl_sqls_split(path, flag='load'):
    """
    # 该函数读取旧dl加载数据的脚本，并拆分为单个表的文件,包含加载和初始化
    """
    abspath = os.path.abspath(os.path.dirname(path))
    if flag == 'init':
        split_path = abspath + "\\odl_init_sqls_split"
    else:
        split_path = abspath + "\\odl_load_sqls_split"
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
        f = (split_path + '/' + file).replace("\\", "/")
        with open(f, 'w', encoding='utf-8') as f:
            f.write('\n'.join(files[file]))
    if flag == 'init':
        logger.info("旧DL拆分初始化文件完成 {} 个".format(file_num))
        logger.info("旧DL拆分初始语句位置: " + os.path.abspath(split_path))
    else:
        logger.info("旧DL拆分加载文件完成 {} 个".format(file_num))
        logger.info("旧DL拆分加载语句位置: " + os.path.abspath(split_path))
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
        ods, dms = row[6:8]
        ods = ods.strip()
        dms = dms.strip()
        if ods == '' or dms == '':
            logger.error('要件输入不符合要求,ods-dms表映射有空 ！！！！！')
        else:
            if dms not in mapping:
                mapping[dms] = ods
    return mapping


def create_all(pro):
    """
    该函数执行dl与ods的建表及加载脚本，并将结果写到对应路径
    """
    path = pro.get("excel_path")
    paths = {
        "旧dl初始化": pro.get("target_old_dl_init_sqls", "old_dl/old_dl_init_sqls.sql"),
        "旧dl加载": pro.get("target_old_dl_load_sqls", "old_dl/old_dl_load_sqls.sql")
    }
    data = xlrd.open_workbook(path)
    dms_ods_mapping = get_ods_tables(data)
    sheet_names = data.sheet_names()[3:40]
    all = {"旧dl初始化": [], "旧dl加载": []}
    multi_table_num = 0
    multi_tables = []
    for sheet_name in sheet_names:
        is_join = '单表'
        table = data.sheet_by_name(sheet_name)
        sheet_info = analysis_page(table, dms_ods_mapping)
        if sheet_info['flag'] == 2:
            is_join = '多表'
            multi_table_num = multi_table_num + 1
            multi_tables.append(sheet_info['odl_table_name'])
        load_sql, init_sql = old_dl_sql_create(sheet_name, sheet_info)  # 旧dl加载和初始化数据
        all["旧dl加载"] += load_sql
        all["旧dl初始化"] += init_sql
        logger.info("old_dl加载&初始化sql-完成sheet页({}): {}".format(is_join,sheet_name))
    logger.info("多写一情况的旧dl表个数: {}".format(multi_table_num))
    logger.info("多写一情况的旧dl表: {}".format(' '.join(multi_tables)))
    with open(paths["旧dl初始化"], 'w', encoding='utf-8') as f:
        f.write('\n'.join(all["旧dl初始化"]))
        logger.info("{}语句位置: {}".format("旧dl初始化", os.path.abspath(paths["旧dl初始化"])))
    with open(paths["旧dl加载"], 'w', encoding='utf-8') as f:
        f.write('\n'.join(all["旧dl加载"]))
        logger.info("{}语句位置: {}".format("旧dl加载", os.path.abspath(paths["旧dl加载"])))
    odl_sqls_split(paths["旧dl初始化"], flag='init')
    odl_sqls_split(paths["旧dl加载"], flag='load')


if __name__ == "__main__":
    pro = {
        "excel_path": r'C:\Users\dj\Desktop\001.xlsx',
        "target_old_dl_init_sqls": "old_dl/old_dl_init_sqls.sql",
        "target_old_dl_load_sqls": "old_dl/old_dl_load_sqls.sql"
    }
    create_all(pro)
    # print(get_old_dl_distinct({'aa':'0'}))
    # print(join_relation)
