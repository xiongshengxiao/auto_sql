"""
@auther dj
@date 2022/5/16
@node: 
"""

# 是否存在null
import os

import xlrd

from auto_sql.func.functions import TypeMap
from auto_sql.func.table_v2 import Table
from auto_sql.func.table_v2 import Table_old

try:
    from loguru import logger
except:
    import logging as logger

    logger.basicConfig(level=logger.DEBUG,
                       format='%(asctime)s [%(name)s:%(lineno)d] [%(module)s:%(funcName)s] -%(levelname)s:%(message)s')


def get_test_sql(path, flag, pro):
    data = xlrd.open_workbook(path)
    if flag == 'dl':
        sheet_names = data.sheet_names()[3:]
        database = 'dl.'
    elif flag == 'dwd':
        sheet_names = data.sheet_names()[4:]
        database = 'dwd.'
    else:
        sheet_names = data.sheet_names()
        database = 'ods.'
    head = ''
    sql = ''
    num = 0
    if flag == 'dl' or flag == 'dwd':
        for sheet in sheet_names:
            table = data.sheet_by_name(sheet)  # 通过名称获取sheet内容
            table_name = database + table.cell(4, 2).value.strip()   # 表名称
            num += 1
            head += f"--sheet{num}:{sheet}:{table_name}\n".format(num=num, sheet=sheet, table_name=table_name)
            nrows = table.nrows  # 数据行数
            table_dict = {}
            for rownum in range(6, nrows):
                row = table.row_values(rownum)
                field_name, field_id, aa, field_type = row[1:5]
                field_type = TypeMap().ods_map_target(field_type)
                field_id = field_id.strip()
                if field_id == '':
                    # logger.error('sheet: {sheet}  第{row}行-字段名为空！！！'.format(sheet=sheet,row=rownum))
                    break
                table_dict[field_id] = [field_id, field_name, field_type]
            my_table = Table(table_name, table_dict)
            sql += my_table.get_test_all_sql()
            logger.info('测试sql-完成sheet页: ' + sheet)
    else:
        for sheet in sheet_names:
            table = data.sheet_by_name(sheet)  # 通过名称获取sheet内容
            # 1,先判断列表是否为空
            # 2,
            if len(pro.get("ods_table_list")) == 0:
                table_name = database + 'ods_dms_' + table.cell(2, 0).value.strip() + table.cell(2, 2).value.strip()[
                                                                                      1:]  # 表名称
                num += 1
                head += f"--sheet{num}:{sheet}:{table_name}\n".format(num=num, sheet=sheet, table_name=table_name)
                nrows = table.nrows  # 数据行数
                table_dict = {}
                for rownum in range(4, nrows):
                    row = table.row_values(rownum)
                    field_name, field_id, field_type = row[1:4]
                    field_type = TypeMap().ods_map_target(field_type)
                    field_id = field_id.strip()
                    if field_id == '':
                        # logger.error('sheet: {sheet}  第{row}行-字段名为空！！！'.format(sheet=sheet,row=rownum))
                        break
                    table_dict[field_id] = [field_id, field_name, field_type]
                table_dict['dl_batch_id'] = ['dl_batch_id', 'dl加载批次', 'string']
                table_dict['extract_time'] = ['extract_time', '抽取表时的系统时间（以北京时间为标准时区时间）', 'string']
                table_dict['load_time'] = ['load_time', '抽取表时的系统时间（以北京时间为标准时区时间）', 'string']
                table_dict['source_system'] = ['source_system', '来源系统', 'string']
                table_dict['insert_time'] = ['insert_time', '数据写入时间', 'string']
                table_dict['biz_date'] = ['biz_date', '分区字段', 'string']
                my_table = Table_old(table_name, table_dict, {'id':'id'})
                sql += my_table.get_test_all_sql()
                logger.info('测试sql-完成sheet页: ' + sheet)


            else:
                if 'ods_dms_' + table.cell(2, 0).value.strip() + table.cell(2, 2).value.strip()[1:] in pro.get(
                        "ods_table_list").keys():
                    table_name = database + 'ods_dms_' + table.cell(2, 0).value.strip() + table.cell(2, 2).value.strip()[
                                                                                          1:]  # 表名称
                    num += 1
                    head += f"--sheet{num}:{sheet}:{table_name}\n".format(num=num, sheet=sheet, table_name=table_name)
                    nrows = table.nrows  # 数据行数
                    table_dict = {}
                    for rownum in range(4, nrows):
                        row = table.row_values(rownum)
                        field_name, field_id, field_type = row[1:4]
                        field_type = TypeMap().ods_map_target(field_type)
                        field_id = field_id.strip()
                        if field_id == '':
                            # logger.error('sheet: {sheet}  第{row}行-字段名为空！！！'.format(sheet=sheet,row=rownum))
                            break
                        table_dict[field_id] = [field_id, field_name, field_type]
                    table_dict['dl_batch_id'] = ['dl_batch_id', 'dl加载批次', 'string']
                    table_dict['extract_time'] = ['extract_time', '抽取表时的系统时间（以北京时间为标准时区时间）', 'string']
                    table_dict['load_time'] = ['load_time', '抽取表时的系统时间（以北京时间为标准时区时间）', 'string']
                    table_dict['source_system'] = ['source_system', '来源系统', 'string']
                    table_dict['insert_time'] = ['insert_time', '数据写入时间', 'string']
                    table_dict['biz_date'] = ['biz_date', '分区字段', 'string']
                    my_table = Table_old(table_name, table_dict,pro.get("ods_table_list"))
                    sql += my_table.get_test_all_sql()
                    logger.info('测试sql-完成sheet页: ' + sheet)


    return head + sql


def create_all(pro):
    flag = pro.get("test_flag")
    if flag == 'dl':
        path = pro.get("old_dl_excel_path")
        test_sql_path = pro.get("target_old_dl_test_sqls", "old_dl/old_dl_test_sqls.sql")
        info_head = "旧dl测试语句位置: "
    elif flag == 'dwd':
        path = pro.get("dwd_excel_path")
        test_sql_path = pro.get("target_dwd_test_sqls", "dwd/dwd_test_sqls.sql")
        info_head = "dwd测试语句位置: "
    elif flag == 'ods':
        path = pro.get("ods_excel_path")
        test_sql_path = pro.get("target_ods_test_sqls", "ods/ods_test_sqls.sql")
        info_head = "ods测试语句位置: "
    else:
        logger.error('main_test.py --test_flag未按要求输入')
        return False

    abspath = os.path.abspath(os.path.dirname(test_sql_path))
    isExists = os.path.exists(abspath)
    if not isExists:
        os.makedirs(abspath)

    sql = get_test_sql(path, flag,pro)
    with open(test_sql_path, 'w', encoding='utf-8') as f:
        f.write(sql)
        logger.info(info_head + os.path.abspath(test_sql_path))


if __name__ == '__main__':
    path = r'/DMS/02要件设计/各模块要件中间版本【持续添加】/2.2-销售线索/新DMS入湖变更及整合-DL方案要件(线索).xlsx'
    print(get_test_sql(path,flag='ods'))
    # print(null_format.format(table_name='dl.tg_tact_vhc_order_tb',filter_cdt='a is null \nor b is null'))
