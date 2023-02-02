"""
dl&ods主方法使用:
1.配置pro中入湖表dms表结构路径：excel_path
2.配置pro中dl建表语句存储文件路径(可绝对路径可相对路径): target_dl_table_ddls
3.配置pro中dl加载数据语句存储文件路径(可绝对路径可相对路径): target_dl_load_sqls
4.配置pro中ods建表语句存储文件路径(可绝对路径可相对路径): target_ods_table_ddls
5.配置pro中ods初始化语句存储文件路径(可绝对路径可相对路径): target_ods_init_sqls
    ,该文件父目录下会自动创建ods_init_sqls_split目录，生成拆分的单个表的初始化文件
6.配置pro中ods加载数据语句存储文件路径(可绝对路径可相对路径): target_ods_load_sqls
    ,该文件父目录下会自动创建ods_load_sqls_split目录，生成拆分的单个表的初始化文件
7.执行，以上按需配置想要的路径即可，其它默认路径
"""
from auto_sql.func import functions as func

if __name__ == "__main__":
    pro = {
        "excel_path": r"E:\GuangFeng\dms二手车表结构_自己.xlsx",
        "kafka_topic": "sale_dms_supply_prd",
        "target_dl_table_ddls": "dl/dl_table_ddls.sql",
        "target_dl_load_sqls": "dl/dl_load_sqls.sql",
        "target_ods_table_ddls": "ods/ods_table_ddls.sql",
        "target_ods_init_sqls": "ods/ods_init_sqls.sql",
        "target_ods_load_sqls": "ods/ods_load_sqls.sql"
    }
    func.create_all(pro)
