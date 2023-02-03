"""
dwd主方法使用:
1.配置pro中dwd要件路径：excel_path
2.配置pro中dwd建表语句存储路径(可绝对路径可相对路径): target_dwd_table_ddls
3.配置pro中dwd加载数据语句存储路径(可绝对路径可相对路径): target_dwd_load_sqls
4.执行
"""
from func import dwd_functions_yixiao as func

if __name__ == "__main__":
    pro = {
        "excel_path": r"E:\GuangFeng\【线索】新DMS入湖变更及整合DWD要件v1.8.5.xlsx",
        "target_dwd_table_ddls": "dwd/dwd_table_ddls.sql",
        "target_dwd_load_sqls": "dwd/dwd_load_sqls.sql",
        "target_dwd_old_dl_init_sqls": "dwd/dwd_old_dl_init_sqls.sql"
    }
    func.create_all(pro)
