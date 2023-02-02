"""
dwd主方法使用:
1.配置pro中dwd要件路径：excel_path
2.配置pro中dwd建表语句存储路径(可绝对路径可相对路径): target_dwd_table_ddls
3.配置pro中dwd加载数据语句存储路径(可绝对路径可相对路径): target_dwd_load_sqls
4.执行
"""
from func import old_dl_functions_wangjian as func

if __name__ == "__main__":
    pro = {
        "excel_path": r"E:\GuangFeng\【二手车】新DMS入湖变更及整合-DL方案要件v1.7(单表测试).xlsx",
        "target_old_dl_init_sqls": "old_dl/old_dl_init_sqls.sql",
        "target_old_dl_load_sqls": "old_dl/old_dl_load_sqls.sql"
    }
    func.create_all(pro)