"""
@auther dj
@date 2022/5/17
@node:
测试主方法使用:
1.配置pro中旧DL要件路径：old_dl_excel_path
2.配置pro中DWD要件路径: dwd_excel_path
3.配置pro中旧DL测试sql存储目标路径(可绝对路径可相对路径): target_old_dl_test_sqls
4.配置pro中DWD测试sql存储目标路径(可绝对路径可相对路径): target_dwd_test_sqls
5.配置pro中测试标识: test_flag - dl:生成旧DL测试语句  dwd:生成dwd测试语句 默认dl
"""
from func import test_functions as func

if __name__ == "__main__":
    pro = {
        "old_dl_excel_path": r"E:\GuangFeng\【线索】新DMS入湖变更及整合-DL方案要件V2.4.xlsx",
        "ods_excel_path": r"E:\GuangFeng\dms二手车表结构_自己.xlsx",
        "dwd_excel_path": r'D:\DMS\02要件设计\各模块要件中间版本【持续添加】\5.1-DWD\要件\售后\售后保修\【保修】新DMS入湖变更及整合DWD方案要件v2.0.xlsx',
        "target_old_dl_test_sqls": "old_dl/old_dl_test_sqls.sql",
        "target_dwd_test_sqls": "dwd/dwd_test_sqls.sql",
        "test_flag": 'dl',
        # ods表名-业务主键字典：代码会根据这个字典读取对应表名做ods测试
        "ods_table_list":{}

    }
    func.create_all(pro)
