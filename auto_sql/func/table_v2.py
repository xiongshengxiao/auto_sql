"""
@auther dj
@date 2022/5/16
@node: 
"""


class Table:
    def __init__(self, table_name, table_dict, key='id'):
        self.table_name = table_name
        self.fields = table_dict
        self.key = key

    def get_field_type(self, field):
        return self.fields[field][2]

    def get_field_comment(self, field):
        return self.fields[field][1]

    def get_test_row_num_sql(self):
        str_format = '''-- TC-01 数据条数:{table_name}
                    with target_table as ( select count(1) as row_num from {table_name} where 1=1),
                    source_table as ( select count(1) as row_num from `源表` where 1=1)
                    select 'TC-01' as `测试分类`,
                    '目标表' as `测试表`,
                    '数据条数' as `测试项` ,
                    row_num as `数据条数` ,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from target_table t
                    union all
                    select 'TC-01' as `测试分类`,
                    '源表' as `测试表`,
                    '数据条数' as `测试项` ,
                    row_num as `数据条数` ,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from source_table s '''
        sql = str_format.format(table_name=self.table_name)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_enumerate_sql(self):
        str_format = '''-- TC-02 列值表转换:{table_name}
                    with target_table as ( select * from {table_name} where 1=1),
                    source_table as ( select * from `源表` where 1=1)
                    select 'TC-02' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '枚举转换' as `测试项` ,
                    s.field_s ,t.field_t,count(1),
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from target_table t
                    join source_table s 
                    on s.id_s = t.id_t 
                    group by s.field_s ,t.field_t'''
        sql = str_format.format(table_name=self.table_name)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_time_sql(self):
        str_format = '''-- TC-03 日期/时间格式转换:{table_name}
                    with test_table as ( select * from {table_name} where 1=1)
                    select 'TC-03' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '日期格式' as `测试项` ,
                    num as `异常行数`,
                    if(num=0,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from (
                    select count(1) as num from test_table
                    where {filter_cdt})a'''
        filter_cdt = ''
        for field in self.fields:
            if len(field) > 4 and field[-4:] in ('time', 'date') and field not in (
                    'extract_time', 'load_time', 'biz_date', 'insert_time'):
                filter_cdt = filter_cdt + ("\nor (if ({field} regexp ".format(field=field)) + (
                    "'^[1,2][0-9]{3}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', 1, 0)=0") + (
                                 " and {field}<>'' and {field}<>'0000-00-00 00:00:00')".format(field=field))
        filter_cdt = filter_cdt[4:]
        sql = str_format.format(table_name=self.table_name, filter_cdt=filter_cdt)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_null_sql(self):
        str_format = '''-- TC-04 是否存在null值(数值) :{table_name}
                    with test_table as ( select * from {table_name} where 1=1)
                    select 'TC-04' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '是否存在null' as `测试项` ,
                    num as `null数`,
                    if(num=0,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from (
                    select count(1) as num from test_table
                    where {filter_cdt})a'''
        filter_cdt = ''
        for field in self.fields:
            if self.get_field_type(field) != "string":
                filter_cdt = filter_cdt + '\nor {} is null'.format(field)
        filter_cdt = filter_cdt[4:]
        sql = str_format.format(table_name=self.table_name, filter_cdt=filter_cdt)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_null_string_sql(self):
        str_format = '''-- TC-05 是否存在null值(字符串):{table_name}
                    with test_table as ( select * from {table_name} where 1=1)
                    select 'TC-05' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '是否存在null' as `测试项` ,
                    num as `null数`,
                    if(num=0,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from (
                    select count(1) as num from test_table
                    where {filter_cdt})a'''
        filter_cdt = ''
        for field in self.fields:
            if self.get_field_type(field) == "string":
                filter_cdt = filter_cdt + '\nor {} is null'.format(field)
        filter_cdt = filter_cdt[4:]
        sql = str_format.format(table_name=self.table_name, filter_cdt=filter_cdt)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_space_sql(self):
        str_format = '''-- TC-06 数据内容前后去空格:{table_name}
                    with test_table as ( select * from {table_name} where 1=1)
                    select 'TC-06' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '是否存在前后空格' as `测试项` ,
                    num as `异常行数`,
                    if(num=0,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from (
                    select count(1) as num from test_table
                    where {filter_cdt})a'''
        filter_cdt = ''
        for field in self.fields:
            if self.get_field_type(field).split('(')[0] in ('varchar', 'string'):
                filter_cdt = filter_cdt + "\nor {field}<>trim({field})".format(field=field)
        filter_cdt = filter_cdt[4:]
        sql = str_format.format(table_name=self.table_name, filter_cdt=filter_cdt)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_key_sql(self):
        str_format = '''-- TC-07 主键重复:{table_name}
                    with test_table as ( select * from {table_name} where 1=1)
                    select 'TC-07' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '业务主键去重' as `测试项` ,
                    row_num-dis_num as `异常行数`,
                    if(row_num=dis_num,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from (
                    select count(1) as row_num,
                    count(distinct {key}) as dis_num 
                    from test_table
                    where {filter_cdt})a'''
        filter_cdt = '1=1'
        sql = str_format.format(table_name=self.table_name, key=self.key, filter_cdt=filter_cdt)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_foreign_key_sql(self):
        str_format = '''-- TC-08 外键校验:{table_name}
                    with test_table as ( select '填写外键字段' as foreign_key
                    from {table_name} where 1=1 group by '填写外键字段'),
                    dim_table as (select '关联字段' as join_key
                    from '子表或维表' where 1=1 group by '关联字段')
                    select 'TC-08' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '外键校验' as `测试项` ,
                    num as `异常行数`,
                    if(num=0,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from ( select count(1) as num from 
                    test_table t 
                    left join dim_table d 
                    on t.foreign_key = d.join_key 
                    where d.join_key is null
                    )a'''
        sql = str_format.format(table_name=self.table_name)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_stagger_sql(self):
        str_format = '''-- TC-09 数据错列:{table_name}
                    with test_table as ( select * from {table_name} where 1=1)
                    select 'TC-09' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '是否有错列' as `测试项` ,
                    num as `异常行数`,
                    if(num=0,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from (
                    select count(1) as num from test_table
                    where {filter_cdt})a'''
        filter_cdt = ''
        for field in self.fields:
            if field in ('insert_time'):
                filter_cdt = filter_cdt + ("\nor if ({field} regexp ".format(field=field)) + (
                    "'^[1,2][0-9]{3}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', 1, 0)=0") + (
                                 " or {field}='' or {field} is null ".format(field=field))
        filter_cdt = filter_cdt[4:]
        sql = str_format.format(table_name=self.table_name, filter_cdt=filter_cdt)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_all_sql(self):
        head = "\n--============" + "=" * len(self.table_name) + "============--\n"
        head = head + "--XX============" + self.table_name + "============--\n"
        head = head + "--============" + "=" * len(self.table_name) + "============--\n"
        sql = ';\n\n'.join([self.get_test_row_num_sql(),
                            self.get_test_enumerate_sql(),
                            self.get_test_time_sql(),
                            self.get_test_null_sql(),
                            self.get_test_null_string_sql(),
                            self.get_test_space_sql(),
                            self.get_test_key_sql(),
                            self.get_test_foreign_key_sql(),
                            self.get_test_stagger_sql()]
                           )
        return head + sql + "\n\n"



"""
@auther dj
@date 2022/5/16
@node: 
"""


class Table_old:
    def __init__(self, table_name, table_dict, key):
        self.table_name = table_name
        self.fields = table_dict
        self.key = key

    def get_field_type(self, field):
        return self.fields[field][2]

    def get_field_comment(self, field):
        return self.fields[field][1]

    def get_test_row_num_sql(self):
        str_format = '''-- TC-01 数据条数:{table_name}
                    with target_table as ( select count(1) as row_num from {table_name} where 1=1),
                    source_table as ( select count(distinct {key}) as row_num from {dl_table_name} where biz_date <= '20221023')
                    select 'TC-01' as `测试分类`,
                    '目标表' as `测试表`,
                    '数据条数' as `测试项` ,
                    row_num as `数据条数` ,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from target_table t
                    union all
                    select 'TC-01' as `测试分类`,
                    '源表' as `测试表`,
                    '数据条数' as `测试项` ,
                    row_num as `数据条数` ,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from source_table s '''
        sql = str_format.format(table_name=self.table_name,dl_table_name = 'dl.tg_dms_' + self.table_name[11:].split('_')[1] + '_t' + self.table_name[self.table_name.find('_',12):], key = self.key.get(self.table_name[4:]))
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_enumerate_sql(self):
        str_format = '''-- TC-02 列值表转换:{table_name}
                    -- with target_table as ( select * from {table_name} where 1=1),
                    -- source_table as ( select * from {dl_table_name} where 1=1)
                    -- select 'TC-02' as `测试分类`,
                    -- '{table_name}' as `测试表`,
                    -- '枚举转换' as `测试项` ,
                    -- s.field_s ,t.field_t,count(1),
                    -- substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    -- from target_table t
                    -- join source_table s 
                    -- on s.id_s = t.id_t 
                    -- group by s.field_s ,t.field_t'''
        sql = str_format.format(table_name=self.table_name,dl_table_name = 'dl.tg_dms_' + self.table_name[11:].split('_')[1] + '_t' + self.table_name[self.table_name.find('_',12):-4])
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_time_sql(self):
        str_format = '''-- TC-03 日期/时间格式转换:{table_name}
                    with test_table as ( select * from {table_name} where 1=1)
                    select 'TC-03' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '日期格式' as `测试项` ,
                    num as `异常行数`,
                    if(num=0,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from (
                    select count(1) as num from test_table
                    where {filter_cdt})a'''
        filter_cdt = ''
        for field in self.fields:
            if len(field) > 4 and field[-4:] in ('time', 'date') and field not in (
                    'extract_time', 'load_time', 'biz_date', 'insert_time'):
                filter_cdt = filter_cdt + ("\nor (if ({field} regexp ".format(field=field)) + (
                    "'^[1,2][0-9]{3}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', 1, 0)=0") + (
                                 " and {field}<>'' and {field}<>'0000-00-00 00:00:00')".format(field=field))
        filter_cdt = filter_cdt[4:]
        sql = str_format.format(table_name=self.table_name, filter_cdt=filter_cdt)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_null_sql(self):
        str_format = '''-- TC-04 是否存在null值(数值) :{table_name}
                    with test_table as ( select * from {table_name} where 1=1)
                    select 'TC-04' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '是否存在null' as `测试项` ,
                    num as `null数`,
                    if(num=0,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from (
                    select count(1) as num from test_table
                    where {filter_cdt})a'''
        filter_cdt = ''
        for field in self.fields:
            if self.get_field_type(field) != "string":
                filter_cdt = filter_cdt + '\nor {} is null'.format(field)
        filter_cdt = filter_cdt[4:]
        sql = str_format.format(table_name=self.table_name, filter_cdt=filter_cdt)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_null_string_sql(self):
        str_format = '''-- TC-05 是否存在null值(字符串):{table_name}
                    with test_table as ( select * from {table_name} where 1=1)
                    select 'TC-05' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '是否存在null' as `测试项` ,
                    num as `null数`,
                    if(num=0,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from (
                    select count(1) as num from test_table
                    where {filter_cdt})a'''
        filter_cdt = ''
        for field in self.fields:
            if self.get_field_type(field) == "string":
                filter_cdt = filter_cdt + '\nor {} is null'.format(field)
        filter_cdt = filter_cdt[4:]
        sql = str_format.format(table_name=self.table_name, filter_cdt=filter_cdt)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_space_sql(self):
        str_format = '''-- TC-06 数据内容前后去空格:{table_name}
                    with test_table as ( select * from {table_name} where 1=1)
                    select 'TC-06' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '是否存在前后空格' as `测试项` ,
                    num as `异常行数`,
                    if(num=0,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from (
                    select count(1) as num from test_table
                    where {filter_cdt})a'''
        filter_cdt = ''
        for field in self.fields:
            if self.get_field_type(field).split('(')[0] in ('varchar', 'string'):
                filter_cdt = filter_cdt + "\nor {field}<>trim({field})".format(field=field)
        filter_cdt = filter_cdt[4:]
        sql = str_format.format(table_name=self.table_name, filter_cdt=filter_cdt)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_key_sql(self):
        str_format = '''-- TC-07 主键重复:{table_name}
                    with test_table as ( select * from {table_name} where 1=1)
                    select 'TC-07' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '业务主键去重' as `测试项` ,
                    row_num-dis_num as `异常行数`,
                    if(row_num=dis_num,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from (
                    select count(1) as row_num,
                    count(distinct {key}) as dis_num 
                    from test_table
                    where {filter_cdt})a'''
        filter_cdt = '1=1'
        key_01 = self.key.get(self.table_name[4:])
        sql = str_format.format(table_name=self.table_name, key=key_01, filter_cdt=filter_cdt)
        sql = sql.replace(' ' * 20, '')
        return sql

    # def get_test_foreign_key_sql(self):
    #     str_format = '''-- TC-08 外键校验:{table_name}
    #                 -- with test_table as ( select '填写外键字段' as foreign_key
    #                 -- from {table_name} where 1=1 group by '填写外键字段'),
    #                 -- dim_table as (select '关联字段' as join_key
    #                 -- from '子表或维表' where 1=1 group by '关联字段')
    #                 -- select 'TC-08' as `测试分类`,
    #                 -- '{table_name}' as `测试表`,
    #                 -- '外键校验' as `测试项` ,
    #                 -- num as `异常行数`,
    #                 -- if(num=0,'通过','未通过') as `测试结果`,
    #                 -- substring (cast (current_timestamp() as string),1,19) as `测试时间`
    #                 -- from ( select count(1) as num from
    #                 -- test_table t
    #                 -- left join dim_table d
    #                 -- on t.foreign_key = d.join_key
    #                 -- where d.join_key is null
    #                 -- )a'''
    #     sql = str_format.format(table_name=self.table_name)
    #     sql = sql.replace(' ' * 20, '')
    #     return sql

    def get_test_stagger_sql(self):
        str_format = '''-- TC-08 数据错列:{table_name}
                    with test_table as ( select * from {table_name} where 1=1)
                    select 'TC-08' as `测试分类`,
                    '{table_name}' as `测试表`,
                    '是否有错列' as `测试项` ,
                    num as `异常行数`,
                    if(num=0,'通过','未通过') as `测试结果`,
                    substring (cast (current_timestamp() as string),1,19) as `测试时间`
                    from (
                    select count(1) as num from test_table
                    where {filter_cdt})a;'''
        filter_cdt = ''
        for field in self.fields:
            if field in ('load_time', 'insert_time'):
                filter_cdt = filter_cdt + ("\nor if ({field} regexp ".format(field=field)) + (
                    "'^[1,2][0-9]{3}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', 1, 0)=0") + (
                                 " or {field}='' or {field} is null ".format(field=field))
        filter_cdt = filter_cdt[4:]
        sql = str_format.format(table_name=self.table_name, filter_cdt=filter_cdt)
        sql = sql.replace(' ' * 20, '')
        return sql

    def get_test_all_sql(self):
        head = "\n--============" + "=" * len(self.table_name) + "============--\n"
        head = head + "--XX============" + self.table_name + "============--\n"
        head = head + "--============" + "=" * len(self.table_name) + "============--\n"
        sql = ';\n\n'.join([self.get_test_row_num_sql(),
                            self.get_test_enumerate_sql(),
                            self.get_test_time_sql(),
                            self.get_test_null_sql(),
                            self.get_test_null_string_sql(),
                            self.get_test_space_sql(),
                            self.get_test_key_sql(),
                            # self.get_test_foreign_key_sql(),
                            self.get_test_stagger_sql()]
                           )
        return head + sql + "\n\n"

