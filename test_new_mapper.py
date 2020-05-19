# import json
# import os
# import pandas as pd
# import moz_sql_parser as msp
# from parsers import SQLParser, custom_reducer, custom_mapper
#
#
# def custom_mapper(file_name, key_column, col_names, field_delimiter):
#     def update_col_names():
#         df_col_names = list(data_frame)
#         if col_names[0]['old_name'] == '*':
#             res = data_frame.copy()
#             return res
#
#         for i in range(len(col_names)):
#             for j in range(len(df_col_names)):
#                 if df_col_names[j] == col_names[i]['old_name']:
#                     df_col_names[j] = col_names[i]['new_name']
#
#         res = data_frame.copy()
#         res.columns = df_col_names
#         return res
#
#     data_frame = pd.read_csv(file_name, sep=field_delimiter)
#
#     data_frame = update_col_names()
#
#     data_frame['key_column'] = data_frame[key_column]
#
#     return data_frame
#
#
# def sql_parser(sql_query):
#     parsed_sql = json.dumps(msp.parse(sql_query))
#     json_res = json.loads(parsed_sql)
#     res = {}
#     if 'select' in json_res:
#         res['select'] = SQLParser.select_parser(json_res['select'])
#     if 'groupby' in json_res:
#         res['groupby'] = SQLParser.group_by_parser(json_res['groupby'])
#     if 'from' in json_res:
#         res['from'] = SQLParser.from_parser(json_res['from'])
#     return res
#
#
# def get_key_col(parsed_sql):
#     if 'groupby' in parsed_sql:
#         return parsed_sql['groupby'][0]['key_name']
#     if 'select' in parsed_sql:
#         return parsed_sql['select'][0]['new_name']
#
#
# # custom_mapper('A.csv', 'Artist').to_csv('data/A_mapped.csv', index=False, mode="w", sep=',')
#
# sql = """SELECT Artist as A
# FROM A.csv
# GROUP BY 'A'"""
#
# parsed_sql = sql_parser(sql)
# print(parsed_sql)
# key_col = get_key_col(parsed_sql)
# custom_mapper('A.csv', key_col, parsed_sql['select'], ',').to_csv('data/A_mapped.csv', index=False, mode="w", sep=',')
# # mapper = custom_mapper(parsed_select)
# # reducer = custom_reducer(parsed_select, parsed_group_by[0])
#
# # print(parsed_from)
# # print(parsed_select)
# #
# # file_path = os.path.abspath(file_name)
