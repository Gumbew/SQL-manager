import json
import moz_sql_parser as msp


class SQLParser:
    @staticmethod
    def parse_aggregation_value(name, data):
        res = {'old_name': data['value'][name]}
        if 'name' in data.keys():
            res['new_name'] = f"{data['name']}"
        else:
            res['new_name'] = f"{name.upper()}_{data['value'][name]}"
        res['aggregate_f_name'] = name
        return res

    @staticmethod
    def from_parser(sql_from):
        if type(sql_from) == list:
            if 'inner join' in sql_from[1]:
                return sql_from[0], sql_from[1]['inner join']
        else:
            return sql_from

    @staticmethod
    def join_parser(data):
        join_info = data['from'][1]
        join_type = next(iter(join_info)).split(' ')[0]

        on = join_info['on']['eq']
        res = {
            'join_type': join_type,
            'on': on
        }
        return res

    @staticmethod
    def select_parser(select_data):
        res = []
        item_dict = {}
        if select_data == '*':
            item_dict['old_name'] = select_data
            item_dict['new_name'] = select_data
            res.append(item_dict)
        else:
            if type(select_data) is list:
                for i in select_data:
                    res.append(SQLParser.process_dict_item(i))
            else:
                res.append(SQLParser.process_dict_item(select_data))
        return res

    @staticmethod
    def process_dict_item(diction):
        item_dict = {}
        if type(diction['value']) is not dict:
            item_dict['old_name'] = diction['value']
            if 'name' in diction.keys():
                item_dict['new_name'] = diction['name']
            else:
                item_dict['new_name'] = diction['value']
        elif 'literal' in diction['value'].keys():
            item_dict['old_name'] = diction['value']['literal']
            if 'name' in diction.keys():
                item_dict['new_name'] = diction['name']
            else:
                item_dict['new_name'] = diction['value']['literal']
        elif 'sum' in diction['value'].keys():
            item_dict = SQLParser.parse_aggregation_value('sum', diction)

        elif 'min' in diction['value'].keys():
            item_dict = SQLParser.parse_aggregation_value('min', diction)
        elif 'max' in diction['value'].keys():
            item_dict = SQLParser.parse_aggregation_value('max', diction)
        elif 'avg' in diction['value'].keys():
            item_dict = SQLParser.parse_aggregation_value('avg', diction)
        elif 'count' in diction['value'].keys():
            item_dict = SQLParser.parse_aggregation_value('count', diction)

        return item_dict

    @staticmethod
    def group_by_parser(sql_group_by):
        res = []
        if type(sql_group_by) is list:
            for item in sql_group_by:
                item_dict = {}

                if 'literal' in item['value'].keys():
                    item_dict['key_name'] = item['value']['literal']
                else:
                    item_dict['key_name'] = item['value']

                res.append(item_dict)
        else:
            item_dict = {}
            if 'literal' in sql_group_by['value'].keys():
                item_dict['key_name'] = sql_group_by['value']['literal']
            else:
                item_dict['key_name'] = sql_group_by['value']

            res.append(item_dict)
        return res

    @staticmethod
    def sql_parser(sql_query):
        parsed_sql = json.dumps(msp.parse(sql_query))
        json_res = json.loads(parsed_sql)
        res = {}
        if 'select' in json_res:
            res['select'] = SQLParser.select_parser(json_res['select'])
        if 'groupby' in json_res:
            res['groupby'] = SQLParser.group_by_parser(json_res['groupby'])
        if 'from' in json_res:
            res['from'] = SQLParser.from_parser(json_res['from'])
        return res

    @staticmethod
    def get_key_col(parsed_sql):
        if 'groupby' in parsed_sql:
            return parsed_sql['groupby'][0]['key_name']
        if 'select' in parsed_sql:
            return parsed_sql['select'][0]['new_name']


# def custom_mapper(cols):
#     return f"""
# def custom_mapper(data_frame):
#     old_names = []
#     new_names = []
#     for i in {cols}:
#         if i['old_name'] == '*':
#             res = data_frame.copy()
#             return res
#
#         old_names.append(i['old_name'])
#         new_names.append(i['new_name'])
#
#     res = data_frame[old_names].copy()
#     res.columns = new_names
#
#     return res"""


def custom_reducer(col_names, groupby_cols):
    return f"""
def custom_reducer(data_frame):
    for i in {col_names}:
        if 'aggregate_f_name' in i.keys():
            if {groupby_cols}['key_name']:
                data_frame[i['old_name']] = data_frame.groupby({groupby_cols}['key_name'])[i['old_name']].transform(
                    i['aggregate_f_name'])
            else:
                data_frame[i['old_name']] = data_frame.groupby(i['old_name'])[i['old_name']].transform(
                    i['aggregate_f_name'])

    res = data_frame.drop_duplicates({groupby_cols}['key_name'])

    return res
"""


def custom_mapper(file_name, key_column, col_names, field_delimiter):
    return f"""
def custom_mapper(file_name):
    import pandas as pd

    def update_col_names():
        df_col_names = list(data_frame)
        if {col_names}[0]['old_name'] == '*':
            res = data_frame.copy()
            return res

        for i in range(len({col_names})):
            for j in range(len(df_col_names)):
                if df_col_names[j] == {col_names}[i]['old_name']:
                    df_col_names[j] = {col_names}[i]['new_name']

        res = data_frame.copy()
        res.columns = df_col_names
        return res

    data_frame = pd.read_csv(file_name, sep='{field_delimiter}')

    data_frame = update_col_names()

    data_frame['key_column'] = data_frame['{key_column}']

    return data_frame
"""
