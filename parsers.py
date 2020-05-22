import json
import moz_sql_parser as msp
import os


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
    def split_select_cols(file_name, parsed_select):
        file_name, ext = os.path.splitext(file_name)
        res = []
        for i in parsed_select:
            col_file_name, col_name = i['old_name'].split('.')
            if col_file_name == file_name:
                if i['old_name'] != i['new_name']:
                    res.append(
                        {'old_name': col_name,
                         'new_name': i['new_name']
                         }
                    )
                else:
                    res.append(
                        {'old_name': col_name,
                         'new_name': col_name
                         }
                    )

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
            if type(json_res['from']) is list:
                res['from'] = SQLParser.from_parser(json_res['from'])
                res['join'] = SQLParser.join_parser(json_res)
            else:
                res['from'] = SQLParser.from_parser(json_res['from'])
        return res

    @staticmethod
    def get_key_col(parsed_sql, file_name=None):
        if 'join' in parsed_sql:
            if file_name:
                file_name, ext = os.path.splitext(file_name)
                col_file_name, col_name = parsed_sql['join']['on'][0].split('.')
                if col_file_name == file_name:
                    return col_name
                else:
                    col_file_name, col_name = parsed_sql['join']['on'][1].split('.')
                    return col_name

        if 'groupby' in parsed_sql:
            return parsed_sql['groupby'][0]['key_name']
        if 'select' in parsed_sql:
            return parsed_sql['select'][0]['new_name']


def custom_reducer(parsed_sql, field_delimiter):
    from_file = parsed_sql['from']

    res = f"""
def custom_reducer(file_name):
    import pandas as pd
    if type(file_name) is tuple:
        l_file_name, r_file_name = file_name
        """
    if type(from_file) is tuple:
        parsed_join = parsed_sql['join']
        res += f"""
    left_df = pd.read_csv(l_file_name)
    right_df = pd.read_csv(r_file_name)
    left_df = left_df.drop(columns=['key_column'])
    right_df = right_df.drop(columns=['key_column'])
    left_df_col_name = '{parsed_join['on'][0].split('.')[1]}'
    right_df_col_name = '{parsed_join['on'][1].split('.')[1]}'

    data_frame = pd.merge(left=left_df, how='{parsed_join['join_type']}', right=right_df, left_on=left_df_col_name,
                        right_on=right_df_col_name)
    """
    else:
        select_cols = parsed_sql['select']
        if 'groupby' in parsed_sql:
            groupby_col = parsed_sql['groupby']
            res += f"""
    for i in {select_cols}:
        if 'aggregate_f_name' in i.keys():
            if {groupby_col}['key_name']:
                data_frame[i['new_name']] = data_frame.groupby({groupby_col}['key_name'])[i['new_name']].transform(
                    i['aggregate_f_name'])
            else:
                data_frame[i['new_name']] = data_frame.groupby(i['new_name'])[i['new_name']].transform(
                    i['aggregate_f_name'])

    data_frame = data_frame.drop_duplicates({groupby_col}['key_name'])
    """
        elif 'orderby' in parsed_sql:
            pass
        else:
            select_cols = [i['new_name'] for i in parsed_sql['select']]
            res += f"""
    data_frame = pd.read_csv(file_name, sep='{field_delimiter}')
    data_frame = data_frame[{select_cols}]
        """

    res += """
    return data_frame
    """
    return res


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
