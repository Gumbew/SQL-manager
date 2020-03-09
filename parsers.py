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
    def from_parser(data):
        res = {}
        if type(data['from']) is not dict:
            res['file_name'] = data['from']
        return res

    @staticmethod
    def select_parser(data):
        select_data = data['select']
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
    def group_by_parser(data):
        select_data = data['groupby']
        res = []
        if type(select_data) is list:
            for item in select_data:
                item_dict = {}

                if 'literal' in item['value'].keys():
                    item_dict['key_name'] = item['value']['literal']
                else:
                    item_dict['key_name'] = item['value']

                res.append(item_dict)
        else:
            item_dict = {}
            if 'literal' in select_data['value'].keys():
                item_dict['key_name'] = select_data['value']['literal']
            else:
                item_dict['key_name'] = select_data['value']

            res.append(item_dict)
        return res


def custom_mapper(cols):
    return f"""
def custom_mapper(data_frame):
    old_names = []
    new_names = []
    for i in {cols}:
        if i['old_name'] == '*':
            res = data_frame.copy()
            return res

        old_names.append(i['old_name'])
        new_names.append(i['new_name'])

    res = data_frame[old_names].copy()
    res.columns = new_names

    return res"""


def custom_reducer(col_names,groupby_cols):
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
