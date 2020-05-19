
def custom_reducer(data_frame):
    for i in [{'old_name': 'URL', 'new_name': 'URL'}]:
        if 'aggregate_f_name' in i.keys():
            if {'key_name': 'URL'}['key_name']:
                data_frame[i['old_name']] = data_frame.groupby({'key_name': 'URL'}['key_name'])[i['old_name']].transform(
                    i['aggregate_f_name'])
            else:
                data_frame[i['old_name']] = data_frame.groupby(i['old_name'])[i['old_name']].transform(
                    i['aggregate_f_name'])

    res = data_frame.drop_duplicates({'key_name': 'URL'}['key_name'])

    return res
