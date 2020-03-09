import pandas as pd
import moz_sql_parser as msp
import json


def custom_reducer(data_frame, col_names, groupby_cols=None):
    for i in col_names:
        if 'aggregate_f_name' in i.keys():
            if groupby_cols:
                data_frame[i['old_name']] = data_frame.groupby(groupby_cols)[i['old_name']].transform(
                    i['aggregate_f_name'])
            else:
                data_frame[i['old_name']] = data_frame.groupby(i['old_name'])[i['old_name']].transform(
                    i['aggregate_f_name'])

    res = data_frame.drop_duplicates(groupby_cols)

    return res


# df = pd.read_csv('../client_data/spotify_data_f.csv')
# df = df.head(15)
# print(df)
# sl = "SELECT 'Track Name', Streams, MAX (Position) FROM ggg GROUP BY 'Position'"
#
# parsed_sql = json.dumps(msp.parse(sl))
# json_res = json.loads(parsed_sql)
# import mapreduce.task_runner_proxy as tr
#
# print(json_res)
# gb = tr.TaskRunner.group_by_parser(json_res)
# sb = tr.TaskRunner.select_parser(json_res)
# print(sb)
# print(custom_reducer(df, sb))
