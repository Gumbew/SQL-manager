import moz_sql_parser as msp
import json
import os
import pandas as pd

from parsers import SQLParser, custom_reducer, custom_mapper


def inner_join(full_file_name_l, full_file_name_r, parsed_join):
    left_df = pd.read_csv(full_file_name_l)
    right_df = pd.read_csv(full_file_name_r)
    left_df_col_name = parsed_join['on'][0].split('.')[1]
    right_df_col_name = parsed_join['on'][1].split('.')[1]

    merged_inner = pd.merge(left=left_df, how=parsed_join['join_type'], right=right_df, left_on=left_df_col_name,
                            right_on=right_df_col_name)
    return merged_inner


sql = """SELECT MIN(Position), 'Track Name', Artist
FROM spotify_data_f.csv
GROUP BY 'Position'"""

sql2 = """
SELECT A.URL, B.Streams, A.Artist
FROM A.csv
INNER JOIN B.csv ON A.URL=B.URL;
"""
parsed_sql = json.dumps(msp.parse(sql2))
json_res = json.loads(parsed_sql)

parsed_select = SQLParser.select_parser(json_res)
parsed_from = SQLParser.from_parser(json_res)
parsed_join = SQLParser.join_parser(json_res)
mapper = custom_mapper(parsed_select)
# print(mapper)
print(parsed_from)
print(parsed_join)
# reducer = custom_reducer(parsed_select, parsed_group_by[0])
print(json_res)

inner_join(parsed_from[0], parsed_from[1], parsed_join).to_csv('joinded.csv', index=False)
