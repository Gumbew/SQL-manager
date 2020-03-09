import moz_sql_parser as msp
import json
import os

from parsers import SQLParser, custom_reducer, custom_mapper

sql = """SELECT MIN(Position), 'Track Name', Artist
FROM spotify_data_f.csv
GROUP BY 'Position'"""

sql2 = """SELECT 'Track Name'
FROM spotify_data_f.csv
GROUP BY 'Artist'"""

parsed_sql = json.dumps(msp.parse(sql2))
json_res = json.loads(parsed_sql)

parsed_select = SQLParser.select_parser(json_res)
parsed_group_by = SQLParser.group_by_parser(json_res)
parsed_from = SQLParser.from_parser(json_res)
mapper = custom_mapper(parsed_select)
reducer = custom_reducer(parsed_select, parsed_group_by[0])

file_name = 'spotify_data_f.csv'
file_path = os.path.abspath(file_name)
with open(f"data\\{file_name}_reducer.py", 'w') as r:
    r.write(reducer)

with open(f"data\\{file_name}_mapper.py", 'w') as m:
    m.write(mapper)


mapper_path = os.path.abspath(f"data\\{file_name}_mapper.py")
reducer_path = os.path.abspath(f"data\\{file_name}_reducer.py")

os.system(
   f"python C:\\Users\\gumbe\\workspace\\GitHub\\Diploma\\mr-client\\client.py --mf {mapper_path} "
   f"--rf {reducer_path}  --src {file_path} --dest 'spotify_data_f.csv' "
   f"--key {parsed_group_by[0]['key_name']} ")
#os.system(f"python C:\\Users\\gumbe\\workspace\\GitHub\\Diploma\\mr-client\\client.py --rem '{file_name}',1 ")
