import moz_sql_parser as msp
import json
import os
import threading

from parsers import SQLParser, custom_reducer, custom_mapper


def remove_file_from_cluster(file_name):
    os.system(f"python C:\\Users\\gumbe\\workspace\\GitHub\\Diploma\\mr-client\\client.py --rem '{file_name}',1 ")


def run_tasks(sql, file_name):
    parsed_sql = SQLParser.sql_parser(sql)
    key_col = SQLParser.get_key_col(parsed_sql)
    mapper = custom_mapper('A.csv', key_col, parsed_sql['select'], ',')
    reducer = ""

    file_path = os.path.abspath(file_name)
    with open(f"data\\{file_name}_reducer.py", 'w') as r:
        r.write(reducer)

    with open(f"data\\{file_name}_mapper.py", 'w') as m:
        m.write(mapper)

    mapper_path = os.path.abspath(f"data\\{file_name}_mapper.py")
    reducer_path = os.path.abspath(f"data\\{file_name}_reducer.py")

    os.system(
        f"python C:\\Users\\gumbe\\workspace\\GitHub\\Diploma\\mr-client\\client.py --mf {mapper_path} "
        f"--rf {reducer_path}  --src {file_path} --dest {file_name} "
        f"--key {key_col} ")


def main():
    # sql = """SELECT MIN(Position), 'Track Name', Artist
    # FROM spotify_data_f.csv
    # GROUP BY 'Position'"""
    #
    # sql2 = """SELECT 'Track Name'
    # FROM spotify_data_f.csv
    # GROUP BY 'Artist'"""
    sql2 = """SELECT Artist
    FROM A.csv
    GROUP BY 'Artist'
    
    """

    sql = """SELECT URL
    FROM B.csv
    GROUP BY 'URL'
    """

    file_name_B = 'B.csv'
    # run_tasks(sql, file_name)
    #remove_file_from_cluster(file_name_B)

    file_name_A = 'A.csv'
    #run_tasks(sql2, file_name_A)
    remove_file_from_cluster(file_name_A)

    # file_name = 'spotify_data_f.csv'

    # t1 = threading.Thread(target=run_tasks, args=(sql, file_name_B))
    # t2 = threading.Thread(target=run_tasks, args=(sql2, file_name_A))
    # t1.start()
    # t2.start()


if __name__ == "__main__":
    main()
