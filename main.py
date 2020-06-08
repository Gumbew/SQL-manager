import os
import json
import gui

from parsers import SQLParser, custom_reducer, custom_mapper

# windows
# path_to_client_file = 'C:\\Users\\gumbe\\workspace\\GitHub\\Diploma\\mr-client\\client.py'
# path_to_client_config_file = 'C:\\Users\\gumbe\\workspace\\GitHub\\Diploma\\mr-client\\config\\json\\client_config.json'

# ubuntu
path_to_client_file = "/home/mranch/workspace/Diploma/mr-client/client.py"
path_to_client_config_file = "/home/mranch/workspace/Diploma/mr-client/config/json/client_config.json"


def remove_file_from_cluster(file_name, clear_all):
    # windows
    # os.system(f"python {path_to_client_file} --rem {file_name},1 ")
    # ubuntu
    os.system(f"python3 /home/mranch/workspace/Diploma/mr-client/client.py --rem '{file_name}',{clear_all} ")


def push_file_on_cluster(file_path, file_name):
    # pushing file on cluster
    # windows
    # os.system(f"python {path_to_client_file} --pfc '1' --src {file_path} --dest {file_name}")
    # ubuntu
    os.system(f"python3 {path_to_client_file} --pfc '1' --src {file_path} --dest {file_name}")


def run_map(mapper_path, file_path, file_name):
    # mapping
    # windows
    # os.system(f"python {path_to_client_file} --map '1' --mf {mapper_path} --src {file_path} --dest {file_name}")
    # ubuntu
    os.system(f"python3 {path_to_client_file} --map '1' --mf {mapper_path} --src {file_path} --dest {file_name}")


def get_field_delimiter():
    with open(path_to_client_config_file) as config:
        client_config = json.load(config)
    return client_config['map_reduce']['field_delimiter']


def run_reduce(reducer_path, file_path, file_name):
    if type(file_path) is tuple:
        # windows
        # os.system(
        #     f"python {path_to_client_file} --reduce '1' --rf {reducer_path} --src {file_path[0]},{file_path[1]} --dest {file_name}")
        # ubuntu
        os.system(
            f"python3 {path_to_client_file} --reduce '1' --rf {reducer_path} --src {file_path[0]},{file_path[1]} --dest {file_name}")
    else:
        # windows
        # os.system(f"python {path_to_client_file} --reduce '1' --rf {reducer_path} --src {file_path} --dest {file_name}")
        # ubuntu
        os.system(
            f"python3 {path_to_client_file} --reduce '1' --rf {reducer_path} --src {file_path} --dest {file_name}")


def run_shuffle(file_path):
    # windows
    # os.system(f"python {path_to_client_file} --shuffle '1' --src {file_path} --key {key}")
    # ubuntu
    os.system(f"python3 {path_to_client_file} --shuffle '1' --src {file_path}")


def get_file_from_cluster(file_name, dest_file_name):
    os.system(f"python3 {path_to_client_file} --gffc '1' --src {file_name} --dest {dest_file_name}")


def run_tasks(sql):
    parsed_sql = sql if type(sql) is dict else SQLParser.sql_parser(sql)
    field_delimiter = get_field_delimiter()
    fn = parsed_sql['from']
    from_file = fn
    if type(from_file) is dict:
        from_file = run_tasks(from_file)
    if type(from_file) is tuple:
        reducer = custom_reducer(parsed_sql, field_delimiter)
        merged_file_names = f"{os.path.splitext(from_file[0])[0]}_{os.path.splitext(from_file[1])[0]}"
        reducer_path = os.path.abspath(f"data{os.sep}{merged_file_names}_reducer.py")

        with open(reducer_path, 'w') as r:
            r.write(reducer)

        for file_name in from_file:
            own_select = SQLParser.split_select_cols(file_name, parsed_sql['select'])
            key_col = SQLParser.get_key_col(parsed_sql, file_name)

            mapper = custom_mapper(file_name, key_col, own_select, field_delimiter)
            mapper_path = os.path.abspath(f"data{os.sep}{file_name}_mapper.py")
            file_path = os.path.abspath(file_name)

            with open(mapper_path, 'w') as m:
                m.write(mapper)

            push_file_on_cluster(file_path, file_name)
            run_map(mapper_path, file_path, file_name)
            run_shuffle(file_path)

        file_name = from_file[0]

        run_reduce(reducer_path, from_file, file_name)
        return file_name

    else:
        key_col = SQLParser.get_key_col(parsed_sql, from_file)
        reducer = custom_reducer(parsed_sql, field_delimiter)
        reducer_path = os.path.abspath(f"data{os.sep}{from_file}_reducer.py")

        mapper = custom_mapper(from_file, key_col, parsed_sql['select'], field_delimiter)
        mapper_path = os.path.abspath(f"data{os.sep}{from_file}_mapper.py")
        if type(from_file) is tuple:
            from_file = from_file[0]
        file_path = os.path.abspath(from_file)

        with open(reducer_path, 'w') as r:
            r.write(reducer)

        with open(mapper_path, 'w') as m:
            m.write(mapper)

        push_file_on_cluster(file_path, from_file)
        run_map(mapper_path, file_path, from_file)
        run_shuffle(file_path)
        run_reduce(reducer_path, file_path, from_file)
        return from_file


if __name__ == "__main__":
    gui.run_gui()
