
def custom_mapper(file_name):
    import pandas as pd

    def update_col_names():
        df_col_names = list(data_frame)
        if [{'old_name': 'Artist', 'new_name': 'Artist'}][0]['old_name'] == '*':
            res = data_frame.copy()
            return res

        for i in range(len([{'old_name': 'Artist', 'new_name': 'Artist'}])):
            for j in range(len(df_col_names)):
                if df_col_names[j] == [{'old_name': 'Artist', 'new_name': 'Artist'}][i]['old_name']:
                    df_col_names[j] = [{'old_name': 'Artist', 'new_name': 'Artist'}][i]['new_name']

        res = data_frame.copy()
        res.columns = df_col_names
        return res

    data_frame = pd.read_csv(file_name, sep=',')

    data_frame = update_col_names()

    data_frame['key_column'] = data_frame['Artist']

    return data_frame
