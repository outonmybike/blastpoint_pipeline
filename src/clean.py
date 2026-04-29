import config
import pandas as pd
from pathlib import Path
import numpy as np

########################################################################################################
def birth_and_gender(df):
    cols = df.columns.tolist()
    df['gender'] = np.where((df.birth_number.astype(str).str[2:4].astype(int)) < 13,'M','F')
    df['year'] = '19'+df.birth_number.astype(str).str[:2]
    df['month'] = (df.birth_number.astype(str).str[2:4].astype(int))%50
    df['day'] = df.birth_number.astype(str).str[4:6]
    df['birth_date'] = pd.to_datetime(df[['year','month','day']])
    cols.append('gender')
    cols.append('birth_date')
    df = df[cols]
    return df

def snake_case_cols(df):
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace('-','_')
    df.columns = df.columns.str.replace(' ','_')
    df.columns = df.columns.str.replace('.','_')
    return df

def column_rename(df,new_cols:dict):
    df.rename(columns=new_cols, inplace=True)
    return df

def translate_values(df,col,lkup:dict):
    df[col] = df[col].replace(lkup)
    return df

def date_parse(date_col,df):
    df[date_col] = pd.to_datetime(df[date_col].astype(str).str[:6], format='%y%m%d')
    return df

def num_formats(df,col,fmt):
    df[col] = pd.to_numeric(df[col], errors='coerce')
    df[col] = df[col].astype(fmt)
    return df

def handle_nulls(df):
    empty_text = 'none'
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) : df[col] = df[col].fillna('1900-01-01')
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].fillna(empty_text) 
            df[col] = df[col].replace(" ",empty_text)
    return df

def write_out(df,write_path,file_name):
    path = Path(write_path)
    path.mkdir(parents=True, exist_ok=True)
    write_path_name = write_path+file_name+'.parquet'
    df.to_parquet(write_path_name, engine='pyarrow',index=False)
########################################################################################################


def raw_to_bronze():
    for data_obj in config.FILE_LIST:
        client_id = data_obj["client_id"]
        file_name = data_obj["file_name"]
        source_path = f"outputs/raw/{client_id}/{file_name}"
        write_path = f"outputs/bronze/{client_id}/"
        available_files = [f for f in Path(source_path).iterdir() if f.is_file()]

        df_init = pd.DataFrame()
        for file in available_files:
            df = pd.read_csv(f'{file}', sep=";", low_memory=False)

            if "custom_clean" in data_obj:
                cleaning_list = data_obj['custom_clean'] 
                for x in cleaning_list:
                    if x == 'birth_and_gender':
                        df = birth_and_gender(df)

            if "date_fields" in data_obj:
                date_fields = data_obj['date_fields']
                for date_col in date_fields:
                    date_parse(date_col,df)

            if "translate_values" in data_obj:
                col_list = data_obj['translate_values']
                for y in col_list:
                    col = next(iter(y))
                    lkup = y[col]
                    df = translate_values(df,col,lkup)
            
            if "column_rename" in data_obj:
                rename_list = data_obj["column_rename"]
                for new_cols in rename_list:
                    df = column_rename(df,new_cols)

            if "num_formats" in data_obj:
                num_cols = data_obj["num_formats"]
                for num_col in num_cols:
                    col,fmt = num_col,num_cols[num_col]
                    num_formats(df,col,fmt)

            df = handle_nulls(df)

            df_init = pd.concat([df_init,df], ignore_index=True).drop_duplicates()

        write_out(df_init,write_path,file_name)

if __name__ == "__main__":
    raw_to_bronze()