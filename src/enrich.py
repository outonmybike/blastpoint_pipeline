import pandas as pd
from pathlib import Path
import clean
import numpy as np

SOURCE_PATH = 'outputs/bronze/czech_bank/'
EXP_PATH = 'outputs/silver/czech_bank/'

def account_model():
    # reading in DFs
    account_df = pd.read_parquet(f"{SOURCE_PATH}account.parquet")
    district_df = pd.read_parquet(f'{SOURCE_PATH}district.parquet')
    loan_df = pd.read_parquet(f'{SOURCE_PATH}loan.parquet')
    card_df = pd.read_parquet(f'{SOURCE_PATH}card.parquet')
    
    account_df.rename(columns={'district_id':'district_id_account','date':'account_date'}, inplace=True)
    
    
    loan_df.rename(columns={'date':'loan_date'}, inplace=True)
    
    card_df.rename(columns={'issued':'card_issue_date'}, inplace=True)
    disp_df = pd.read_parquet(f'{SOURCE_PATH}disp.parquet')
    disp_df.rename(columns={'type':'disp_type'}, inplace=True)
    disp_owner_df = disp_df[disp_df['disp_type'] == 'OWNER']
    client_df = pd.read_parquet(f'{SOURCE_PATH}client.parquet')
    client_df.rename(columns={'district_id':'district_id_client'}, inplace=True)
    
    account_model_df = pd.merge(account_df,disp_owner_df, on='account_id',how='left')
    account_model_df = pd.merge(account_model_df,client_df, on='client_id',how='left')
    account_model_df = pd.merge(account_model_df,district_df, left_on='district_id_account', right_on='district_id', how='left')
    account_model_df = pd.merge(account_model_df,loan_df,on='account_id',how='left')
    account_model_df = pd.merge(account_model_df,card_df,on='disp_id',how='left')

    account_model_df['account_age_months'] = (( pd.Timestamp("1998-12-31") - account_model_df['account_date']).dt.days)/30.4
    account_model_df['owner_age_at_account_creation'] = ((account_model_df['account_date'] - account_model_df['birth_date']).dt.days)/365
    account_model_df['has_loan'] = account_model_df['loan_id'].notnull()
    account_model_df['has_card'] = account_model_df['card_id'].notnull()
    account_model_df['loan_status_category'] = np.where(account_model_df['status'].isna(), 'none',
        np.where(account_model_df['status'].isin(['A', 'C']), 'good', 'bad'))
    account_model_df = clean.snake_case_cols(account_model_df)
    clean.write_out(account_model_df,EXP_PATH,'account_model')
    return account_model_df


def clean_transaction():
    # slowest step. Very inefficient
    trans_df = pd.read_parquet(f"{SOURCE_PATH}trans.parquet")
    trans_df['prev_balance'] = trans_df.apply(lambda x: x['balance'] + x['amount'] if x['type'] == 'debit' 
        else x['balance'] - x['amount'], axis=1)
    trans_df['prev_balance'] = trans_df['prev_balance'].round(1)
    trans_df = trans_df.sort_values(['account_id','date'])
    referenced = trans_df.groupby(['account_id', 'date'])['prev_balance'].apply(set).reset_index()
    referenced.columns = ['account_id', 'date', 'referenced_balances']   
    trans_df = trans_df.merge(referenced, on=['account_id', 'date'], how='left')
    trans_df['last_of_day'] = trans_df.apply(lambda x: 1 if x['balance'] not in x['referenced_balances'] else 0, axis=1)
    trans_df = trans_df.drop(columns=['referenced_balances','prev_balance'])
    # additional transaction cleaning logic here
    clean.write_out(trans_df,EXP_PATH,'transaction_model')

def silver():
    account_model()
    clean_transaction()

if __name__ == "__main__":
    silver()