import config
import pandas as pd
import clean

SOURCE_PATH = 'outputs/silver/czech_bank/'
EXP_PATH = 'outputs/gold/czech_bank/'

def account_monthly_profile():
    # read in DFs
    og_df = pd.read_parquet(f"{SOURCE_PATH}transaction_model.parquet")
    owner_og_df = pd.read_parquet(f"{SOURCE_PATH}account_model.parquet")

    og_df['month'] = og_df['date'].values.astype('datetime64[M]')
    # end of month balance logic
    eom_bal_df = og_df.query('last_of_day == 1').copy()
    eom_bal_df['rn'] = (eom_bal_df.sort_values('date', ascending=False).groupby(['account_id', 'month']).cumcount())
    eom_bal_df = eom_bal_df.query('rn == 0').copy()
    eom_bal_df = eom_bal_df[['account_id','month','balance']]
    eom_bal_df.rename(columns={'balance':'end_of_month_balance'}, inplace=True)
    
    # calculated field logic
    credit_df = og_df.query('type == "credit"').copy()
    credit_df = credit_df.groupby(['account_id','month']).agg(total_credits=('amount','sum'))
    debit_df = og_df.query('type == "debit"').copy()
    debit_df = debit_df.groupby(['account_id','month']).agg(total_debits=('amount','sum'))
    tran_ct_df = og_df.groupby(['account_id','month']).agg(transaction_count=('account_id','count'))
    card_wd_df = og_df[og_df['operation'] == 'card withdrawal'].groupby(['account_id', 'month']).agg(card_withdrawal_count=('account_id', 'count'))
    loan_pmt_df = og_df[og_df['k_symbol'] == 'loan payment'].groupby(['account_id', 'month']).agg(made_loan_pmt=('account_id', 'count'))

    owner_df = owner_og_df[['account_id','gender','region','owner_age_at_account_creation','account_date']].copy()
    bins,labels = config.AGE_BUCKETS['bins'],config.AGE_BUCKETS['labels']
    owner_df['age_bucket'] = pd.cut(owner_df['owner_age_at_account_creation'], bins=bins, labels=labels, include_lowest=True)

    months_df = pd.DataFrame({'month': og_df['month'].unique()})
    months_df = pd.merge(months_df,owner_og_df[['account_id','account_date']],how='cross')
    months_df['account_date'] = months_df['account_date'].dt.to_period('M').dt.to_timestamp()
    months_df = months_df[months_df['month']>= months_df['account_date']]

    base_df = months_df[['account_id','month']].drop_duplicates()
    base_df = pd.merge(base_df,credit_df, on=['account_id','month'], how='left')
    base_df['total_credits'] = base_df['total_credits'].fillna(0)
    base_df = pd.merge(base_df,debit_df, on=['account_id','month'], how='left')
    base_df['total_debits'] = base_df['total_debits'].fillna(0)
    base_df['net_cash_flow'] = base_df['total_credits'] - base_df['total_debits']
    base_df = pd.merge(base_df,tran_ct_df, on=['account_id','month'], how='left')
    base_df['transaction_count'] = base_df['transaction_count'].fillna(0)
    
    base_df = pd.merge(base_df,eom_bal_df[['account_id','month','end_of_month_balance']],on=['account_id','month'], how='left')
    base_df = base_df.sort_values(['account_id','month'])
    base_df['end_of_month_balance'] = base_df.groupby('account_id')['end_of_month_balance'].ffill()
    base_df = pd.merge(base_df,card_wd_df, on=['account_id','month'], how='left')
    base_df['card_withdrawal_count'] = base_df['card_withdrawal_count'].fillna(0)
    base_df = pd.merge(base_df,loan_pmt_df, on=['account_id','month'], how='left')
    base_df['made_loan_pmt'] = base_df['made_loan_pmt'].notnull()
    base_df = pd.merge(base_df,owner_df[['account_id','gender','age_bucket','region']], on=['account_id'], how='left')
    
    clean.write_out(base_df,EXP_PATH,'account_monthly_profile')
    sample_df = base_df.head(100)
    clean.write_out(sample_df,'outputs/samples/','sample_account_monthly_profile')

def regional_segment_summary():
    amp_df = pd.read_parquet(f"{EXP_PATH}account_monthly_profile.parquet")
    am_df = pd.read_parquet(f"{SOURCE_PATH}account_model.parquet")

    amp_df['year'] = amp_df['month'].dt.year
    am_df['default'] = (am_df['loan_status_category'] == 'bad').astype(int)
    am_df['gold_holder'] = (am_df['type'] == 'gold').astype(int)

    def_gold_df = (am_df.groupby('region').agg(
        gold_card_count=('gold_holder','sum'),
        default_count=('default', 'sum'),
        total_count=('loan_status_category', 'count')
        ).reset_index())

    def_gold_df['loan_default_rate_pct'] = (def_gold_df['default_count']/def_gold_df['total_count'])*100

    dist_df = pd.read_parquet('outputs/bronze/czech_bank/district.parquet')
    dist_df['tot_sal'] = dist_df['population'] * dist_df['avg_salary']
    dist_sal_df = (dist_df.groupby('region').agg(
        tot_pop=('population','sum'),
        tot_sal=('tot_sal','sum')
        )).reset_index()
    dist_sal_df['avg_sal'] = dist_sal_df['tot_sal']/dist_sal_df['tot_pop']

    base_df = (amp_df.groupby(['year', 'region']).agg(
        median_net_cash_flow=('net_cash_flow','median'),
        avg_transaction_count=('transaction_count','mean'),
        active_account_count=('account_id', 'nunique')
        ))
    base_df = base_df.sort_values(['year','region']).reset_index()
    base_df = pd.merge(base_df,dist_sal_df[['region','avg_sal']], on='region', how='left')
    base_df = pd.merge(base_df,def_gold_df[['loan_default_rate_pct','gold_card_count','region']],on='region',how='left')

    clean.write_out(base_df,EXP_PATH,'regional_segment_summary')
    sample_df = base_df.head(100)
    clean.write_out(sample_df,'outputs/samples/','sample_regional_segment_summary')

def gold():
     account_monthly_profile()
     regional_segment_summary()


if __name__ == "__main__":
    gold()