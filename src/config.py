FORCE_REFRESH = True

DISTRICT_COL_LKUP = {
    "A1":"district_id", 
    "A2":"district_name", 
    "A3":"region", 
    "A4":"population", 
    "A5":"village_count", 
    "A6":"small_town_count", 
    "A7":"medium_town_count", 
    "A8":"large_town_count", 
    "A9":"num_cities",
    "A10":"urban_ratio_pct", 
    "A11":"avg_salary", 
    "A12":"unemployment_rate_95", 
    "A13":"unemployment_rate_96", 
    "A14":"entrepreneurs_per_1k", 
    "A15":"crimes_95", 
    "A16":"crimes_96",
    "A17":"avg_salary_98",
}

TYPE = {
     "PRIJEM": "credit", 
     "VYDAJ": "debit", 
     "VYBER": "cash withdrawal",
}

OPERATION = {
     "VYBER": "cash withdrawal", 
     "VKLAD": "cash deposit", 
     "PREVOD NA UCET": "transfer out", 
     "PREVOD Z UCTU": "transfer in", 
     "VYBER KARTOU": "card withdrawal", 
}

K_SYMBOL = {
     "UROK": "interest",
     "SLUZBY": "statement fee",
     "SIPO": "household payment",
     "DUCHOD": "pension",
     "POJISTNE": "insurance",
     "UVER": "loan payment",
     "SANKC. UROK": "penalty interest",
}

FREQUENCY = {
    "POPLATEK MESICNE": "monthly", 
    "POPLATEK TYDNE": "weekly", 
    "POPLATEK PO OBRATU": "per-transaction",
}

# REQ keys: client_id, file_name, source_url, file_type
FILE_LIST = [
    {
    "client_id":"czech_bank",
    "file_name": "account",
    "source_url": "ext_data",
    "file_type": "csv",
    "date_fields": ["date"],
    "translate_values":[{"frequency":FREQUENCY}]
    },
    {
    "client_id":"czech_bank",
    "file_name": "card",
    "source_url": "ext_data",
    "file_type": "csv",
    "date_fields": ["issued"],
    },
    {
    "client_id":"czech_bank",
    "file_name": "client",
    "source_url": "ext_data",
    "file_type": "csv",
    "custom_clean":["birth_and_gender"],
    },
    {
    "client_id":"czech_bank",
    "file_name": "disp",
    "source_url": "ext_data",
    "file_type": "csv",
    },
    {
    "client_id":"czech_bank",
    "file_name": "district",
    "source_url": "ext_data",
    "file_type": "csv",
    'num_formats': {'unemployment_rate_95':'float','crimes_95':'Int64'},
    "column_rename":[DISTRICT_COL_LKUP],
    },
    {
    "client_id":"czech_bank",
    "file_name": "loan",
    "source_url": "ext_data",
    "file_type": "csv",
    "date_fields": ["date"],
    },
    {
    "client_id":"czech_bank",
    "file_name": "order",
    "source_url": "ext_data",
    "file_type": "csv",
    "translate_values":[{'k_symbol':K_SYMBOL}],
    },
    {
    "client_id":"czech_bank",
    "file_name": "trans",
    "source_url": "ext_data",
    "file_type": "csv",
    "compression": "gz",
    "translate_values":[{'k_symbol':K_SYMBOL},{'type':TYPE},{'operation':OPERATION}],
    "date_fields": ["date"],
    },
]

AGE_BUCKETS = {
    "bins":[0,18,30,45,60,1000],
    "labels":['<18','18-30','31-45','46-60','60+']
    }