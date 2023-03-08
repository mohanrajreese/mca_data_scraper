import pandas as pd
import sqlite3


def convert_db_to_csv(db_name, csv_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    dataframe = pd.read_sql('SELECT cin,company_name,roc,status FROM indian_company_data where status == "Active" LIMIT 100' ,conn)
    dataframe.to_csv(csv_name, index=False)


convert_db_to_csv("/home/mohanraj/projects/mca_data/db_merger/final_merged_database.db","/home/mohanraj/projects/mca_data/db_merger/100.csv")



