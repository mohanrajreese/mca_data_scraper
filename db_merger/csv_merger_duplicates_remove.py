import os

import pandas as pd

dir_path = "/home/mohanraj/projects/mca_data/db_merger"

out_file = 'master.csv'
first_file = True
file_paths:[str] = []
for filename in os.listdir(dir_path):
    if filename.endswith(".csv"):
        csv_files = os.path.join(dir_path, filename)
        file_paths.append(csv_files)

for fp in file_paths:
    df = pd.read_csv(fp)
    df = df.dropna()  # remove records with blanks
    if first_file:
        df.to_csv(out_file, index=False)
        first_file = False
    else:
        df.to_csv(out_file, index=False, header=False, mode='a')