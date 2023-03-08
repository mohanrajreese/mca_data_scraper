import sqlite3
import os

# Set the path to the directory containing the databases
path = "/home/mohanraj/projects/mca_data/db_merger"

# Set the name of the merged database
merged_db_name = "final_merged_database.db"

# Create a connection pool for the merged database
merged_db_pool = sqlite3.connect(os.path.join(path, merged_db_name), check_same_thread=False, timeout=30, isolation_level=None)

# Create a cursor object for the merged database
merged_db_cursor = merged_db_pool.cursor()

# Create the table in the merged database if it does not exist
merged_db_cursor.execute('''CREATE TABLE IF NOT EXISTS indian_company_data (id INTEGER ,
                page_no INTEGER(8),cin VARCHAR(30) UNIQUE, company_name TEXT UNIQUE, roc VARCHAR, status VARCHAR)''')

# Loop through each database file in the directory
for filename in os.listdir(path):
    if filename.endswith(".db"):  # only process .db files
        db_file = os.path.join(path, filename)

        # Create a connection pool for the database file
        db_pool = sqlite3.connect(db_file, check_same_thread=False, timeout=30, isolation_level=None)
        cursor = db_pool.cursor()

        # Get the data from the table
        cursor.execute("SELECT * FROM indian_company_data")
        data = cursor.fetchall()

        # Insert the data into the merged database
        merged_db_cursor.executemany("""INSERT OR IGNORE INTO indian_company_data (id,page_no, cin, company_name, roc, status)
                              VALUES (?, ?, ?, ?, ?, ?)""", data)

        # Close the cursor and connection to the database file
        cursor.close()
        db_pool.close()

# Commit the changes to the merged database and close the connection pool
merged_db_pool.commit()
merged_db_pool.close()
