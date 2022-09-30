import sqlite3
import logging

DATABASE_NAME = 'parser.db'
conn = sqlite3.connect(f'{DATABASE_NAME}')
cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS bankrupt (
    id TEXT PRIMARY KEY,
    region TEXT,
    year TEXT,
    number_in_document TEXT,
    individual_number TEXT,
    name TEXT,
    number_of_registration TEXT,
    address TEXT,
    cour_name TEXT,
    bankrupt_init_date TEXT,
    interim_manager_date TEXT,
    interim_manager_name TEXT,
    approving_date_from TEXT,
    approving_date_to TEXT,
    approving_address TEXT,
    interim_manager_contact TEXT,
    publication_date TEXT);
""")
conn.commit()

logging.info(f"DATABASE {DATABASE_NAME} UP")

