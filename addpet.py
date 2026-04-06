#!/usr/bin/python3

"""
Script for incorporating sanitized PET data into the SQL database
"""

import sqlite3
import pandas as pd

DATABASE = 'path/to/charmed.db'

conn = sqlite3.connect(DATABASE)
cursor = conn.cursor()

BASEPATH = "path/to/PET_results/"

FILEPATHs = {
    "First_PET_AV": "cleaned_PET_AV.xlsx",
    "First_PET_AV_R1": "cleaned_PET_AV_R1.xlsx",
    "First_PET_PK": "cleaned_PET_PK.xlsx",
    "First_PET_PK_R1": "cleaned_PET_PK_R1.xlsx",
    "First_PET_PIB": "cleaned_PET_PIB.xlsx",
    "First_PETMR_UCBJ": "cleaned_PETMR_UCBJ.xlsx",
    "First_PETMR_AV": "cleaned_PETMR_AV.xlsx",
    "First_PETMR_PIB": "cleaned_PETMR_PIB.xlsx",
    "First_PETMR_AV_R1": "cleaned_PETMR_AV_R1.xlsx",
    "First_PETMR_UCBJ_R1": "cleaned_PETMR_UCBJ_R1.xlsx"
}

for table_name, file_name in FILEPATHs.items():

    FILEPATH = BASEPATH + file_name

    df = pd.read_excel(FILEPATH)

    df.to_sql(table_name,
              conn,
              if_exists='replace',
              index=False)

    print(f"Data from {file_name} has been inserted into {table_name} table.")

conn.commit()

print("All data has been successfully inserted into the database.")

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

all_tables = [table[0] for table
              in tables]
tables_to_drop = [table for table
                  in all_tables
                  if table.startswith("TEMP_")
                  or table.startswith("PET")]

for table_name in tables_to_drop:
    drop_query = f"DROP TABLE IF EXISTS {table_name}"
    cursor.execute(drop_query)

# Create temporary table TEMP_PET from sessions table
TEMP_PET = '''
CREATE TABLE TEMP_PET AS
SELECT
    sessions_full.sessid,
    sessions_full.charmed,
    sessions_full.visits_PET_AV1451_date,
    sessions_full.visits_PET_AV1451_pkey,
    sessions_full.visits_PET_pk11195_date,
    sessions_full.visits_PET_pk11195_pkey,
    sessions_full.visits_PET_PiB_date,
    sessions_full.visits_PET_PiB_pkey,
    sessions_full.visits_PET_UCBJ_date,
    sessions_full.visits_PET_UCBJ_pkey
FROM sessions_full
'''
cursor.execute(TEMP_PET)

# create main tables
PET_AV = '''
CREATE TABLE PET_AV AS
SELECT
    TEMP_PET.charmed,
    TEMP_PET.sessid,
    TEMP_PET.visits_PET_AV1451_date,
    TEMP_PET.visits_PET_AV1451_pkey,
    First_PET_AV.wbic_parcels,
    First_PET_AV.StructName,
    First_PET_AV.side,
    First_PET_AV.AV_CSF_corrected_BP,
    First_PET_AV.AV_QC
FROM
    First_PET_AV
LEFT JOIN
    TEMP_PET
    ON First_PET_AV.charmed = TEMP_PET.charmed
    AND First_PET_AV.visits_PET_AV1451_date = TEMP_PET.visits_PET_AV1451_date;
'''
cursor.execute(PET_AV)

PET_AV_R1 = '''
CREATE TABLE PET_AV_R1 AS
SELECT
    TEMP_PET.charmed,
    TEMP_PET.sessid,
    TEMP_PET.visits_PET_AV1451_date,
    TEMP_PET.visits_PET_AV1451_pkey,
    First_PET_AV_R1.wbic_parcels,
    First_PET_AV_R1.StructName,
    First_PET_AV_R1.side,
    First_PET_AV_R1.AV_R1_CSF_corrected_BP_50thr,
    First_PET_AV_R1.AV_R1_QC
FROM
    First_PET_AV_R1
LEFT JOIN
    TEMP_PET
    ON First_PET_AV_R1.charmed = TEMP_PET.charmed
    AND First_PET_AV_R1.visits_PET_AV1451_date =
       TEMP_PET.visits_PET_AV1451_date;
'''
cursor.execute(PET_AV_R1)

PET_PK = '''
CREATE TABLE PET_PK AS
SELECT
    TEMP_PET.charmed,
    TEMP_PET.sessid,
    TEMP_PET.visits_PET_pk11195_date,
    TEMP_PET.visits_PET_pk11195_pkey,
    First_PET_PK.wbic_parcels,
    First_PET_PK.StructName,
    First_PET_PK.side,
    First_PET_PK.PK_CSF_corrected_BP,
    First_PET_PK.PK_QC
FROM
    First_PET_PK
LEFT JOIN
    TEMP_PET
    ON First_PET_PK.charmed = TEMP_PET.charmed
    AND First_PET_PK.visits_PET_pk11195_date =
        TEMP_PET.visits_PET_pk11195_date;
'''
cursor.execute(PET_PK)

PET_PK_R1 = '''
CREATE TABLE PET_PK_R1 AS
SELECT
    TEMP_PET.charmed,
    TEMP_PET.sessid,
    TEMP_PET.visits_PET_pk11195_date,
    TEMP_PET.visits_PET_pk11195_pkey,
    First_PET_PK_R1.wbic_parcels,
    First_PET_PK_R1.StructName,
    First_PET_PK_R1.side,
    First_PET_PK_R1.PK_R1_CSF_corrected_BP,
    First_PET_PK_R1.PK_R1_CSF_corrected_BP_50thr,
    First_PET_PK_R1.PK_R1_QC
FROM
    First_PET_PK_R1
LEFT JOIN
    TEMP_PET
    ON First_PET_PK_R1.charmed = TEMP_PET.charmed
    AND First_PET_PK_R1.visits_PET_pk11195_date =
        TEMP_PET.visits_PET_pk11195_date;
'''
cursor.execute(PET_PK_R1)

PET_PIB = '''
CREATE TABLE PET_PiB AS
SELECT
    TEMP_PET.charmed,
    TEMP_PET.sessid,
    TEMP_PET.visits_PET_PiB_date,
    TEMP_PET.visits_PET_PiB_pkey,
    First_PET_PIB.CTX_SUVR_40_70,
    First_PET_PIB.Jack_2017_threshold_CL19_SUVR121,
    Jack_2017_threshold_CL19_SUVR142
FROM
    First_PET_PIB
LEFT JOIN
    TEMP_PET
    ON First_PET_PIB.charmed = TEMP_PET.charmed
    AND First_PET_PIB.visits_PET_PiB_date = TEMP_PET.visits_PET_PiB_date;
'''
cursor.execute(PET_PIB)

PETMR_UCBJ = '''
CREATE TABLE PETMR_UCBJ AS
SELECT
    TEMP_PET.charmed,
    TEMP_PET.sessid,
    TEMP_PET.visits_PET_UCBJ_date,
    TEMP_PET.visits_PET_UCBJ_pkey,
    First_PETMR_UCBJ.wbic_parcels,
    First_PETMR_UCBJ.StructName,
    First_PETMR_UCBJ.side,
    First_PETMR_UCBJ.UCBJ_CSF_corrected_BP,
    First_PETMR_UCBJ.UCBJ_CSF_corrected_BP_50thr,
    First_PETMR_UCBJ.UCBJ_QC
FROM
    First_PETMR_UCBJ
LEFT JOIN
    TEMP_PET
    ON First_PETMR_UCBJ.charmed = TEMP_PET.charmed
    AND First_PETMR_UCBJ.visits_PET_UCBJ_date = TEMP_PET.visits_PET_UCBJ_date;
'''
cursor.execute(PETMR_UCBJ)

PETMR_UCBJ_R1 = '''
CREATE TABLE PETMR_UCBJ_R1 AS
SELECT
    TEMP_PET.charmed,
    TEMP_PET.sessid,
    TEMP_PET.visits_PET_UCBJ_date,
    TEMP_PET.visits_PET_UCBJ_pkey,
    First_PETMR_UCBJ_R1.wbic_parcels,
    First_PETMR_UCBJ_R1.StructName,
    First_PETMR_UCBJ_R1.side,
    First_PETMR_UCBJ_R1.UCBJ_R1_CSF_corrected_BP,
    First_PETMR_UCBJ_R1.UCBJ_R1_QC
FROM
    First_PETMR_UCBJ_R1
LEFT JOIN
    TEMP_PET
    ON First_PETMR_UCBJ_R1.charmed = TEMP_PET.charmed
    AND First_PETMR_UCBJ_R1.visits_PET_UCBJ_date =
        TEMP_PET.visits_PET_UCBJ_date;
'''
cursor.execute(PETMR_UCBJ_R1)

PETMR_AV = '''
CREATE TABLE PETMR_AV AS
SELECT
    TEMP_PET.charmed,
    TEMP_PET.sessid,
    TEMP_PET.visits_PET_AV1451_date,
    TEMP_PET.visits_PET_AV1451_pkey,
    First_PETMR_AV.wbic_parcels,
    First_PETMR_AV.StructName,
    First_PETMR_AV.side,
    First_PETMR_AV.PETMR_AV_CSF_corrected_BP,
    First_PETMR_AV.PETMR_AV_CSF_corrected_BP_50thr,
    First_PETMR_AV.PETMR_AV_QC
FROM
    First_PETMR_AV
LEFT JOIN
    TEMP_PET
    ON First_PETMR_AV.charmed = TEMP_PET.charmed
    AND First_PETMR_AV.visits_PET_AV1451_date =
        TEMP_PET.visits_PET_AV1451_date;
'''
cursor.execute(PETMR_AV)

PETMR_AV_R1 = '''
CREATE TABLE PETMR_AV_R1 AS
SELECT
    TEMP_PET.charmed,
    TEMP_PET.sessid,
    TEMP_PET.visits_PET_AV1451_date,
    TEMP_PET.visits_PET_AV1451_pkey,
    First_PETMR_AV_R1.wbic_parcels,
    First_PETMR_AV_R1.StructName,
    First_PETMR_AV_R1.side,
    First_PETMR_AV_R1.PETMR_AV_R1_CSF_corrected_BP,
    First_PETMR_AV_R1.PETMR_AV_R1_QC
FROM
    First_PETMR_AV_R1
LEFT JOIN
    TEMP_PET
    ON First_PETMR_AV_R1.charmed = TEMP_PET.charmed
    AND First_PETMR_AV_R1.visits_PET_AV1451_date =
        TEMP_PET.visits_PET_AV1451_date;
'''
cursor.execute(PETMR_AV_R1)

PETMR_PIB = '''
CREATE TABLE PETMR_PiB AS
SELECT
    TEMP_PET.charmed,
    TEMP_PET.sessid,
    TEMP_PET.visits_PET_PiB_date,
    TEMP_PET.visits_PET_PiB_pkey,
    First_PETMR_PIB.SUVR_WholeCereb_50_70,
    First_PETMR_PIB.Jack_2017_threshold_CL19_SUVR121,
    First_PETMR_PIB.SUVR_CerebGM_40_60,
    First_PETMR_PIB.Jack_2017_threshold_CL19_SUVR142
FROM
    First_PETMR_PIB
LEFT JOIN
    TEMP_PET
    ON First_PETMR_PIB.charmed = TEMP_PET.charmed
    AND First_PETMR_PIB.visits_PET_PiB_date = TEMP_PET.visits_PET_PiB_date;
'''
cursor.execute(PETMR_PIB)

# remove temporary and first tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

all_tables = [table[0] for table in tables]
tables_to_drop = [table for table in all_tables if table.startswith(
    "TEMP_") or table.startswith("First_")]

for table_name in tables_to_drop:
    drop_query = f"DROP TABLE IF EXISTS {table_name}"
    cursor.execute(drop_query)

conn.commit()
conn.close()
