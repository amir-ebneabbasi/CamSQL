#!/usr/bin/python3

'''
Script for adding metabolomic data to SQL database
'''

import sqlite3
from os import path
import pandas as pd

DATABASE = 'path/to/charmed.db'

conn = sqlite3.connect(DATABASE)
cursor = conn.cursor()

BASEPATH = "path/to/metabolomics_results/"

# Main column for joining data and dates
PSM = "PARENT_SAMPLE_NAME"

# read IDs (PSM) from metabolomic data
METAID = pd.read_excel(path.join(BASEPATH,
                                 "RAWDATA_MDCLP_HD4_DATA_TABLES.xlsx"),
                       sheet_name="Sample Meta Data").loc[:, [PSM]]

# read test dates
MetaDates = pd.read_excel(path.join(BASEPATH, "Meta_dates.xlsx"))

# join dates to identifiers
MetaInfo = METAID.merge(MetaDates, how="inner", on=PSM)

# read metabolomic tables (i.e., four analytic types)
DPEAK = pd.read_excel(path.join(BASEPATH,
                                "RAWDATA_MDCLP_HD4_DATA_TABLES.xlsx"),
                      sheet_name="Peak Area Data")
DBN = pd.read_excel(path.join(BASEPATH,
                              "RAWDATA_MDCLP_HD4_DATA_TABLES.xlsx"),
                    sheet_name="Batch-normalized Data")
DBNI = pd.read_excel(path.join(BASEPATH,
                               "RAWDATA_MDCLP_HD4_DATA_TABLES.xlsx"),
                     sheet_name="Batch-norm Imputed Data")
DLOG = pd.read_excel(path.join(BASEPATH,
                               "RAWDATA_MDCLP_HD4_DATA_TABLES.xlsx"),
                     sheet_name="Log Transformed Data")

# merge MetaInfo to metabolomic sheets
DPEAK = DPEAK.merge(MetaInfo, how="left", on=PSM)
DBN = DBN.merge(MetaInfo, how="left", on=PSM)
DBNI = DBNI.merge(MetaInfo, how="left", on=PSM)
DLOG = DLOG.merge(MetaInfo, how="left", on=PSM)

DPEAK.insert(0, 'charmed', DPEAK.pop("charmed"))
DBN.insert(0, 'charmed', DBN.pop("charmed"))
DBNI.insert(0, 'charmed', DBNI.pop("charmed"))
DLOG.insert(0, 'charmed', DLOG.pop("charmed"))

DPEAK = pd.melt(
    DPEAK,
    id_vars=['charmed',
             'PARENT_SAMPLE_NAME',
             'visits_bloods_date'],
    var_name='Chem_ID',
    value_name='Value_Peak'
).sort_values("charmed")

DBN = pd.melt(
    DBN,
    id_vars=['charmed',
             'PARENT_SAMPLE_NAME',
             'visits_bloods_date'],
    var_name='Chem_ID',
    value_name='Value_Batch_Norm'
).sort_values("charmed")

DBNI = pd.melt(
    DBNI,
    id_vars=['charmed',
             'PARENT_SAMPLE_NAME',
             'visits_bloods_date'],
    var_name='Chem_ID',
    value_name='Value_Data_Batch_Norm_Imputed'
).sort_values("charmed")

DLOG = pd.melt(
    DLOG,
    id_vars=['charmed',
             'PARENT_SAMPLE_NAME',
             'visits_bloods_date'],
    var_name='Chem_ID',
    value_name='Value_Log'
).sort_values("charmed")

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
all_tables = [table[0] for table in tables]
tables_to_drop = [table for table
                  in all_tables
                  if table.startswith("Meta_")
                  or table.startswith("TEMP_")]
for table_name in tables_to_drop:
    drop_query = f"DROP TABLE IF EXISTS {table_name}"
    cursor.execute(drop_query)

# convert data to SQL tables
DPEAK.to_sql('First_Data_Peak',
             conn,
             if_exists='replace',
             index=False)
DBN.to_sql('First_Data_Batch_Norm',
           conn,
           if_exists='replace',
           index=False)
DBNI.to_sql('First_Data_Batch_Norm_Imputed',
            conn,
            if_exists='replace',
            index=False)
DLOG.to_sql('First_Data_Log',
            conn,
            if_exists='replace',
            index=False)

# Create temporary table Temp_Blood from sessions table
DROPTEMPBLOOD = '''
DROP TABLE IF EXISTS Temp_Blood
'''
cursor.execute(DROPTEMPBLOOD)

TEMPBLOOD = '''
CREATE TABLE Temp_Blood AS
SELECT
    CAST(charmed AS INTEGER) AS charmed,
    CAST(sessid AS INTEGER) AS sessid,
    sessions.visits_bloods_date,
    sessions.visits_bloods_pkey
FROM sessions
WHERE sessions.visits_bloods_date is not null
'''
cursor.execute(TEMPBLOOD)

# create main tables
DROPMETAPEAK = '''
DROP TABLE IF EXISTS METAPEAK
'''
cursor.execute(DROPMETAPEAK)

METAPEAK = '''
CREATE TABLE METAPEAK AS
SELECT
    Temp_Blood.visits_bloods_pkey,
    First_Data_Peak.PARENT_SAMPLE_NAME,
    Temp_Blood.visits_bloods_date,
    First_Data_Peak.Chem_ID,
    First_Data_Peak.Value_Peak
FROM
    First_Data_Peak
LEFT JOIN
    Temp_Blood
    ON First_Data_Peak.charmed = Temp_Blood.charmed
    AND First_Data_Peak.visits_bloods_date = Temp_Blood.visits_bloods_date;
'''
cursor.execute(METAPEAK)

DROPMETABATCHNORM = '''
DROP TABLE IF EXISTS METABATCHNORM
'''
cursor.execute(DROPMETABATCHNORM)

METABATCHNORM = '''
CREATE TABLE METABATCHNORM AS
SELECT
    Temp_Blood.visits_bloods_pkey,
    First_Data_Batch_Norm.PARENT_SAMPLE_NAME,
    Temp_Blood.visits_bloods_date,
    First_Data_Batch_Norm.Chem_ID,
    First_Data_Batch_Norm.Value_Batch_Norm
FROM
    First_Data_Batch_Norm
LEFT JOIN
    Temp_Blood
    ON First_Data_Batch_Norm.charmed = Temp_Blood.charmed
    AND
    First_Data_Batch_Norm.visits_bloods_date = Temp_Blood.visits_bloods_date;
'''
cursor.execute(METABATCHNORM)

DROPMETABATCHNORMI = '''
DROP TABLE IF EXISTS METABATCHNORMI
'''
cursor.execute(DROPMETABATCHNORMI)

METABATCHNORMI = '''
CREATE TABLE METABATCHNORMI AS
SELECT
    Temp_Blood.visits_bloods_pkey,
    First_Data_Batch_Norm_Imputed.PARENT_SAMPLE_NAME,
    Temp_Blood.visits_bloods_date,
    First_Data_Batch_Norm_Imputed.Chem_ID,
    First_Data_Batch_Norm_Imputed.Value_Data_Batch_Norm_Imputed
FROM
    First_Data_Batch_Norm_Imputed
LEFT JOIN
    Temp_Blood
    ON First_Data_Batch_Norm_Imputed.charmed = Temp_Blood.charmed
    AND
    First_Data_Batch_Norm_Imputed.visits_bloods_date =
        Temp_Blood.visits_bloods_date;
'''
cursor.execute(METABATCHNORMI)

DROPMETALOG = '''
DROP TABLE IF EXISTS METALOG
'''
cursor.execute(DROPMETALOG)

METALOG = '''
CREATE TABLE METALOG AS
SELECT
    Temp_Blood.visits_bloods_pkey,
    First_Data_Log.PARENT_SAMPLE_NAME,
    Temp_Blood.visits_bloods_date,
    First_Data_Log.Chem_ID,
    First_Data_Log.Value_Log
FROM
    First_Data_Log
LEFT JOIN
    Temp_Blood
    ON First_Data_Log.charmed = Temp_Blood.charmed
    AND First_Data_Log.visits_bloods_date = Temp_Blood.visits_bloods_date;
'''
cursor.execute(METALOG)

# remove temporary and first tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
all_tables = [table[0] for table in tables]
tables_to_drop = [table for table
                  in all_tables
                  if table.startswith("First_")
                  or table.startswith("TEMP_")]
for table_name in tables_to_drop:
    drop_query = f"DROP TABLE IF EXISTS {table_name}"
    cursor.execute(drop_query)

conn.commit()
conn.close()
