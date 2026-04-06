# 🧩 REDCap to SQL Utility

This module provides tools to extract data from multiple REDCap study databases and prepare it for integration into an SQL database. The work was originally started by **Timothy Rittman** (tr332@medschl.cam.ac.uk) and maintained by me between **2022–2024**.

---

## ✨ Key Features

- 🔄 Standardised data extraction across studies  
- 🧩 Handling of study-specific field and event differences  
- ⏱️ Session alignment using date matching (±6 months)  
- 🧮 Utility functions for ID mapping and SQL data type inference  
- 🧠 Adds cognitive and clinical test data to an SQL database  
- 🧬 Supports metabolomic and blood biomarker data integration  

---

## 🛠️ Requirements

- **Python packages**:
  - `redcaptosql` – core module for REDCap extraction
  - `sqlite3` – for SQL database operations  
  - `requests` – API requests to REDCap  
  - `itertools` – for data transformations  
  - `pandas` – data manipulation  
  - `numpy` – numerical operations
- **Data**:
  - REDCap API tokens for each study  
  - Local paths to metabolomics and PET data spreadsheets  
- **SQL database**: SQLite for storing the merged study data  

