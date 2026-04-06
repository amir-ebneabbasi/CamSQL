# 🧩 REDCap to SQL Utility

This module provides tools to extract data from multiple REDCap study databases and prepare it for integration into an SQL database. The work was originally started by **Timothy Rittman** (tr332@medschl.cam.ac.uk) and maintained by me between **2022–2024**.

---

## 📖 Overview

The core `Study` class connects to REDCap via API and retrieves structured data, including:

- 👤 Participant IDs  
- 📅 Visit dates  
- 🧠 MRI, PET, and blood sessions  
- 🧪 Study-specific test data  

It supports multiple study formats (e.g., **PIPPIN** and **IMPRINT**) with built-in adjustments for differing REDCap schemas.

---

## ✨ Key Features

- 🔄 Standardised data extraction across studies  
- 🧩 Handling of study-specific field and event differences  
- ⏱️ Session alignment using date matching (±6 months)  
- 🧮 Utility functions for ID mapping and SQL data type inference
