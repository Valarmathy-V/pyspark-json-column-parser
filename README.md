# PySpark JSON Array Column Parser

This script transforms JSON stringified array columns in a PySpark DataFrame into structured columns.

### 🔧 Technologies Used
- PySpark
- Databricks
- JSON
- Dynamic DataFrame transformation

### 💡 What It Does
- Detects stringified array columns
- Converts them into arrays using `from_json`
- Dynamically expands elements into new columns (e.g., `col_1`, `col_2`, etc.)
- Maintains original columns alongside exploded values

