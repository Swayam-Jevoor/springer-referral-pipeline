# Springer Capital – Referral Pipeline (Data Engineer Intern Assessment)

## Overview
This project implements a complete data processing pipeline for Springer Capital’s referral program.  
It performs data profiling, cleaning, transformation, business-rule validation, and delivers the final referral validation report required for the assessment.

The solution is fully containerized using Docker and includes all required documentation outputs.

---

## Features
- Data profiling (null counts & distinct counts for all tables)
- Data cleaning and merging of all input datasets
- UTC → Local timezone conversion
- Referral reward validation (valid & invalid logic implementation)
- Final output of **46 unique referral records**
- Dockerized pipeline for reproducibility
- Data dictionary included

---

## Repository Contents
- `main.py`
- `Dockerfile`
- `final_report_46.csv`
- `final_report.csv`
- `final_data_dictionary.csv`
- `profile_*.csv` (profiling reports for each table)

---

## Skills Demonstrated
### **Data Engineering Skills**
- Data profiling & quality assessment  
- Data cleaning & preprocessing  
- Handling missing values & type inconsistencies  
- Multi-table joins & relational data modeling  
- Feature engineering  
- Business rule implementation for data validation  
- Timezone conversions (UTC → Local Time)  
- Boolean rule evaluation for fraud detection  
- Aggregation and deduplication logic  
- CSV report generation  
- Building reproducible ETL-like pipelines  

### **Programming & Technical Skills**
- **Python 3.10+**
- **Pandas** for data manipulation  
- **Dateutil & PyTZ** for datetime parsing & timezone handling  
- **File I/O handling** and structured data processing  
- **Exception-safe code design** and defensive programming  

### **Software Engineering Skills**
- Modular code structure (single executable pipeline)
- Error handling & input validation
- Clear documentation for business and engineering users
- Maintainable script layout

### **DevOps / Deployment Skills**
- **Docker** containerization  
- Using volumes to manage input/output data  
- Reproducible environment setup  
- Separation of input, output, and processing code  

---
