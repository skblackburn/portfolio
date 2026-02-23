# Universal Vehicle Data Processor (GUI Application)
*A Python-based desktop application for automated OEM survey email file preparation, VIN decoding, UCC validation, and multi-stage data cleaning.*

---

## Overview
The Universal Vehicle Data Processor is a full desktop application that automates a complex, multi-step workflow originally performed through a 30–40-year-old SAS process. The tool prepares OEM customer email files for survey distribution by validating, cleaning, deduplicating, and enriching vehicle records across multiple data sources.

The application replaces a legacy workflow that required manual column counting using Boxer, multiple SAS scripts, and repeated Excel manipulation. While reverse-engineering the SAS logic, I discovered that the original process was incorrectly filtering out legitimate customer records—for example, any name containing “usa” or “ari” (such as *Susan*, *Maria*, or *Arie*) was automatically rejected. This Python application corrects those issues and produces accurate, audit-ready outputs with zero manual intervention.

Although the organization had not yet transitioned away from the SAS workflow, this application is production-ready and significantly improves data quality, maintainability, and auditability.

---

## Key Features
- **Full GUI Application (Tkinter + ThemedTk)** — Structured, user-friendly interface for selecting inputs, configuring options, and running the pipeline.
- **17-Step ETL Pipeline** — Automated processing from raw OEM files to final survey-ready email lists.
- **Config-Driven Architecture** — YAML-based rules for VIN decoding, ZIP/state validation, business filtering, and column mapping.
- **VIN Decoding & Validation** — Extracts VIN segments, validates structure, maps model year, assigns cell codes, and enriches records.
- **UCC Master Integration** — Merges OEM data with UCC master files, normalizes model year, and validates cell/modyy combinations.
- **Business Filtering** — Identifies business entities using regex patterns, OEM name detection, and configurable exclusion lists.
- **Geographic Validation** — ZIP normalization, ZIP2 prefix creation, and state/ZIP matching using YAML rules.
- **Deduplication Engine** — Compares against historical files and TrueCar files, removes duplicates, and generates audit reports.
- **Audit-Ready Outputs** — Every rejection reason is logged and written to a separate CSV for traceability.
- **Threaded Processing with Live Logging** — Background thread runs the pipeline while the UI remains responsive.

---

## GUI Preview
This interface provides a structured, multi-step workflow for loading OEM input files, selecting configuration options, running the processing pipeline, and reviewing logs and status messages. The design is intentionally functional and optimized for internal workflow automation rather than consumer aesthetics.

**Screenshot 1: Main Application Window**  

![Main GUI Screenshot](https://github.com/skblackburn/portfolio/blob/main/screenshots/GUI%20Image.jpg)




**Screenshot 2: Processing in Action**  
*(Insert screenshot here)*

---

## Architecture

### GUI Layer
- Tkinter + ThemedTk interface  
- File selection dialogs  
- Progress bar  
- Real-time logging console  
- Status bar  
- Summary report button  

### Processing Pipeline
A 17-step ETL workflow including:
- Email cleaning  
- Address normalization  
- Address deduplication  
- Sales type filtering  
- ZIP normalization  
- State/ZIP validation  
- Business filtering  
- VIN validation  
- VIN decoding  
- VIN mapping  
- UCC validation  
- EV merge  
- Description merge  
- Deduplication  
- Panelization  
- Summary generation  

### Configuration System
- YAML rules for VIN mappings  
- YAML rules for ZIP/state validation  
- YAML business terms  
- YAML column maps  
- JSON user settings  

### Utilities
- Threading helpers  
- Queue-based logging  
- Column standardization  
- Metrics tracking  
- File dialog helpers  

---

## Processing Workflow
The pipeline follows a deterministic, auditable sequence:

1. Load input file  
2. Standardize column names  
3. Clean email fields  
4. Remove invalid or missing emails  
5. Normalize addresses  
6. Deduplicate addresses  
7. Identify and remove invalid sales types  
8. Normalize ZIP codes  
9. Validate ZIP/state combinations  
10. Apply business filtering  
11. Validate VIN structure  
12. Decode VIN segments  
13. Map VIN to cell/brand/description  
14. Validate model year  
15. Merge UCC master data  
16. Merge EV and description files  
17. Deduplicate against historical and TrueCar files  
18. Generate summary report  

Each step logs metrics and writes rejected records to separate CSVs.

---

## Outputs
The application generates a complete set of audit-ready files, including:

- `email_cleaned.csv`  
- `after_address_dedup.csv`  
- `sales_filtered.csv`  
- `geo_filtered.csv`  
- `business_filtered.csv`  
- `vin_processed.csv`  
- `deduped.csv`  
- `duplicates_rejected.csv`  
- `vin_rejected.csv`  
- `missing_cell_codes.csv`  
- `summary_report.xlsx`  

---

## Skills Demonstrated
- Python application development  
- GUI engineering (Tkinter + ThemedTk)  
- Multi-threading and queue-based logging  
- Data engineering and ETL pipeline design  
- YAML/JSON configuration systems  
- VIN decoding and automotive data logic  
- Regex-based validation  
- Excel automation (openpyxl)  
- Workflow automation  
- Audit-ready reporting  
- Reverse-engineering legacy SAS logic  
- Modernizing outdated processes  
- UX design for internal tools  

---

## Background and Modernization Story
This application was created to replace a legacy SAS workflow that had been in use for decades. The original process required manual column counting using Boxer, multiple SAS scripts, and repeated Excel manipulation. While validating the Python version against the SAS output, I discovered that the SAS code was incorrectly removing legitimate customer records based on substring matches in names (e.g., “usa” or “ari”). This Python application corrects those issues and produces more accurate, consistent results.

---

## Installation & Usage
```bash
python main.py
