VIN Scraping Workflow: Multi‑Source Decoding & Integration System
This workflow supports the end‑to‑end processing of large VIN datasets by combining three independent decoding sources—VINAudit, NHTSA, and Blackbook—into a unified, validated output. It was designed to streamline recurring VIN‑processing cycles, reduce manual effort, and ensure consistent, accurate decoding across multiple platforms.

Purpose of the Workflow
Organizations often rely on VIN data from multiple external sources, each with its own format, decoding logic, and output structure. This workflow provides a repeatable, documented system that:
- prepares and standardizes VIN input files
- automates decoding through Python scripts, SQL Server, and GUI tools
- processes large VIN volumes in batches
- reconciles and merges outputs from three independent decoding systems
- produces a single, consolidated VIN dataset for downstream analysis
The result is a reliable, auditable process that reduces manual work and ensures consistent data quality.


System Overview

VINAudit Automation (Python + GUI)
A Python‑based GUI allows non‑technical users to select input files and output locations. The script processes VINs in batches of 1,000 and generates multiple output files. A separate Python script merges these batch outputs into a single dataset.
Capabilities:
- GUI‑driven file selection
- batch processing
- automated scraping
- output consolidation

NHTSA VIN Decoding (SQL Server + Python)
This component uses the official NHTSA vPIC database, restored locally through SQL Server Management Studio. Python scripts connect to SQL Server, execute stored procedures, and decode VINs using authoritative NHTSA data.
Capabilities:
- SQL Server database restoration
- stored procedure validation
- Python‑SQL integration
- logging and error handling
- monthly/quarterly update support

Blackbook VIN Processing (Web Platform)
VIN files are uploaded through the Blackbook web interface, mapped to the correct fields, processed, and downloaded. The workflow includes steps for reconciling main and secondary output files and removing duplicates.
Capabilities:
- web‑based VIN decoding
- header validation
- duplicate management
- manual reconciliation for accuracy

Final Integration (Python Merge Script)
After each source is processed, a final Python script standardizes column names and merges the three datasets into one unified VIN file. This ensures consistent structure and eliminates duplicate records.
Capabilities:
- cross‑source reconciliation
- column standardization
- duplicate removal
- unified output generation


High‑Level Workflow
Qualtrics / Source File
        ↓
Input Preparation
        ↓
 ┌───────────────┬───────────────────┬────────────────────┐
 │   VINAudit     │      NHTSA        │     Blackbook       │
 │ (Python GUI)   │ (SQL + Python)    │   (Web Platform)    │
 └───────────────┴───────────────────┴────────────────────┘
        ↓
Individual Outputs
        ↓
Final Merge Script (Python)
        ↓
Unified VIN Dataset



Key Features
- multi‑source VIN decoding
- Python automation with GUI support
- SQL Server stored procedure execution
- batch processing for large VIN volumes
- data validation and reconciliation
- fully documented workflow for repeatability
- modular design for monthly/quarterly updates

Skills Demonstrated
- Python scripting and automation
- GUI development
- SQL Server database restoration and management
- stored procedure execution and validation
- data engineering and ETL workflow design
- Excel‑based data preparation and reconciliation
- technical documentation and process standardization

Files Included in This Folder
- Work Instructions (redacted) — High‑level documentation outlining the full VIN‑scraping workflow, including preparation steps, decoding processes, and integration logic.
- README.md — Overview of the system architecture, purpose, and key components of the multi‑source VIN decoding pipeline.
- Workflow Diagram (optional) — A visual representation of the end‑to‑end VIN processing pipeline, showing how VINAudit, NHTSA, and Blackbook outputs flow into the final integration step. This diagram helps viewers quickly understand the structure and dependencies of the system.
- Sample Input/Output Files (optional) — Small, anonymized examples demonstrating the expected structure of VIN input files and the standardized format of the final merged output. These samples illustrate how the workflow transforms raw VIN lists into a unified, decoded dataset.

Redaction Notice
This public version of the VIN scraping workflow has been sanitized to remove internal file paths, credentials, server names, and proprietary organizational details. All technical descriptions, workflow steps, and architectural components remain accurate and representative of the full system, but sensitive information has been intentionally omitted to maintain security and confidentiality.








