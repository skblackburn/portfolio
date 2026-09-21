# Portfolio — automation, data processing, and design

A collection of the operational tools I've designed and built, mostly in automotive consumer research: vehicle data processing, VIN decoding, reporting automation, and reconciliation workflows. Client data and production credentials are excluded; sample configs, redacted screenshots, and write-ups stand in for them.

## Python — data processing tools

| Project | What it does |
| --- | --- |
| [Universal Vehicle Data Processor](python/Universal%20Vehicle%20Data%20Processor) | GUI tool that normalizes vehicle buyer files from multiple providers into one standardized output, driven by YAML/CSV brand-mapping config instead of hardcoded rules |
| [VIN Decoder](python/vin-decoder) | NHTSA-based VIN decoding and enrichment, including reject-code handling so bad decodes fail loudly rather than passing through |
| [Vehicle Classifications](python/Vehicle%20Classifications%20) | Classification and cell/segment-code assignment logic for vehicle records |

Design notes on how I approach these tools are in [python/README.md](python/README.md).

## Excel — reporting and QA

| Project | What it does |
| --- | --- |
| [Audit & Reconciliation](excel/Audit_Reconciliation) | Reconciliation workbook for catching record-level discrepancies between source and delivered files |
| [Availability Report](excel/Availability%20Report%20) | Monthly vehicle-availability reporting automation |
| [Dashboard](excel/Dashboard) | Operational dashboard for tracking processing status |

## Documentation

- [VIN data collection workflow](documentation/vin-scrapping-workflow) — end-to-end process documentation for the vehicle data pipeline
- [Screenshots](screenshots) — redacted GUI captures of the processing tools in use

## Design

Print and brand work in [Illustrator](design/Illustrator), [InDesign](design/InDesign), and [Canva](design/Canva), including a full cafe menu project.

## Related work

- [echome](https://github.com/skblackburn/echome) — TypeScript family legacy platform (public)
- Next.js/Supabase web apps and a CMS-backed company website live in private repositories; happy to demo on request.
