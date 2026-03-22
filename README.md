# Payment-Failure-SQL-Analysis
Queries used for payment failure pattern analysis and triage validation. These queries were developed as part of business analysis work on high-volume payment transaction datasets (5,000–10,000+ daily transactions). 
## What is in this repo A collection of SQL queries for: 
- Identifying recurring payment failure patterns by gateway and error type
- Validating data pipeline outputs against expected business metrics
- Supporting root-cause analysis for payment operations teams
- Building reporting logic for failure trend dashboards 
## Sample queries included 
1. failure_frequency_by_gateway.sql — ranks failure types by volume per gateway
2. daily_failure_trend.sql — tracks failure counts over time using window functions
3. upstream_service_error_rate.sql — calculates error rates per upstream service
4. reconciliation_gap_detection.sql — finds transactions missing from downstream reconciliation
5. failure_resolution_time.sql — measures time between failure detection and resolution 
## Tools used SQL (PostgreSQL-style syntax), AWS Athena, data validation for analytics pipelines 
## These queries supported data validation and root-cause analysis work. All data has been anonymised and no proprietary business logic is included.
