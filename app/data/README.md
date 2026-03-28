# Data Directory

This directory is reserved for future local DailyMed ZIP ingestion, alias dictionaries,
RxNorm crosswalk artifacts, and export jobs.

Version 1 does not depend on local DailyMed downloads. It uses the live DailyMed API
for product search and product detail retrieval and keeps only lightweight SQLite cache
data in the repository root by default.
