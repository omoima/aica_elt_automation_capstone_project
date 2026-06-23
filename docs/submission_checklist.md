# Submission Checklist

## GitHub

- [ ] Push this repository to GitHub.
- [ ] Confirm all notebook scripts, dashboard SQL, job template, README, and screenshots are visible.
- [ ] Confirm `online+retail.zip` is not committed to GitHub.
- [ ] Submit the GitHub link in the Google Form.

## Databricks Setup

- [ ] Import all six notebooks from `notebooks/`.
- [ ] Run `00_setup.py`.
- [ ] Upload `online+retail.zip` to `/Volumes/workspace/online_retail_capstone/files/raw/online+retail.zip`.
- [ ] Run `01_download_and_partition.py`.
- [ ] Confirm monthly files exist in `/Volumes/workspace/online_retail_capstone/files/archives`.

## Databricks Workflow Screenshots

Save screenshots in this folder before final submission.

- [ ] `workflow.png`: Databricks workflow graph showing tasks `02` through `05`.
- [ ] `first_successful_run.png`: First successful workflow run after one archive file lands.
- [ ] `last_successful_run.png`: Final successful workflow run that processes the last archive file.

## Dashboard Screenshots

- [ ] `dashboard_first_run.png`: Dashboard after the first pipeline run.
- [ ] `dashboard_last_run.png`: Dashboard after the final pipeline run.

## Expected Tables

- [ ] `workspace.online_retail_capstone.bronze_online_retail`
- [ ] `workspace.online_retail_capstone.silver_online_retail`
- [ ] `workspace.online_retail_capstone.gold_weekly_revenue`
- [ ] `workspace.online_retail_capstone.gold_top_products`
- [ ] `workspace.online_retail_capstone.gold_top_customers`
- [ ] `workspace.online_retail_capstone.gold_revenue_by_country`
