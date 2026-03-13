---
name: afe-engagement-bandwidth
description: "Deploy and manage the AFE Engagement Bandwidth Dashboard. Use when: deploying AFE engagement dashboard, updating the dashboard, checking AFE engagement metrics, tracking specialist comments. Triggers: afe engagement, afe bandwidth, afe dashboard, deploy afe app, team comments dashboard, specialist engagement tracker."
---

# AFE Engagement Bandwidth Dashboard

A Streamlit dashboard tracking AFE (Application Field Engineer) engagement on use cases for Rithesh Makkena's DE org. Deployed to Snowhouse at `TEMP.AI_PSE_IMPACT.DEAFE_ENGAGEMENT_BANDWIDTH`.

---

## Features

### Engagement Classification (Three-Tier)
| Status | Criteria | Color |
|--------|----------|-------|
| **Active** | Has specialist comments AND modified in last 7 days | Green |
| **Stale** | Has specialist comments but older than 7 days | Amber |
| **Not Active** | No specialist comments | Red |

### Summary View
- Metric cards: Total AFEs, Active, Stale, Not Active, Total Use Cases
- Tabs: All, Not Active, Stale, Active
- Search by AFE name, filter by Product Category
- Color-coded rows by engagement status

### Use Case Detail
- Filter by AFE, Quarter, Engagement status
- Columns: AFE, Account, Use Case, Stage, Status, EACV, Theater, Products, Engagement, Decision Date, Go Live Date, Last Modified, SFDC link

### Ask a Question (Cortex Complete)
- Natural language Q&A about the dashboard data
- Uses `SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', prompt)` via raw cursor with `?` bind params
- Context: AFE summary + top 50 use cases by EACV

---

## Org Scope

People reporting to **David Hare** and **Brendan Tisseur**, plus **Nagesh Cherukuri** as a direct report exception under Rithesh Makkena.

Source: `SALES.SE_REPORTING.SE_ORG_HIERARCHY_VW`

---

## Data Filters

- **Team Role**: `SE - Workload FCTO` (displayed as AFE)
- **Org**: `MANAGER_NAME IN ('David Hare', 'Brendan Tisseur') OR FIRST_LINE_MANAGER IN ('David Hare', 'Brendan Tisseur')` + Nagesh
- **Stages Excluded**: "8 - Use Case Lost", "7 - Deployed"
- **Fiscal Quarter**: Current quarter + next quarter (Snowflake FY starts Feb 1)
- **Quarter dates**: Decision Date or Go Live Date must fall within this/next FQ

---

## Key Tables

| Table | Purpose |
|-------|---------|
| `MDM.MDM_INTERFACES.DIM_USE_CASE` | Use case data with specialist comments, team arrays, dates |
| `SALES.SE_REPORTING.SE_ORG_HIERARCHY_VW` | Org hierarchy for scoping AFEs |

### Key Columns (DIM_USE_CASE)
- `USE_CASE_TEAM_NAME_LIST` (ARRAY) + `USE_CASE_TEAM_ROLE_LIST` (ARRAY) — paired arrays joined by `f.index = r.index`
- `SPECIALIST_COMMENTS`, `LAST_MODIFIED_DATE`, `DECISION_DATE`, `GO_LIVE_DATE`
- `USE_CASE_EACV`, `USE_CASE_STAGE`, `USE_CASE_STATUS`, `PRODUCT_CATEGORY_ARRAY`

---

## Deployment

- **App**: `TEMP.AI_PSE_IMPACT.DEAFE_ENGAGEMENT_BANDWIDTH`
- **URL**: https://app.snowflake.com/SFCOGSOPS/snowhouse_aws_us_west_2/#/streamlit-apps/TEMP.AI_PSE_IMPACT.DEAFE_ENGAGEMENT_BANDWIDTH
- **Role**: `PSE_ROLE` (owner), `SALES_ENGINEER` (usage granted)
- **Warehouse**: `PSE_WH`
- **Deploy command**:
  ```bash
  cd /Users/rmakkena/Desktop/RMCOCO/Team\ comments/afe-engagement-bandwidth/streamlit && snow streamlit deploy --replace --connection snowhouse --role PSE_ROLE
  ```
- Uses legacy SiS (no SPCS container runtime) with `environment.yml`

---

## Technical Notes

- Cortex Complete: must use raw cursor with `?` qmark paramstyle (not `%s`, not f-strings)
- Custom `add_months()` helper avoids `dateutil` dependency
- Pandas Styler for row-level color coding
- `st.connection("snowflake")` with `@st.cache_data(ttl=1800)`
