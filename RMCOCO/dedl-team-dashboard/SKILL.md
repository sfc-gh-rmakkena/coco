---
name: dedl-team-dashboard
description: "DE/DL Team Engagements Dashboard. Use when: viewing DE/DL team use cases, analyzing engagements by stage buckets, tracking DE/DL consumption credits. Triggers: de dl dashboard, de/dl engagements, de dl team, data engineering dashboard, dedl dashboard."
---

# DE/DL Team Engagements Dashboard

A Streamlit dashboard showing DE/DL team engagements with Won+ and All use cases, DE/DL consumption credits, and summary analytics by stage buckets.

---

## Features

### Tabs
| Tab | Purpose |
|-----|---------|
| **Won+ Engagements** | Stage 4-7 engagements with decision/go-live dates in selected range |
| **All Engagements** | All active engagements (Stages 1-7) |
| **DE/DL Summary** | Summary view by stage buckets (1-3, 4-5, 6-7) with filters for Date and Priority DE Features |

### Filters (Sidebar)
- **Date Range**: Start/End date for filtering engagements
- **Manager**: Filter by first-line manager
- **Engineer**: Filter by individual engineer
- **Feature Area**: DE: Ingestion, Transformation, Interoperable Storage
- **Key Features**: DE - Openflow, DE - Iceberg, DE - Snowpark DE, DE - Dynamic Tables, DE - Snowpipe Streaming, DE - Snowpipe, DE - Serverless Task, DE - Connectors, DE - dbt Projects, DE - SAP Integration, DE - Basic
- **GVP**: Account GVP filter
- **Theater**: AMSAcquisition, AMSExpansion, USMajors, USPubSec

### DE/DL Consumption Credits
The dashboard shows DE/DL consumption credits per account from:
- Table: `SNOWSCIENCE.JOB_ANALYTICS.WORKLOAD_ACCOUNT_SUB_CREDITS_2024_05_01`
- Primary classes: `data_engineering`, `data_lake`
- Auto-adjusts date range to available data (2021-02-01 to 2025-02-03)

---

## Prerequisites

- Snowflake connection to SNOWHOUSE
- **SALES_ENGINEER role** (required for all operations)
- Access to:
  - `TEMP.AI_PSE_IMPACT` schema
  - `MDM.MDM_INTERFACES.DIM_USE_CASE`
  - `SALES.SE_REPORTING.USE_CASE_ATTRIBUTION`
  - `SALES.SE_REPORTING.SE_ORG_HIERARCHY_VW`
  - `SNOWSCIENCE.JOB_ANALYTICS.WORKLOAD_ACCOUNT_SUB_CREDITS_2024_05_01`
  - `snowscience.dimensions.dim_accounts_history`

---

## Directory Structure

```
dedl-team-dashboard/
├── SKILL.md                    # This file
└── streamlit/                  # Streamlit app
    ├── streamlit_app.py        # Main dashboard app
    ├── snowflake.yml           # Snowflake deployment config
    └── environment.yml         # Dependencies
```

---

## Deployment

### Deploy to Snowflake

```bash
cd /Users/rmakkena/Desktop/cocoskills/pse-ai-qbr/.cortex/skills/dedl-team-dashboard/streamlit
snow streamlit deploy --connection snowhouse --role SALES_ENGINEER
```

### Deployed App Location
- **Database**: TEMP
- **Schema**: AI_PSE_IMPACT
- **App Name**: DEDL_TEAM_DASHBOARD
- **URL**: https://app.snowflake.com/SFCOGSOPS/snowhouse_aws_us_west_2/#/streamlit-apps/TEMP.AI_PSE_IMPACT.DEDL_TEAM_DASHBOARD

---

## Data Sources

### Use Cases
- `MDM.MDM_INTERFACES.DIM_USE_CASE` - Main use case data
- `SALES.SE_REPORTING.USE_CASE_ATTRIBUTION` - DE/DL team attribution
- `SALES.SE_REPORTING.SE_ORG_HIERARCHY_VW` - Org hierarchy for filtering

### DE/DL Consumption
- `SNOWSCIENCE.JOB_ANALYTICS.WORKLOAD_ACCOUNT_SUB_CREDITS_2024_05_01` - Workload credits
- `snowscience.dimensions.dim_accounts_history` - Account name mapping

---

## Engineer/Manager Mapping

| Manager | Engineers |
|---------|-----------|
| Rithesh Makkena | Anika Shahi, Chandra Nayak, Chris Atkinson, Chris Cardillo, Kelsey Hammock, Kesav Rayaprolu, Nagesh Cherukuri, Naveen Alan Thomas, Niels ter Keurs, Prash Medirattaa, Randy Pettus, Rithesh Makkena, Sam Mittal, Shawn Namdar, Varun Kumar, Venkat Suru, Venkatesh Sekar |
| Puneet Lakhanpal | Chinmayee Lakkad, Dharmendra Shavkani, Gayatri Ghanakota, Hanbing Yan, Jason Ho, Jonathan Sierra, Jonathan Tao, Kiran Kumar Earalli, Manrique Vargas, Nirav Shah, Pallavi Sharma, Phani Raj, Prathamesh Nimkar, Priya Joseph, Puneet Lakhanpal, Rahul Reddy, Ravi Kumar, Ripu Jain, Rogerio Rizzio, Sam Gupta, Santosh Ubale, Su Dogra, Tom Manfredi |
| David Hare | David Hare, Jason Hughes, Jeremiah Hansen, Jon Bennett, Keith Gaputis, Marc Henderson, Marcin Kulakowski, Parag Jain, Sean Petrie, Shantanu Gope, Sharvan Kumar |
| Brendan Tisseur | Brendan Tisseur, Prasad Revalkar, Ryan Templeton, Salar Rowhani, Summiya Khalid, Venkat Medida |
| Zahir Gadiwan | Ali Khosro, Andries Engelbrecht, Eric Tolotti, James Sun, Matt Marzillo, Zahir Gadiwan |
| Gopal Raghavan | Akash Bhatt, Anthony Alteirac, Dave Freriks, David Richert, Gopal Raghavan, Mayur Mahadeshwar |

---

## Metrics Displayed

### Per Tab
| Metric | Description |
|--------|-------------|
| Total Engagements | Count of use cases |
| Total EACV | Sum of USE_CASE_EACV |
| Unique Accounts | Distinct account count |
| Engineers | Distinct engineer count |

### DE/DL Summary Tab (Additional)
| Metric | Description |
|--------|-------------|
| Use Cases by Stage Bucket | Count per bucket (1-3, 4-5, 6-7) |
| EACV by Stage Bucket | Sum per bucket |
| DE/DL Credits by Stage Bucket | Total consumption credits per bucket |
| Accounts by Stage Bucket | Unique accounts per bucket |
