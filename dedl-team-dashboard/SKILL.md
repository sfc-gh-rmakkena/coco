---
name: Rithesh_DE_Latest
description: "Deploy and manage the DE/DL Team Dashboard Streamlit app. Use when: deploying DE/DL dashboard, updating the dashboard, checking PSE use case metrics. Triggers: de dl dashboard, dedl team dashboard, deploy de dashboard, pse impact dashboard."
---

# DE/DL Team Engagements Dashboard

A Streamlit dashboard showing DE/DL team engagements with Won+ and All use cases, DE/DL consumption credits, weekly key updates, and summary analytics by stage buckets.

---

## Features

### Tabs
| Tab | Purpose |
|-----|---------|
| **Won+ Engagements** | Stage 4-7 engagements with decision/go-live dates in selected range |
| **All Engagements** | All active engagements (Stages 1-7) |
| **DE/DL Summary** | Summary view by stage buckets (1-3, 4-5, 6-7) with filters for Date and Priority DE Features |
| **Weekly Key Updates** | Recent weekly updates grouped by **Key Feature** (expanders) with **Stage Bucket** sub-groupings inside each feature. Metrics row: Total Use Cases, Total EACV, Unique Accounts + Stage 1-3/4-5/6-7 counts with EACV. Only non-empty comments shown. |

### Key Capabilities

#### Checkbox Row Selection
- All data tables use `st.data_editor` with `st.column_config.CheckboxColumn` for row selection
- Users can select specific rows to include in AI analysis or email

#### AI Summary (Cortex AI - mistral-large2)
- Available on Tabs 1-3 via "AI Analyze" button
- Produces structured output in 3 sections:
  - **What is Working** - Specific wins with account names, dollar amounts, credit consumption
  - **Issues / Problems** - Risk accounts with problem, impact, and action format
  - **Action Items** - Prioritized next steps with owner and timeline
- Individual row AI analysis also uses same 3-section format
- Prompt functions: `build_bulk_ai_prompt(df)` for portfolio, `build_usecase_ai_prompt(row)` for individual

#### Email Integration (Gmail + Okta SSO)
- Email button on ALL 4 tabs using `st.link_button("Compose Email", gmail_url)`
- Opens Gmail web compose (`https://mail.google.com/mail/?view=cm&fs=1&su=...&body=...&to=...`)
- Auto-populates To field with Account Owner emails from data
- Name-to-email conversion: "John Smith" -> "john.smith@snowflake.com" via `name_to_email()`
- From address is dynamic based on whoever is logged into Gmail via corporate Okta
- Helper functions: `name_to_email()`, `get_recipient_emails()`, `build_gmail_url()`
- Email body includes AI summary (if available) + use case details via `build_email_content()`
- Tab 4 does NOT have ACCOUNT_OWNER column, so To field is empty there
- `st.link_button()` does NOT support `key` parameter in the installed Streamlit version

### Filters (Sidebar)
- **Date Range**: Start/End date for filtering engagements
- **Manager**: Filter by first-line manager
- **Engineer**: Filter by individual engineer
- **Feature Area**: DE: Ingestion, Transformation, Interoperable Storage
- **Key Features**: DE - Openflow, DE - Iceberg, DE - Snowpark DE, DE - Dynamic Tables, DE - Snowpipe Streaming, DE - Snowpipe, DE - Serverless Task, DE - Connectors, DE - dbt Projects, DE - SAP Integration, DE - Basic
- **GVP**: Account GVP filter
- **Theater**: AMSAcquisition, AMSExpansion, USMajors, USPubSec
- **Min EACV**: Number input filter (sidebar, applies to all tabs)
- **Last N Days**: Number input filter for Tab 4 weekly updates (sidebar)

### DE/DL Consumption Credits
- Table: `SNOWSCIENCE.JOB_ANALYTICS.WORKLOAD_ACCOUNT_SUB_CREDITS_2024_05_01`
- Primary classes: `data_engineering`, `data_lake`
- Cached with `@st.cache_data(ttl=600)`
- `LISTAGG(DISTINCT ...)` for aggregating engineer names in SQL

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
    ├── streamlit_app.py        # Main dashboard app (~680 lines)
    ├── snowflake.yml           # Snowflake deployment config
    └── environment.yml         # Dependencies
```

---

## Deployment

### Local Testing
```bash
SNOWFLAKE_CONNECTION_NAME=snowhouse streamlit run /Users/rmakkena/Desktop/RMCOCO/dedl-team-dashboard/streamlit/streamlit_app.py --server.port 8501
```

### Deploy to Snowflake
```bash
cd /Users/rmakkena/Desktop/RMCOCO/dedl-team-dashboard/streamlit
snow streamlit deploy --connection snowhouse --role SALES_ENGINEER --replace
```

### Source Code Location
- **Working copy**: `/Users/rmakkena/Desktop/RMCOCO/dedl-team-dashboard/streamlit/streamlit_app.py`
- **Skill copy**: `/Users/rmakkena/Desktop/cocoskills/pse-ai-qbr/.cortex/skills/dedl-team-dashboard/streamlit/streamlit_app.py`
- Always edit the working copy first, then copy to skill directory

### Deployed App Location
- **Database**: TEMP
- **Schema**: AI_PSE_IMPACT
- **App Name**: DEDL_TEAM_DASHBOARD
- **Stage**: DEDL_TEAM_DASHBOARD_STAGE
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

## Key Technical Notes

### Streamlit Version Constraints
- `st.link_button()` does NOT support `key` parameter
- Use `st.data_editor` with `st.column_config.CheckboxColumn` for row selection (not AgGrid)

### Email URL Construction
- Use `urllib.parse.urlencode(params, quote_via=urllib.parse.quote)` for Gmail URLs
- `ACCOUNT_OWNER` column available in Tabs 1-3 (from `UC.ACCOUNT_OWNER_NAME AS ACCOUNT_OWNER`)
- Tab 4 weekly query does NOT include ACCOUNT_OWNER

### AI Prompt Structure
- Both `build_bulk_ai_prompt` and `build_usecase_ai_prompt` use 3 sections:
  - **What is Working**
  - **Issues / Problems**
  - **Action Items**
- Cortex model: `mistral-large2`

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
