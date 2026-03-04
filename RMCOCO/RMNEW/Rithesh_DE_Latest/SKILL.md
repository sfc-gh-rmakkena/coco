---
name: Rithesh_DE_Latest
description: "Deploy and manage the DE/DL Team Dashboard Streamlit app. Use when: deploying DE/DL dashboard, updating the dashboard, checking PSE use case metrics. Triggers: de dl dashboard, dedl team dashboard, deploy de dashboard, pse impact dashboard."
---

# DE/DL Team Dashboard

A Snowflake Streamlit app for tracking Data Engineering/Data Lake team engagements with AI-powered analysis.

## Prerequisites

- Snowflake connection with `SALES_ENGINEER` role
- Access to `TEMP.AI_PSE_IMPACT` schema
- Source: `/Users/rmakkena/Desktop/RMCOCO/dedl-team-dashboard/streamlit/`

## Workflow

### Step 1: Deploy/Update Dashboard

**Deploy to Snowflake:**
```bash
cd /Users/rmakkena/Desktop/RMCOCO/dedl-team-dashboard/streamlit && snow streamlit deploy --connection snowhouse --role SALES_ENGINEER --replace
```

**Test Locally:**
```bash
SNOWFLAKE_CONNECTION_NAME=snowhouse streamlit run /Users/rmakkena/Desktop/RMCOCO/dedl-team-dashboard/streamlit/streamlit_app.py --server.port 8501
```

**App URL:** https://app.snowflake.com/SFCOGSOPS/snowhouse_aws_us_west_2/#/streamlit-apps/TEMP.AI_PSE_IMPACT.DEDL_TEAM_DASHBOARD

### Step 2: Verify Deployment

1. Open the app URL in browser
2. Test filters (Manager, Engineer, GVP, Theater, Stage)
3. Verify AI Summary buttons work (uses `mistral-large2` Cortex model)
4. Check all 4 tabs render correctly

## Key Features

| Feature | Description |
|---------|-------------|
| **Manager/Engineer Filter** | Hierarchical team filtering |
| **Checkbox Selection** | Select rows in data tables for analysis |
| **Bulk AI Summary** | Portfolio-level analysis (up to 25 use cases) |
| **Individual AI Analysis** | Detailed analysis for 1-5 selected use cases |
| **Stage Charts** | Distinct use case count by stage |
| **4 Tabs** | AFE Won+Engagement, AFE All Engagements, Overall DE/DL Summary, Weekly Key Updates |

## Tabs

1. **AFE Involved Won+Engagement**: Won+ use cases (Stages 4-7) with DE/DL engineers
2. **AFE All Engagements**: All active engagements with DE/DL engineers
3. **Overall DE/DL Summary**: Stage bucket breakdown (1-3, 4-5, 6-7) with all DE use cases
4. **Weekly Key Updates**: Stage 4-5 use cases by key feature with all comments

## Comment Fields

The app displays these comment fields from DIM_USE_CASE:
- SE_COMMENTS
- IMPLEMENTATION_COMMENTS
- NEXT_STEPS
- SPECIALIST_COMMENTS
- PARTNER_COMMENTS

## Technical Details

- **Cortex Model:** `mistral-large2`
- **Database:** `TEMP`
- **Schema:** `AI_PSE_IMPACT`
- **App Name:** `DEDL_TEAM_DASHBOARD`
- **Key Tables:**
  - `MDM.MDM_INTERFACES.DIM_USE_CASE` - Use case data
  - `SNOWSCIENCE.JOB_ANALYTICS.WORKLOAD_ACCOUNT_SUB_CREDITS_2024_05_01` - DE/DL credit consumption
  - `SALES.SE_REPORTING.USE_CASE_ATTRIBUTION` - Engineer attribution
  - `SALES.SE_REPORTING.SE_ORG_HIERARCHY_VW` - Team hierarchy

## Performance Optimizations

- Combined filter queries (5 queries into 1)
- Cache TTL: 1800s (30 min) for main data
- Cache TTL: 604800s (1 week) for filter options
- Unique keys with index suffix to prevent duplicate key errors

## Troubleshooting

**AI Summary not working:**
- Check Cortex availability in account
- Verify `mistral-large2` model access

**Data not loading:**
- Verify `SALES_ENGINEER` role has SELECT on source views
- Check engineer names match `ENGINEER_LIST` in code

**Slow performance on Snowflake:**
- First load after deploy warms caches
- Subsequent loads should be faster

## Output

Deployed Streamlit app with:
- Won+ engagement tracking
- All engagement metrics  
- DE/DL summary with AI insights
- Weekly key updates with all comment fields
