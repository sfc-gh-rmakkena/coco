import streamlit as st
from datetime import datetime, timedelta
import plotly.express as px
import pandas as pd
import os
import snowflake.connector
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

st.set_page_config(page_title="DE/DL Team Engagements", layout="wide")

ENGINEER_LIST = {
    "Rithesh Makkena": ["Anika Shahi", "Chandra Nayak", "Chris Atkinson", "Chris Cardillo", "Kelsey Hammock", "Kesav Rayaprolu", "Nagesh Cherukuri", "Naveen Alan Thomas", "Niels ter Keurs", "Prash Medirattaa", "Randy Pettus", "Rithesh Makkena", "Sam Mittal", "Shawn Namdar", "Varun Kumar", "Venkat Suru", "Venkatesh Sekar"],
    "Puneet Lakhanpal": ["Chinmayee Lakkad", "Dharmendra Shavkani", "Gayatri Ghanakota", "Hanbing Yan", "Jason Ho", "Jonathan Sierra", "Jonathan Tao", "Kiran Kumar Earalli", "Manrique Vargas", "Nirav Shah", "Pallavi Sharma", "Phani Raj", "Prathamesh Nimkar", "Priya Joseph", "Puneet Lakhanpal", "Rahul Reddy", "Ravi Kumar", "Ripu Jain", "Rogerio Rizzio", "Sam Gupta", "Santosh Ubale", "Su Dogra", "Tom Manfredi"],
    "David Hare": ["David Hare", "Jason Hughes", "Jeremiah Hansen", "Jon Bennett", "Keith Gaputis", "Marc Henderson", "Marcin Kulakowski", "Parag Jain", "Sean Petrie", "Shantanu Gope", "Sharvan Kumar"],
    "Brendan Tisseur": ["Brendan Tisseur", "Prasad Revalkar", "Ryan Templeton", "Salar Rowhani", "Summiya Khalid", "Venkat Medida"],
    "Zahir Gadiwan": ["Ali Khosro", "Andries Engelbrecht", "Eric Tolotti", "James Sun", "Matt Marzillo", "Zahir Gadiwan"],
    "Gopal Raghavan": ["Akash Bhatt", "Anthony Alteirac", "Dave Freriks", "David Richert", "Gopal Raghavan", "Mayur Mahadeshwar"]
}

def get_connection():
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except:
        conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "snowhouse")
        return conn

def run_query(query):
    conn = get_connection()
    if hasattr(conn, 'sql'):
        return conn.sql(query).to_pandas()
    else:
        cur = conn.cursor()
        cur.execute(query)
        df = cur.fetch_pandas_all()
        cur.close()
        return df

def build_bulk_ai_prompt(df):
    summary_data = []
    for _, row in df.head(25).iterrows():
        account = row.get('ACCOUNT_NAME', 'Unknown')
        use_case = row.get('USE_CASE_NAME', 'Unknown')
        stage = row.get('STAGE', 'Unknown')
        eacv = row.get('EACV', 0) or 0
        credits = row.get('DE_DL_CREDITS', 0) or 0
        features = row.get('KEY_FEATURES', '') or 'None'
        engineer = row.get('ENGINEER', 'Unknown')
        ratio = (credits / eacv * 100) if eacv > 0 else 0
        summary_data.append(f"- {account} | Stage: {stage} | EACV: ${eacv:,.0f} | Credits: {credits:,.0f} | Ratio: {ratio:.1f}% | Features: {features[:60]} | PSE: {engineer}")
    
    use_cases_text = "\n".join(summary_data)
    
    top_eacv = df.nlargest(5, 'EACV')[['ACCOUNT_NAME', 'EACV', 'DE_DL_CREDITS']].to_dict('records')
    top_eacv_text = "\n".join([f"- {r['ACCOUNT_NAME']}: ${r['EACV']:,.0f} EACV, {r['DE_DL_CREDITS']:,.0f} credits" for r in top_eacv])
    
    zero_credits = df[df['DE_DL_CREDITS'] == 0].nlargest(5, 'EACV')[['ACCOUNT_NAME', 'EACV', 'STAGE']].to_dict('records')
    zero_credits_text = "\n".join([f"- {r['ACCOUNT_NAME']}: ${r['EACV']:,.0f} EACV, {r['STAGE']}" for r in zero_credits]) if zero_credits else "None"
    
    avg_credits = df['DE_DL_CREDITS'].mean()
    total_eacv = df['EACV'].sum()
    total_credits = df['DE_DL_CREDITS'].sum()
    
    return f"""You are a Data Engineering/Data Lake sales strategist. Analyze this portfolio and provide EXECUTIVE-LEVEL insights with SPECIFIC ACTION ITEMS.

**PORTFOLIO OVERVIEW:**
- Total Use Cases: {len(df)} | Total EACV: ${total_eacv:,.0f} | Total Credits: {total_credits:,.0f}
- Unique Accounts: {df['ACCOUNT_NAME'].nunique()} | Engineers: {df['ENGINEER'].nunique()}
- Avg Credits/Account: {avg_credits:,.0f}

**TOP 5 BY EACV:**
{top_eacv_text}

**HIGH EACV WITH ZERO CREDITS (RISK):**
{zero_credits_text}

**SAMPLE USE CASES:**
{use_cases_text}
{'... and ' + str(len(df) - 25) + ' more' if len(df) > 25 else ''}

**REQUIRED OUTPUT FORMAT:**

**✅ What is Working:**
- Cite 3-4 SPECIFIC wins with account names, dollar amounts, and credit consumption
- Example format: "JP Morgan ($8M EACV, 1.8M credits) - strong Snowpipe Streaming adoption driving 22% credit/EACV ratio"
- Highlight which DE features are driving the most success
- Name top-performing PSEs and their impact

**⚠️ What Needs to be Improved:**
For EACH issue, provide this format:
1. **[Account Name]** - Problem: [specific issue] | Impact: [$ at risk] | Action: [specific next step with owner]

Example:
1. **Comcast TPX** - Problem: $5.5M EACV with 0 credits (Stage 4) | Impact: Risk of churn | Action: Schedule technical deep-dive with PSE to identify adoption blockers by [timeframe]

Include 4-5 specific accounts with clear action items.

Keep response under 350 words. Be direct and actionable."""

def build_usecase_ai_prompt(row):
    account = row.get('ACCOUNT_NAME', 'Unknown')
    use_case = row.get('USE_CASE_NAME', 'Unknown')
    stage = row.get('STAGE', 'Unknown')
    eacv = row.get('EACV', 0) or 0
    credits = row.get('DE_DL_CREDITS', 0) or 0
    features = row.get('KEY_FEATURES', '') or 'None'
    engineer = row.get('ENGINEER', 'Unknown')
    tech_use_case = row.get('TECHNICAL_USE_CASE', '') or 'Not specified'
    comments = row.get('SE_COMMENTS', '') or 'None'
    next_steps = row.get('NEXT_STEPS', '') or 'None'
    ratio = (credits / eacv * 100) if eacv > 0 else 0
    
    return f"""Analyze this specific DE/DL use case and provide actionable insights:

**USE CASE DETAILS:**
- Account: {account}
- Use Case: {use_case}
- Stage: {stage}
- EACV: ${eacv:,.0f}
- DE/DL Credits: {credits:,.0f}
- Credit/EACV Ratio: {ratio:.1f}%
- Key Features: {features}
- PSE: {engineer}
- Technical Use Case: {tech_use_case}
- SE Comments: {comments[:200] if comments else 'None'}
- Next Steps: {next_steps[:200] if next_steps else 'None'}

**PROVIDE:**
1. **Assessment** (2-3 sentences): Is this engagement healthy? Why?
2. **Risk Level**: Low/Medium/High with reason
3. **Recommended Actions** (3 bullet points): Specific next steps to drive success
4. **Feature Opportunities**: Which additional DE features could benefit this account?

Keep response under 200 words. Be specific and actionable."""

def get_previous_fiscal_quarter():
    today = datetime.now()
    month, year = today.month, today.year
    if month >= 2 and month <= 4:
        return datetime(year-1, 11, 1).date(), datetime(year, 1, 31).date()
    elif month >= 5 and month <= 7:
        return datetime(year, 2, 1).date(), datetime(year, 4, 30).date()
    elif month >= 8 and month <= 10:
        return datetime(year, 5, 1).date(), datetime(year, 7, 31).date()
    elif month >= 11:
        return datetime(year, 8, 1).date(), datetime(year, 10, 31).date()
    else:
        return datetime(year-1, 8, 1).date(), datetime(year-1, 10, 31).date()

def get_dedl_attribution_cte(emp_filter=""):
    return f"""
    DEDL_ATTRIBUTION AS (
        SELECT DISTINCT UCA.USE_CASE_ID, ORG.EMPLOYEE_NAME, ORG.SE_ID,
               COALESCE(NULLIF(ORG.FIRST_LINE_MANAGER, ''), 'Rithesh Makkena') AS FIRST_LINE_MANAGER
        FROM SALES.SE_REPORTING.USE_CASE_ATTRIBUTION AS UCA
        LEFT JOIN SALES.SE_REPORTING.SE_ORG_HIERARCHY_VW AS ORG ON UCA.USER_ID = ORG.SE_ID
        WHERE UCA.SE_GROUP = 'Partner SE'
          AND (
              ORG.FIRST_LINE_MANAGER IN ('Puneet Lakhanpal', 'Sam Mittal', 'David Hare', 'Brendan Tisseur', 'Zahir Gadiwan', 'Gopal Raghavan')
              OR ORG.SECOND_LINE_MANAGER IN ('Puneet Lakhanpal', 'Sam Mittal', 'David Hare', 'Brendan Tisseur', 'Zahir Gadiwan')
              OR ORG.THIRD_LINE_MANAGER = 'Rithesh Makkena'
          )
        {emp_filter}
    )"""

def build_use_case_query(start_date, end_date, manager_filter, engineer_filter, feature_area, key_features, gvp_filter, theater_filter, is_won=False):
    emp_filter = ""
    if engineer_filter != "All":
        emp_filter = f"AND ORG.EMPLOYEE_NAME = '{engineer_filter}'"
    elif manager_filter != "All":
        emp_filter = f"AND COALESCE(NULLIF(ORG.FIRST_LINE_MANAGER, ''), 'Rithesh Makkena') = '{manager_filter}'"
    
    feature_filter = ""
    if feature_area != "All":
        feature_filter = f"AND UC.TECHNICAL_USE_CASE LIKE '%{feature_area}%'"
    
    key_feat_filter = ""
    if key_features:
        conditions = [f"UC.PRIORITIZED_FEATURES ILIKE '%{kf}%'" for kf in key_features]
        key_feat_filter = "AND (" + " OR ".join(conditions) + ")"
    
    gvp_clause = ""
    if gvp_filter != "All":
        gvp_clause = f"AND UC.ACCOUNT_GVP = '{gvp_filter}'"
    
    theater_clause = ""
    if theater_filter:
        theaters = "', '".join(theater_filter)
        theater_clause = f"AND UC.THEATER_NAME IN ('{theaters}')"
    
    if is_won:
        date_clause = f"""
            AND (UC.USE_CASE_STAGE LIKE '4 -%' OR UC.USE_CASE_STAGE LIKE '5 -%' OR UC.USE_CASE_STAGE LIKE '6 -%' OR UC.USE_CASE_STAGE LIKE '7 -%')
            AND CASE WHEN UC.USE_CASE_STAGE LIKE '4 -%' THEN UC.DECISION_DATE ELSE UC.GO_LIVE_DATE END 
            BETWEEN '{start_date}' AND '{end_date}'
        """
    else:
        date_clause = f"""
            AND UC.USE_CASE_STAGE NOT LIKE '0 -%' AND UC.USE_CASE_STAGE NOT LIKE '8 -%'
            AND (
                UC.DECISION_DATE BETWEEN '{start_date}' AND '{end_date}' 
                OR UC.GO_LIVE_DATE BETWEEN '{start_date}' AND '{end_date}'
                OR (UC.USE_CASE_STAGE LIKE '1 -%' OR UC.USE_CASE_STAGE LIKE '2 -%' OR UC.USE_CASE_STAGE LIKE '3 -%')
            )
        """
    
    dedl_cte = get_dedl_attribution_cte(emp_filter)
    
    return f"""
    WITH {dedl_cte}
    SELECT 
        UC.USE_CASE_ID,
        D.EMPLOYEE_NAME AS ENGINEER,
        D.FIRST_LINE_MANAGER AS MANAGER,
        UC.ACCOUNT_NAME,
        UC.USE_CASE_NAME,
        UC.USE_CASE_STAGE AS STAGE,
        UC.USE_CASE_EACV AS EACV,
        UC.TECHNICAL_USE_CASE,
        UC.PRIORITIZED_FEATURES AS KEY_FEATURES,
        UC.THEATER_NAME AS THEATER,
        UC.ACCOUNT_GVP AS GVP,
        UC.DECISION_DATE,
        UC.GO_LIVE_DATE,
        UC.SE_COMMENTS,
        UC.NEXT_STEPS
    FROM DEDL_ATTRIBUTION D
    INNER JOIN MDM.MDM_INTERFACES.DIM_USE_CASE UC ON D.USE_CASE_ID = UC.USE_CASE_ID
    WHERE 1=1
    {date_clause}
    {feature_filter}
    {key_feat_filter}
    {gvp_clause}
    {theater_clause}
    ORDER BY UC.USE_CASE_EACV DESC NULLS LAST
    """

def display_metrics(df):
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Engagements", len(df))
    with col2:
        total_eacv = df['EACV'].sum() if not df.empty else 0
        st.metric("Total EACV", f"${total_eacv/1_000_000:.1f}M")
    with col3:
        accounts = df['ACCOUNT_NAME'].nunique() if not df.empty else 0
        st.metric("Unique Accounts", accounts)
    with col4:
        engineers = df['ENGINEER'].nunique() if not df.empty else 0
        st.metric("Engineers", engineers)

def display_stage_chart(df, title):
    if not df.empty:
        stage_data = df.groupby('STAGE')['USE_CASE_ID'].nunique().reset_index()
        stage_data.columns = ['STAGE', 'USE_CASES']
        stage_data = stage_data.sort_values('STAGE')
        fig = px.bar(stage_data, x='STAGE', y='USE_CASES', title=title)
        fig.update_layout(height=300)
        fig.update_yaxes(tickformat=",.0f")
        st.plotly_chart(fig, use_container_width=True)

def create_aggrid(df, display_cols, key):
    df_display = df[display_cols].copy()
    
    gb = GridOptionsBuilder.from_dataframe(df_display)
    gb.configure_selection('multiple', use_checkbox=True, groupSelectsChildren=True)
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=50)
    gb.configure_default_column(filterable=True, sortable=True, resizable=True)
    gb.configure_column("EACV", type=["numericColumn"], valueFormatter="'$' + value.toLocaleString()")
    gb.configure_column("DE_DL_CREDITS", type=["numericColumn"], valueFormatter="value.toLocaleString()")
    gb.configure_column("KEY_FEATURES", width=200)
    gb.configure_column("ACCOUNT_NAME", width=180)
    gb.configure_grid_options(domLayout='normal')
    grid_options = gb.build()
    
    grid_response = AgGrid(
        df_display,
        gridOptions=grid_options,
        height=400,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
        fit_columns_on_grid_load=False,
        theme='streamlit',
        key=key
    )
    
    return grid_response

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_use_cases(start_date, end_date, manager_filter, engineer_filter, feature_area, key_features, gvp_filter, theater_filter, is_won):
    query = build_use_case_query(start_date, end_date, manager_filter, engineer_filter, feature_area, key_features, gvp_filter, theater_filter, is_won)
    return run_query(query)

@st.cache_data(ttl=600, show_spinner=False)
def fetch_de_consumption(account_names, start_date, end_date):
    if not account_names:
        return {}
    accounts_list = "', '".join([a.replace("'", "''") for a in account_names])
    query = f"""
    WITH date_range AS (
        SELECT 
            GREATEST('{start_date}'::DATE - INTERVAL '1 YEAR', (SELECT MIN(ds) FROM SNOWSCIENCE.JOB_ANALYTICS.WORKLOAD_ACCOUNT_SUB_CREDITS_2024_05_01)) as adj_start,
            LEAST('{end_date}'::DATE, (SELECT MAX(ds) FROM SNOWSCIENCE.JOB_ANALYTICS.WORKLOAD_ACCOUNT_SUB_CREDITS_2024_05_01)) as adj_end
    )
    SELECT 
        a.salesforce_account_name as account_name,
        ROUND(SUM(w.credits), 0) as total_credits
    FROM SNOWSCIENCE.JOB_ANALYTICS.WORKLOAD_ACCOUNT_SUB_CREDITS_2024_05_01 w
    JOIN snowscience.dimensions.dim_accounts_history a
        ON w.deployment = a.snowflake_deployment 
        AND w.account_id = a.snowflake_account_id 
        AND w.ds = a.general_date
    CROSS JOIN date_range dr
    WHERE w.ds BETWEEN dr.adj_start AND dr.adj_end
      AND w.primary_class IN ('data_engineering', 'data_lake')
      AND a.salesforce_account_name IN ('{accounts_list}')
    GROUP BY a.salesforce_account_name
    """
    try:
        df = run_query(query)
        return dict(zip(df['ACCOUNT_NAME'], df['TOTAL_CREDITS']))
    except:
        return {}

st.title("DE/DL Team Engagements Dashboard")
st.info("Click 'Generate AI Summary' button to see portfolio-level insights for all use cases")

prev_q_start, prev_q_end = get_previous_fiscal_quarter()

st.sidebar.title("Filters")
start_date = st.sidebar.date_input("Start Date", prev_q_start)
end_date = st.sidebar.date_input("End Date", prev_q_end)

manager_options = ["All", "Rithesh Makkena", "Puneet Lakhanpal", "David Hare", "Brendan Tisseur", "Zahir Gadiwan", "Gopal Raghavan"]
manager_filter = st.sidebar.selectbox("Manager", manager_options)

if manager_filter != "All":
    engineer_options = ["All"] + ENGINEER_LIST.get(manager_filter, [])
else:
    all_engineers = sorted(set([e for engineers in ENGINEER_LIST.values() for e in engineers]))
    engineer_options = ["All"] + all_engineers
engineer_filter = st.sidebar.selectbox("Engineer", engineer_options)

feature_area_options = ["All", "DE: Ingestion", "DE: Transformation", "DE: Interoperable Storage"]
feature_area = st.sidebar.selectbox("Feature Area", feature_area_options)

key_features_options = ["DE - Openflow", "DE - Iceberg", "DE - Snowpark DE", "DE - Dynamic Tables", "DE - Snowpipe Streaming", "DE - Snowpipe", "DE - Serverless Task", "DE - Connectors", "DE - dbt Projects", "DE - SAP Integration", "DE - Basic"]
key_features = st.sidebar.multiselect("Key Features", key_features_options)

gvp_options = ["All", "Jennifer Chronis", "Jon Robertson", "Jonathan Beaulier", "Keegan Riley", "Mark Fleming", "Stuart Nyemecz"]
gvp_filter = st.sidebar.selectbox("GVP", gvp_options)

theater_options = ["AMSAcquisition", "AMSExpansion", "USMajors", "USPubSec"]
theater_filter = st.sidebar.multiselect("Theater", theater_options, default=theater_options)

tab1, tab2, tab3 = st.tabs(["AFE Involved Won+Engagement", "AFE All Engagements", "Overall DE/DL Summary"])

display_cols = ['ACCOUNT_NAME', 'DE_DL_CREDITS', 'EACV', 'STAGE', 'USE_CASE_NAME', 'ENGINEER', 'MANAGER', 'KEY_FEATURES', 'THEATER', 'GVP']

with tab1:
    st.subheader("Won+ Engagements (Stages 4-7)")
    
    with st.spinner('Loading won+ engagements...'):
        df_won = fetch_use_cases(str(start_date), str(end_date), manager_filter, engineer_filter, feature_area, tuple(key_features) if key_features else (), gvp_filter, tuple(theater_filter) if theater_filter else (), is_won=True)
    
    display_metrics(df_won)
    display_stage_chart(df_won, "Won+ Use Cases by Stage")
    
    st.markdown("---")
    
    if not df_won.empty:
        consumption_data = fetch_de_consumption(tuple(df_won['ACCOUNT_NAME'].unique().tolist()), str(start_date), str(end_date))
        df_won['DE_DL_CREDITS'] = df_won['ACCOUNT_NAME'].map(consumption_data).fillna(0).astype(int)
    else:
        df_won['DE_DL_CREDITS'] = 0
    
    if not df_won.empty:
        st.caption("Use checkboxes to select rows for AI analysis. Use column headers to filter/sort.")
        grid_response = create_aggrid(df_won, display_cols, "won_grid")
        
        selected_rows = pd.DataFrame(grid_response['selected_rows']) if grid_response['selected_rows'] is not None else pd.DataFrame()
        st.caption(f"Showing {len(df_won)} won+ engagements | {len(selected_rows)} selected")
    else:
        st.dataframe(df_won, use_container_width=True, height=400)
        st.caption(f"Showing {len(df_won)} won+ engagements")
        selected_rows = pd.DataFrame()
    
    if not df_won.empty:
        st.markdown("### 🤖 AI Summary")
        
        col_ai1, col_ai2 = st.columns([1, 1])
        with col_ai1:
            if st.button("📊 Generate AI Summary for All Won+ Use Cases", key="won_bulk_btn"):
                st.session_state['won_bulk_analysis'] = True
                st.session_state['won_usecase_analysis'] = False
        with col_ai2:
            if len(selected_rows) > 0 and len(selected_rows) <= 5:
                if st.button(f"🔎 AI Analysis for {len(selected_rows)} Selected", key="won_usecase_btn"):
                    st.session_state['won_usecase_analysis'] = True
                    st.session_state['won_bulk_analysis'] = False
            elif len(selected_rows) > 5:
                st.warning("Select max 5 use cases")
            else:
                st.info("💡 Select accounts above for individual analysis, or use bulk summary for all")
        
        if st.session_state.get('won_bulk_analysis'):
            with st.container():
                prompt = build_bulk_ai_prompt(df_won)
                response_placeholder = st.empty()
                try:
                    conn = get_connection()
                    escaped_prompt = prompt.replace("'", "''")
                    query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{escaped_prompt}') as summary"
                    if hasattr(conn, 'sql'):
                        result = conn.sql(query).to_pandas()['SUMMARY'].iloc[0]
                    else:
                        cur = conn.cursor()
                        cur.execute(query)
                        result = cur.fetchone()[0]
                        cur.close()
                    response_placeholder.markdown(result)
                except Exception as e:
                    response_placeholder.error(f"Could not generate AI summary: {str(e)}")
                if st.button("Clear Summary", key="won_clear"):
                    st.session_state['won_bulk_analysis'] = False
                    st.rerun()
        
        if st.session_state.get('won_usecase_analysis') and len(selected_rows) > 0:
            st.markdown("#### 🔍 Selected Use Case Analysis")
            for idx, row in selected_rows.iterrows():
                with st.expander(f"📋 {row['ACCOUNT_NAME']} - {row['USE_CASE_NAME']} (${row['EACV']:,.0f})", expanded=True):
                    prompt = build_usecase_ai_prompt(row)
                    try:
                        conn = get_connection()
                        escaped_prompt = prompt.replace("'", "''")
                        query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{escaped_prompt}') as summary"
                        if hasattr(conn, 'sql'):
                            result = conn.sql(query).to_pandas()['SUMMARY'].iloc[0]
                        else:
                            cur = conn.cursor()
                            cur.execute(query)
                            result = cur.fetchone()[0]
                            cur.close()
                        st.markdown(result)
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            if st.button("Clear Use Case Analysis", key="won_usecase_clear"):
                st.session_state['won_usecase_analysis'] = False
                st.rerun()

with tab2:
    st.subheader("All Engagements")
    
    with st.spinner('Loading all engagements...'):
        df_all = fetch_use_cases(str(start_date), str(end_date), manager_filter, engineer_filter, feature_area, tuple(key_features) if key_features else (), gvp_filter, tuple(theater_filter) if theater_filter else (), is_won=False)
    
    display_metrics(df_all)
    display_stage_chart(df_all, "All Use Cases by Stage")
    
    st.markdown("---")
    
    if not df_all.empty:
        consumption_data_all = fetch_de_consumption(tuple(df_all['ACCOUNT_NAME'].unique().tolist()), str(start_date), str(end_date))
        df_all['DE_DL_CREDITS'] = df_all['ACCOUNT_NAME'].map(consumption_data_all).fillna(0).astype(int)
    else:
        df_all['DE_DL_CREDITS'] = 0
    
    if not df_all.empty:
        st.caption("Use checkboxes to select rows for AI analysis. Use column headers to filter/sort.")
        grid_response_all = create_aggrid(df_all, display_cols, "all_grid")
        
        selected_rows_all = pd.DataFrame(grid_response_all['selected_rows']) if grid_response_all['selected_rows'] is not None else pd.DataFrame()
        st.caption(f"Showing {len(df_all)} engagements | {len(selected_rows_all)} selected")
    else:
        st.dataframe(df_all, use_container_width=True, height=400)
        st.caption(f"Showing {len(df_all)} engagements")
        selected_rows_all = pd.DataFrame()
    
    if not df_all.empty:
        st.markdown("### 🤖 AI Summary")
        
        col_ai1, col_ai2 = st.columns([1, 1])
        with col_ai1:
            if st.button("📊 Generate AI Summary for All Engagements", key="all_bulk_btn"):
                st.session_state['all_bulk_analysis'] = True
                st.session_state['all_usecase_analysis'] = False
        with col_ai2:
            if len(selected_rows_all) > 0 and len(selected_rows_all) <= 5:
                if st.button(f"🔎 AI Analysis for {len(selected_rows_all)} Selected", key="all_usecase_btn"):
                    st.session_state['all_usecase_analysis'] = True
                    st.session_state['all_bulk_analysis'] = False
            elif len(selected_rows_all) > 5:
                st.warning("Select max 5 use cases")
            else:
                st.info("💡 Select accounts above for individual analysis, or use bulk summary for all")
        
        if st.session_state.get('all_bulk_analysis'):
            with st.container():
                prompt = build_bulk_ai_prompt(df_all)
                response_placeholder = st.empty()
                try:
                    conn = get_connection()
                    escaped_prompt = prompt.replace("'", "''")
                    query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{escaped_prompt}') as summary"
                    if hasattr(conn, 'sql'):
                        result = conn.sql(query).to_pandas()['SUMMARY'].iloc[0]
                    else:
                        cur = conn.cursor()
                        cur.execute(query)
                        result = cur.fetchone()[0]
                        cur.close()
                    response_placeholder.markdown(result)
                except Exception as e:
                    response_placeholder.error(f"Could not generate AI summary: {str(e)}")
                if st.button("Clear Summary", key="all_clear"):
                    st.session_state['all_bulk_analysis'] = False
                    st.rerun()
        
        if st.session_state.get('all_usecase_analysis') and len(selected_rows_all) > 0:
            st.markdown("#### 🔍 Selected Use Case Analysis")
            for idx, row in selected_rows_all.iterrows():
                with st.expander(f"📋 {row['ACCOUNT_NAME']} - {row['USE_CASE_NAME']} (${row['EACV']:,.0f})", expanded=True):
                    prompt = build_usecase_ai_prompt(row)
                    try:
                        conn = get_connection()
                        escaped_prompt = prompt.replace("'", "''")
                        query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{escaped_prompt}') as summary"
                        if hasattr(conn, 'sql'):
                            result = conn.sql(query).to_pandas()['SUMMARY'].iloc[0]
                        else:
                            cur = conn.cursor()
                            cur.execute(query)
                            result = cur.fetchone()[0]
                            cur.close()
                        st.markdown(result)
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
            if st.button("Clear Use Case Analysis", key="all_usecase_clear"):
                st.session_state['all_usecase_analysis'] = False
                st.rerun()

with tab3:
    st.subheader("DE/DL Use Cases Summary")
    
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        summary_start = st.date_input("Summary Start Date", prev_q_start, key="summary_start")
    with col_f2:
        summary_end = st.date_input("Summary End Date", prev_q_end, key="summary_end")
    
    summary_features = st.multiselect("Priority DE Features", key_features_options, key="summary_features")
    
    @st.cache_data(ttl=1800, show_spinner=False)
    def fetch_summary_use_cases(start_date, end_date, key_features_tuple):
        key_feat_filter = ""
        if key_features_tuple:
            conditions = [f"UC.PRIORITIZED_FEATURES ILIKE '%{kf}%'" for kf in key_features_tuple]
            key_feat_filter = "AND (" + " OR ".join(conditions) + ")"
        
        dedl_cte = get_dedl_attribution_cte("")
        
        query = f"""
        WITH {dedl_cte}
        SELECT 
            UC.USE_CASE_ID,
            D.EMPLOYEE_NAME AS ENGINEER,
            D.FIRST_LINE_MANAGER AS MANAGER,
            UC.ACCOUNT_NAME,
            UC.USE_CASE_NAME,
            UC.USE_CASE_STAGE AS STAGE,
            UC.USE_CASE_EACV AS EACV,
            UC.TECHNICAL_USE_CASE,
            UC.PRIORITIZED_FEATURES AS KEY_FEATURES,
            UC.THEATER_NAME AS THEATER,
            UC.ACCOUNT_GVP AS GVP,
            UC.SE_COMMENTS,
            UC.NEXT_STEPS,
            CASE 
                WHEN UC.USE_CASE_STAGE LIKE '1 -%' OR UC.USE_CASE_STAGE LIKE '2 -%' OR UC.USE_CASE_STAGE LIKE '3 -%' THEN 'Stage 1-3'
                WHEN UC.USE_CASE_STAGE LIKE '4 -%' OR UC.USE_CASE_STAGE LIKE '5 -%' THEN 'Stage 4-5'
                WHEN UC.USE_CASE_STAGE LIKE '6 -%' OR UC.USE_CASE_STAGE LIKE '7 -%' THEN 'Stage 6-7'
                ELSE 'Other'
            END AS STAGE_BUCKET
        FROM DEDL_ATTRIBUTION D
        INNER JOIN MDM.MDM_INTERFACES.DIM_USE_CASE UC ON D.USE_CASE_ID = UC.USE_CASE_ID
        WHERE UC.USE_CASE_STAGE NOT LIKE '0 -%' AND UC.USE_CASE_STAGE NOT LIKE '8 -%'
          AND (
              UC.DECISION_DATE BETWEEN '{start_date}' AND '{end_date}' 
              OR UC.GO_LIVE_DATE BETWEEN '{start_date}' AND '{end_date}'
              OR (UC.USE_CASE_STAGE LIKE '1 -%' OR UC.USE_CASE_STAGE LIKE '2 -%' OR UC.USE_CASE_STAGE LIKE '3 -%')
          )
        {key_feat_filter}
        ORDER BY UC.USE_CASE_EACV DESC NULLS LAST
        """
        return run_query(query)
    
    with st.spinner('Loading summary...'):
        df_summary = fetch_summary_use_cases(str(summary_start), str(summary_end), tuple(summary_features) if summary_features else ())
    
    if not df_summary.empty:
        consumption_summary = fetch_de_consumption(tuple(df_summary['ACCOUNT_NAME'].unique().tolist()), str(summary_start), str(summary_end))
        df_summary['DE_DL_CREDITS'] = df_summary['ACCOUNT_NAME'].map(consumption_summary).fillna(0).astype(int)
    else:
        df_summary['DE_DL_CREDITS'] = 0
    
    st.markdown("### Summary by Stage Bucket")
    
    if not df_summary.empty:
        bucket_order = ['Stage 1-3', 'Stage 4-5', 'Stage 6-7']
        summary_agg = df_summary.groupby('STAGE_BUCKET').agg({
            'USE_CASE_ID': 'count',
            'EACV': 'sum',
            'DE_DL_CREDITS': 'sum',
            'ACCOUNT_NAME': 'nunique'
        }).reset_index()
        summary_agg.columns = ['Stage Bucket', 'Use Cases', 'Total EACV', 'Total DE/DL Credits', 'Unique Accounts']
        summary_agg['Stage Bucket'] = pd.Categorical(summary_agg['Stage Bucket'], categories=bucket_order, ordered=True)
        summary_agg = summary_agg.sort_values('Stage Bucket')
        summary_agg = summary_agg[summary_agg['Stage Bucket'].isin(bucket_order)]
        
        col1, col2, col3 = st.columns(3)
        for idx, bucket in enumerate(bucket_order):
            row = summary_agg[summary_agg['Stage Bucket'] == bucket]
            with [col1, col2, col3][idx]:
                st.markdown(f"**{bucket}**")
                if not row.empty:
                    st.metric("Use Cases", int(row['Use Cases'].values[0]))
                    st.metric("EACV", f"${row['Total EACV'].values[0]/1_000_000:.1f}M")
                    st.metric("DE/DL Credits", f"{row['Total DE/DL Credits'].values[0]:,.0f}")
                    st.metric("Accounts", int(row['Unique Accounts'].values[0]))
                else:
                    st.metric("Use Cases", 0)
                    st.metric("EACV", "$0M")
                    st.metric("DE/DL Credits", "0")
                    st.metric("Accounts", 0)
        
        st.markdown("---")
        
        fig = px.bar(summary_agg, x='Stage Bucket', y='Total EACV', title='EACV by Stage Bucket', text_auto='.2s')
        fig.update_layout(height=300)
        fig.update_yaxes(tickformat="$,.0f")
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### All DE/DL Use Cases")
        summary_display_cols = ['STAGE_BUCKET', 'ACCOUNT_NAME', 'DE_DL_CREDITS', 'EACV', 'STAGE', 'USE_CASE_NAME', 'KEY_FEATURES', 'ENGINEER']
        
        st.dataframe(
            df_summary[summary_display_cols],
            use_container_width=True,
            height=400,
            column_config={
                "STAGE_BUCKET": st.column_config.TextColumn("Stage Bucket"),
                "EACV": st.column_config.NumberColumn("EACV", format="$%.0f"),
                "DE_DL_CREDITS": st.column_config.NumberColumn("DE/DL Credits", format="%.0f"),
            }
        )
        st.caption(f"Showing {len(df_summary)} DE/DL use cases")
        
        st.markdown("### 🤖 AI Summary")
        if st.button("📊 Generate AI Summary for All DE/DL Use Cases", key="summary_bulk_btn"):
            st.session_state['summary_bulk_analysis'] = True
        
        if st.session_state.get('summary_bulk_analysis'):
            with st.container():
                prompt = build_bulk_ai_prompt(df_summary)
                response_placeholder = st.empty()
                try:
                    conn = get_connection()
                    escaped_prompt = prompt.replace("'", "''")
                    query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{escaped_prompt}') as summary"
                    if hasattr(conn, 'sql'):
                        result = conn.sql(query).to_pandas()['SUMMARY'].iloc[0]
                    else:
                        cur = conn.cursor()
                        cur.execute(query)
                        result = cur.fetchone()[0]
                        cur.close()
                    response_placeholder.markdown(result)
                except Exception as e:
                    response_placeholder.error(f"Could not generate AI summary: {str(e)}")
                if st.button("Clear Summary", key="summary_clear"):
                    st.session_state['summary_bulk_analysis'] = False
                    st.rerun()
    else:
        st.info("No use cases found with the selected filters.")
