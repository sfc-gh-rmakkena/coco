import streamlit as st
from datetime import datetime, timedelta
import plotly.express as px
import pandas as pd
import os
import snowflake.connector
import urllib.parse

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
        session = get_active_session()
        session.sql("USE WAREHOUSE PSE_WH").collect()
        return session
    except:
        conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "snowhouse")
        conn.cursor().execute("USE WAREHOUSE PSE_WH")
        return conn

@st.cache_data(ttl=600, show_spinner=False)
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

@st.cache_data(ttl=600, show_spinner=False)
def query_customer_consumption(search_term):
    query = f"""
    SELECT LATEST_SALESFORCE_ACCOUNT_NAME, FEATURE, USE_CASE, AE, SEGMENT, REGION, DEPLOYMENT,
           SUM(CREDITS) as TOTAL_CREDITS, COUNT(DISTINCT DS) as ACTIVE_DAYS,
           MIN(DS) as FIRST_SEEN, MAX(DS) as LAST_SEEN
    FROM FINANCE.CUSTOMER.FY26_PRODUCT_CATEGORY_REVENUE
    WHERE PRODUCT_CATEGORY = 'Data Engineering'
      AND DS >= DATEADD('day', -60, CURRENT_DATE())
      AND (UPPER(LATEST_SALESFORCE_ACCOUNT_NAME) LIKE UPPER('%{search_term}%')
           OR UPPER(ORIGINAL_SALESFORCE_ACCOUNT_NAME) LIKE UPPER('%{search_term}%')
           OR UPPER(USE_CASE) LIKE UPPER('%{search_term}%'))
    GROUP BY LATEST_SALESFORCE_ACCOUNT_NAME, FEATURE, USE_CASE, AE, SEGMENT, REGION, DEPLOYMENT
    ORDER BY TOTAL_CREDITS DESC
    LIMIT 100
    """
    return run_query(query)

@st.cache_data(ttl=600, show_spinner=False)
def get_customer_list():
    query = """
    SELECT DISTINCT LATEST_SALESFORCE_ACCOUNT_NAME
    FROM FINANCE.CUSTOMER.FY26_PRODUCT_CATEGORY_REVENUE
    WHERE PRODUCT_CATEGORY = 'Data Engineering'
      AND DS >= DATEADD('day', -60, CURRENT_DATE())
      AND LATEST_SALESFORCE_ACCOUNT_NAME IS NOT NULL
    ORDER BY LATEST_SALESFORCE_ACCOUNT_NAME
    """
    return run_query(query)['LATEST_SALESFORCE_ACCOUNT_NAME'].tolist()

def name_to_email(name):
    if not name or name == 'Unknown' or pd.isna(name):
        return None
    name = name.strip()
    parts = name.lower().split()
    if len(parts) >= 2:
        return f"{parts[0]}.{parts[-1]}@snowflake.com"
    elif len(parts) == 1:
        return f"{parts[0]}@snowflake.com"
    return None

def get_recipient_emails(df):
    names = set()
    if 'ACCOUNT_OWNER' in df.columns:
        names.update(df['ACCOUNT_OWNER'].dropna().unique())
    if 'ACCOUNT_SE' in df.columns:
        names.update(df['ACCOUNT_SE'].dropna().unique())
    emails = sorted(set(filter(None, [name_to_email(n) for n in names])))
    return ",".join(emails)

def build_gmail_url(subject, to=""):
    p = {"view": "cm", "fs": "1", "su": subject}
    if to:
        p["to"] = to
    params = urllib.parse.urlencode(p)
    return f"https://mail.google.com/mail/?{params}"

def render_email_section(df, subject, ai_summary=None, key_prefix=""):
    body = build_email_content(df, subject, ai_summary)
    to = get_recipient_emails(df)
    gmail_url = build_gmail_url(subject, to)
    label = f"📧 Email ({len(df)} use cases)"
    with st.expander(label, expanded=False):
        st.markdown(f'<a href="{gmail_url}" target="_blank" rel="noopener noreferrer" style="display:inline-block;padding:0.4rem 1rem;background-color:#ff4b4b;color:white;border-radius:0.5rem;text-decoration:none;font-weight:600;">Compose Email in Gmail</a>', unsafe_allow_html=True)
        if to:
            st.caption("**To (copy & paste into recipients):**")
            st.code(to, language=None)
        st.caption("**Email body (copy & paste):**")
        st.code(body, language=None)

def build_email_content(df, title="DE/DL Use Cases", ai_summary=None):
    body_lines = [f"{title}", f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", ""]
    if ai_summary:
        body_lines.append("=== AI Summary ===")
        body_lines.append(ai_summary)
        body_lines.append("")
        body_lines.append("=== Use Case Details ===")
    body_lines.append(f"Total Use Cases: {len(df)}")
    total_eacv = df['EACV'].sum() if not df.empty else 0
    body_lines.append(f"Total EACV: ${total_eacv:,.0f}")
    body_lines.append("")
    for _, row in df.head(20).iterrows():
        body_lines.append(f"Account: {row.get('ACCOUNT_NAME', 'Unknown')}")
        body_lines.append(f"  Use Case: {row.get('USE_CASE_NAME', 'Unknown')}")
        body_lines.append(f"  Stage: {row.get('STAGE', 'Unknown')} | EACV: ${(row.get('EACV', 0) or 0):,.0f}")
        body_lines.append("")
    return "\n".join(body_lines)

def build_bulk_ai_prompt(df):
    summary_data = []
    for _, row in df.head(25).iterrows():
        account = row.get('ACCOUNT_NAME', 'Unknown')
        use_case = row.get('USE_CASE_NAME', 'Unknown')
        stage = row.get('STAGE', 'Unknown')
        eacv = row.get('EACV', 0) or 0
        features = row.get('KEY_FEATURES', '') or 'None'
        engineer = row.get('ENGINEER', 'Unknown')
        summary_data.append(f"- {account} | Stage: {stage} | EACV: ${eacv:,.0f} | Features: {features[:60]} | PSE: {engineer}")
    
    use_cases_text = "\n".join(summary_data)
    
    top_eacv = df.nlargest(5, 'EACV')[['ACCOUNT_NAME', 'EACV']].to_dict('records')
    top_eacv_text = "\n".join([f"- {r['ACCOUNT_NAME']}: ${r['EACV']:,.0f} EACV" for r in top_eacv])
    
    total_eacv = df['EACV'].sum()
    
    return f"""You are a Data Engineering/Data Lake sales strategist. Analyze this portfolio and provide EXECUTIVE-LEVEL insights with SPECIFIC ACTION ITEMS.

**PORTFOLIO OVERVIEW:**
- Total Use Cases: {len(df)} | Total EACV: ${total_eacv:,.0f}
- Unique Accounts: {df['ACCOUNT_NAME'].nunique()} | Engineers: {df['ENGINEER'].nunique()}

**TOP 5 BY EACV:**
{top_eacv_text}

**SAMPLE USE CASES:**
{use_cases_text}
{'... and ' + str(len(df) - 25) + ' more' if len(df) > 25 else ''}

**REQUIRED OUTPUT FORMAT (use these EXACT section headers):**

**What is Working:**
- Cite 3-4 SPECIFIC wins with account names, dollar amounts
- Example format: "JP Morgan ($8M EACV) - strong Snowpipe Streaming adoption"
- Highlight which DE features are driving the most success
- Name top-performing PSEs and their impact

**Issues / Problems:**
For EACH issue, provide this format:
1. **[Account Name]** - Problem: [specific issue] | Impact: [$ at risk] | Action: [specific next step with owner]

Example:
1. **Comcast TPX** - Problem: $5.5M EACV with 0 credits (Stage 4) | Impact: Risk of churn | Action: Schedule technical deep-dive with PSE to identify adoption blockers by [timeframe]

Include 4-5 specific accounts with clear action items.

**Action Items:**
- List 5 specific next action items with owner and timeline
- Prioritize by EACV impact

Keep response under 350 words. Be direct and actionable."""

def build_usecase_ai_prompt(row):
    account = row.get('ACCOUNT_NAME', 'Unknown')
    use_case = row.get('USE_CASE_NAME', 'Unknown')
    stage = row.get('STAGE', 'Unknown')
    eacv = row.get('EACV', 0) or 0
    features = row.get('KEY_FEATURES', '') or 'None'
    engineer = row.get('ENGINEER', 'Unknown')
    tech_use_case = row.get('TECHNICAL_USE_CASE', '') or 'Not specified'
    comments = row.get('SE_COMMENTS', '') or 'None'
    next_steps = row.get('NEXT_STEPS', '') or 'None'
    
    return f"""Analyze this specific DE/DL use case and provide actionable insights:

**USE CASE DETAILS:**
- Account: {account}
- Use Case: {use_case}
- Stage: {stage}
- EACV: ${eacv:,.0f}
- Key Features: {features}
- PSE: {engineer}
- Technical Use Case: {tech_use_case}
- SE Comments: {comments[:200] if comments else 'None'}
- Next Steps: {next_steps[:200] if next_steps else 'None'}

**REQUIRED OUTPUT FORMAT (use these EXACT section headers):**

**What is Working:**
- What is going well with this engagement (adoption, feature usage, stage progress)
- Be specific with numbers and features

**Issues / Problems:**
- What risks or problems exist (stalled stage, missing features, adoption blockers)
- Include risk level: Low/Medium/High with reason

**Action Items:**
- 3 specific next steps to drive success with owner and timeline
- Include any additional DE features that could benefit this account

Keep response under 200 words. Be specific and actionable."""

def build_weekly_usecase_ai_prompt(row):
    account = row.get('ACCOUNT_NAME', 'Unknown')
    use_case = row.get('USE_CASE_NAME', 'Unknown')
    stage = row.get('STAGE', 'Unknown')
    eacv = row.get('EACV', 0) or 0
    features = row.get('KEY_FEATURES', '') or 'None'
    tech_use_case = row.get('TECHNICAL_USE_CASE', '') or 'Not specified'
    se_comments = row.get('SE_COMMENTS', '') or 'None'
    impl_comments = row.get('IMPLEMENTATION_COMMENTS', '') or 'None'
    next_steps = row.get('NEXT_STEPS', '') or 'None'
    specialist = row.get('SPECIALIST_COMMENTS', '') or 'None'
    partner = row.get('PARTNER_COMMENTS', '') or 'None'
    gvp = row.get('GVP', '') or 'Unknown'
    theater = row.get('THEATER', '') or 'Unknown'

    return f"""Analyze this DE/DL use case and provide actionable insights:

**USE CASE DETAILS:**
- Account: {account}
- Use Case: {use_case}
- Stage: {stage}
- EACV: ${eacv:,.0f}
- Key Features: {features}
- Technical Use Case: {tech_use_case}
- GVP: {gvp} | Theater: {theater}
- SE Comments: {se_comments[:300] if se_comments else 'None'}
- Implementation Comments: {impl_comments[:300] if impl_comments else 'None'}
- Next Steps: {next_steps[:300] if next_steps else 'None'}
- Specialist Comments: {specialist[:200] if specialist else 'None'}
- Partner Comments: {partner[:200] if partner else 'None'}

**REQUIRED OUTPUT FORMAT (use these EXACT section headers):**

**What is Working:**
- What is going well with this engagement (adoption, feature usage, stage progress)
- Be specific with numbers and features

**Issues / Problems:**
- What risks or problems exist (stalled stage, missing features, adoption blockers)
- Include risk level: Low/Medium/High with reason

**Action Items:**
- 3 specific next steps to drive success with owner and timeline
- Include any additional DE features that could benefit this account

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
          AND (ORG.FIRST_LINE_MANAGER IN ('Puneet Lakhanpal', 'Sam Mittal', 'David Hare', 'Brendan Tisseur', 'Zahir Gadiwan', 'Gopal Raghavan')
              OR ORG.SECOND_LINE_MANAGER IN ('Puneet Lakhanpal', 'Sam Mittal', 'David Hare', 'Brendan Tisseur', 'Zahir Gadiwan')
              OR ORG.THIRD_LINE_MANAGER = 'Rithesh Makkena')
        {emp_filter}
    )"""

def build_afe_query(start_date, end_date, emp_filter, feature_filter, key_feat_filter, gvp_clause, theater_clause, stage_filter_clause="", won_only=False):
    dedl_cte = get_dedl_attribution_cte(emp_filter)
    if stage_filter_clause:
        afe_stage_clause = stage_filter_clause
    elif won_only:
        afe_stage_clause = "AND (UC.USE_CASE_STAGE LIKE '4 -%' OR UC.USE_CASE_STAGE LIKE '5 -%' OR UC.USE_CASE_STAGE LIKE '6 -%' OR UC.USE_CASE_STAGE LIKE '7 -%')"
    else:
        afe_stage_clause = "AND UC.USE_CASE_STAGE NOT LIKE '0 -%' AND UC.USE_CASE_STAGE NOT LIKE '8 -%'"
    return f"""
    WITH {dedl_cte}
    SELECT UC.USE_CASE_ID, LISTAGG(DISTINCT D.EMPLOYEE_NAME, ', ') WITHIN GROUP (ORDER BY D.EMPLOYEE_NAME) AS ENGINEER,
        MAX(D.FIRST_LINE_MANAGER) AS MANAGER, UC.ACCOUNT_NAME, UC.ACCOUNT_OWNER_NAME AS ACCOUNT_OWNER, UC.ACCOUNT_LEAD_SE_NAME AS ACCOUNT_SE, UC.USE_CASE_NAME,
        UC.USE_CASE_STAGE AS STAGE, UC.USE_CASE_EACV AS EACV, UC.TECHNICAL_USE_CASE, UC.PRIORITIZED_FEATURES AS KEY_FEATURES,
        UC.THEATER_NAME AS THEATER, UC.ACCOUNT_GVP AS GVP, UC.DECISION_DATE, UC.GO_LIVE_DATE, UC.SE_COMMENTS, UC.NEXT_STEPS, UC.LAST_MODIFIED_DATE
    FROM DEDL_ATTRIBUTION D
    INNER JOIN MDM.MDM_INTERFACES.DIM_USE_CASE UC ON D.USE_CASE_ID = UC.USE_CASE_ID
    WHERE 1=1 {afe_stage_clause}
      AND (UC.DECISION_DATE BETWEEN '{start_date}' AND '{end_date}' OR UC.GO_LIVE_DATE BETWEEN '{start_date}' AND '{end_date}')
      {feature_filter} {key_feat_filter} {gvp_clause} {theater_clause} {modified_clause} {account_name_clause}
    GROUP BY UC.USE_CASE_ID, UC.ACCOUNT_NAME, UC.ACCOUNT_OWNER_NAME, UC.ACCOUNT_LEAD_SE_NAME, UC.USE_CASE_NAME, UC.USE_CASE_STAGE, UC.USE_CASE_EACV,
             UC.TECHNICAL_USE_CASE, UC.PRIORITIZED_FEATURES, UC.THEATER_NAME, UC.ACCOUNT_GVP, UC.DECISION_DATE, UC.GO_LIVE_DATE, UC.SE_COMMENTS, UC.NEXT_STEPS, UC.LAST_MODIFIED_DATE
    ORDER BY UC.USE_CASE_EACV DESC NULLS LAST
    """

def display_metrics(df):
    col1, col2, col3, col4 = st.columns(4)
    with col1: st.metric("Total Engagements", len(df))
    with col2: st.metric("Total EACV", f"${(df['EACV'].sum() if not df.empty else 0)/1_000_000:.1f}M")
    with col3: st.metric("Unique Accounts", df['ACCOUNT_NAME'].nunique() if not df.empty else 0)
    with col4: st.metric("Engineers", df['ENGINEER'].nunique() if not df.empty else 0)

def display_stage_chart(df, title):
    if not df.empty:
        stage_data = df.groupby('STAGE')['USE_CASE_ID'].nunique().reset_index()
        stage_data.columns = ['STAGE', 'USE_CASES']
        fig = px.bar(stage_data.sort_values('STAGE'), x='STAGE', y='USE_CASES', title=title)
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

@st.cache_data(ttl=600, show_spinner=False)
def generate_ai_summary(prompt):
    try:
        conn = get_connection()
        escaped_prompt = prompt.replace("'", "''")
        query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{escaped_prompt}') as summary"
        if hasattr(conn, 'sql'):
            return conn.sql(query).to_pandas()['SUMMARY'].iloc[0]
        else:
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchone()[0]
            cur.close()
            return result
    except Exception as e:
        return f"Error generating summary: {str(e)}"

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
st.info("Select rows using checkboxes for individual AI analysis, or use bulk summary for all")

prev_q_start, prev_q_end = get_previous_fiscal_quarter()
st.sidebar.title("Filters")
start_date = st.sidebar.date_input("Start Date", prev_q_start)
end_date = st.sidebar.date_input("End Date", prev_q_end)

manager_options = ["Rithesh Makkena", "Puneet Lakhanpal", "David Hare", "Brendan Tisseur", "Zahir Gadiwan", "Gopal Raghavan"]
manager_filter = st.sidebar.multiselect("Manager", manager_options)

all_engineers = sorted(set([e for engineers in ENGINEER_LIST.values() for e in engineers]))
if manager_filter:
    filtered_engineers = []
    for mgr in manager_filter:
        filtered_engineers.extend(ENGINEER_LIST.get(mgr, []))
    engineer_options = sorted(set(filtered_engineers))
else:
    engineer_options = all_engineers
engineer_filter = st.sidebar.multiselect("Engineer", engineer_options)

feature_area = st.sidebar.multiselect("Feature Area", ["DE: Ingestion", "DE: Transformation", "DE: Interoperable Storage"])
key_features = st.sidebar.multiselect("Key Features", ["DE - Openflow", "DE - Openflow Oracle", "DE - Iceberg", "DE - Snowpark DE", "DE - Snowpark Connect", "DE - Dynamic Tables", "DE - Snowpipe Streaming", "DE - Snowpipe", "DE - Serverless Task", "DE - Connectors", "DE - dbt Projects", "DE - SAP Integration", "DE - Basic"])
gvp_filter = st.sidebar.multiselect("GVP", ["Jennifer Chronis", "Jon Robertson", "Jonathan Beaulier", "Keegan Riley", "Mark Fleming", "Stuart Nyemecz"])
theater_filter = st.sidebar.multiselect("Theater", ["AMSAcquisition", "AMSExpansion", "USMajors", "USPubSec"], default=["AMSAcquisition", "AMSExpansion", "USMajors", "USPubSec"])
stage_options = ["1 - Discovery", "2 - Scoping", "3 - Technical / Business Validation", "4 - Use Case Won / Migration Plan", "5 - Implementation In Progress", "6 - Implementation Complete", "7 - Deployed"]
stage_filter = st.sidebar.multiselect("Use Case Stage", stage_options)
account_name_search = st.sidebar.text_input("Account Name", value="", key="account_name_filter", help="Filter by account name (case-insensitive). Leave blank for all.")
min_acv = st.sidebar.number_input("Min EACV ($)", min_value=0, value=1, step=1, key="min_acv_filter")
last_n_days = st.sidebar.text_input("Last N Days Modified", value="", key="last_n_days_filter", help="Filter use cases modified in last N days. Leave blank for all.")


emp_filter = ""
if engineer_filter:
    emp_filter = f"AND ORG.EMPLOYEE_NAME IN ('" + "', '".join(engineer_filter) + "')"
elif manager_filter:
    emp_filter = f"AND COALESCE(NULLIF(ORG.FIRST_LINE_MANAGER, ''), 'Rithesh Makkena') IN ('" + "', '".join(manager_filter) + "')"

feature_filter = ""
if feature_area:
    feature_filter = "AND (" + " OR ".join([f"UC.TECHNICAL_USE_CASE LIKE '%{fa}%'" for fa in feature_area]) + ")"

key_feat_filter = ""
if key_features:
    key_feat_filter = "AND (" + " OR ".join([f"UC.PRIORITIZED_FEATURES ILIKE '%{kf}%'" for kf in key_features]) + ")"

gvp_clause = f"AND UC.ACCOUNT_GVP IN ('" + "', '".join(gvp_filter) + "')" if gvp_filter else ""
theater_clause = f"AND UC.THEATER_NAME IN ('" + "', '".join(theater_filter) + "')" if theater_filter else ""
stage_clause = f"AND UC.USE_CASE_STAGE IN ('" + "', '".join(stage_filter) + "')" if stage_filter else ""
account_name_clause = f"AND UC.ACCOUNT_NAME ILIKE '%{account_name_search.strip().replace(chr(39), chr(39)+chr(39))}%'" if account_name_search.strip() else ""
modified_clause = f"AND UC.LAST_MODIFIED_DATE >= DATEADD('day', -{int(last_n_days)}, CURRENT_DATE())" if last_n_days.strip() else ""

tab1, tab2, tab3, tab4, tab5 = st.tabs(["AFE Involved Won+Engagement", "AFE All Engagements", "Overall DE/DL Summary", "Weekly Key Updates", "Consumption Credits"])

display_cols = ['ACCOUNT_NAME', 'ACCOUNT_OWNER', 'EACV', 'STAGE', 'USE_CASE_NAME', 'ENGINEER', 'MANAGER', 'KEY_FEATURES', 'THEATER', 'GVP']

with tab1:
    st.subheader("Won+ Engagements (Stages 4-7)")
    with st.spinner('Loading won+ engagements...'):
        query = build_afe_query(str(start_date), str(end_date), emp_filter, feature_filter, key_feat_filter, gvp_clause, theater_clause, stage_clause, won_only=True)
        df_won = run_query(query)
    
    if not df_won.empty:
        pass
    else:
        pass
    
    display_metrics(df_won)
    display_stage_chart(df_won, "Won+ Use Cases by Stage")
    
    st.markdown("---")
    
    if not df_won.empty:
        df_won_display = df_won[display_cols].copy()
        df_won_display.insert(0, 'Select', False)
        
        edited_won = st.data_editor(
            df_won_display,
            use_container_width=True,
            height=400,
            column_config={
                "Select": st.column_config.CheckboxColumn("Select", default=False, width="small"),
                "EACV": st.column_config.NumberColumn("EACV", format="$%.0f"),
                "KEY_FEATURES": st.column_config.TextColumn("Key Features", width="medium"),
            },
            disabled=[c for c in display_cols],
            hide_index=True,
            key="won_editor"
        )
        st.caption(f"Showing {len(df_won)} won+ engagements | Select rows using checkboxes")
        
        selected_won_indices = edited_won[edited_won['Select'] == True].index.tolist()
        selected_won_rows = df_won.iloc[selected_won_indices] if selected_won_indices else pd.DataFrame()
        
        email_df_won = selected_won_rows if not selected_won_rows.empty else df_won
        render_email_section(email_df_won, "DE/DL Won+ Portfolio Summary", st.session_state.get('won_ai_result'), "won")
        
        st.markdown("### 🤖 AI Summary")
        col_ai1, col_ai2 = st.columns([1, 1])
        with col_ai1:
            if st.button("📊 Generate AI Summary for All Won+", key="won_bulk_btn"):
                st.session_state['won_bulk_analysis'] = True
                st.session_state['won_usecase_analysis'] = False
        with col_ai2:
            if len(selected_won_rows) > 0 and len(selected_won_rows) <= 5:
                if st.button(f"🔎 AI Analysis for {len(selected_won_rows)} Selected", key="won_usecase_btn"):
                    st.session_state['won_usecase_analysis'] = True
                    st.session_state['won_bulk_analysis'] = False
            elif len(selected_won_rows) > 5:
                st.warning("Select max 5 use cases")
            else:
                st.info("💡 Select accounts above for individual analysis")
        
        if st.session_state.get('won_bulk_analysis'):
            with st.container():
                prompt = build_bulk_ai_prompt(df_won)
                with st.spinner("Generating AI Summary..."):
                    result = generate_ai_summary(prompt)
                st.markdown(result)
                st.session_state['won_ai_result'] = result
                
                if st.button("Clear Summary", key="won_clear"):
                    st.session_state['won_bulk_analysis'] = False
                    st.rerun()
        
        if st.session_state.get('won_usecase_analysis') and len(selected_won_rows) > 0:
            st.markdown("#### 🔍 Selected Use Case Analysis")
            for idx, row in selected_won_rows.iterrows():
                with st.expander(f"📋 {row['ACCOUNT_NAME']} - {row['USE_CASE_NAME']} (${row['EACV']:,.0f})", expanded=True):
                    prompt = build_usecase_ai_prompt(row)
                    with st.spinner("Analyzing..."):
                        result = generate_ai_summary(prompt)
                    st.markdown(result)
            if st.button("Clear Use Case Analysis", key="won_usecase_clear"):
                st.session_state['won_usecase_analysis'] = False
                st.rerun()
    else:
        st.info("No won+ engagements found with current filters.")

with tab2:
    st.subheader("All Engagements")
    with st.spinner('Loading all engagements...'):
        query = build_afe_query(str(start_date), str(end_date), emp_filter, feature_filter, key_feat_filter, gvp_clause, theater_clause, stage_clause, won_only=False)
        df_all = run_query(query)
    
    if not df_all.empty:
        pass
    else:
        pass
    
    display_metrics(df_all)
    display_stage_chart(df_all, "All Use Cases by Stage")
    
    st.markdown("---")
    
    if not df_all.empty:
        df_all_display = df_all[display_cols].copy()
        df_all_display.insert(0, 'Select', False)
        
        edited_all = st.data_editor(
            df_all_display,
            use_container_width=True,
            height=400,
            column_config={
                "Select": st.column_config.CheckboxColumn("Select", default=False, width="small"),
                "EACV": st.column_config.NumberColumn("EACV", format="$%.0f"),
                "KEY_FEATURES": st.column_config.TextColumn("Key Features", width="medium"),
            },
            disabled=[c for c in display_cols],
            hide_index=True,
            key="all_editor"
        )
        st.caption(f"Showing {len(df_all)} engagements | Select rows using checkboxes")
        
        selected_all_indices = edited_all[edited_all['Select'] == True].index.tolist()
        selected_all_rows = df_all.iloc[selected_all_indices] if selected_all_indices else pd.DataFrame()
        
        email_df_all = selected_all_rows if not selected_all_rows.empty else df_all
        render_email_section(email_df_all, "DE/DL All Engagements Summary", st.session_state.get('all_ai_result'), "all")
        
        st.markdown("### 🤖 AI Summary")
        col_ai1, col_ai2 = st.columns([1, 1])
        with col_ai1:
            if st.button("📊 Generate AI Summary for All Engagements", key="all_bulk_btn"):
                st.session_state['all_bulk_analysis'] = True
                st.session_state['all_usecase_analysis'] = False
        with col_ai2:
            if len(selected_all_rows) > 0 and len(selected_all_rows) <= 5:
                if st.button(f"🔎 AI Analysis for {len(selected_all_rows)} Selected", key="all_usecase_btn"):
                    st.session_state['all_usecase_analysis'] = True
                    st.session_state['all_bulk_analysis'] = False
            elif len(selected_all_rows) > 5:
                st.warning("Select max 5 use cases")
            else:
                st.info("💡 Select accounts above for individual analysis")
        
        if st.session_state.get('all_bulk_analysis'):
            with st.container():
                prompt = build_bulk_ai_prompt(df_all)
                with st.spinner("Generating AI Summary..."):
                    result = generate_ai_summary(prompt)
                st.markdown(result)
                st.session_state['all_ai_result'] = result
                
                if st.button("Clear Summary", key="all_clear"):
                    st.session_state['all_bulk_analysis'] = False
                    st.rerun()
        
        if st.session_state.get('all_usecase_analysis') and len(selected_all_rows) > 0:
            st.markdown("#### 🔍 Selected Use Case Analysis")
            for idx, row in selected_all_rows.iterrows():
                with st.expander(f"📋 {row['ACCOUNT_NAME']} - {row['USE_CASE_NAME']} (${row['EACV']:,.0f})", expanded=True):
                    prompt = build_usecase_ai_prompt(row)
                    with st.spinner("Analyzing..."):
                        result = generate_ai_summary(prompt)
                    st.markdown(result)
            if st.button("Clear Use Case Analysis", key="all_usecase_clear"):
                st.session_state['all_usecase_analysis'] = False
                st.rerun()
    else:
        st.info("No engagements found with current filters.")

with tab3:
    st.subheader("DE/DL Use Cases Summary")
    with st.spinner('Loading summary...'):
        summary_stage_where = stage_clause if stage_filter else "AND UC.USE_CASE_STAGE NOT LIKE '0 -%' AND UC.USE_CASE_STAGE NOT LIKE '8 -%'"
        summary_query = f"""
        SELECT UC.USE_CASE_ID, UC.USE_CASE_LEAD_SE_NAME AS ENGINEER, UC.ACCOUNT_SE_MANAGER AS MANAGER,
            UC.ACCOUNT_NAME, UC.ACCOUNT_OWNER_NAME AS ACCOUNT_OWNER, UC.ACCOUNT_LEAD_SE_NAME AS ACCOUNT_SE, UC.USE_CASE_NAME,
            UC.USE_CASE_STAGE AS STAGE, UC.USE_CASE_EACV AS EACV, UC.PRIORITIZED_FEATURES AS KEY_FEATURES,
            UC.THEATER_NAME AS THEATER, UC.ACCOUNT_GVP AS GVP, UC.DECISION_DATE, UC.GO_LIVE_DATE, UC.LAST_MODIFIED_DATE,
            CASE WHEN UC.USE_CASE_STAGE LIKE '1 -%' OR UC.USE_CASE_STAGE LIKE '2 -%' OR UC.USE_CASE_STAGE LIKE '3 -%' THEN 'Stage 1-3'
                 WHEN UC.USE_CASE_STAGE LIKE '4 -%' OR UC.USE_CASE_STAGE LIKE '5 -%' THEN 'Stage 4-5'
                 WHEN UC.USE_CASE_STAGE LIKE '6 -%' OR UC.USE_CASE_STAGE LIKE '7 -%' THEN 'Stage 6-7' ELSE 'Other' END AS STAGE_BUCKET
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE UC
        WHERE 1=1 {summary_stage_where}
          AND (UC.TECHNICAL_USE_CASE LIKE '%DE:%' OR UC.PRIORITIZED_FEATURES LIKE '%DE -%')
          AND (UC.DECISION_DATE BETWEEN '{start_date}' AND '{end_date}' OR UC.GO_LIVE_DATE BETWEEN '{start_date}' AND '{end_date}')
          {feature_filter} {key_feat_filter} {gvp_clause} {theater_clause} {modified_clause} {account_name_clause}
        ORDER BY EACV DESC NULLS LAST
        """
        df_summary = run_query(summary_query)
    
    if not df_summary.empty:
        pass
    else:
        pass
    
    if not df_summary.empty:
        bucket_order = ['Stage 1-3', 'Stage 4-5', 'Stage 6-7']
        summary_agg = df_summary.groupby('STAGE_BUCKET').agg({
            'USE_CASE_ID': 'count', 
            'EACV': 'sum', 
            'ACCOUNT_NAME': 'nunique'
        }).reset_index()
        summary_agg.columns = ['Stage Bucket', 'Use Cases', 'Total EACV', 'Unique Accounts']
        
        col1, col2, col3 = st.columns(3)
        for idx, bucket in enumerate(bucket_order):
            row = summary_agg[summary_agg['Stage Bucket'] == bucket]
            with [col1, col2, col3][idx]:
                st.markdown(f"**{bucket}**")
                if not row.empty:
                    st.metric("Use Cases", int(row['Use Cases'].values[0]))
                    st.metric("EACV", f"${row['Total EACV'].values[0]/1_000_000:.1f}M")
                else:
                    st.metric("Use Cases", 0)
                    st.metric("EACV", "$0M")
        
        st.markdown("---")
        
        fig = px.bar(summary_agg[summary_agg['Stage Bucket'].isin(bucket_order)], x='Stage Bucket', y='Total EACV', title='EACV by Stage Bucket', text_auto='.2s')
        fig.update_layout(height=300)
        fig.update_yaxes(tickformat="$,.0f")
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("### All DE/DL Use Cases")
        summary_display_cols = ['STAGE_BUCKET', 'ACCOUNT_NAME', 'ACCOUNT_OWNER', 'EACV', 'STAGE', 'USE_CASE_NAME', 'KEY_FEATURES', 'ENGINEER']
        df_summary_display = df_summary[summary_display_cols].copy()
        df_summary_display.insert(0, 'Select', False)
        
        edited_summary = st.data_editor(
            df_summary_display,
            use_container_width=True,
            height=400,
            column_config={
                "Select": st.column_config.CheckboxColumn("Select", default=False, width="small"),
                "STAGE_BUCKET": st.column_config.TextColumn("Stage Bucket"),
                "EACV": st.column_config.NumberColumn("EACV", format="$%.0f"),
            },
            disabled=[c for c in summary_display_cols],
            hide_index=True,
            key="summary_editor"
        )
        st.caption(f"Showing {len(df_summary)} DE/DL use cases | Select rows using checkboxes")
        
        selected_summary_indices = edited_summary[edited_summary['Select'] == True].index.tolist()
        selected_summary_rows = df_summary.iloc[selected_summary_indices] if selected_summary_indices else pd.DataFrame()
        
        email_df_summary = selected_summary_rows if not selected_summary_rows.empty else df_summary
        render_email_section(email_df_summary, "DE/DL Portfolio Summary", st.session_state.get('summary_ai_result'), "summary")
        
        st.markdown("### 🤖 AI Summary")
        if st.button("📊 Generate AI Summary for All DE/DL Use Cases", key="summary_bulk_btn"):
            st.session_state['summary_bulk_analysis'] = True
        
        if st.session_state.get('summary_bulk_analysis'):
            with st.container():
                prompt = build_bulk_ai_prompt(df_summary)
                with st.spinner("Generating AI Summary..."):
                    result = generate_ai_summary(prompt)
                st.markdown(result)
                st.session_state['summary_ai_result'] = result
                
                if st.button("Clear Summary", key="summary_clear"):
                    st.session_state['summary_bulk_analysis'] = False
                    st.rerun()
    else:
        st.info("No use cases found with the selected filters.")

with tab4:
    st.subheader("Weekly Key Updates")
    st.caption("Use cases grouped by Key Feature, broken down by stage bucket (1-3, 4-5, 6-7)")
    with st.spinner('Loading...'):
        min_acv_value = min_acv
        weekly_stage_where = stage_clause if stage_filter else "AND UC.USE_CASE_STAGE NOT LIKE '0 -%' AND UC.USE_CASE_STAGE NOT LIKE '8 -%'"
        weekly_query = f"""
        SELECT UC.USE_CASE_ID, UC.ACCOUNT_NAME, UC.ACCOUNT_OWNER_NAME AS ACCOUNT_OWNER, UC.ACCOUNT_LEAD_SE_NAME AS ACCOUNT_SE, UC.USE_CASE_NAME, UC.USE_CASE_STAGE AS STAGE,
            UC.USE_CASE_EACV AS EACV, UC.PRIORITIZED_FEATURES AS KEY_FEATURES, UC.TECHNICAL_USE_CASE,
            UC.THEATER_NAME AS THEATER, UC.ACCOUNT_GVP AS GVP, UC.SE_COMMENTS, UC.IMPLEMENTATION_COMMENTS,
            UC.NEXT_STEPS, UC.SPECIALIST_COMMENTS, UC.PARTNER_COMMENTS, UC.DECISION_DATE, UC.GO_LIVE_DATE,
            UC.LAST_MODIFIED_DATE,
            CASE WHEN UC.USE_CASE_STAGE LIKE '1 -%' OR UC.USE_CASE_STAGE LIKE '2 -%' OR UC.USE_CASE_STAGE LIKE '3 -%' THEN 'Stage 1-3'
                 WHEN UC.USE_CASE_STAGE LIKE '4 -%' OR UC.USE_CASE_STAGE LIKE '5 -%' THEN 'Stage 4-5'
                 WHEN UC.USE_CASE_STAGE LIKE '6 -%' OR UC.USE_CASE_STAGE LIKE '7 -%' THEN 'Stage 6-7' ELSE 'Other' END AS STAGE_BUCKET
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE UC
        WHERE 1=1 {weekly_stage_where}
          AND UC.PRIORITIZED_FEATURES ILIKE '%DE -%'
          AND (UC.DECISION_DATE BETWEEN '{start_date}' AND '{end_date}' OR UC.GO_LIVE_DATE BETWEEN '{start_date}' AND '{end_date}')
          AND UC.USE_CASE_EACV >= {min_acv_value}
          {gvp_clause} {theater_clause} {key_feat_filter} {modified_clause} {account_name_clause}
        ORDER BY UC.USE_CASE_EACV DESC NULLS LAST
        """
        df_weekly = run_query(weekly_query)
    
    if not df_weekly.empty:
        all_key_features = ["DE - Openflow", "DE - Openflow Oracle", "DE - Iceberg", "DE - Snowpark DE", "DE - Snowpark Connect", "DE - Dynamic Tables", "DE - Snowpipe Streaming", "DE - Snowpipe", "DE - Serverless Task", "DE - Connectors", "DE - dbt Projects", "DE - SAP Integration", "DE - Basic"]
        key_features_list = [kf for kf in all_key_features if kf in key_features] if key_features else all_key_features
        stage_buckets = ['Stage 1-3', 'Stage 4-5', 'Stage 6-7']

        col1, col2, col3 = st.columns(3)
        with col1: st.metric("Total Use Cases", len(df_weekly))
        with col2: st.metric("Total EACV", f"${df_weekly['EACV'].sum()/1_000_000:.1f}M")
        with col3: st.metric("Unique Accounts", df_weekly['ACCOUNT_NAME'].nunique())

        stage_cols = st.columns(len(stage_buckets))
        for sc, sb in zip(stage_cols, stage_buckets):
            db = df_weekly[df_weekly['STAGE_BUCKET'] == sb]
            sc.metric(f"{sb} Use Cases", len(db), f"${db['EACV'].sum()/1_000_000:.1f}M EACV")

        st.markdown("---")

        weekly_selected_ids = []

        for key_feat in key_features_list:
            df_feat_all = df_weekly[df_weekly['KEY_FEATURES'].str.contains(key_feat, case=False, na=False)]
            if df_feat_all.empty:
                continue
            with st.expander(f"**{key_feat}** — {len(df_feat_all)} use cases (${df_feat_all['EACV'].sum()/1_000_000:.1f}M EACV)", expanded=True):
                feat_select_key = f"wfeat_{key_feat}"
                ai_btn_key = f"weekly_ai_{key_feat}"
                top_col1, top_col2 = st.columns([3, 1])
                with top_col1:
                    feat_all = st.checkbox(f"✅ Select All {key_feat}", key=feat_select_key)
                with top_col2:
                    if st.button(f"🤖 AI Summary", key=ai_btn_key):
                        st.session_state[f'weekly_ai_{key_feat}'] = True
                active_buckets = [sb for sb in stage_buckets if not df_feat_all[df_feat_all['STAGE_BUCKET'] == sb].empty]
                if active_buckets:
                    stage_cols = st.columns(len(active_buckets))
                    for sc, sb in zip(stage_cols, active_buckets):
                        df_feat_stage = df_feat_all[df_feat_all['STAGE_BUCKET'] == sb]
                        with sc:
                            stage_select_key = f"wstg_{key_feat}_{sb}"
                            st.markdown(f"###### {sb} ({len(df_feat_stage)})")
                            stage_all = st.checkbox(f"Select All", key=stage_select_key)
                            for _, row in df_feat_stage.iterrows():
                                uc_id = row['USE_CASE_ID']
                                cb_key = f"wcb_{key_feat}_{sb}_{uc_id}"
                                default_val = feat_all or stage_all
                                checked = st.checkbox(
                                    f"{row['ACCOUNT_NAME']} — {row['USE_CASE_NAME']} (${row['EACV']:,.0f})",
                                    key=cb_key,
                                    value=default_val
                                )
                                if checked:
                                    weekly_selected_ids.append(uc_id)
                feat_selected = [uid for uid in weekly_selected_ids if uid in df_feat_all['USE_CASE_ID'].values]
                feat_selected_rows = df_feat_all[df_feat_all['USE_CASE_ID'].isin(feat_selected)]
                if st.session_state.get(f'weekly_ai_{key_feat}') and len(feat_selected_rows) > 0:
                    st.markdown("---")
                    for _, row in feat_selected_rows.iterrows():
                        with st.expander(f"📋 {row['ACCOUNT_NAME']} — {row['USE_CASE_NAME']} (${row['EACV']:,.0f})", expanded=True):
                            prompt = build_weekly_usecase_ai_prompt(row)
                            with st.spinner(f"Analyzing {row['ACCOUNT_NAME']}..."):
                                ai_result = generate_ai_summary(prompt)
                            st.markdown(ai_result)
                    if st.button("Clear Analysis", key=f"weekly_clear_{key_feat}"):
                        st.session_state[f'weekly_ai_{key_feat}'] = False
                        st.rerun()
                elif st.session_state.get(f'weekly_ai_{key_feat}') and len(feat_selected_rows) == 0:
                    st.info("Select use cases above, then click 🤖 AI Summary.")

        email_df_weekly = df_weekly[df_weekly['USE_CASE_ID'].isin(weekly_selected_ids)] if weekly_selected_ids else df_weekly
        render_email_section(email_df_weekly, "DE/DL Weekly Key Updates", key_prefix="weekly")
    else:
        st.info("No use cases found matching the criteria.")

with tab5:
    st.subheader("DE Feature Consumption Credits (Last 60 Days)")

    with st.spinner('Loading consumption data...'):
        consumption_daily_query = """
        SELECT DS, FEATURE, SUM(CREDITS) as CREDITS
        FROM FINANCE.CUSTOMER.FY26_PRODUCT_CATEGORY_REVENUE
        WHERE PRODUCT_CATEGORY = 'Data Engineering'
          AND DS >= DATEADD('day', -60, CURRENT_DATE())
        GROUP BY DS, FEATURE
        ORDER BY DS, FEATURE
        """
        df_consumption_daily = run_query(consumption_daily_query)

        consumption_wow_query = """
        WITH daily AS (
            SELECT DS, FEATURE, SUM(CREDITS) as CREDITS
            FROM FINANCE.CUSTOMER.FY26_PRODUCT_CATEGORY_REVENUE
            WHERE PRODUCT_CATEGORY = 'Data Engineering'
              AND DS >= DATEADD('day', -60, CURRENT_DATE())
            GROUP BY DS, FEATURE
        ),
        weekly AS (
            SELECT FEATURE,
                SUM(CASE WHEN DS >= DATE_TRUNC('week', DATEADD('week', -1, CURRENT_DATE())) AND DS < DATE_TRUNC('week', CURRENT_DATE()) THEN CREDITS ELSE 0 END) as CURRENT_WEEK,
                SUM(CASE WHEN DS >= DATE_TRUNC('week', DATEADD('week', -2, CURRENT_DATE())) AND DS < DATE_TRUNC('week', DATEADD('week', -1, CURRENT_DATE())) THEN CREDITS ELSE 0 END) as PREV_WEEK,
                SUM(CASE WHEN DS >= DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) AND DS < DATE_TRUNC('month', CURRENT_DATE()) THEN CREDITS ELSE 0 END) as CURRENT_MONTH,
                SUM(CASE WHEN DS >= DATE_TRUNC('month', DATEADD('month', -2, CURRENT_DATE())) AND DS < DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE())) THEN CREDITS ELSE 0 END) as PREV_MONTH,
                SUM(CREDITS) as TOTAL_60D
            FROM daily
            GROUP BY FEATURE
        )
        SELECT FEATURE, CURRENT_WEEK, PREV_WEEK,
            CASE WHEN PREV_WEEK > 0 THEN ROUND((CURRENT_WEEK - PREV_WEEK) / PREV_WEEK * 100, 1) ELSE NULL END AS WOW_PCT,
            CURRENT_MONTH, PREV_MONTH,
            CASE WHEN PREV_MONTH > 0 THEN ROUND((CURRENT_MONTH - PREV_MONTH) / PREV_MONTH * 100, 1) ELSE NULL END AS MOM_PCT,
            TOTAL_60D
        FROM weekly
        WHERE TOTAL_60D > 0
        ORDER BY TOTAL_60D DESC
        """
        df_consumption_wow = run_query(consumption_wow_query)

    if not df_consumption_daily.empty:
        key_feature_to_consumption = {
            "DE - Openflow": ["Openflow", "Iceberg Openflow"],
            "DE - Openflow Oracle": ["Openflow", "Iceberg Openflow"],
            "DE - Iceberg": ["Iceberg DML", "Iceberg COPY", "Iceberg COPY / UNLOAD", "Iceberg UNLOAD", "Iceberg DT refresh",
                             "Iceberg Data Engineering tools", "Iceberg Data Engineering tools - Ingestion", "Iceberg Data Engineering tools - Transformation",
                             "Iceberg Native app connector", "Iceberg Openflow", "Iceberg Snowpark DE", "Iceberg Snowpipe",
                             "Iceberg Spark connector", "Iceberg Stream access", "Iceberg Task", "Iceberg dbt projects in Snowflake", "Iceberg usage"],
            "DE - Snowpark DE": ["Snowpark DE", "Iceberg Snowpark DE"],
            "DE - Snowpark Connect": ["Spark connector", "Iceberg Spark connector"],
            "DE - Dynamic Tables": ["DT refresh", "Iceberg DT refresh"],
            "DE - Snowpipe Streaming": ["Snowpipe streaming", "Snowpipe streaming Kafka", "Snowpipe streaming v1", "Snowpipe streaming v2", "Snowpipe streaming DB connector"],
            "DE - Snowpipe": ["Snowpipe", "Snowpipe Kafka", "Snowpipe kafka", "Iceberg Snowpipe"],
            "DE - Serverless Task": ["Task", "Iceberg Task"],
            "DE - Connectors": ["Native app connector", "Iceberg Native app connector", "Spark connector", "Iceberg Spark connector"],
            "DE - dbt Projects": ["dbt projects in Snowflake", "Iceberg dbt projects in Snowflake"],
            "DE - SAP Integration": ["Native app connector", "Iceberg Native app connector"],
            "DE - Basic": ["DML", "COPY", "COPY / UNLOAD", "UNLOAD", "Copy files", "Data Engineering tools",
                           "Data Engineering tools - Ingestion", "Data Engineering tools - Transformation", "Stream access"],
        }

        current_kf = tuple(sorted(key_features)) if key_features else ()
        if st.session_state.get('_prev_key_features') != current_kf:
            st.session_state['_prev_key_features'] = current_kf
            if 'consumption_feature_filter' in st.session_state:
                del st.session_state['consumption_feature_filter']
                st.rerun()

        if key_features:
            mapped_features = set()
            for kf in key_features:
                mapped_features.update(key_feature_to_consumption.get(kf, []))
            df_consumption_daily = df_consumption_daily[df_consumption_daily['FEATURE'].isin(mapped_features)]
            if not df_consumption_wow.empty:
                df_consumption_wow = df_consumption_wow[df_consumption_wow['FEATURE'].isin(mapped_features)]

        all_features_ranked = df_consumption_daily.groupby('FEATURE')['CREDITS'].sum().sort_values(ascending=False).index.tolist()
        top_10_default = all_features_ranked[:min(10, len(all_features_ranked))]

        selected_features = st.multiselect(
            "Filter by Feature",
            options=all_features_ranked,
            default=top_10_default,
            key="consumption_feature_filter",
            help="Select features to display in the chart. Defaults to top 10 by total credits."
        )

        if not selected_features:
            selected_features = top_10_default

        df_filtered = df_consumption_daily[df_consumption_daily['FEATURE'].isin(selected_features)]
        df_wow_filtered = df_consumption_wow[df_consumption_wow['FEATURE'].isin(selected_features)] if not df_consumption_wow.empty else df_consumption_wow

        col1, col2, col3 = st.columns(3)
        with col1:
            total_credits = df_filtered['CREDITS'].sum()
            st.metric("Total Credits (60D)", f"{total_credits/1_000_000:.1f}M")
        with col2:
            st.metric("Selected Features", len(selected_features))
        with col3:
            if not df_wow_filtered.empty:
                avg_wow = df_wow_filtered['WOW_PCT'].dropna().mean()
                st.metric("Avg WoW Change", f"{avg_wow:+.1f}%")

        fig = px.line(df_filtered, x='DS', y='CREDITS', color='FEATURE', title=f'Daily Credits by Selected DE Features (Last 60 Days)')
        fig.update_layout(height=450, xaxis_title='Date', yaxis_title='Credits', legend_title='Feature')
        fig.update_yaxes(tickformat=",.0f")
        st.plotly_chart(fig, use_container_width=True)

        if not df_wow_filtered.empty:
            st.markdown("### WoW% and MoM% Change by Feature")
            wow_display = df_wow_filtered.copy()
            wow_display.columns = ['Feature', 'Current Week', 'Prev Week', 'WoW %', 'Current Month', 'Prev Month', 'MoM %', 'Total 60D']
            st.data_editor(
                wow_display,
                use_container_width=True,
                height=400,
                column_config={
                    "Current Week": st.column_config.NumberColumn(format="%.0f"),
                    "Prev Week": st.column_config.NumberColumn(format="%.0f"),
                    "WoW %": st.column_config.NumberColumn(format="%.1f%%"),
                    "Current Month": st.column_config.NumberColumn(format="%.0f"),
                    "Prev Month": st.column_config.NumberColumn(format="%.0f"),
                    "MoM %": st.column_config.NumberColumn(format="%.1f%%"),
                    "Total 60D": st.column_config.NumberColumn(format="%.0f"),
                },
                disabled=True,
                hide_index=True,
                key="consumption_editor"
            )

        st.markdown("### 🤖 AI Summary")
        if st.button("📊 Generate AI Summary for DE Consumption", key="consumption_ai_btn"):
            st.session_state['consumption_ai_analysis'] = True

        if st.session_state.get('consumption_ai_analysis'):
            with st.container():
                top_data = df_wow_filtered.head(15).to_string(index=False) if not df_wow_filtered.empty else "No data"
                prompt = f"""Analyze the following DE (Data Engineering) feature consumption credit data for the last 60 days.
Provide insights on:
1. Top consuming features and their trends
2. Notable WoW (Week-over-Week) changes - which features are growing or declining
3. Notable MoM (Month-over-Month) changes
4. Any concerning trends or opportunities
5. Brief recommendations

Data (top features by total credits):
{top_data}

Keep it concise with bullet points."""
                with st.spinner("Generating AI Summary..."):
                    result = generate_ai_summary(prompt)
                st.markdown(result)
                st.session_state['consumption_ai_result'] = result
                if st.button("Clear Summary", key="consumption_clear"):
                    st.session_state['consumption_ai_analysis'] = False
                    st.rerun()

        st.markdown("### 💬 Ask About Consumption")
        ask_mode = st.radio("Query Mode", ["General Trends", "Customer / Use Case Lookup"], horizontal=True, key="consumption_ask_mode")

        if ask_mode == "General Trends":
            consumption_question = st.text_input("Ask about DE consumption trends...", key="consumption_q")
            if consumption_question:
                top_data = df_wow_filtered.head(15).to_string(index=False) if not df_wow_filtered.empty else "No data"
                prompt = f"""Based on this DE feature consumption data (last 60 days):
{top_data}

Answer this question: {consumption_question}
Be concise and data-driven."""
                with st.spinner("Analyzing..."):
                    answer = generate_ai_summary(prompt)
                st.markdown(answer)
        else:
            customer_search = st.text_input("Enter customer name or use case to search...", key="customer_search_input", placeholder="e.g. Netflix, Uber, streaming pipeline...")
            if customer_search:
                with st.spinner(f"Searching for '{customer_search}'..."):
                    df_customer = query_customer_consumption(customer_search)
                if df_customer.empty:
                    st.warning(f"No DE consumption data found for '{customer_search}' in the last 60 days.")
                else:
                    unique_accounts = df_customer['LATEST_SALESFORCE_ACCOUNT_NAME'].nunique()
                    total_credits = df_customer['TOTAL_CREDITS'].sum()
                    st.success(f"Found {unique_accounts} account(s) matching '{customer_search}' — Total DE Credits: {total_credits:,.2f}")

                    st.dataframe(df_customer, use_container_width=True, hide_index=True)

                    customer_question = st.text_input("Ask a question about this customer's consumption...", key="customer_q", placeholder="e.g. What features are they using most? Any growth trends?")
                    if customer_question:
                        customer_data = df_customer.to_string(index=False)
                        prompt = f"""Based on the following customer-level DE consumption data (last 60 days):

{customer_data}

Answer this question: {customer_question}

Provide account name, top features, total credits, and any notable patterns. Be concise and data-driven."""
                        with st.spinner("Analyzing customer data..."):
                            answer = generate_ai_summary(prompt)
                        st.markdown(answer)
                    elif not df_customer.empty:
                        auto_prompt = f"""Provide a brief summary of this customer's DE consumption data (last 60 days):

{df_customer.to_string(index=False)}

Include: account name(s), top features by credits, use cases, segments, and any notable patterns. Be concise."""
                        with st.spinner("Generating customer summary..."):
                            summary = generate_ai_summary(auto_prompt)
                        st.markdown("**AI Summary:**")
                        st.markdown(summary)
    else:
        st.info("No consumption data found for the last 60 days.")

st.markdown("---")
st.markdown("## 💬 Ask Cortex")

def build_cortex_data_context(df, max_rows=150):
    if df is None or df.empty:
        return ""
    comment_cols = [c for c in ['SE_COMMENTS', 'NEXT_STEPS', 'IMPLEMENTATION_COMMENTS', 'SPECIALIST_COMMENTS', 'PARTNER_COMMENTS'] if c in df.columns]
    context_cols = ['USE_CASE_ID', 'ACCOUNT_NAME', 'USE_CASE_NAME', 'STAGE', 'EACV'] + comment_cols
    context_cols = [c for c in context_cols if c in df.columns]
    ctx_df = df[context_cols].head(max_rows).copy()
    for c in comment_cols:
        if c in ctx_df.columns:
            ctx_df[c] = ctx_df[c].fillna('').astype(str).str[:200]
    return ctx_df.to_string(index=False)

def parse_matching_ids(ai_response):
    import re
    ids = re.findall(r'UC[-_]?\d+[-_]?\d*[-_]?\d*', ai_response)
    if not ids:
        ids = re.findall(r'\b(\d{6,})\b', ai_response)
    return list(set(ids))

try:
    cortex_base_df = df_all if 'df_all' in dir() and df_all is not None and not df_all.empty else (df_won if 'df_won' in dir() and df_won is not None and not df_won.empty else pd.DataFrame())
except:
    cortex_base_df = pd.DataFrame()

if 'chat_messages' not in st.session_state:
    st.session_state.chat_messages = []
if 'cortex_filtered_df' not in st.session_state:
    st.session_state.cortex_filtered_df = None

for msg in st.session_state.chat_messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg["role"] == "assistant" and msg.get("filtered_df") is not None and not msg["filtered_df"].empty:
            st.dataframe(msg["filtered_df"], use_container_width=True, height=300)

if prompt := st.chat_input("Ask about DE/DL engagements (e.g., 'show me use cases with concerns or red flags')..."):
    st.session_state.chat_messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"): st.markdown(prompt)
    with st.chat_message("assistant"):
        if not cortex_base_df.empty:
            data_context = build_cortex_data_context(cortex_base_df)
            ai_prompt = f"""You are a DE/DL team dashboard analyst. Below is use case data with comments and status fields.

DATA:
{data_context}

USER QUESTION: {prompt}

INSTRUCTIONS:
1. Analyze the data to answer the user's question.
2. Look through SE_COMMENTS, NEXT_STEPS, IMPLEMENTATION_COMMENTS, SPECIALIST_COMMENTS, PARTNER_COMMENTS, and STAGE fields for relevant information.
3. For each matching use case, mention the USE_CASE_ID, ACCOUNT_NAME, and why it matches.
4. At the end, provide a section titled "MATCHING_IDS:" followed by a comma-separated list of the matching USE_CASE_ID values.
5. If no use cases match, say so and set MATCHING_IDS: NONE

Be concise and data-driven."""
            with st.spinner("Analyzing use case data..."):
                result = generate_ai_summary(ai_prompt)
            display_text = result.split("MATCHING_IDS:")[0].strip() if "MATCHING_IDS:" in result else result
            st.markdown(display_text)

            matching_ids = parse_matching_ids(result.split("MATCHING_IDS:")[-1] if "MATCHING_IDS:" in result else "")
            filtered_df = pd.DataFrame()
            if matching_ids and 'USE_CASE_ID' in cortex_base_df.columns:
                mask = cortex_base_df['USE_CASE_ID'].astype(str).isin([str(i) for i in matching_ids])
                filtered_df = cortex_base_df[mask]
            if not filtered_df.empty:
                show_cols = [c for c in ['ACCOUNT_NAME', 'USE_CASE_NAME', 'STAGE', 'EACV', 'SE_COMMENTS', 'NEXT_STEPS', 'ENGINEER'] if c in filtered_df.columns]
                st.markdown(f"**Matching Use Cases ({len(filtered_df)}):**")
                st.dataframe(filtered_df[show_cols], use_container_width=True, height=300)
                st.session_state.chat_messages.append({"role": "assistant", "content": display_text, "filtered_df": filtered_df[show_cols]})
            else:
                st.session_state.chat_messages.append({"role": "assistant", "content": display_text, "filtered_df": None})
        else:
            result = generate_ai_summary(f"Answer about DE/DL engagements: {prompt}")
            st.markdown(result)
            st.session_state.chat_messages.append({"role": "assistant", "content": result, "filtered_df": None})
