import streamlit as st
from datetime import datetime, date, timedelta
import plotly.express as px
import pandas as pd
import os
import snowflake.connector
import urllib.parse
import re

st.set_page_config(page_title="DE/DL Team Engagements", layout="wide")

st.markdown("""
<style>
    .metric-card { border-radius: 12px; padding: 20px 24px; text-align: center; }
    .metric-card .label { font-size: 14px; font-weight: 500; opacity: 0.8; margin-bottom: 4px; }
    .metric-card .value { font-size: 36px; font-weight: 700; line-height: 1.2; }
    .metric-neutral { background: #23262b; border: 1px solid #3a3f47; }
    .metric-neutral .value { color: #ffffff; }
    .metric-green { background: #0a2e1a; border: 1px solid #21c354; }
    .metric-green .label { color: #21c354; }
    .metric-green .value { color: #21c354; }
    .metric-red { background: #2e0a0a; border: 1px solid #ff4b4b; }
    .metric-red .label { color: #ff4b4b; }
    .metric-red .value { color: #ff4b4b; }
    .metric-amber { background: #2e2400; border: 1px solid #f59e0b; }
    .metric-amber .label { color: #f59e0b; }
    .metric-amber .value { color: #f59e0b; }
</style>
""", unsafe_allow_html=True)

ENGINEER_LIST = {
    "Rithesh Makkena": ["Anika Shahi", "Chandra Nayak", "Chris Atkinson", "Chris Cardillo", "Kelsey Hammock", "Kesav Rayaprolu", "Nagesh Cherukuri", "Naveen Alan Thomas", "Niels ter Keurs", "Prash Medirattaa", "Randy Pettus", "Rithesh Makkena", "Sam Mittal", "Shawn Namdar", "Varun Kumar", "Venkat Suru", "Venkatesh Sekar"],
    "Puneet Lakhanpal": ["Chinmayee Lakkad", "Dharmendra Shavkani", "Gayatri Ghanakota", "Hanbing Yan", "Jason Ho", "Jonathan Sierra", "Jonathan Tao", "Kiran Kumar Earalli", "Manrique Vargas", "Nirav Shah", "Pallavi Sharma", "Phani Raj", "Prathamesh Nimkar", "Priya Joseph", "Puneet Lakhanpal", "Rahul Reddy", "Ravi Kumar", "Ripu Jain", "Rogerio Rizzio", "Sam Gupta", "Santosh Ubale", "Su Dogra", "Tom Manfredi"],
    "David Hare": ["David Hare", "Jason Hughes", "Jeremiah Hansen", "Jon Bennett", "Keith Gaputis", "Marc Henderson", "Marcin Kulakowski", "Parag Jain", "Sean Petrie", "Shantanu Gope", "Sharvan Kumar"],
    "Brendan Tisseur": ["Brendan Tisseur", "Prasad Revalkar", "Ryan Templeton", "Salar Rowhani", "Summiya Khalid", "Venkat Medida"],
    "Zahir Gadiwan": ["Ali Khosro", "Andries Engelbrecht", "Eric Tolotti", "James Sun", "Matt Marzillo", "Zahir Gadiwan"],
    "Gopal Raghavan": ["Akash Bhatt", "Anthony Alteirac", "Dave Freriks", "David Richert", "Gopal Raghavan", "Mayur Mahadeshwar"]
}

@st.cache_resource
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
        drivers = row.get('ENGAGEMENT_DRIVERS', '') or 'SE'
        summary_data.append(f"- {account} | Stage: {stage} | EACV: ${eacv:,.0f} | Features: {features[:400]} | Drivers: {drivers} | PSE: {engineer}")
    
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

IMPORTANT: Always reference EACV dollar amounts in your narrative. Quantify the pipeline at risk, wins by dollar value, and prioritize action items by EACV impact.

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
    drivers = row.get('ENGAGEMENT_DRIVERS', '') or 'SE'
    
    return f"""Analyze this specific DE/DL use case and provide actionable insights:

**USE CASE DETAILS:**
- Account: {account}
- Use Case: {use_case}
- Stage: {stage}
- EACV: ${eacv:,.0f}
- Key Features: {features}
- Engagement Drivers: {drivers}
- PSE: {engineer}
- Technical Use Case: {tech_use_case}
- SE Comments: {comments[:400] if comments else 'None'}
- Next Steps: {next_steps[:400] if next_steps else 'None'}

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

IMPORTANT: Always reference the EACV dollar amount in your narrative — quantify the opportunity size and its implications.

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
    drivers = row.get('ENGAGEMENT_DRIVERS', '') or 'SE'

    return f"""Analyze this DE/DL use case and provide actionable insights:

**USE CASE DETAILS:**
- Account: {account}
- Use Case: {use_case}
- Stage: {stage}
- EACV: ${eacv:,.0f}
- Key Features: {features}
- Engagement Drivers: {drivers}
- Technical Use Case: {tech_use_case}
- GVP: {gvp} | Theater: {theater}
- SE Comments: {se_comments[:400] if se_comments else 'None'}
- Implementation Comments: {impl_comments[:400] if impl_comments else 'None'}
- Next Steps: {next_steps[:400] if next_steps else 'None'}
- Specialist Comments: {specialist[:400] if specialist else 'None'}
- Partner Comments: {partner[:400] if partner else 'None'}

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

IMPORTANT: Always reference the EACV dollar amount in your narrative — quantify the opportunity size and its implications.

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
    WITH {dedl_cte},
    specialist_cte AS (
        SELECT UC2.USE_CASE_ID,
            LISTAGG(DISTINCT f_name.value::STRING || ' (' ||
                CASE f_role.value::STRING
                    WHEN 'SE - Workload FCTO' THEN 'AFE'
                    WHEN 'Platform Specialist' THEN 'Platform'
                    WHEN 'SE - Partner' THEN 'Partner SE'
                    WHEN 'Partner Account Manager' THEN 'Partner'
                    WHEN 'Partner Solutions Architect' THEN 'Partner SA'
                    WHEN 'Services Delivery Manager' THEN 'Services'
                    WHEN 'SE - Enterprise Architect' THEN 'EA'
                    WHEN 'Industry Principal' THEN 'Industry'
                    WHEN 'SE - Industry CTO' THEN 'Industry'
                    WHEN 'SE - Security FCTO' THEN 'Security'
                    WHEN 'SE - Performance FCTO' THEN 'Performance'
                END || ')', ', ') AS SPECIALISTS
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE UC2,
        LATERAL FLATTEN(UC2.USE_CASE_TEAM_NAME_LIST) f_name,
        LATERAL FLATTEN(UC2.USE_CASE_TEAM_ROLE_LIST) f_role
        WHERE f_name.index = f_role.index
          AND f_role.value::STRING IN ('SE - Workload FCTO', 'Platform Specialist', 'SE - Partner', 'Partner Account Manager', 'Partner Solutions Architect', 'Services Delivery Manager', 'SE - Enterprise Architect', 'Industry Principal', 'SE - Industry CTO', 'SE - Security FCTO', 'SE - Performance FCTO')
        GROUP BY UC2.USE_CASE_ID
    )
    SELECT UC.USE_CASE_ID, LISTAGG(DISTINCT D.EMPLOYEE_NAME, ', ') WITHIN GROUP (ORDER BY D.EMPLOYEE_NAME) AS ENGINEER,
        MAX(D.FIRST_LINE_MANAGER) AS MANAGER, UC.ACCOUNT_NAME, UC.ACCOUNT_OWNER_NAME AS ACCOUNT_OWNER, UC.ACCOUNT_LEAD_SE_NAME AS ACCOUNT_SE, UC.USE_CASE_NAME,
        UC.USE_CASE_STAGE AS STAGE, UC.USE_CASE_EACV AS EACV, UC.TECHNICAL_USE_CASE, UC.PRIORITIZED_FEATURES AS KEY_FEATURES,
        UC.THEATER_NAME AS THEATER, UC.ACCOUNT_GVP AS GVP, UC.DECISION_DATE, UC.GO_LIVE_DATE, UC.SE_COMMENTS, UC.NEXT_STEPS, UC.LAST_MODIFIED_DATE,
        COALESCE(sp.SPECIALISTS, '') AS SPECIALISTS,
        ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Workload FCTO%', 'AFE', NULL),
            IFF(ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Platform Specialist%', 'Platform Specialist', NULL),
            IFF(UC.IS_PARTNER_ATTACHED = TRUE OR ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Partner%', 'Partner', NULL),
            IFF(UC.IS_PS_ENGAGED = TRUE OR ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Services Delivery%', 'Services', NULL),
            IFF(ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Industry%' OR ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%FCTO - Industry%', 'Industry', NULL)
        )), ', ') AS ENGAGEMENT_DRIVERS
    FROM DEDL_ATTRIBUTION D
    INNER JOIN MDM.MDM_INTERFACES.DIM_USE_CASE UC ON D.USE_CASE_ID = UC.USE_CASE_ID
    LEFT JOIN specialist_cte sp ON sp.USE_CASE_ID = UC.USE_CASE_ID
    WHERE 1=1 {afe_stage_clause}
      AND (UC.DECISION_DATE BETWEEN '{start_date}' AND '{end_date}' OR UC.GO_LIVE_DATE BETWEEN '{start_date}' AND '{end_date}')
      {feature_filter} {key_feat_filter} {gvp_clause} {theater_clause} {account_name_clause}
    GROUP BY UC.USE_CASE_ID, UC.ACCOUNT_NAME, UC.ACCOUNT_OWNER_NAME, UC.ACCOUNT_LEAD_SE_NAME, UC.USE_CASE_NAME, UC.USE_CASE_STAGE, UC.USE_CASE_EACV,
             UC.TECHNICAL_USE_CASE, UC.PRIORITIZED_FEATURES, UC.THEATER_NAME, UC.ACCOUNT_GVP, UC.DECISION_DATE, UC.GO_LIVE_DATE, UC.SE_COMMENTS, UC.NEXT_STEPS, UC.LAST_MODIFIED_DATE,
             UC.USE_CASE_TEAM_ROLE_LIST, UC.IS_PARTNER_ATTACHED, UC.IS_PS_ENGAGED, sp.SPECIALISTS
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
        query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', '{escaped_prompt}') as summary"
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

def escape_latex(text):
    return text.replace("$", "\\$") if isinstance(text, str) else text

def md_to_rich_html(md_text):
    lines = md_text.split("\n")
    html_parts = []
    in_list = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if in_list:
                html_parts.append("</ul>")
                in_list = False
            html_parts.append("<br>")
            continue
        heading_match = re.match(r'^(#{1,4})\s+(.*)', stripped)
        if heading_match:
            if in_list:
                html_parts.append("</ul>")
                in_list = False
            level = len(heading_match.group(1))
            text = heading_match.group(2)
            text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
            colors = {1: "#29B5E8", 2: "#29B5E8", 3: "#f59e0b", 4: "#f59e0b"}
            sizes = {1: "22px", 2: "18px", 3: "16px", 4: "15px"}
            html_parts.append(f'<div style="font-size:{sizes.get(level,"15px")};font-weight:700;color:{colors.get(level,"#29B5E8")};margin:16px 0 8px 0;border-bottom:1px solid #3a3f47;padding-bottom:6px">{text}</div>')
            continue
        numbered_match = re.match(r'^(\d+)\.\s+(.*)', stripped)
        if numbered_match and not stripped.startswith("- "):
            if in_list:
                html_parts.append("</ul>")
                in_list = False
            text = numbered_match.group(2)
            text = re.sub(r'\*\*(.+?)\*\*', r'<strong style="color:#29B5E8">\1</strong>', text)
            text = re.sub(r'__(.*?)__', r'<u>\1</u>', text)
            html_parts.append(f'<div style="font-size:16px;font-weight:700;color:#f59e0b;margin:14px 0 6px 0">{numbered_match.group(1)}. {text}</div>')
            continue
        bullet_match = re.match(r'^[-*]\s+(.*)', stripped)
        sub_bullet_match = re.match(r'^\s+[-*]\s+(.*)', line)
        if sub_bullet_match:
            text = sub_bullet_match.group(1)
            text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
            text = re.sub(r'__(.*?)__', r'<u>\1</u>', text)
            html_parts.append(f'<li style="margin-left:24px;margin-bottom:4px;color:#d1d5db;font-size:14px;list-style-type:circle">{text}</li>')
            continue
        if bullet_match:
            if not in_list:
                html_parts.append('<ul style="margin:4px 0;padding-left:20px">')
                in_list = True
            text = bullet_match.group(1)
            text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
            text = re.sub(r'__(.*?)__', r'<u>\1</u>', text)
            html_parts.append(f'<li style="margin-bottom:6px;color:#e5e7eb;font-size:14px">{text}</li>')
            continue
        if in_list:
            html_parts.append("</ul>")
            in_list = False
        stripped = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', stripped)
        stripped = re.sub(r'__(.*?)__', r'<u>\1</u>', stripped)
        html_parts.append(f'<p style="margin:4px 0;color:#e5e7eb;font-size:14px">{stripped}</p>')
    if in_list:
        html_parts.append("</ul>")
    return "\n".join(html_parts)

def render_rich_ai_summary(ai_result, summary_id="ai_summary"):
    rich_html = md_to_rich_html(ai_result)
    container_html = f'''
    <div id="{summary_id}_container" style="background:#1a1d23;border:1px solid #3a3f47;border-radius:12px;padding:24px 28px;margin:12px 0;position:relative">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
            <span style="font-size:12px;color:#6b7280;font-weight:600;text-transform:uppercase;letter-spacing:1px">AI Analysis</span>
        </div>
        <div id="{summary_id}_content" style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif">
            {rich_html}
        </div>
    </div>
    '''
    st.markdown(container_html, unsafe_allow_html=True)
    if st.button("📋 Copy for Slides / Email", key=f"{summary_id}_copy_btn"):
        st.session_state[f'{summary_id}_show_copy'] = True
    if st.session_state.get(f'{summary_id}_show_copy'):
        st.code(ai_result, language=None)
        st.caption("Use the copy button (top-right of code block) to copy, then click below to hide.")
        if st.button("Hide", key=f"{summary_id}_hide_btn"):
            st.session_state[f'{summary_id}_show_copy'] = False
            st.rerun()

SFDC_BASE = "https://snowforce.lightning.force.com/lightning/r/vh__Deliverable__c"

@st.cache_data(ttl=600, show_spinner=False)
def load_afe_org():
    query = """
        SELECT EMPLOYEE_NAME AS NAME
        FROM SALES.SE_REPORTING.SE_ORG_HIERARCHY_VW
        WHERE IS_ACTIVE = TRUE
          AND (
            EMPLOYEE_NAME IN ('David Hare', 'Brendan Tisseur', 'Nagesh Cherukuri')
            OR MANAGER_NAME IN ('David Hare', 'Brendan Tisseur')
            OR FIRST_LINE_MANAGER IN ('David Hare', 'Brendan Tisseur')
          )
    """
    return run_query(query)

@st.cache_data(ttl=600, show_spinner=False)
def load_afe_use_cases():
    query = """
        WITH specialist_cte AS (
            SELECT UC2.USE_CASE_ID,
                LISTAGG(DISTINCT f_name.value::STRING || ' (' ||
                    CASE f_role.value::STRING
                        WHEN 'SE - Workload FCTO' THEN 'AFE'
                        WHEN 'Platform Specialist' THEN 'Platform'
                        WHEN 'SE - Partner' THEN 'Partner SE'
                        WHEN 'Partner Account Manager' THEN 'Partner'
                        WHEN 'Partner Solutions Architect' THEN 'Partner SA'
                        WHEN 'Services Delivery Manager' THEN 'Services'
                        WHEN 'SE - Enterprise Architect' THEN 'EA'
                        WHEN 'Industry Principal' THEN 'Industry'
                        WHEN 'SE - Industry CTO' THEN 'Industry'
                        WHEN 'SE - Security FCTO' THEN 'Security'
                        WHEN 'SE - Performance FCTO' THEN 'Performance'
                    END || ')', ', ') AS SPECIALISTS
            FROM MDM.MDM_INTERFACES.DIM_USE_CASE UC2,
            LATERAL FLATTEN(UC2.USE_CASE_TEAM_NAME_LIST) f_name,
            LATERAL FLATTEN(UC2.USE_CASE_TEAM_ROLE_LIST) f_role
            WHERE f_name.index = f_role.index
              AND f_role.value::STRING IN ('SE - Workload FCTO', 'Platform Specialist', 'SE - Partner', 'Partner Account Manager', 'Partner Solutions Architect', 'Services Delivery Manager', 'SE - Enterprise Architect', 'Industry Principal', 'SE - Industry CTO', 'SE - Security FCTO', 'SE - Performance FCTO')
            GROUP BY UC2.USE_CASE_ID
        ),
        comment_history_cte AS (
            SELECT PARENT_ID AS USE_CASE_ID, MAX(CREATED_DATE) AS LAST_SPECIALIST_COMMENT_DATE
            FROM FIVETRAN.SALESFORCE.VH_DELIVERABLE_HISTORY
            WHERE FIELD = 'Specialist_Comments__c'
              AND IS_DELETED = FALSE
            GROUP BY PARENT_ID
        )
        SELECT
            d.USE_CASE_ID, d.USE_CASE_NAME, d.ACCOUNT_NAME, d.USE_CASE_EACV,
            d.USE_CASE_STAGE, d.USE_CASE_STATUS, d.THEATER_NAME, d.REGION_NAME,
            ARRAY_TO_STRING(d.PRODUCT_CATEGORY_ARRAY, ', ') AS PRODUCT_CATEGORIES,
            d.PRIORITIZED_FEATURES AS KEY_FEATURES,
            d.USE_CASE_LEAD_SE_NAME AS ENGINEER,
            f.value::STRING AS SPECIALIST,
            IFF(d.SPECIALIST_COMMENTS IS NOT NULL AND TRIM(d.SPECIALIST_COMMENTS) != '', TRUE, FALSE) AS HAS_SPECIALIST_COMMENTS,
            d.SPECIALIST_COMMENTS,
            d.LAST_MODIFIED_DATE, d.DECISION_DATE, d.GO_LIVE_DATE,
            COALESCE(sp.SPECIALISTS, '') AS SPECIALISTS,
            ch.LAST_SPECIALIST_COMMENT_DATE,
            ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(
                IFF(ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Workload FCTO%', 'AFE', NULL),
                IFF(ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Platform Specialist%', 'Platform Specialist', NULL),
                IFF(d.IS_PARTNER_ATTACHED = TRUE OR ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Partner%', 'Partner', NULL),
                IFF(d.IS_PS_ENGAGED = TRUE OR ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Services Delivery%', 'Services', NULL),
                IFF(ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Industry%' OR ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%FCTO - Industry%', 'Industry', NULL)
            )), ', ') AS ENGAGEMENT_DRIVERS
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE d
        LEFT JOIN specialist_cte sp ON sp.USE_CASE_ID = d.USE_CASE_ID
        LEFT JOIN comment_history_cte ch ON ch.USE_CASE_ID = d.USE_CASE_ID,
            LATERAL FLATTEN(d.USE_CASE_TEAM_NAME_LIST) f,
            LATERAL FLATTEN(d.USE_CASE_TEAM_ROLE_LIST) r
        WHERE f.index = r.index
          AND r.value::STRING = 'SE - Workload FCTO'
        ORDER BY d.USE_CASE_EACV DESC NULLS LAST
    """
    return run_query(query)

@st.cache_data(ttl=600, show_spinner=False)
def load_pss_org():
    query = """
        SELECT DISTINCT PREFERRED_NAME AS NAME
        FROM SALES.SE_REPORTING.AFE_PSS_ORG
        WHERE OVERLAY_LEAD_L2 = 'Kevin Hannon'
           OR MANAGER_NAME = 'Kevin Hannon'
           OR PREFERRED_NAME = 'Kevin Hannon'
    """
    return run_query(query)

@st.cache_data(ttl=600, show_spinner=False)
def load_pss_use_cases(pss_names):
    names_str = "', '".join([n.replace("'", "''") for n in pss_names])
    query = f"""
        WITH specialist_cte AS (
            SELECT UC2.USE_CASE_ID,
                LISTAGG(DISTINCT f_name.value::STRING || ' (' ||
                    CASE f_role.value::STRING
                        WHEN 'SE - Workload FCTO' THEN 'AFE'
                        WHEN 'Platform Specialist' THEN 'Platform'
                        WHEN 'SE - Partner' THEN 'Partner SE'
                        WHEN 'Partner Account Manager' THEN 'Partner'
                        WHEN 'Partner Solutions Architect' THEN 'Partner SA'
                        WHEN 'Services Delivery Manager' THEN 'Services'
                        WHEN 'SE - Enterprise Architect' THEN 'EA'
                        WHEN 'Industry Principal' THEN 'Industry'
                        WHEN 'SE - Industry CTO' THEN 'Industry'
                        WHEN 'SE - Security FCTO' THEN 'Security'
                        WHEN 'SE - Performance FCTO' THEN 'Performance'
                    END || ')', ', ') AS SPECIALISTS
            FROM MDM.MDM_INTERFACES.DIM_USE_CASE UC2,
            LATERAL FLATTEN(UC2.USE_CASE_TEAM_NAME_LIST) f_name,
            LATERAL FLATTEN(UC2.USE_CASE_TEAM_ROLE_LIST) f_role
            WHERE f_name.index = f_role.index
              AND f_role.value::STRING IN ('SE - Workload FCTO', 'Platform Specialist', 'SE - Partner', 'Partner Account Manager', 'Partner Solutions Architect', 'Services Delivery Manager', 'SE - Enterprise Architect', 'Industry Principal', 'SE - Industry CTO', 'SE - Security FCTO', 'SE - Performance FCTO')
            GROUP BY UC2.USE_CASE_ID
        ),
        comment_history_cte AS (
            SELECT PARENT_ID AS USE_CASE_ID, MAX(CREATED_DATE) AS LAST_SPECIALIST_COMMENT_DATE
            FROM FIVETRAN.SALESFORCE.VH_DELIVERABLE_HISTORY
            WHERE FIELD = 'Specialist_Comments__c'
              AND IS_DELETED = FALSE
            GROUP BY PARENT_ID
        )
        SELECT
            d.USE_CASE_ID, d.USE_CASE_NAME, d.ACCOUNT_NAME, d.USE_CASE_EACV,
            d.USE_CASE_STAGE, d.USE_CASE_STATUS, d.THEATER_NAME, d.REGION_NAME,
            ARRAY_TO_STRING(d.PRODUCT_CATEGORY_ARRAY, ', ') AS PRODUCT_CATEGORIES,
            d.PRIORITIZED_FEATURES AS KEY_FEATURES,
            d.USE_CASE_LEAD_SE_NAME AS ENGINEER,
            f.value::STRING AS SPECIALIST,
            IFF(d.SPECIALIST_COMMENTS IS NOT NULL AND TRIM(d.SPECIALIST_COMMENTS) != '', TRUE, FALSE) AS HAS_SPECIALIST_COMMENTS,
            d.SPECIALIST_COMMENTS,
            d.LAST_MODIFIED_DATE, d.DECISION_DATE, d.GO_LIVE_DATE,
            COALESCE(sp.SPECIALISTS, '') AS SPECIALISTS,
            ch.LAST_SPECIALIST_COMMENT_DATE,
            ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(
                IFF(ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Workload FCTO%', 'AFE', NULL),
                IFF(ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Platform Specialist%', 'Platform Specialist', NULL),
                IFF(d.IS_PARTNER_ATTACHED = TRUE OR ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Partner%', 'Partner', NULL),
                IFF(d.IS_PS_ENGAGED = TRUE OR ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Services Delivery%', 'Services', NULL),
                IFF(ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Industry%' OR ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%FCTO - Industry%', 'Industry', NULL)
            )), ', ') AS ENGAGEMENT_DRIVERS
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE d
        LEFT JOIN specialist_cte sp ON sp.USE_CASE_ID = d.USE_CASE_ID
        LEFT JOIN comment_history_cte ch ON ch.USE_CASE_ID = d.USE_CASE_ID,
            LATERAL FLATTEN(d.USE_CASE_TEAM_NAME_LIST) f
        WHERE f.value::STRING IN ('{names_str}')
        ORDER BY d.USE_CASE_EACV DESC NULLS LAST
    """
    return run_query(query)

@st.cache_data(ttl=600, show_spinner=False)
def load_services_org():
    query = """
        SELECT DISTINCT EMPLOYEE_NAME AS NAME
        FROM SALES.SE_REPORTING.SE_ORG_HIERARCHY_DS
        WHERE DS = (SELECT MAX(DS) FROM SALES.SE_REPORTING.SE_ORG_HIERARCHY_DS WHERE EMPLOYEE_NAME = 'Ganesh Krishnamurthy')
          AND IS_ACTIVE = TRUE
          AND (MANAGER_NAME = 'Ganesh Krishnamurthy'
            OR FIRST_LINE_MANAGER = 'Ganesh Krishnamurthy'
            OR SECOND_LINE_MANAGER = 'Ganesh Krishnamurthy'
            OR EMPLOYEE_NAME = 'Ganesh Krishnamurthy')
    """
    return run_query(query)

@st.cache_data(ttl=600, show_spinner=False)
def load_services_use_cases(svc_names):
    names_str = "', '".join([n.replace("'", "''") for n in svc_names])
    query = f"""
        WITH specialist_cte AS (
            SELECT UC2.USE_CASE_ID,
                LISTAGG(DISTINCT f_name.value::STRING || ' (' ||
                    CASE f_role.value::STRING
                        WHEN 'SE - Workload FCTO' THEN 'AFE'
                        WHEN 'Platform Specialist' THEN 'Platform'
                        WHEN 'SE - Partner' THEN 'Partner SE'
                        WHEN 'Partner Account Manager' THEN 'Partner'
                        WHEN 'Partner Solutions Architect' THEN 'Partner SA'
                        WHEN 'Services Delivery Manager' THEN 'Services'
                        WHEN 'SE - Enterprise Architect' THEN 'EA'
                        WHEN 'Industry Principal' THEN 'Industry'
                        WHEN 'SE - Industry CTO' THEN 'Industry'
                        WHEN 'SE - Security FCTO' THEN 'Security'
                        WHEN 'SE - Performance FCTO' THEN 'Performance'
                    END || ')', ', ') AS SPECIALISTS
            FROM MDM.MDM_INTERFACES.DIM_USE_CASE UC2,
            LATERAL FLATTEN(UC2.USE_CASE_TEAM_NAME_LIST) f_name,
            LATERAL FLATTEN(UC2.USE_CASE_TEAM_ROLE_LIST) f_role
            WHERE f_name.index = f_role.index
              AND f_role.value::STRING IN ('SE - Workload FCTO', 'Platform Specialist', 'SE - Partner', 'Partner Account Manager', 'Partner Solutions Architect', 'Services Delivery Manager', 'SE - Enterprise Architect', 'Industry Principal', 'SE - Industry CTO', 'SE - Security FCTO', 'SE - Performance FCTO')
            GROUP BY UC2.USE_CASE_ID
        ),
        comment_history_cte AS (
            SELECT PARENT_ID AS USE_CASE_ID, MAX(CREATED_DATE) AS LAST_IMPLEMENTATION_COMMENT_DATE
            FROM FIVETRAN.SALESFORCE.VH_DELIVERABLE_HISTORY
            WHERE FIELD = 'Implementation_Comments__c'
              AND IS_DELETED = FALSE
            GROUP BY PARENT_ID
        )
        SELECT
            d.USE_CASE_ID, d.USE_CASE_NAME, d.ACCOUNT_NAME, d.USE_CASE_EACV,
            d.USE_CASE_STAGE, d.USE_CASE_STATUS, d.THEATER_NAME, d.REGION_NAME,
            ARRAY_TO_STRING(d.PRODUCT_CATEGORY_ARRAY, ', ') AS PRODUCT_CATEGORIES,
            d.PRIORITIZED_FEATURES AS KEY_FEATURES,
            d.USE_CASE_LEAD_SE_NAME AS ENGINEER,
            f.value::STRING AS SPECIALIST,
            IFF(d.IMPLEMENTATION_COMMENTS IS NOT NULL AND TRIM(d.IMPLEMENTATION_COMMENTS) != '', TRUE, FALSE) AS HAS_IMPLEMENTATION_COMMENTS,
            d.IMPLEMENTATION_COMMENTS,
            d.LAST_MODIFIED_DATE, d.DECISION_DATE, d.GO_LIVE_DATE,
            COALESCE(sp.SPECIALISTS, '') AS SPECIALISTS,
            ch.LAST_IMPLEMENTATION_COMMENT_DATE,
            ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(
                IFF(ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Workload FCTO%', 'AFE', NULL),
                IFF(ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Platform Specialist%', 'Platform Specialist', NULL),
                IFF(d.IS_PARTNER_ATTACHED = TRUE OR ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Partner%', 'Partner', NULL),
                IFF(d.IS_PS_ENGAGED = TRUE OR ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Services Delivery%', 'Services', NULL),
                IFF(ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Industry%' OR ARRAY_TO_STRING(d.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%FCTO - Industry%', 'Industry', NULL)
            )), ', ') AS ENGAGEMENT_DRIVERS
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE d
        LEFT JOIN specialist_cte sp ON sp.USE_CASE_ID = d.USE_CASE_ID
        LEFT JOIN comment_history_cte ch ON ch.USE_CASE_ID = d.USE_CASE_ID,
            LATERAL FLATTEN(d.USE_CASE_TEAM_NAME_LIST) f
        WHERE f.value::STRING IN ('{names_str}')
        ORDER BY d.USE_CASE_EACV DESC NULLS LAST
    """
    return run_query(query)

def afe_add_months(d, m):
    month = d.month - 1 + m
    year = d.year + month // 12
    month = month % 12 + 1
    return date(year, month, 1)

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

driver_filter = st.sidebar.multiselect("Engagement Driver", ["AFE", "Platform Specialist", "Partner", "Services", "Industry"])

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

def apply_driver_filter(df):
    if not driver_filter or df.empty or 'ENGAGEMENT_DRIVERS' not in df.columns:
        return df
    mask = pd.Series(False, index=df.index)
    for d in driver_filter:
        mask = mask | df['ENGAGEMENT_DRIVERS'].fillna('').str.contains(d, case=False, na=False)
    return df[mask].reset_index(drop=True)

TAB_NAMES = ["Overall DE/DL Summary", "AFE All Engagements", "Weekly Key Updates", "Consumption Credits", "PSS-AFE Team Commentry", "Services Team Commentry"]
active_tab = st.radio("", TAB_NAMES, horizontal=True, label_visibility="collapsed", key="active_tab")

display_cols = ['ACCOUNT_NAME', 'ACCOUNT_OWNER', 'EACV', 'STAGE', 'USE_CASE_NAME', 'ENGINEER', 'MANAGER', 'KEY_FEATURES', 'ENGAGEMENT_DRIVERS', 'SPECIALISTS', 'THEATER', 'GVP', 'SFDC']

if active_tab == "AFE All Engagements":
    st.subheader("All Engagements")
    with st.spinner('Loading all engagements...'):
        query = build_afe_query(str(start_date), str(end_date), emp_filter, feature_filter, key_feat_filter, gvp_clause, theater_clause, stage_clause, won_only=False)
        df_all = apply_driver_filter(run_query(query))
    
    if not df_all.empty:
        df_all["SFDC"] = df_all["USE_CASE_ID"].apply(lambda uid: f"{SFDC_BASE}/{uid}/view" if uid else "")
    
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
                "ENGAGEMENT_DRIVERS": st.column_config.TextColumn("Drivers", width="medium"),
                "SPECIALISTS": st.column_config.TextColumn("Specialists", width="medium"),
                "SFDC": st.column_config.LinkColumn("SFDC", display_text=r".*/(.+)/view"),
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
                st.markdown(escape_latex(result))
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
                    st.markdown(escape_latex(result))
            if st.button("Clear Use Case Analysis", key="all_usecase_clear"):
                st.session_state['all_usecase_analysis'] = False
                st.rerun()
    else:
        st.info("No engagements found with current filters.")

if active_tab == "Overall DE/DL Summary":
    st.subheader("DE/DL Use Cases Summary")
    with st.spinner('Loading summary...'):
        summary_stage_where = stage_clause if stage_filter else "AND UC.USE_CASE_STAGE NOT LIKE '0 -%' AND UC.USE_CASE_STAGE NOT LIKE '8 -%'"
        summary_query = f"""
        WITH specialist_cte AS (
            SELECT UC2.USE_CASE_ID,
                LISTAGG(DISTINCT f_name.value::STRING || ' (' ||
                    CASE f_role.value::STRING
                        WHEN 'SE - Workload FCTO' THEN 'AFE'
                        WHEN 'Platform Specialist' THEN 'Platform'
                        WHEN 'SE - Partner' THEN 'Partner SE'
                        WHEN 'Partner Account Manager' THEN 'Partner'
                        WHEN 'Partner Solutions Architect' THEN 'Partner SA'
                        WHEN 'Services Delivery Manager' THEN 'Services'
                        WHEN 'SE - Enterprise Architect' THEN 'EA'
                        WHEN 'Industry Principal' THEN 'Industry'
                        WHEN 'SE - Industry CTO' THEN 'Industry'
                        WHEN 'SE - Security FCTO' THEN 'Security'
                        WHEN 'SE - Performance FCTO' THEN 'Performance'
                    END || ')', ', ') AS SPECIALISTS
            FROM MDM.MDM_INTERFACES.DIM_USE_CASE UC2,
            LATERAL FLATTEN(UC2.USE_CASE_TEAM_NAME_LIST) f_name,
            LATERAL FLATTEN(UC2.USE_CASE_TEAM_ROLE_LIST) f_role
            WHERE f_name.index = f_role.index
              AND f_role.value::STRING IN ('SE - Workload FCTO', 'Platform Specialist', 'SE - Partner', 'Partner Account Manager', 'Partner Solutions Architect', 'Services Delivery Manager', 'SE - Enterprise Architect', 'Industry Principal', 'SE - Industry CTO', 'SE - Security FCTO', 'SE - Performance FCTO')
            GROUP BY UC2.USE_CASE_ID
        )
        SELECT UC.USE_CASE_ID, UC.USE_CASE_LEAD_SE_NAME AS ENGINEER, UC.ACCOUNT_SE_MANAGER AS MANAGER,
            UC.ACCOUNT_NAME, UC.ACCOUNT_OWNER_NAME AS ACCOUNT_OWNER, UC.ACCOUNT_LEAD_SE_NAME AS ACCOUNT_SE, UC.USE_CASE_NAME,
            UC.USE_CASE_STAGE AS STAGE, UC.USE_CASE_EACV AS EACV, UC.PRIORITIZED_FEATURES AS KEY_FEATURES,
            UC.THEATER_NAME AS THEATER, UC.ACCOUNT_GVP AS GVP, UC.DECISION_DATE, UC.GO_LIVE_DATE, UC.LAST_MODIFIED_DATE,
            COALESCE(sp.SPECIALISTS, '') AS SPECIALISTS,
            ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(
                IFF(ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Workload FCTO%', 'AFE', NULL),
                IFF(ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Platform Specialist%', 'Platform Specialist', NULL),
                IFF(UC.IS_PARTNER_ATTACHED = TRUE OR ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Partner%', 'Partner', NULL),
                IFF(UC.IS_PS_ENGAGED = TRUE OR ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Services Delivery%', 'Services', NULL),
                IFF(ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Industry%' OR ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%FCTO - Industry%', 'Industry', NULL)
            )), ', ') AS ENGAGEMENT_DRIVERS,
            CASE WHEN UC.USE_CASE_STAGE LIKE '1 -%' OR UC.USE_CASE_STAGE LIKE '2 -%' OR UC.USE_CASE_STAGE LIKE '3 -%' THEN 'Stage 1-3'
                 WHEN UC.USE_CASE_STAGE LIKE '4 -%' OR UC.USE_CASE_STAGE LIKE '5 -%' THEN 'Stage 4-5'
                 WHEN UC.USE_CASE_STAGE LIKE '6 -%' OR UC.USE_CASE_STAGE LIKE '7 -%' THEN 'Stage 6-7' ELSE 'Other' END AS STAGE_BUCKET
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE UC
        LEFT JOIN specialist_cte sp ON sp.USE_CASE_ID = UC.USE_CASE_ID
        WHERE 1=1 {summary_stage_where}
          AND (UC.TECHNICAL_USE_CASE LIKE '%DE:%' OR UC.PRIORITIZED_FEATURES LIKE '%DE -%')
          AND (UC.DECISION_DATE BETWEEN '{start_date}' AND '{end_date}' OR UC.GO_LIVE_DATE BETWEEN '{start_date}' AND '{end_date}')
          {feature_filter} {key_feat_filter} {gvp_clause} {theater_clause} {account_name_clause}
        ORDER BY EACV DESC NULLS LAST
        """
        df_summary = apply_driver_filter(run_query(summary_query))
    
    if not df_summary.empty:
        df_summary["SFDC"] = df_summary["USE_CASE_ID"].apply(lambda uid: f"{SFDC_BASE}/{uid}/view" if uid else "")
    
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
        summary_display_cols = ['STAGE_BUCKET', 'ACCOUNT_NAME', 'ACCOUNT_OWNER', 'EACV', 'STAGE', 'USE_CASE_NAME', 'KEY_FEATURES', 'ENGAGEMENT_DRIVERS', 'SPECIALISTS', 'ENGINEER', 'SFDC']
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
                "ENGAGEMENT_DRIVERS": st.column_config.TextColumn("Drivers", width="medium"),
                "SFDC": st.column_config.LinkColumn("SFDC", display_text=r".*/(.+)/view"),
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
                st.markdown(escape_latex(result))
                st.session_state['summary_ai_result'] = result
                
                if st.button("Clear Summary", key="summary_clear"):
                    st.session_state['summary_bulk_analysis'] = False
                    st.rerun()
    else:
        st.info("No use cases found with the selected filters.")

if active_tab == "Weekly Key Updates":
    st.subheader("Weekly Key Updates")
    st.caption("Use cases with recent SFDC history updates, filterable by Key Feature and stage bucket")

    filter_col1, filter_col2 = st.columns(2)
    with filter_col1:
        last_n_history_days = st.number_input("Last N Days Modified (based on SFDC history)", min_value=1, value=10, step=1, key="weekly_last_n_history_days", help="Filter use cases with any SFDC field change in last N days.")
    with filter_col2:
        comment_type_options = {"All Fields": "", "Specialist Comments": "Specialist_Comments__c", "SE Comments": "Use_Case_Comments__c", "Partner Comments": "Partners__c", "Professional Services": "Implementation_Comments__c"}
        selected_comment_type = st.selectbox("Show modified date based on", list(comment_type_options.keys()), index=0, key="weekly_comment_type_filter", help="Filter the modification date to only track changes to a specific comment field.")
    history_field_clause = f"AND FIELD = '{comment_type_options[selected_comment_type]}'" if comment_type_options[selected_comment_type] else ""

    with st.spinner('Loading...'):
        min_acv_value = min_acv
        weekly_stage_where = stage_clause if stage_filter else "AND UC.USE_CASE_STAGE NOT LIKE '0 -%' AND UC.USE_CASE_STAGE NOT LIKE '8 -%'"
        weekly_query = f"""
        WITH specialist_cte AS (
            SELECT UC2.USE_CASE_ID,
                LISTAGG(DISTINCT f_name.value::STRING || ' (' ||
                    CASE f_role.value::STRING
                        WHEN 'SE - Workload FCTO' THEN 'AFE'
                        WHEN 'Platform Specialist' THEN 'Platform'
                        WHEN 'SE - Partner' THEN 'Partner SE'
                        WHEN 'Partner Account Manager' THEN 'Partner'
                        WHEN 'Partner Solutions Architect' THEN 'Partner SA'
                        WHEN 'Services Delivery Manager' THEN 'Services'
                        WHEN 'SE - Enterprise Architect' THEN 'EA'
                        WHEN 'Industry Principal' THEN 'Industry'
                        WHEN 'SE - Industry CTO' THEN 'Industry'
                        WHEN 'SE - Security FCTO' THEN 'Security'
                        WHEN 'SE - Performance FCTO' THEN 'Performance'
                    END || ')', ', ') AS SPECIALISTS
            FROM MDM.MDM_INTERFACES.DIM_USE_CASE UC2,
            LATERAL FLATTEN(UC2.USE_CASE_TEAM_NAME_LIST) f_name,
            LATERAL FLATTEN(UC2.USE_CASE_TEAM_ROLE_LIST) f_role
            WHERE f_name.index = f_role.index
              AND f_role.value::STRING IN ('SE - Workload FCTO', 'Platform Specialist', 'SE - Partner', 'Partner Account Manager', 'Partner Solutions Architect', 'Services Delivery Manager', 'SE - Enterprise Architect', 'Industry Principal', 'SE - Industry CTO', 'SE - Security FCTO', 'SE - Performance FCTO')
            GROUP BY UC2.USE_CASE_ID
        ),
        history_cte AS (
            SELECT PARENT_ID AS USE_CASE_ID, MAX(CREATED_DATE) AS LAST_HISTORY_UPDATE_DATE
            FROM FIVETRAN.SALESFORCE.VH_DELIVERABLE_HISTORY
            WHERE IS_DELETED = FALSE
              {history_field_clause}
            GROUP BY PARENT_ID
        )
        SELECT UC.USE_CASE_ID, UC.ACCOUNT_NAME, UC.ACCOUNT_OWNER_NAME AS ACCOUNT_OWNER, UC.ACCOUNT_LEAD_SE_NAME AS ACCOUNT_SE, UC.USE_CASE_NAME, UC.USE_CASE_STAGE AS STAGE,
            UC.USE_CASE_EACV AS EACV, UC.PRIORITIZED_FEATURES AS KEY_FEATURES, UC.TECHNICAL_USE_CASE, UC.USE_CASE_LEAD_SE_NAME AS ENGINEER,
            UC.THEATER_NAME AS THEATER, UC.ACCOUNT_GVP AS GVP, UC.SE_COMMENTS, UC.IMPLEMENTATION_COMMENTS,
            UC.NEXT_STEPS, UC.SPECIALIST_COMMENTS, UC.PARTNER_COMMENTS, UC.DECISION_DATE, UC.GO_LIVE_DATE,
            UC.LAST_MODIFIED_DATE,
            COALESCE(sp.SPECIALISTS, '') AS SPECIALISTS,
            hc.LAST_HISTORY_UPDATE_DATE,
            ARRAY_TO_STRING(ARRAY_COMPACT(ARRAY_CONSTRUCT(
                IFF(ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Workload FCTO%', 'AFE', NULL),
                IFF(ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Platform Specialist%', 'Platform Specialist', NULL),
                IFF(UC.IS_PARTNER_ATTACHED = TRUE OR ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Partner%', 'Partner', NULL),
                IFF(UC.IS_PS_ENGAGED = TRUE OR ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Services Delivery%', 'Services', NULL),
                IFF(ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%Industry%' OR ARRAY_TO_STRING(UC.USE_CASE_TEAM_ROLE_LIST, ',') ILIKE '%FCTO - Industry%', 'Industry', NULL)
            )), ', ') AS ENGAGEMENT_DRIVERS,
            CASE WHEN UC.USE_CASE_STAGE LIKE '1 -%' OR UC.USE_CASE_STAGE LIKE '2 -%' OR UC.USE_CASE_STAGE LIKE '3 -%' THEN 'Stage 1-3'
                 WHEN UC.USE_CASE_STAGE LIKE '4 -%' OR UC.USE_CASE_STAGE LIKE '5 -%' THEN 'Stage 4-5'
                 WHEN UC.USE_CASE_STAGE LIKE '6 -%' OR UC.USE_CASE_STAGE LIKE '7 -%' THEN 'Stage 6-7' ELSE 'Other' END AS STAGE_BUCKET
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE UC
        LEFT JOIN specialist_cte sp ON sp.USE_CASE_ID = UC.USE_CASE_ID
        LEFT JOIN history_cte hc ON hc.USE_CASE_ID = UC.USE_CASE_ID
        WHERE 1=1 {weekly_stage_where}
          AND UC.PRIORITIZED_FEATURES ILIKE '%DE -%'
          AND (UC.DECISION_DATE BETWEEN '{start_date}' AND '{end_date}' OR UC.GO_LIVE_DATE BETWEEN '{start_date}' AND '{end_date}')
          AND UC.USE_CASE_EACV >= {min_acv_value}
          {gvp_clause} {theater_clause} {key_feat_filter} {account_name_clause}
        ORDER BY UC.USE_CASE_EACV DESC NULLS LAST
        """
        df_weekly = apply_driver_filter(run_query(weekly_query))
    
    if not df_weekly.empty:
        df_weekly["LAST_HISTORY_UPDATE_DATE"] = pd.to_datetime(df_weekly["LAST_HISTORY_UPDATE_DATE"], errors="coerce")
        cutoff_date = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=last_n_history_days)
        df_weekly = df_weekly[df_weekly["LAST_HISTORY_UPDATE_DATE"].notna() & (df_weekly["LAST_HISTORY_UPDATE_DATE"] >= cutoff_date)].reset_index(drop=True)
    if not df_weekly.empty:
        stage_buckets = ['Stage 1-3', 'Stage 4-5', 'Stage 6-7']

        all_weekly_key_features = sorted(set(
            kf.strip()
            for kfs in df_weekly["KEY_FEATURES"].dropna()
            for kf in kfs.split(";")
            if kf.strip() and kf.strip().startswith("DE -")
        ))

        filter_col_kf1, filter_col_kf2 = st.columns([3, 1])
        with filter_col_kf1:
            selected_weekly_key_features = st.multiselect("Filter by Key Feature", all_weekly_key_features, key="weekly_key_features_filter")

        if selected_weekly_key_features:
            kf_mask = df_weekly["KEY_FEATURES"].apply(
                lambda x: any(kf.strip() in selected_weekly_key_features for kf in (x or "").split(";"))
            )
            df_weekly_filtered = df_weekly[kf_mask].reset_index(drop=True)
        else:
            df_weekly_filtered = df_weekly

        col1, col2, col3 = st.columns(3)
        with col1: st.metric("Total Use Cases", len(df_weekly_filtered))
        with col2: st.metric("Total EACV", f"${df_weekly_filtered['EACV'].sum()/1_000_000:.1f}M")
        with col3: st.metric("Unique Accounts", df_weekly_filtered['ACCOUNT_NAME'].nunique())

        stage_cols = st.columns(len(stage_buckets))
        for sc, sb in zip(stage_cols, stage_buckets):
            db = df_weekly_filtered[df_weekly_filtered['STAGE_BUCKET'] == sb]
            sc.metric(f"{sb} Use Cases", len(db), f"${db['EACV'].sum()/1_000_000:.1f}M EACV")

        st.markdown("---")

        df_weekly_filtered["SFDC"] = df_weekly_filtered["USE_CASE_ID"].apply(lambda uid: f"{SFDC_BASE}/{uid}/view" if uid else "")

        grid_cols = ["ACCOUNT_NAME", "USE_CASE_NAME", "STAGE", "EACV", "STAGE_BUCKET", "ENGINEER", "KEY_FEATURES", "ENGAGEMENT_DRIVERS", "SPECIALISTS", "THEATER", "DECISION_DATE", "GO_LIVE_DATE", "LAST_HISTORY_UPDATE_DATE", "SFDC"]
        display_cols = [c for c in grid_cols if c in df_weekly_filtered.columns]
        weekly_display = df_weekly_filtered[display_cols].copy()
        weekly_display = weekly_display.sort_values("EACV", ascending=False).reset_index(drop=True)

        for dc in ["DECISION_DATE", "GO_LIVE_DATE"]:
            if dc in weekly_display.columns:
                weekly_display[dc] = pd.to_datetime(weekly_display[dc], errors="coerce").dt.strftime("%Y-%m-%d")

        st.dataframe(
            weekly_display,
            hide_index=True,
            use_container_width=True,
            height=min(600, 35 * len(weekly_display) + 38),
            column_config={
                "ACCOUNT_NAME": st.column_config.TextColumn("Account"),
                "USE_CASE_NAME": st.column_config.TextColumn("Use Case"),
                "STAGE": st.column_config.TextColumn("Stage"),
                "EACV": st.column_config.NumberColumn("EACV", format="$%,.0f"),
                "STAGE_BUCKET": st.column_config.TextColumn("Bucket"),
                "ENGINEER": st.column_config.TextColumn("Engineer"),
                "KEY_FEATURES": st.column_config.TextColumn("Key Features"),
                "ENGAGEMENT_DRIVERS": st.column_config.TextColumn("Drivers"),
                "SPECIALISTS": st.column_config.TextColumn("Specialists"),
                "THEATER": st.column_config.TextColumn("Theater"),
                "DECISION_DATE": st.column_config.TextColumn("Decision"),
                "GO_LIVE_DATE": st.column_config.TextColumn("Go Live"),
                "LAST_HISTORY_UPDATE_DATE": st.column_config.DatetimeColumn("Last Updated", format="YYYY-MM-DD"),
                "SFDC": st.column_config.LinkColumn("SFDC", display_text=r".*/(.+)/view"),
            },
        )

        if st.button("🤖 AI Summary", key="weekly_ai_btn"):
            comment_cols = ['SE_COMMENTS', 'SPECIALIST_COMMENTS', 'IMPLEMENTATION_COMMENTS', 'PARTNER_COMMENTS', 'NEXT_STEPS']
            rows_with_comments = df_weekly_filtered[df_weekly_filtered[comment_cols].apply(lambda r: r.str.strip().ne('').any(), axis=1)]
            if rows_with_comments.empty:
                rows_with_comments = df_weekly_filtered
            comments_sorted = rows_with_comments.sort_values("EACV", ascending=False)
            max_uc = 30
            comments_subset = comments_sorted.head(max_uc)
            comment_lines = []
            for _, row in comments_subset.iterrows():
                se = str(row.get("SE_COMMENTS", "") or "").strip()[:300]
                spec = str(row.get("SPECIALIST_COMMENTS", "") or "").strip()[:300]
                impl = str(row.get("IMPLEMENTATION_COMMENTS", "") or "").strip()[:300]
                ns = str(row.get("NEXT_STEPS", "") or "").strip()[:300]
                comment_lines.append(
                    f"- **{row['ACCOUNT_NAME']}** | {row['USE_CASE_NAME']} | Stage: {row['STAGE']} | EACV: ${row['EACV']:,.0f} | Engineer: {row.get('ENGINEER','')}\n"
                    f"  SE: {se}\n  Specialist: {spec}\n  Implementation: {impl}\n  Next Steps: {ns}"
                )
            comments_block = "\n".join(comment_lines)
            feat_label = ", ".join(selected_weekly_key_features) if selected_weekly_key_features else "All DE Features"
            ai_prompt = (
                f"You are a DE/DL team analyst. Analyze these {feat_label} use cases.\n\n"
                f"There are {len(rows_with_comments)} use cases with comments (out of {len(df_weekly_filtered)} total).\n\n"
                f"USE CASE DATA:\n{comments_block}\n\n"
                "Provide a structured summary with these sections:\n"
                "1. **What's Working** — Positive themes, successful patterns, products gaining traction.\n"
                "2. **Key Risks / Blockers** — Concerns, delays, blockers, at-risk engagements.\n"
                "3. **Next Action Items** — Specific recommended next steps. Reference account names.\n\n"
                "IMPORTANT: Always include EACV dollar amounts when referencing accounts.\n"
                "Be concise, data-driven, and reference specific accounts. Format with markdown."
            )
            with st.spinner(f"Analyzing {feat_label} with Cortex AI..."):
                ai_result = generate_ai_summary(ai_prompt)
            st.session_state['weekly_ai_result'] = ai_result

        if isinstance(st.session_state.get('weekly_ai_result'), str):
            render_rich_ai_summary(st.session_state['weekly_ai_result'], summary_id="weekly_ai_summary")
            if st.button("Clear Summary", key="weekly_clear_ai"):
                st.session_state['weekly_ai_result'] = None
                st.rerun()

        render_email_section(df_weekly_filtered, "DE/DL Weekly Key Updates", key_prefix="weekly")
    else:
        st.info("No use cases found matching the criteria.")

if active_tab == "Consumption Credits":
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
                st.markdown(escape_latex(result))
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
                st.markdown(escape_latex(answer))
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
                        st.markdown(escape_latex(answer))
                    elif not df_customer.empty:
                        auto_prompt = f"""Provide a brief summary of this customer's DE consumption data (last 60 days):

{df_customer.to_string(index=False)}

Include: account name(s), top features by credits, use cases, segments, and any notable patterns. Be concise."""
                        with st.spinner("Generating customer summary..."):
                            summary = generate_ai_summary(auto_prompt)
                        st.markdown("**AI Summary:**")
                        st.markdown(escape_latex(summary))
    else:
        st.info("No consumption data found for the last 60 days.")

if active_tab == "PSS-AFE Team Commentry":
    st.subheader("DE Team Engagement Tracker")
    with st.spinner("Loading engagement data..."):
        afe_org = load_afe_org()
        afe_team_members = set(afe_org["NAME"].str.strip().tolist())
        afe_data = load_afe_use_cases()
        pss_org = load_pss_org()
        pss_team_members = set(pss_org["NAME"].str.strip().tolist())
        pss_data = load_pss_use_cases(list(pss_team_members))

    team_selection = st.multiselect("Team Filter", ["DE/AFE Team (Rithesh)", "PSS Team (Kevin Hannon)"], default=["DE/AFE Team (Rithesh)"], key="afe_bw_team_filter")
    if not team_selection:
        team_selection = ["DE/AFE Team (Rithesh)"]

    include_afe = "DE/AFE Team (Rithesh)" in team_selection
    include_pss = "PSS Team (Kevin Hannon)" in team_selection

    if include_afe and include_pss:
        combined_members = afe_team_members | pss_team_members
        combined_data = pd.concat([afe_data, pss_data], ignore_index=True).drop_duplicates(subset=["USE_CASE_ID", "SPECIALIST"])
    elif include_pss:
        combined_members = pss_team_members
        combined_data = pss_data.copy()
    else:
        combined_members = afe_team_members
        combined_data = afe_data.copy()

    combined_data = apply_driver_filter(combined_data)
    cutoff_7d = date.today() - timedelta(days=7)
    cutoff_14d = date.today() - timedelta(days=14)
    today_dt = date.today()
    fy_year = today_dt.year if today_dt.month >= 2 else today_dt.year - 1
    fy_q_month = ((today_dt.month - 2) % 12) // 3 * 3 + 2
    fy_q_year = fy_year if fy_q_month >= 2 else fy_year + 1
    cq_start = date(fy_q_year, fy_q_month, 1)
    cq_end = afe_add_months(cq_start, 3) - timedelta(days=1)
    nq_start = cq_end + timedelta(days=1)
    nq_end = afe_add_months(nq_start, 3) - timedelta(days=1)
    fy_q_num = ((today_dt.month - 2) % 12) // 3 + 1
    cq_label = f"This Quarter (FY{fy_year % 100 + 1} Q{fy_q_num})"
    nq_num = fy_q_num % 4 + 1
    nq_label = f"Next Quarter (FY{fy_year % 100 + 1} Q{nq_num})"

    expanded = combined_data.copy()
    expanded["SPECIALIST"] = expanded["SPECIALIST"].str.strip()
    expanded = expanded[expanded["SPECIALIST"].isin(combined_members)].reset_index(drop=True)
    expanded = expanded[~expanded["USE_CASE_STAGE"].isin(["8 - Use Case Lost", "7 - Deployed"])].reset_index(drop=True)

    if key_features:
        kf_mask = expanded["KEY_FEATURES"].apply(
            lambda x: any(kf.lower() in (x or "").lower() for kf in key_features)
        )
        expanded = expanded[kf_mask].reset_index(drop=True)

    expanded["LAST_MODIFIED_DATE_DT"] = pd.to_datetime(expanded["LAST_MODIFIED_DATE"], errors="coerce")
    expanded["LAST_SPECIALIST_COMMENT_DATE_DT"] = pd.to_datetime(expanded["LAST_SPECIALIST_COMMENT_DATE"], errors="coerce")
    expanded["DECISION_DATE_DT"] = pd.to_datetime(expanded["DECISION_DATE"], errors="coerce").dt.date
    expanded["GO_LIVE_DATE_DT"] = pd.to_datetime(expanded["GO_LIVE_DATE"], errors="coerce").dt.date
    expanded["HAS_SPECIALIST_COMMENTS"] = expanded["HAS_SPECIALIST_COMMENTS"].astype(bool)

    expanded["IN_CQ"] = (
        ((expanded["DECISION_DATE_DT"] >= cq_start) & (expanded["DECISION_DATE_DT"] <= cq_end))
        | ((expanded["GO_LIVE_DATE_DT"] >= cq_start) & (expanded["GO_LIVE_DATE_DT"] <= cq_end))
    )
    expanded["IN_NQ"] = (
        ((expanded["DECISION_DATE_DT"] >= nq_start) & (expanded["DECISION_DATE_DT"] <= nq_end))
        | ((expanded["GO_LIVE_DATE_DT"] >= nq_start) & (expanded["GO_LIVE_DATE_DT"] <= nq_end))
    )
    expanded = expanded[expanded["IN_CQ"] | expanded["IN_NQ"]].reset_index(drop=True)
    expanded["RECENTLY_UPDATED_7D"] = (
        expanded["HAS_SPECIALIST_COMMENTS"]
        & expanded["LAST_SPECIALIST_COMMENT_DATE_DT"].notna()
        & (expanded["LAST_SPECIALIST_COMMENT_DATE_DT"].dt.date >= cutoff_7d)
    )
    expanded["RECENTLY_UPDATED_14D"] = (
        expanded["HAS_SPECIALIST_COMMENTS"]
        & expanded["LAST_SPECIALIST_COMMENT_DATE_DT"].notna()
        & (expanded["LAST_SPECIALIST_COMMENT_DATE_DT"].dt.date >= cutoff_14d)
    )

    all_key_features_pss = sorted(set(
        kf.strip()
        for kfs in expanded["KEY_FEATURES"].dropna()
        for kf in kfs.split(";")
        if kf.strip() and kf.strip().startswith("DE -")
    ))

    afe_summary = expanded.groupby("SPECIALIST").agg(
        total_use_cases=("USE_CASE_ID", "count"),
        with_comments=("HAS_SPECIALIST_COMMENTS", "sum"),
        active_7d=("RECENTLY_UPDATED_7D", "sum"),
        active_14d=("RECENTLY_UPDATED_14D", "sum"),
        total_eacv=("USE_CASE_EACV", "sum"),
    ).reset_index()
    afe_summary["without_comments"] = afe_summary["total_use_cases"] - afe_summary["with_comments"]
    afe_summary["coverage_pct"] = (afe_summary["with_comments"] / afe_summary["total_use_cases"] * 100).round(1)

    def classify_engagement(row):
        if row["active_7d"] > 0:
            return "Active (7d)"
        elif row["active_14d"] > 0:
            return "Active (14d)"
        elif row["with_comments"] > 0:
            return "Stale Engagement"
        else:
            return "Not Active Engagement"

    afe_summary["engagement"] = afe_summary.apply(classify_engagement, axis=1)
    engagement_order = {"Not Active Engagement": 0, "Stale Engagement": 1, "Active (14d)": 2, "Active (7d)": 3}
    afe_summary["_sort"] = afe_summary["engagement"].map(engagement_order)
    afe_summary = afe_summary.sort_values(["_sort", "total_use_cases"], ascending=[True, False]).reset_index(drop=True)

    team_label = " + ".join([t.split(" (")[0] for t in team_selection])
    st.caption(f"{team_label} · {cq_label}: {cq_start.strftime('%b %d')} – {cq_end.strftime('%b %d, %Y')} · {nq_label}: {nq_start.strftime('%b %d')} – {nq_end.strftime('%b %d, %Y')} · Active (7d) = updated in last 7 days · Active (14d) = updated in last 14 days")

    total_specialists = len(afe_summary)
    active_7d_count = int((afe_summary["engagement"] == "Active (7d)").sum())
    active_14d_count = int((afe_summary["engagement"] == "Active (14d)").sum())
    stale_count = int((afe_summary["engagement"] == "Stale Engagement").sum())
    not_active_count = int((afe_summary["engagement"] == "Not Active Engagement").sum())
    total_uc = len(expanded)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.markdown(f'<div class="metric-card metric-neutral"><div class="label">Total AFEs</div><div class="value">{total_specialists}</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="metric-card metric-green"><div class="label">Active (7d)</div><div class="value">{active_7d_count}</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="metric-card" style="background-color:#1a3a2a;border-left:4px solid #22c55e"><div class="label">Active (14d)</div><div class="value">{active_14d_count}</div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="metric-card metric-amber"><div class="label">Stale Engagement</div><div class="value">{stale_count}</div></div>', unsafe_allow_html=True)
    with c5:
        st.markdown(f'<div class="metric-card metric-red"><div class="label">Not Active</div><div class="value">{not_active_count}</div></div>', unsafe_allow_html=True)
    with c6:
        st.markdown(f'<div class="metric-card metric-neutral"><div class="label">Total Use Cases</div><div class="value">{total_uc:,}</div></div>', unsafe_allow_html=True)

    st.markdown("")

    filter_cols = st.columns([2, 3])
    with filter_cols[0]:
        afe_search = st.text_input("Search AFE", placeholder="Type a name to search...", key="afe_bw_search")
    with filter_cols[1]:
        selected_key_features_pss = st.multiselect("Filter by Key Feature", all_key_features_pss, key="afe_bw_key_features")

    if selected_key_features_pss:
        kf_mask = expanded["KEY_FEATURES"].apply(
            lambda x: any(kf.strip() in selected_key_features_pss for kf in (x or "").split(";"))
        )
        filtered_expanded = expanded[kf_mask]
        filtered_specialists = set(filtered_expanded["SPECIALIST"].unique())
        summary_view = afe_summary[afe_summary["SPECIALIST"].isin(filtered_specialists)].reset_index(drop=True)
    else:
        filtered_expanded = expanded
        summary_view = afe_summary

    sv_total = len(summary_view)
    sv_active_7d = int((summary_view["engagement"] == "Active (7d)").sum())
    sv_active_14d = int((summary_view["engagement"] == "Active (14d)").sum())
    sv_stale = int((summary_view["engagement"] == "Stale Engagement").sum())
    sv_not_active = int((summary_view["engagement"] == "Not Active Engagement").sum())

    ENGAGEMENT_COLORS = {
        "Active (7d)": "background-color: #0a2e1a; color: #4ade80",
        "Active (14d)": "background-color: #1a3a2a; color: #22c55e",
        "Stale Engagement": "background-color: #2e2400; color: #f59e0b",
        "Not Active Engagement": "background-color: #3d1111; color: #ff6b6b",
    }

    def style_afe_summary(df):
        display = df[["SPECIALIST", "engagement", "total_use_cases", "active_7d", "active_14d", "with_comments", "without_comments", "coverage_pct", "total_eacv"]].copy()
        display.columns = ["AFE", "Engagement", "Total Use Cases", "Active (7d)", "Active (14d)", "With Comments", "Without Comments", "Coverage %", "Total EACV"]
        def color_row(row):
            style = ENGAGEMENT_COLORS.get(row["Engagement"], "")
            return [style] * len(row)
        styled = display.style.apply(color_row, axis=1).format({"Coverage %": "{:.1f}%", "Total EACV": "${:,.0f}"})
        return styled

    def filter_by_afe_search(df, query):
        if query:
            return df[df["SPECIALIST"].str.contains(query, case=False, na=False)]
        return df

    tab_all, tab_not, tab_stale, tab_active_14d, tab_active_7d = st.tabs([
        f"All ({sv_total})", f"Not Active ({sv_not_active})",
        f"Stale ({sv_stale})", f"Active 14d ({sv_active_14d})", f"Active 7d ({sv_active_7d})",
    ])

    with tab_all:
        filtered = filter_by_afe_search(summary_view, afe_search)
        st.dataframe(style_afe_summary(filtered), hide_index=True, use_container_width=True, height=500)

    with tab_not:
        not_active = summary_view[summary_view["engagement"] == "Not Active Engagement"].reset_index(drop=True)
        filtered = filter_by_afe_search(not_active, afe_search)
        if filtered.empty:
            st.success("All AFEs have engagement!")
        else:
            st.dataframe(style_afe_summary(filtered), hide_index=True, use_container_width=True, height=500)

    with tab_stale:
        stale = summary_view[summary_view["engagement"] == "Stale Engagement"].reset_index(drop=True)
        filtered = filter_by_afe_search(stale, afe_search)
        if filtered.empty:
            st.info("No stale engagements found.")
        else:
            st.dataframe(style_afe_summary(filtered), hide_index=True, use_container_width=True, height=500)

    with tab_active_14d:
        active_14d = summary_view[summary_view["engagement"] == "Active (14d)"].reset_index(drop=True)
        filtered = filter_by_afe_search(active_14d, afe_search)
        if filtered.empty:
            st.info("No active (14d) engagements found.")
        else:
            st.dataframe(style_afe_summary(filtered), hide_index=True, use_container_width=True, height=500)

    with tab_active_7d:
        active_7d = summary_view[summary_view["engagement"] == "Active (7d)"].reset_index(drop=True)
        filtered = filter_by_afe_search(active_7d, afe_search)
        if filtered.empty:
            st.info("No active (7d) engagements found.")
        else:
            st.dataframe(style_afe_summary(filtered), hide_index=True, use_container_width=True, height=500)

    st.markdown("---")
    st.subheader("Use Case Detail")

    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    with col_f1:
        detail_specialists = ["All"] + sorted(filtered_expanded["SPECIALIST"].unique().tolist())
        selected_afe = st.selectbox("Filter by AFE", detail_specialists, key="afe_bw_specialist")
    with col_f2:
        quarter_options = ["Both Quarters", cq_label, nq_label]
        quarter_filter = st.selectbox("Filter by Quarter", quarter_options, key="afe_bw_quarter")
    with col_f3:
        engagement_filter = st.selectbox("Filter by Engagement", ["All", "Active (7d)", "Active (14d)", "Stale (comments but older)", "No Comments"], key="afe_bw_engagement")
    with col_f4:
        de_features = sorted(set(
            kf.strip()
            for kfs in filtered_expanded["KEY_FEATURES"].dropna()
            for kf in kfs.split(";")
            if kf.strip() and kf.strip().startswith("DE -")
        ))
        selected_detail_cats = st.multiselect("Key Feature", de_features, key="afe_bw_detail_cats")

    if selected_afe == "All":
        detail = filtered_expanded.copy()
    else:
        detail = filtered_expanded[filtered_expanded["SPECIALIST"] == selected_afe].copy()

    if quarter_filter == cq_label:
        detail = detail[detail["IN_CQ"]].copy()
    elif quarter_filter == nq_label:
        detail = detail[detail["IN_NQ"]].copy()

    if engagement_filter == "Active (7d)":
        detail = detail[detail["RECENTLY_UPDATED_7D"] == True]
    elif engagement_filter == "Active (14d)":
        detail = detail[detail["RECENTLY_UPDATED_14D"] == True]
    elif engagement_filter == "Stale (comments but older)":
        detail = detail[(detail["HAS_SPECIALIST_COMMENTS"] == True) & (detail["RECENTLY_UPDATED_14D"] == False)]
    elif engagement_filter == "No Comments":
        detail = detail[detail["HAS_SPECIALIST_COMMENTS"] == False]

    if selected_detail_cats:
        detail = detail[detail["KEY_FEATURES"].apply(
            lambda x: any(kf.strip() in selected_detail_cats for kf in (x or "").split(";"))
        )].reset_index(drop=True)

    detail["SFDC"] = detail["USE_CASE_ID"].apply(lambda uid: f"{SFDC_BASE}/{uid}/view" if uid else "")

    def uc_engagement(row):
        if row["RECENTLY_UPDATED_7D"]:
            return "Active (7d)"
        elif row["RECENTLY_UPDATED_14D"]:
            return "Active (14d)"
        elif row["HAS_SPECIALIST_COMMENTS"]:
            return "Stale"
        return "None"

    detail["Engagement"] = detail.apply(uc_engagement, axis=1)
    detail = detail.reset_index(drop=True)

    detail_display = detail[["SPECIALIST", "ENGINEER", "ACCOUNT_NAME", "USE_CASE_NAME", "USE_CASE_STAGE", "USE_CASE_STATUS", "USE_CASE_EACV", "ENGAGEMENT_DRIVERS", "SPECIALISTS", "THEATER_NAME", "PRODUCT_CATEGORIES", "Engagement", "DECISION_DATE", "GO_LIVE_DATE", "LAST_MODIFIED_DATE", "SFDC"]].rename(columns={
        "SPECIALIST": "AFE", "ACCOUNT_NAME": "Account", "USE_CASE_NAME": "Use Case",
        "USE_CASE_STAGE": "Stage", "USE_CASE_STATUS": "Status", "USE_CASE_EACV": "EACV",
        "ENGAGEMENT_DRIVERS": "Drivers", "SPECIALISTS": "Specialists", "THEATER_NAME": "Theater", "PRODUCT_CATEGORIES": "Products",
        "LAST_MODIFIED_DATE": "Last Modified", "DECISION_DATE": "Decision Date", "GO_LIVE_DATE": "Go Live Date",
    })

    DETAIL_COLORS = {
        "Active (7d)": "background-color: #0a2e1a; color: #4ade80",
        "Active (14d)": "background-color: #1a3a2a; color: #22c55e",
        "Stale": "background-color: #2e2400; color: #f59e0b",
        "None": "background-color: #3d1111; color: #ff6b6b",
    }

    def style_detail_row(row):
        style = DETAIL_COLORS.get(row["Engagement"], "")
        return [style] * len(row)

    styled_detail = detail_display.style.apply(style_detail_row, axis=1).format({"EACV": "${:,.0f}"})

    st.dataframe(
        styled_detail, hide_index=True, use_container_width=True, height=500,
        column_config={"SFDC": st.column_config.LinkColumn("SFDC", display_text=r".*/(.+)/view")},
    )
    st.caption(f"Showing {len(detail):,} use cases · Source: MDM.MDM_INTERFACES.DIM_USE_CASE · Role: SE - Workload FCTO (AFE)")

    st.markdown("---")
    st.subheader("AI Engagement Summary")
    st.caption("Analyze specialist comments for selected use cases — what's working and next action items")

    select_all = st.checkbox("Select All Use Cases", value=False, key="afe_bw_select_all")

    if not detail.empty and not select_all:
        uc_options = [f"{row['ACCOUNT_NAME']} — {row['USE_CASE_NAME']} ({row['SPECIALIST']})" for _, row in detail.iterrows()]
        selected_ucs = st.multiselect(
            f"Or pick specific use cases ({len(detail)} available)",
            uc_options,
            key="afe_bw_uc_select",
        )
    else:
        uc_options = []
        selected_ucs = []

    has_selection = select_all or len(selected_ucs) > 0

    if st.button("Generate AI Summary", key="afe_bw_ai_summary_btn", type="primary", disabled=not has_selection):
        if select_all:
            selected_detail = detail
        else:
            selected_indices = [uc_options.index(uc) for uc in selected_ucs]
            selected_detail = detail.iloc[selected_indices]
        comments_data = selected_detail[selected_detail["SPECIALIST_COMMENTS"].notna() & (selected_detail["SPECIALIST_COMMENTS"].str.strip() != "")]

        if comments_data.empty:
            st.warning("No specialist comments found for the selected use cases.")
        else:
            comments_sorted = comments_data.sort_values("USE_CASE_EACV", ascending=False)
            max_comments = 40
            truncated = len(comments_sorted) > max_comments
            comments_subset = comments_sorted.head(max_comments)
            comment_lines = []
            for _, row in comments_subset.iterrows():
                comment_text = str(row["SPECIALIST_COMMENTS"]).strip()[:400]
                row_features = str(row.get("KEY_FEATURES", "") or "").strip()
                row_drivers = str(row.get("ENGAGEMENT_DRIVERS", "") or "SE").strip()
                comment_lines.append(
                    f"- **{row['ACCOUNT_NAME']}** | {row['USE_CASE_NAME']} | SDM: {row['SPECIALIST']} | "
                    f"Stage: {row['USE_CASE_STAGE']} | EACV: ${row['USE_CASE_EACV']:,.0f} | Features: {row_features} | Drivers: {row_drivers}\n"
                    f"  Comment: {comment_text}"
                )
            comments_block = "\n".join(comment_lines)

            filter_context_parts = []
            if selected_detail_cats:
                filter_context_parts.append(f"Key Feature filter: {', '.join(selected_detail_cats)}")
            if selected_afe != "All":
                filter_context_parts.append(f"Specialist: {selected_afe}")
            if engagement_filter != "All":
                filter_context_parts.append(f"Engagement: {engagement_filter}")
            if quarter_filter != "Both Quarters":
                filter_context_parts.append(f"Quarter: {quarter_filter}")
            filter_context = ""
            if filter_context_parts:
                filter_context = (
                    "ACTIVE FILTERS: " + " | ".join(filter_context_parts) + "\n"
                    "IMPORTANT: Focus your analysis specifically on the filtered context above. "
                    "For example, if filtered to a specific feature like Iceberg or Lakehouse, center your analysis on that technology area — "
                    "what adoption patterns, blockers, and next steps are specific to that feature.\n\n"
                )

            ai_prompt = (
                "You are a specialist engagement analyst. Analyze the following specialist comments from Snowflake use case engagements.\n\n"
                f"{filter_context}"
                f"There are {len(comments_data)} use cases with specialist comments (out of {len(selected_detail)} selected).\n\n"
                f"SPECIALIST COMMENTS:\n{comments_block}\n\n"
                "Provide a structured summary with these sections:\n"
                "1. **What's Working** — Identify positive themes, successful patterns, products gaining traction, and strong engagements from the comments.\n"
                "2. **Key Risks / Blockers** — Any concerns, delays, blockers, or at-risk engagements mentioned in comments.\n"
                "3. **Next Action Items** — Specific recommended next steps based on the comments. Be actionable and reference account names.\n\n"
                "IMPORTANT: Always include EACV dollar amounts when referencing accounts — quantify pipeline at risk, wins, and action items by dollar value.\n\n"
                "Be concise, data-driven, and reference specific accounts/use cases. Format with markdown."
            )
            with st.spinner("Analyzing specialist comments with Cortex AI..."):
                ai_result = generate_ai_summary(ai_prompt)
            st.session_state['afe_bw_ai_result'] = ai_result
            st.session_state['afe_bw_ai_caption'] = f"Analyzed {len(comments_data)} use cases with comments out of {len(selected_detail)} selected" + (f" (top {max_comments} by EACV shown to AI)" if truncated else "")

    if st.session_state.get('afe_bw_ai_result'):
        render_rich_ai_summary(st.session_state['afe_bw_ai_result'], summary_id="afe_bw_ai_summary")
        st.caption(st.session_state.get('afe_bw_ai_caption', ''))
        if st.button("Clear Summary", key="afe_bw_clear_btn"):
            st.session_state['afe_bw_ai_result'] = None
            st.rerun()

    if not has_selection and not detail.empty:
        st.info("Check 'Select All Use Cases' or pick specific use cases above to generate an AI summary of specialist comments.")

    st.markdown("---")
    st.subheader("Ask a Question")
    st.caption("Ask questions about the engagement data using Snowflake Cortex")

    if "afe_bw_chat_messages" not in st.session_state:
        st.session_state.afe_bw_chat_messages = []

    def build_afe_data_context():
        ctx_lines = []
        ctx_lines.append(f"Dashboard: DE Team Engagement Tracker for {team_label}")
        ctx_lines.append(f"Date: {today_dt}, Fiscal quarters: {cq_label} ({cq_start} to {cq_end}), {nq_label} ({nq_start} to {nq_end})")
        ctx_lines.append(f"Total AFEs: {total_specialists}, Active: {active_count}, Stale: {stale_count}, Not Active: {not_active_count}, Total Use Cases: {total_uc}")
        ctx_lines.append("")
        ctx_lines.append("AFE Summary (Name | Engagement | Total Use Cases | Active(7d) | With Comments | Without Comments | Coverage% | Total EACV):")
        for _, row in afe_summary.iterrows():
            ctx_lines.append(f"  {row['SPECIALIST']} | {row['engagement']} | {int(row['total_use_cases'])} | {int(row['active_use_cases'])} | {int(row['with_comments'])} | {int(row['without_comments'])} | {row['coverage_pct']}% | ${row['total_eacv']:,.0f}")
        ctx_lines.append("")
        top_uc = expanded.nlargest(50, "USE_CASE_EACV")[["SPECIALIST", "ACCOUNT_NAME", "USE_CASE_NAME", "USE_CASE_EACV", "USE_CASE_STAGE", "USE_CASE_STATUS", "DECISION_DATE", "GO_LIVE_DATE"]].to_string(index=False)
        ctx_lines.append(f"Top 50 use cases by EACV:\n{top_uc}")
        return "\n".join(ctx_lines)

    afe_bw_question = st.text_input("e.g. Who has the most use cases without comments?", key="afe_bw_question")
    if st.button("Ask", key="afe_bw_ask_btn") and afe_bw_question:
        st.session_state.afe_bw_chat_messages.append({"role": "user", "content": afe_bw_question})
        with st.spinner("Thinking..."):
            data_context = build_afe_data_context()
            full_prompt = (
                "You are a helpful data analyst assistant for the DE Team Engagement Tracker dashboard. "
                "Answer questions using ONLY the data provided below. Be concise and specific. Use numbers and names from the data. "
                "If the data doesn't contain enough information to answer, say so.\n\n"
                f"DATA:\n{data_context}\n\nQuestion: {afe_bw_question}"
            )
            answer = generate_ai_summary(full_prompt)
        st.session_state.afe_bw_chat_messages.append({"role": "assistant", "content": answer})

    for msg in st.session_state.afe_bw_chat_messages:
        if msg["role"] == "user":
            st.markdown(f"**You:** {msg['content']}")
        else:
            st.markdown(f"**Cortex:** {escape_latex(msg['content'])}")

if active_tab == "Services Team Commentry":
    st.subheader("Services Team Engagement Tracker")
    with st.spinner("Loading engagement data..."):
        svc_org = load_services_org()
        svc_team_members = set(svc_org["NAME"].str.strip().tolist())
        svc_data = load_services_use_cases(list(svc_team_members))

    svc_driver_options = ["AFE", "Platform Specialist", "Partner", "Services", "Industry"]
    svc_driver_filter = st.multiselect("Engagement Driver", svc_driver_options, key="svc_bw_driver_filter")

    svc_combined_members = svc_team_members
    svc_combined_data = svc_data.copy()

    if svc_driver_filter and not svc_combined_data.empty and 'ENGAGEMENT_DRIVERS' in svc_combined_data.columns:
        svc_drv_mask = pd.Series(False, index=svc_combined_data.index)
        for d in svc_driver_filter:
            svc_drv_mask = svc_drv_mask | svc_combined_data['ENGAGEMENT_DRIVERS'].fillna('').str.contains(d, case=False, na=False)
        svc_combined_data = svc_combined_data[svc_drv_mask].reset_index(drop=True)

    svc_cutoff_7d = date.today() - timedelta(days=7)
    svc_cutoff_14d = date.today() - timedelta(days=14)
    svc_today_dt = date.today()
    svc_fy_year = svc_today_dt.year if svc_today_dt.month >= 2 else svc_today_dt.year - 1
    svc_fy_q_month = ((svc_today_dt.month - 2) % 12) // 3 * 3 + 2
    svc_fy_q_year = svc_fy_year if svc_fy_q_month >= 2 else svc_fy_year + 1
    svc_cq_start = date(svc_fy_q_year, svc_fy_q_month, 1)
    svc_cq_end = afe_add_months(svc_cq_start, 3) - timedelta(days=1)
    svc_nq_start = svc_cq_end + timedelta(days=1)
    svc_nq_end = afe_add_months(svc_nq_start, 3) - timedelta(days=1)
    svc_fy_q_num = ((svc_today_dt.month - 2) % 12) // 3 + 1
    svc_cq_label = f"This Quarter (FY{svc_fy_year % 100 + 1} Q{svc_fy_q_num})"
    svc_nq_num = svc_fy_q_num % 4 + 1
    svc_nq_label = f"Next Quarter (FY{svc_fy_year % 100 + 1} Q{svc_nq_num})"

    svc_expanded = svc_combined_data.copy()
    if not svc_expanded.empty:
        svc_expanded["SPECIALIST"] = svc_expanded["SPECIALIST"].str.strip()
        svc_expanded = svc_expanded[svc_expanded["SPECIALIST"].isin(svc_combined_members)].reset_index(drop=True)
        svc_expanded = svc_expanded[~svc_expanded["USE_CASE_STAGE"].isin(["8 - Use Case Lost"])].reset_index(drop=True)

        svc_expanded["LAST_MODIFIED_DATE_DT"] = pd.to_datetime(svc_expanded["LAST_MODIFIED_DATE"], errors="coerce")
        svc_expanded["LAST_IMPLEMENTATION_COMMENT_DATE_DT"] = pd.to_datetime(svc_expanded["LAST_IMPLEMENTATION_COMMENT_DATE"], errors="coerce")
        svc_expanded["DECISION_DATE_DT"] = pd.to_datetime(svc_expanded["DECISION_DATE"], errors="coerce").dt.date
        svc_expanded["GO_LIVE_DATE_DT"] = pd.to_datetime(svc_expanded["GO_LIVE_DATE"], errors="coerce").dt.date
        svc_expanded["HAS_IMPLEMENTATION_COMMENTS"] = svc_expanded["HAS_IMPLEMENTATION_COMMENTS"].astype(bool)

        svc_expanded["IN_CQ"] = (
            ((svc_expanded["DECISION_DATE_DT"] >= svc_cq_start) & (svc_expanded["DECISION_DATE_DT"] <= svc_cq_end))
            | ((svc_expanded["GO_LIVE_DATE_DT"] >= svc_cq_start) & (svc_expanded["GO_LIVE_DATE_DT"] <= svc_cq_end))
        )
        svc_expanded["IN_NQ"] = (
            ((svc_expanded["DECISION_DATE_DT"] >= svc_nq_start) & (svc_expanded["DECISION_DATE_DT"] <= svc_nq_end))
            | ((svc_expanded["GO_LIVE_DATE_DT"] >= svc_nq_start) & (svc_expanded["GO_LIVE_DATE_DT"] <= svc_nq_end))
        )
        svc_expanded["RECENTLY_UPDATED_7D"] = (
            svc_expanded["HAS_IMPLEMENTATION_COMMENTS"]
            & svc_expanded["LAST_IMPLEMENTATION_COMMENT_DATE_DT"].notna()
            & (svc_expanded["LAST_IMPLEMENTATION_COMMENT_DATE_DT"].dt.date >= svc_cutoff_7d)
        )
        svc_expanded["RECENTLY_UPDATED_14D"] = (
            svc_expanded["HAS_IMPLEMENTATION_COMMENTS"]
            & svc_expanded["LAST_IMPLEMENTATION_COMMENT_DATE_DT"].notna()
            & (svc_expanded["LAST_IMPLEMENTATION_COMMENT_DATE_DT"].dt.date >= svc_cutoff_14d)
        )

    if not svc_expanded.empty:
        svc_afe_summary = svc_expanded.groupby("SPECIALIST").agg(
            total_use_cases=("USE_CASE_ID", "count"),
            with_comments=("HAS_IMPLEMENTATION_COMMENTS", "sum"),
            active_7d=("RECENTLY_UPDATED_7D", "sum"),
            active_14d=("RECENTLY_UPDATED_14D", "sum"),
            total_eacv=("USE_CASE_EACV", "sum"),
        ).reset_index()
        svc_afe_summary["without_comments"] = svc_afe_summary["total_use_cases"] - svc_afe_summary["with_comments"]
        svc_afe_summary["coverage_pct"] = (svc_afe_summary["with_comments"] / svc_afe_summary["total_use_cases"] * 100).round(1)

        def svc_classify_engagement(row):
            if row["active_7d"] > 0:
                return "Active (7d)"
            elif row["active_14d"] > 0:
                return "Active (14d)"
            elif row["with_comments"] > 0:
                return "Stale Engagement"
            else:
                return "Not Active Engagement"

        svc_afe_summary["engagement"] = svc_afe_summary.apply(svc_classify_engagement, axis=1)
        svc_engagement_order = {"Not Active Engagement": 0, "Stale Engagement": 1, "Active (14d)": 2, "Active (7d)": 3}
        svc_afe_summary["_sort"] = svc_afe_summary["engagement"].map(svc_engagement_order)
        svc_afe_summary = svc_afe_summary.sort_values(["_sort", "total_use_cases"], ascending=[True, False]).reset_index(drop=True)

        svc_driver_label = ", ".join(svc_driver_filter) if svc_driver_filter else "All Drivers"
        st.caption(f"{svc_driver_label} Engagements · {svc_cq_label}: {svc_cq_start.strftime('%b %d')} – {svc_cq_end.strftime('%b %d, %Y')} · {svc_nq_label}: {svc_nq_start.strftime('%b %d')} – {svc_nq_end.strftime('%b %d, %Y')}")

        svc_total_specialists = len(svc_afe_summary)
        svc_active_7d_count = int((svc_afe_summary["engagement"] == "Active (7d)").sum())
        svc_active_14d_count = int((svc_afe_summary["engagement"] == "Active (14d)").sum())
        svc_stale_count = int((svc_afe_summary["engagement"] == "Stale Engagement").sum())
        svc_not_active_count = int((svc_afe_summary["engagement"] == "Not Active Engagement").sum())
        svc_total_uc = len(svc_expanded)

        sc1, sc2, sc3, sc4, sc5, sc6 = st.columns(6)
        with sc1:
            st.markdown(f'<div class="metric-card metric-neutral"><div class="label">Total SDMs</div><div class="value">{svc_total_specialists}</div></div>', unsafe_allow_html=True)
        with sc2:
            st.markdown(f'<div class="metric-card metric-green"><div class="label">Active (7d)</div><div class="value">{svc_active_7d_count}</div></div>', unsafe_allow_html=True)
        with sc3:
            st.markdown(f'<div class="metric-card" style="background-color:#1a3a2a;border-left:4px solid #22c55e"><div class="label">Active (14d)</div><div class="value">{svc_active_14d_count}</div></div>', unsafe_allow_html=True)
        with sc4:
            st.markdown(f'<div class="metric-card metric-amber"><div class="label">Stale Engagement</div><div class="value">{svc_stale_count}</div></div>', unsafe_allow_html=True)
        with sc5:
            st.markdown(f'<div class="metric-card metric-red"><div class="label">Not Active</div><div class="value">{svc_not_active_count}</div></div>', unsafe_allow_html=True)
        with sc6:
            st.markdown(f'<div class="metric-card metric-neutral"><div class="label">Total Use Cases</div><div class="value">{svc_total_uc:,}</div></div>', unsafe_allow_html=True)

        st.markdown("")

        svc_filter_cols = st.columns([2, 3])
        with svc_filter_cols[0]:
            svc_afe_search = st.text_input("Search SDM", placeholder="Type a name to search...", key="svc_bw_search")
        with svc_filter_cols[1]:
            svc_all_key_features = ["DE - Openflow", "DE - Openflow Oracle", "DE - Iceberg", "DE - Snowpark DE", "DE - Snowpark Connect", "DE - Dynamic Tables", "DE - Snowpipe Streaming", "DE - Snowpipe", "DE - Serverless Task", "DE - Connectors", "DE - dbt Projects", "DE - SAP Integration", "DE - Basic"]
            svc_selected_key_features = st.multiselect("Filter by Key Feature", svc_all_key_features, key="svc_bw_key_features")

        if svc_selected_key_features:
            svc_kf_filter_mask = svc_expanded["KEY_FEATURES"].apply(
                lambda x: any(kf.strip() in svc_selected_key_features for kf in (x or "").split(";"))
            )
            svc_filtered_expanded = svc_expanded[svc_kf_filter_mask]
            svc_filtered_specialists = set(svc_filtered_expanded["SPECIALIST"].unique())
            svc_summary_view = svc_afe_summary[svc_afe_summary["SPECIALIST"].isin(svc_filtered_specialists)].reset_index(drop=True)
        else:
            svc_filtered_expanded = svc_expanded
            svc_summary_view = svc_afe_summary

        svc_sv_total = len(svc_summary_view)
        svc_sv_active_7d = int((svc_summary_view["engagement"] == "Active (7d)").sum())
        svc_sv_active_14d = int((svc_summary_view["engagement"] == "Active (14d)").sum())
        svc_sv_stale = int((svc_summary_view["engagement"] == "Stale Engagement").sum())
        svc_sv_not_active = int((svc_summary_view["engagement"] == "Not Active Engagement").sum())

        SVC_ENGAGEMENT_COLORS = {
            "Active (7d)": "background-color: #0a2e1a; color: #4ade80",
            "Active (14d)": "background-color: #1a3a2a; color: #22c55e",
            "Stale Engagement": "background-color: #2e2400; color: #f59e0b",
            "Not Active Engagement": "background-color: #3d1111; color: #ff6b6b",
        }

        def svc_style_afe_summary(df):
            display = df[["SPECIALIST", "engagement", "total_use_cases", "active_7d", "active_14d", "with_comments", "without_comments", "coverage_pct", "total_eacv"]].copy()
            display.columns = ["SDM", "Engagement", "Total Use Cases", "Active (7d)", "Active (14d)", "With Comments", "Without Comments", "Coverage %", "Total EACV"]
            def color_row(row):
                style = SVC_ENGAGEMENT_COLORS.get(row["Engagement"], "")
                return [style] * len(row)
            styled = display.style.apply(color_row, axis=1).format({"Coverage %": "{:.1f}%", "Total EACV": "${:,.0f}"})
            return styled

        def svc_filter_by_afe_search(df, query):
            if query:
                return df[df["SPECIALIST"].str.contains(query, case=False, na=False)]
            return df

        svc_tab_all, svc_tab_not, svc_tab_stale, svc_tab_active_14d, svc_tab_active_7d = st.tabs([
            f"All ({svc_sv_total})", f"Not Active ({svc_sv_not_active})",
            f"Stale ({svc_sv_stale})", f"Active 14d ({svc_sv_active_14d})", f"Active 7d ({svc_sv_active_7d})",
        ])

        with svc_tab_all:
            svc_filtered = svc_filter_by_afe_search(svc_summary_view, svc_afe_search)
            st.dataframe(svc_style_afe_summary(svc_filtered), hide_index=True, use_container_width=True, height=500)

        with svc_tab_not:
            svc_not_active_df = svc_summary_view[svc_summary_view["engagement"] == "Not Active Engagement"].reset_index(drop=True)
            svc_filtered = svc_filter_by_afe_search(svc_not_active_df, svc_afe_search)
            if svc_filtered.empty:
                st.success("All SDMs have engagement!")
            else:
                st.dataframe(svc_style_afe_summary(svc_filtered), hide_index=True, use_container_width=True, height=500)

        with svc_tab_stale:
            svc_stale_df = svc_summary_view[svc_summary_view["engagement"] == "Stale Engagement"].reset_index(drop=True)
            svc_filtered = svc_filter_by_afe_search(svc_stale_df, svc_afe_search)
            if svc_filtered.empty:
                st.info("No stale engagements found.")
            else:
                st.dataframe(svc_style_afe_summary(svc_filtered), hide_index=True, use_container_width=True, height=500)

        with svc_tab_active_14d:
            svc_active_14d_df = svc_summary_view[svc_summary_view["engagement"] == "Active (14d)"].reset_index(drop=True)
            svc_filtered = svc_filter_by_afe_search(svc_active_14d_df, svc_afe_search)
            if svc_filtered.empty:
                st.info("No active (14d) engagements found.")
            else:
                st.dataframe(svc_style_afe_summary(svc_filtered), hide_index=True, use_container_width=True, height=500)

        with svc_tab_active_7d:
            svc_active_7d_df = svc_summary_view[svc_summary_view["engagement"] == "Active (7d)"].reset_index(drop=True)
            svc_filtered = svc_filter_by_afe_search(svc_active_7d_df, svc_afe_search)
            if svc_filtered.empty:
                st.info("No active (7d) engagements found.")
            else:
                st.dataframe(svc_style_afe_summary(svc_filtered), hide_index=True, use_container_width=True, height=500)

        st.markdown("---")
        st.subheader("Use Case Detail")

        svc_col_f1, svc_col_f2, svc_col_f3, svc_col_f4 = st.columns(4)
        with svc_col_f1:
            svc_detail_specialists = ["All"] + sorted(svc_filtered_expanded["SPECIALIST"].unique().tolist())
            svc_selected_afe = st.selectbox("Filter by SDM", svc_detail_specialists, key="svc_bw_specialist")
        with svc_col_f2:
            svc_quarter_options = ["Both Quarters", svc_cq_label, svc_nq_label]
            svc_quarter_filter = st.selectbox("Filter by Quarter", svc_quarter_options, key="svc_bw_quarter")
        with svc_col_f3:
            svc_engagement_filter = st.selectbox("Filter by Engagement", ["All", "Active (7d)", "Active (14d)", "Stale (comments but older)", "No Comments"], key="svc_bw_engagement")
        with svc_col_f4:
            svc_de_features = sorted(set(
                kf.strip()
                for kfs in svc_filtered_expanded["KEY_FEATURES"].dropna()
                for kf in kfs.split(";")
                if kf.strip() and kf.strip().startswith("DE -")
            ))
            svc_selected_detail_cats = st.multiselect("Key Feature", svc_de_features, key="svc_bw_detail_cats")

        if svc_selected_afe == "All":
            svc_detail = svc_filtered_expanded.copy()
        else:
            svc_detail = svc_filtered_expanded[svc_filtered_expanded["SPECIALIST"] == svc_selected_afe].copy()

        if svc_quarter_filter == svc_cq_label:
            svc_detail = svc_detail[svc_detail["IN_CQ"]].copy()
        elif svc_quarter_filter == svc_nq_label:
            svc_detail = svc_detail[svc_detail["IN_NQ"]].copy()

        if svc_engagement_filter == "Active (7d)":
            svc_detail = svc_detail[svc_detail["RECENTLY_UPDATED_7D"] == True]
        elif svc_engagement_filter == "Active (14d)":
            svc_detail = svc_detail[svc_detail["RECENTLY_UPDATED_14D"] == True]
        elif svc_engagement_filter == "Stale (comments but older)":
            svc_detail = svc_detail[(svc_detail["HAS_IMPLEMENTATION_COMMENTS"] == True) & (svc_detail["RECENTLY_UPDATED_14D"] == False)]
        elif svc_engagement_filter == "No Comments":
            svc_detail = svc_detail[svc_detail["HAS_IMPLEMENTATION_COMMENTS"] == False]

        if svc_selected_detail_cats:
            svc_detail = svc_detail[svc_detail["KEY_FEATURES"].apply(
                lambda x: any(kf.strip() in svc_selected_detail_cats for kf in (x or "").split(";"))
            )].reset_index(drop=True)

        svc_detail["SFDC"] = svc_detail["USE_CASE_ID"].apply(lambda uid: f"{SFDC_BASE}/{uid}/view" if uid else "")

        def svc_uc_engagement(row):
            if row["RECENTLY_UPDATED_7D"]:
                return "Active (7d)"
            elif row["RECENTLY_UPDATED_14D"]:
                return "Active (14d)"
            elif row["HAS_IMPLEMENTATION_COMMENTS"]:
                return "Stale"
            return "None"

        svc_detail["Engagement"] = svc_detail.apply(svc_uc_engagement, axis=1)
        svc_detail = svc_detail.reset_index(drop=True)

        svc_detail_display = svc_detail[["SPECIALIST", "ENGINEER", "ACCOUNT_NAME", "USE_CASE_NAME", "USE_CASE_STAGE", "USE_CASE_STATUS", "USE_CASE_EACV", "ENGAGEMENT_DRIVERS", "SPECIALISTS", "THEATER_NAME", "PRODUCT_CATEGORIES", "Engagement", "DECISION_DATE", "GO_LIVE_DATE", "LAST_MODIFIED_DATE", "SFDC"]].rename(columns={
            "SPECIALIST": "SDM", "ACCOUNT_NAME": "Account", "USE_CASE_NAME": "Use Case",
            "USE_CASE_STAGE": "Stage", "USE_CASE_STATUS": "Status", "USE_CASE_EACV": "EACV",
            "ENGAGEMENT_DRIVERS": "Drivers", "SPECIALISTS": "Specialists", "THEATER_NAME": "Theater", "PRODUCT_CATEGORIES": "Products",
            "LAST_MODIFIED_DATE": "Last Modified", "DECISION_DATE": "Decision Date", "GO_LIVE_DATE": "Go Live Date",
        })

        SVC_DETAIL_COLORS = {
            "Active (7d)": "background-color: #0a2e1a; color: #4ade80",
            "Active (14d)": "background-color: #1a3a2a; color: #22c55e",
            "Stale": "background-color: #2e2400; color: #f59e0b",
            "None": "background-color: #3d1111; color: #ff6b6b",
        }

        def svc_style_detail_row(row):
            style = SVC_DETAIL_COLORS.get(row["Engagement"], "")
            return [style] * len(row)

        svc_styled_detail = svc_detail_display.style.apply(svc_style_detail_row, axis=1).format({"EACV": "${:,.0f}"})

        st.dataframe(
            svc_styled_detail, hide_index=True, use_container_width=True, height=500,
            column_config={"SFDC": st.column_config.LinkColumn("SFDC", display_text=r".*/(.+)/view")},
        )
        st.caption(f"Showing {len(svc_detail):,} use cases · Org: Ganesh Krishnamurthy · Source: MDM.MDM_INTERFACES.DIM_USE_CASE")

        st.markdown("---")
        st.subheader("AI Engagement Summary")
        st.caption("Analyze implementation comments for selected use cases — what's working and next action items")

        svc_select_all = st.checkbox("Select All Use Cases", value=False, key="svc_bw_select_all")

        if not svc_detail.empty and not svc_select_all:
            svc_uc_options = [f"{row['ACCOUNT_NAME']} — {row['USE_CASE_NAME']} ({row['SPECIALIST']})" for _, row in svc_detail.iterrows()]
            svc_selected_ucs = st.multiselect(
                f"Or pick specific use cases ({len(svc_detail)} available)",
                svc_uc_options,
                key="svc_bw_uc_select",
            )
        else:
            svc_uc_options = []
            svc_selected_ucs = []

        svc_has_selection = svc_select_all or len(svc_selected_ucs) > 0

        if st.button("Generate AI Summary", key="svc_bw_ai_summary_btn", type="primary", disabled=not svc_has_selection):
            if svc_select_all:
                svc_selected_detail = svc_detail
            else:
                svc_selected_indices = [svc_uc_options.index(uc) for uc in svc_selected_ucs]
                svc_selected_detail = svc_detail.iloc[svc_selected_indices]
            svc_comments_data = svc_selected_detail[svc_selected_detail["IMPLEMENTATION_COMMENTS"].notna() & (svc_selected_detail["IMPLEMENTATION_COMMENTS"].str.strip() != "")]

            if svc_comments_data.empty:
                st.warning("No implementation comments found for the selected use cases.")
            else:
                svc_comments_sorted = svc_comments_data.sort_values("USE_CASE_EACV", ascending=False)
                svc_max_comments = 40
                svc_truncated = len(svc_comments_sorted) > svc_max_comments
                svc_comments_subset = svc_comments_sorted.head(svc_max_comments)
                svc_comment_lines = []
                for _, row in svc_comments_subset.iterrows():
                    comment_text = str(row["IMPLEMENTATION_COMMENTS"]).strip()[:400]
                    row_features = str(row.get("KEY_FEATURES", "") or "").strip()
                    row_drivers = str(row.get("ENGAGEMENT_DRIVERS", "") or "SE").strip()
                    svc_comment_lines.append(
                        f"- **{row['ACCOUNT_NAME']}** | {row['USE_CASE_NAME']} | AFE: {row['SPECIALIST']} | "
                        f"Stage: {row['USE_CASE_STAGE']} | EACV: ${row['USE_CASE_EACV']:,.0f} | Features: {row_features} | Drivers: {row_drivers}\n"
                        f"  Comment: {comment_text}"
                    )
                svc_comments_block = "\n".join(svc_comment_lines)

                svc_filter_context_parts = []
                if svc_selected_detail_cats:
                    svc_filter_context_parts.append(f"Key Feature filter: {', '.join(svc_selected_detail_cats)}")
                if svc_selected_afe != "All":
                    svc_filter_context_parts.append(f"Specialist: {svc_selected_afe}")
                if svc_engagement_filter != "All":
                    svc_filter_context_parts.append(f"Engagement: {svc_engagement_filter}")
                if svc_quarter_filter != "Both Quarters":
                    svc_filter_context_parts.append(f"Quarter: {svc_quarter_filter}")
                svc_filter_context_parts.append("Engagement Driver: Services only")
                svc_filter_context = ""
                if svc_filter_context_parts:
                    svc_filter_context = (
                        "ACTIVE FILTERS: " + " | ".join(svc_filter_context_parts) + "\n"
                        "IMPORTANT: Focus your analysis specifically on the filtered context above. "
                        "These are Services-driven engagements — center your analysis on services delivery patterns, "
                        "what adoption patterns, blockers, and next steps are specific to services engagements.\n\n"
                    )

                svc_ai_prompt = (
                    "You are a specialist engagement analyst. Analyze the following specialist comments from Snowflake use case engagements.\n\n"
                    f"{svc_filter_context}"
                    f"There are {len(svc_comments_data)} use cases with specialist comments (out of {len(svc_selected_detail)} selected).\n\n"
                    f"SPECIALIST COMMENTS:\n{svc_comments_block}\n\n"
                    "Provide a structured summary with these sections:\n"
                    "1. **What's Working** — Identify positive themes, successful patterns, products gaining traction, and strong engagements from the comments.\n"
                    "2. **Key Risks / Blockers** — Any concerns, delays, blockers, or at-risk engagements mentioned in comments.\n"
                    "3. **Next Action Items** — Specific recommended next steps based on the comments. Be actionable and reference account names.\n\n"
                    "IMPORTANT: Always include EACV dollar amounts when referencing accounts — quantify pipeline at risk, wins, and action items by dollar value.\n\n"
                    "Be concise, data-driven, and reference specific accounts/use cases. Format with markdown."
                )
                with st.spinner("Analyzing specialist comments with Cortex AI..."):
                    svc_ai_result = generate_ai_summary(svc_ai_prompt)
                st.session_state['svc_bw_ai_result'] = svc_ai_result
                st.session_state['svc_bw_ai_caption'] = f"Analyzed {len(svc_comments_data)} use cases with comments out of {len(svc_selected_detail)} selected" + (f" (top {svc_max_comments} by EACV shown to AI)" if svc_truncated else "")

        if st.session_state.get('svc_bw_ai_result'):
            render_rich_ai_summary(st.session_state['svc_bw_ai_result'], summary_id="svc_bw_ai_summary")
            st.caption(st.session_state.get('svc_bw_ai_caption', ''))
            if st.button("Clear Summary", key="svc_bw_clear_btn"):
                st.session_state['svc_bw_ai_result'] = None
                st.rerun()

        if not svc_has_selection and not svc_detail.empty:
            st.info("Check 'Select All Use Cases' or pick specific use cases above to generate an AI summary of specialist comments.")

        st.markdown("---")
        st.subheader("Ask a Question")
        st.caption("Ask questions about Services engagement data using Snowflake Cortex")

        if "svc_bw_chat_messages" not in st.session_state:
            st.session_state.svc_bw_chat_messages = []

        def svc_build_data_context():
            ctx_lines = []
            ctx_lines.append(f"Dashboard: Services Team Engagement Tracker ({svc_driver_label})")
            ctx_lines.append(f"Date: {svc_today_dt}, Fiscal quarters: {svc_cq_label} ({svc_cq_start} to {svc_cq_end}), {svc_nq_label} ({svc_nq_start} to {svc_nq_end})")
            ctx_lines.append(f"Total SDMs: {svc_total_specialists}, Active 7d: {svc_active_7d_count}, Active 14d: {svc_active_14d_count}, Stale: {svc_stale_count}, Not Active: {svc_not_active_count}, Total Use Cases: {svc_total_uc}")
            ctx_lines.append(f"Note: This view is filtered to engagement driver(s): {svc_driver_label}.")
            ctx_lines.append("")
            ctx_lines.append("AFE Summary (Name | Engagement | Total Use Cases | Active(7d) | With Comments | Without Comments | Coverage% | Total EACV):")
            for _, row in svc_afe_summary.iterrows():
                ctx_lines.append(f"  {row['SPECIALIST']} | {row['engagement']} | {int(row['total_use_cases'])} | {int(row['active_7d'])} | {int(row['with_comments'])} | {int(row['without_comments'])} | {row['coverage_pct']}% | ${row['total_eacv']:,.0f}")
            ctx_lines.append("")
            top_uc = svc_expanded.nlargest(50, "USE_CASE_EACV")[["SPECIALIST", "ACCOUNT_NAME", "USE_CASE_NAME", "USE_CASE_EACV", "USE_CASE_STAGE", "USE_CASE_STATUS", "DECISION_DATE", "GO_LIVE_DATE"]].to_string(index=False)
            ctx_lines.append(f"Top 50 use cases by EACV:\n{top_uc}")
            return "\n".join(ctx_lines)

        svc_bw_question = st.text_input("e.g. Who has the most use cases without comments?", key="svc_bw_question")
        if st.button("Ask", key="svc_bw_ask_btn") and svc_bw_question:
            st.session_state.svc_bw_chat_messages.append({"role": "user", "content": svc_bw_question})
            with st.spinner("Thinking..."):
                svc_data_context = svc_build_data_context()
                svc_full_prompt = (
                    "You are a helpful data analyst assistant for the Services Team Engagement Tracker dashboard. "
                    "Answer questions using ONLY the data provided below. Be concise and specific. Use numbers and names from the data. "
                    "If the data doesn't contain enough information to answer, say so.\n\n"
                    f"DATA:\n{svc_data_context}\n\nQuestion: {svc_bw_question}"
                )
                svc_answer = generate_ai_summary(svc_full_prompt)
            st.session_state.svc_bw_chat_messages.append({"role": "assistant", "content": svc_answer})

        for msg in st.session_state.svc_bw_chat_messages:
            if msg["role"] == "user":
                st.markdown(f"**You:** {msg['content']}")
            else:
                st.markdown(f"**Cortex:** {escape_latex(msg['content'])}")
    else:
        st.info("No Services engagement data found for the current filters.")

st.markdown("---")
st.markdown("## 💬 Ask Cortex")

def cortex_chat_query(question, df):
    if df is None or df.empty:
        return generate_ai_summary(f"Answer about DE/DL engagements: {question}"), pd.DataFrame()
    comment_cols = [c for c in ['SE_COMMENTS', 'NEXT_STEPS', 'IMPLEMENTATION_COMMENTS', 'SPECIALIST_COMMENTS', 'PARTNER_COMMENTS'] if c in df.columns]
    df_search = df.copy()
    df_search['_ALL_COMMENTS'] = ''
    for c in comment_cols:
        df_search['_ALL_COMMENTS'] = df_search['_ALL_COMMENTS'] + ' ' + df_search[c].fillna('').astype(str)
    df_search['_ALL_COMMENTS'] = df_search['_ALL_COMMENTS'].str.lower()
    df_search['_STAGE_LOWER'] = df_search['STAGE'].fillna('').astype(str).str.lower() if 'STAGE' in df_search.columns else ''
    df_search['_SEARCHABLE'] = df_search['_ALL_COMMENTS'] + ' ' + df_search['_STAGE_LOWER']
    if 'ACCOUNT_NAME' in df_search.columns:
        df_search['_SEARCHABLE'] = df_search['_SEARCHABLE'] + ' ' + df_search['ACCOUNT_NAME'].fillna('').astype(str).str.lower()
    if 'USE_CASE_NAME' in df_search.columns:
        df_search['_SEARCHABLE'] = df_search['_SEARCHABLE'] + ' ' + df_search['USE_CASE_NAME'].fillna('').astype(str).str.lower()

    keyword_prompt = f"""Extract search keywords from this question for filtering use case data. Return ONLY a comma-separated list of keywords/phrases to search for in comments and status fields. No explanation.

Question: {question}

Examples:
- "show me use cases with red flags" -> red, risk, concern, blocker, issue, delayed, not on track
- "which accounts are in discovery stage" -> discovery, 1 -
- "show me high EACV deals" -> (no keywords, this is a numeric filter)

Keywords:"""
    try:
        keywords_raw = generate_ai_summary(keyword_prompt)
        keywords = [k.strip().lower() for k in keywords_raw.split(',') if k.strip() and len(k.strip()) > 1]
    except:
        keywords = []

    if keywords:
        mask = df_search['_SEARCHABLE'].apply(lambda x: any(kw in x for kw in keywords))
        filtered = df[mask]
    else:
        filtered = df

    show_cols = [c for c in ['ACCOUNT_NAME', 'USE_CASE_NAME', 'STAGE', 'EACV', 'ENGINEER'] + comment_cols if c in filtered.columns]
    ctx_rows = filtered.head(30)
    ctx_cols = [c for c in ['ACCOUNT_NAME', 'USE_CASE_NAME', 'STAGE'] + comment_cols if c in ctx_rows.columns]
    ctx_df = ctx_rows[ctx_cols].copy()
    for c in comment_cols:
        if c in ctx_df.columns:
            ctx_df[c] = ctx_df[c].fillna('').astype(str).str[:150]
    data_str = ctx_df.to_string(index=False) if not ctx_df.empty else "No matching use cases found."

    analysis_prompt = f"""You are a DE/DL team analyst. Analyze this filtered use case data to answer the question.

DATA ({len(filtered)} matching use cases, showing up to 30):
{data_str}

QUESTION: {question}

Provide a concise, data-driven answer. Reference specific account names and findings."""
    try:
        analysis = generate_ai_summary(analysis_prompt)
    except:
        analysis = "Error generating analysis."

    return analysis, filtered[show_cols] if not filtered.empty else pd.DataFrame()

try:
    cortex_base_df = df_all if 'df_all' in dir() and df_all is not None and not df_all.empty else (df_won if 'df_won' in dir() and df_won is not None and not df_won.empty else pd.DataFrame())
except:
    cortex_base_df = pd.DataFrame()

if 'chat_messages' not in st.session_state:
    st.session_state.chat_messages = []

for msg in st.session_state.chat_messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg["role"] == "assistant" and msg.get("filtered_df") is not None and len(msg["filtered_df"]) > 0:
            st.dataframe(msg["filtered_df"], use_container_width=True, height=300)

if prompt := st.chat_input("Ask about DE/DL engagements (e.g., 'show me use cases with concerns or red flags')..."):
    st.session_state.chat_messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"): st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Analyzing use case data..."):
            analysis, filtered_df = cortex_chat_query(prompt, cortex_base_df)
        st.markdown(escape_latex(analysis))
        if not filtered_df.empty:
            st.markdown(f"**Matching Use Cases ({len(filtered_df)}):**")
            st.dataframe(filtered_df, use_container_width=True, height=300)
        st.session_state.chat_messages.append({"role": "assistant", "content": analysis, "filtered_df": filtered_df})
