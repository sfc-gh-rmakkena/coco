import streamlit as st
import pandas as pd
from datetime import date, timedelta

st.set_page_config(
    page_title="AFE Engagement Bandwidth",
    page_icon=":material/analytics:",
    layout="wide",
)

st.markdown("""
<style>
    .metric-card {
        border-radius: 12px;
        padding: 20px 24px;
        text-align: center;
    }
    .metric-card .label {
        font-size: 14px;
        font-weight: 500;
        opacity: 0.8;
        margin-bottom: 4px;
    }
    .metric-card .value {
        font-size: 36px;
        font-weight: 700;
        line-height: 1.2;
    }
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

SFDC_BASE = "https://snowforce.lightning.force.com/lightning/r/vh__Deliverable__c"


@st.cache_resource
def get_connection():
    return st.connection("snowflake")


@st.cache_data(ttl=1800)
def load_org():
    conn = get_connection()
    return conn.query("""
        SELECT EMPLOYEE_NAME AS NAME
        FROM SALES.SE_REPORTING.SE_ORG_HIERARCHY_VW
        WHERE IS_ACTIVE = TRUE
          AND (
            EMPLOYEE_NAME IN ('David Hare', 'Brendan Tisseur', 'Nagesh Cherukuri')
            OR MANAGER_NAME IN ('David Hare', 'Brendan Tisseur')
            OR FIRST_LINE_MANAGER IN ('David Hare', 'Brendan Tisseur')
          )
    """)


@st.cache_data(ttl=1800)
def load_data():
    conn = get_connection()
    return conn.query("""
        SELECT
            d.USE_CASE_ID,
            d.USE_CASE_NAME,
            d.ACCOUNT_NAME,
            d.USE_CASE_EACV,
            d.USE_CASE_STAGE,
            d.USE_CASE_STATUS,
            d.THEATER_NAME,
            d.REGION_NAME,
            ARRAY_TO_STRING(d.PRODUCT_CATEGORY_ARRAY, ', ') AS PRODUCT_CATEGORIES,
            f.value::STRING AS SPECIALIST,
            IFF(d.SPECIALIST_COMMENTS IS NOT NULL AND TRIM(d.SPECIALIST_COMMENTS) != '', TRUE, FALSE) AS HAS_SPECIALIST_COMMENTS,
            d.LAST_MODIFIED_DATE,
            d.DECISION_DATE,
            d.GO_LIVE_DATE
        FROM MDM.MDM_INTERFACES.DIM_USE_CASE d,
            LATERAL FLATTEN(d.USE_CASE_TEAM_NAME_LIST) f,
            LATERAL FLATTEN(d.USE_CASE_TEAM_ROLE_LIST) r
        WHERE f.index = r.index
          AND r.value::STRING = 'SE - Workload FCTO'
        ORDER BY d.USE_CASE_EACV DESC NULLS LAST
    """)


with st.spinner("Loading data..."):
    org = load_org()
    team_members = set(org["NAME"].str.strip().tolist())
    data = load_data()

cutoff = date.today() - timedelta(days=7)

today = date.today()
fy_year = today.year if today.month >= 2 else today.year - 1
fy_q_month = ((today.month - 2) % 12) // 3 * 3 + 2
fy_q_year = fy_year if fy_q_month >= 2 else fy_year + 1
cq_start = date(fy_q_year, fy_q_month, 1)
def add_months(d, m):
    month = d.month - 1 + m
    year = d.year + month // 12
    month = month % 12 + 1
    return date(year, month, 1)

cq_end = add_months(cq_start, 3) - timedelta(days=1)
nq_start = cq_end + timedelta(days=1)
nq_end = add_months(nq_start, 3) - timedelta(days=1)

fy_q_num = ((today.month - 2) % 12) // 3 + 1
cq_label = f"This Quarter (FY{fy_year % 100 + 1} Q{fy_q_num})"
nq_num = fy_q_num % 4 + 1
nq_label = f"Next Quarter (FY{fy_year % 100 + 1} Q{nq_num})"

expanded = data.copy()
expanded["SPECIALIST"] = expanded["SPECIALIST"].str.strip()
expanded = expanded[expanded["SPECIALIST"].isin(team_members)].reset_index(drop=True)
expanded = expanded[~expanded["USE_CASE_STAGE"].isin(["8 - Use Case Lost", "7 - Deployed"])].reset_index(drop=True)

expanded["LAST_MODIFIED_DATE_DT"] = pd.to_datetime(expanded["LAST_MODIFIED_DATE"], errors="coerce")
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
expanded["RECENTLY_UPDATED"] = (
    expanded["HAS_SPECIALIST_COMMENTS"]
    & expanded["LAST_MODIFIED_DATE_DT"].notna()
    & (expanded["LAST_MODIFIED_DATE_DT"].dt.date >= cutoff)
)

all_categories = sorted(set(
    cat.strip()
    for cats in expanded["PRODUCT_CATEGORIES"].dropna()
    for cat in cats.split(",")
    if cat.strip()
))

summary = expanded.groupby("SPECIALIST").agg(
    total_use_cases=("USE_CASE_ID", "count"),
    with_comments=("HAS_SPECIALIST_COMMENTS", "sum"),
    active_use_cases=("RECENTLY_UPDATED", "sum"),
    total_eacv=("USE_CASE_EACV", "sum"),
).reset_index()
summary["without_comments"] = summary["total_use_cases"] - summary["with_comments"]
summary["coverage_pct"] = (summary["with_comments"] / summary["total_use_cases"] * 100).round(1)

def classify_engagement(row):
    if row["active_use_cases"] > 0:
        return "Active Engagement"
    elif row["with_comments"] > 0:
        return "Stale Engagement"
    else:
        return "Not Active Engagement"

summary["engagement"] = summary.apply(classify_engagement, axis=1)
engagement_order = {"Not Active Engagement": 0, "Stale Engagement": 1, "Active Engagement": 2}
summary["_sort"] = summary["engagement"].map(engagement_order)
summary = summary.sort_values(["_sort", "total_use_cases"], ascending=[True, False]).reset_index(drop=True)

st.title("DE Team Engagement Tracker")
st.caption(f"Rithesh Makkena's org · {cq_label}: {cq_start.strftime('%b %d')} – {cq_end.strftime('%b %d, %Y')} · {nq_label}: {nq_start.strftime('%b %d')} – {nq_end.strftime('%b %d, %Y')} · Active = comments updated in last 7 days")

total_specialists = len(summary)
active_count = int((summary["engagement"] == "Active Engagement").sum())
stale_count = int((summary["engagement"] == "Stale Engagement").sum())
not_active_count = int((summary["engagement"] == "Not Active Engagement").sum())
total_uc = len(expanded)

c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.markdown(f"""
    <div class="metric-card metric-neutral">
        <div class="label">Total AFEs</div>
        <div class="value">{total_specialists}</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""
    <div class="metric-card metric-green">
        <div class="label">Active Engagement</div>
        <div class="value">{active_count}</div>
    </div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""
    <div class="metric-card metric-amber">
        <div class="label">Stale Engagement</div>
        <div class="value">{stale_count}</div>
    </div>""", unsafe_allow_html=True)
with c4:
    st.markdown(f"""
    <div class="metric-card metric-red">
        <div class="label">Not Active</div>
        <div class="value">{not_active_count}</div>
    </div>""", unsafe_allow_html=True)
with c5:
    st.markdown(f"""
    <div class="metric-card metric-neutral">
        <div class="label">Total Use Cases</div>
        <div class="value">{total_uc:,}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("")

filter_cols = st.columns([2, 3])
with filter_cols[0]:
    search = st.text_input("Search AFE", placeholder="Type a name to search...")
with filter_cols[1]:
    selected_categories = st.multiselect("Filter by Product Category", all_categories)

if selected_categories:
    cat_mask = expanded["PRODUCT_CATEGORIES"].apply(
        lambda x: any(c.strip() in selected_categories for c in (x or "").split(","))
    )
    filtered_expanded = expanded[cat_mask]
    filtered_specialists = set(filtered_expanded["SPECIALIST"].unique())
    summary_view = summary[summary["SPECIALIST"].isin(filtered_specialists)].reset_index(drop=True)
else:
    filtered_expanded = expanded
    summary_view = summary

sv_total = len(summary_view)
sv_active = int((summary_view["engagement"] == "Active Engagement").sum())
sv_stale = int((summary_view["engagement"] == "Stale Engagement").sum())
sv_not_active = int((summary_view["engagement"] == "Not Active Engagement").sum())

tab_all, tab_not, tab_stale, tab_active = st.tabs([
    f"All ({sv_total})",
    f"Not Active ({sv_not_active})",
    f"Stale ({sv_stale})",
    f"Active ({sv_active})",
])

ENGAGEMENT_COLORS = {
    "Active Engagement": "background-color: #0a2e1a; color: #4ade80",
    "Stale Engagement": "background-color: #2e2400; color: #f59e0b",
    "Not Active Engagement": "background-color: #3d1111; color: #ff6b6b",
}


def style_summary(df):
    display = df[["SPECIALIST", "engagement", "total_use_cases", "active_use_cases", "with_comments", "without_comments", "coverage_pct", "total_eacv"]].copy()
    display.columns = ["AFE", "Engagement", "Total Use Cases", "Active (7d)", "With Comments", "Without Comments", "Coverage %", "Total EACV"]

    def color_row(row):
        style = ENGAGEMENT_COLORS.get(row["Engagement"], "")
        return [style] * len(row)

    styled = display.style.apply(color_row, axis=1).format({
        "Coverage %": "{:.1f}%",
        "Total EACV": "${:,.0f}",
    })
    return styled


def filter_by_search(df, query):
    if query:
        return df[df["SPECIALIST"].str.contains(query, case=False, na=False)]
    return df


with tab_all:
    filtered = filter_by_search(summary_view, search)
    st.dataframe(style_summary(filtered), hide_index=True, use_container_width=True, height=500)

with tab_not:
    not_active = summary_view[summary_view["engagement"] == "Not Active Engagement"].reset_index(drop=True)
    filtered = filter_by_search(not_active, search)
    if filtered.empty:
        st.success("All AFEs have engagement!")
    else:
        st.dataframe(style_summary(filtered), hide_index=True, use_container_width=True, height=500)

with tab_stale:
    stale = summary_view[summary_view["engagement"] == "Stale Engagement"].reset_index(drop=True)
    filtered = filter_by_search(stale, search)
    if filtered.empty:
        st.info("No stale engagements found.")
    else:
        st.dataframe(style_summary(filtered), hide_index=True, use_container_width=True, height=500)

with tab_active:
    active = summary_view[summary_view["engagement"] == "Active Engagement"].reset_index(drop=True)
    filtered = filter_by_search(active, search)
    if filtered.empty:
        st.info("No active engagements found.")
    else:
        st.dataframe(style_summary(filtered), hide_index=True, use_container_width=True, height=500)

st.markdown("---")
st.subheader("Use Case Detail")

col_filter1, col_filter2, col_filter3 = st.columns(3)
with col_filter1:
    detail_specialists = ["All"] + sorted(filtered_expanded["SPECIALIST"].unique().tolist())
    selected = st.selectbox("Filter by AFE", detail_specialists)
with col_filter2:
    quarter_options = ["Both Quarters", cq_label, nq_label]
    quarter_filter = st.selectbox("Filter by Quarter", quarter_options)
with col_filter3:
    engagement_filter = st.selectbox("Filter by engagement", ["All", "Active (updated in 7d)", "Stale (comments but older)", "No Comments"])

if selected == "All":
    detail = filtered_expanded.copy()
else:
    detail = filtered_expanded[filtered_expanded["SPECIALIST"] == selected].copy()

if quarter_filter == cq_label:
    detail = detail[detail["IN_CQ"]].copy()
elif quarter_filter == nq_label:
    detail = detail[detail["IN_NQ"]].copy()

if engagement_filter == "Active (updated in 7d)":
    detail = detail[detail["RECENTLY_UPDATED"] == True]
elif engagement_filter == "Stale (comments but older)":
    detail = detail[(detail["HAS_SPECIALIST_COMMENTS"] == True) & (detail["RECENTLY_UPDATED"] == False)]
elif engagement_filter == "No Comments":
    detail = detail[detail["HAS_SPECIALIST_COMMENTS"] == False]

detail["SFDC"] = detail["USE_CASE_ID"].apply(lambda uid: f"{SFDC_BASE}/{uid}/view" if uid else "")

def uc_engagement(row):
    if row["RECENTLY_UPDATED"]:
        return "Active"
    elif row["HAS_SPECIALIST_COMMENTS"]:
        return "Stale"
    return "None"

detail["Engagement"] = detail.apply(uc_engagement, axis=1)

detail_display = detail[["SPECIALIST", "ACCOUNT_NAME", "USE_CASE_NAME", "USE_CASE_STAGE", "USE_CASE_STATUS", "USE_CASE_EACV", "THEATER_NAME", "PRODUCT_CATEGORIES", "Engagement", "DECISION_DATE", "GO_LIVE_DATE", "LAST_MODIFIED_DATE", "SFDC"]].rename(columns={
    "SPECIALIST": "AFE",
    "ACCOUNT_NAME": "Account",
    "USE_CASE_NAME": "Use Case",
    "USE_CASE_STAGE": "Stage",
    "USE_CASE_STATUS": "Status",
    "USE_CASE_EACV": "EACV",
    "THEATER_NAME": "Theater",
    "PRODUCT_CATEGORIES": "Products",
    "LAST_MODIFIED_DATE": "Last Modified",
    "DECISION_DATE": "Decision Date",
    "GO_LIVE_DATE": "Go Live Date",
})

DETAIL_COLORS = {
    "Active": "background-color: #0a2e1a; color: #4ade80",
    "Stale": "background-color: #2e2400; color: #f59e0b",
    "None": "background-color: #3d1111; color: #ff6b6b",
}


def style_detail(row):
    style = DETAIL_COLORS.get(row["Engagement"], "")
    return [style] * len(row)


styled_detail = detail_display.style.apply(style_detail, axis=1).format({
    "EACV": "${:,.0f}",
})

st.dataframe(
    styled_detail,
    hide_index=True,
    use_container_width=True,
    height=500,
    column_config={
        "SFDC": st.column_config.LinkColumn("SFDC", display_text="Open"),
    },
)

st.caption(f"Showing {len(detail):,} use cases · Source: MDM.MDM_INTERFACES.DIM_USE_CASE · Role: SE - Workload FCTO (AFE)")

st.markdown("---")
st.subheader("Ask a Question")
st.caption("Ask questions about the engagement data using Snowflake Cortex")

if "chat_messages" not in st.session_state:
    st.session_state.chat_messages = []


def build_data_context():
    ctx_lines = []
    ctx_lines.append(f"Dashboard: DE Team Engagement Tracker for Rithesh Makkena's org")
    ctx_lines.append(f"Date: {today}, Fiscal quarters: {cq_label} ({cq_start} to {cq_end}), {nq_label} ({nq_start} to {nq_end})")
    ctx_lines.append(f"Total AFEs: {total_specialists}, Active: {active_count}, Stale: {stale_count}, Not Active: {not_active_count}, Total Use Cases: {total_uc}")
    ctx_lines.append("")
    ctx_lines.append("AFE Summary (Name | Engagement | Total Use Cases | Active(7d) | With Comments | Without Comments | Coverage% | Total EACV):")
    for _, row in summary.iterrows():
        ctx_lines.append(f"  {row['SPECIALIST']} | {row['engagement']} | {int(row['total_use_cases'])} | {int(row['active_use_cases'])} | {int(row['with_comments'])} | {int(row['without_comments'])} | {row['coverage_pct']}% | ${row['total_eacv']:,.0f}")
    ctx_lines.append("")
    top_uc = expanded.nlargest(50, "USE_CASE_EACV")[["SPECIALIST", "ACCOUNT_NAME", "USE_CASE_NAME", "USE_CASE_EACV", "USE_CASE_STAGE", "USE_CASE_STATUS", "DECISION_DATE", "GO_LIVE_DATE"]].to_string(index=False)
    ctx_lines.append(f"Top 50 use cases by EACV:\n{top_uc}")
    return "\n".join(ctx_lines)


for msg in st.session_state.chat_messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("e.g. Who has the most use cases without comments?"):
    st.session_state.chat_messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            data_context = build_data_context()
            full_prompt = (
                "You are a helpful data analyst assistant for the DE Team Engagement Tracker dashboard. "
                "Answer questions using ONLY the data provided below. Be concise and specific. Use numbers and names from the data. "
                "If the data doesn't contain enough information to answer, say so.\n\n"
                f"DATA:\n{data_context}\n\nQuestion: {prompt}"
            )
            try:
                conn = get_connection()
                cursor = conn.raw_connection.cursor()
                cursor.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS RESPONSE", ("mistral-large2", full_prompt))
                answer = cursor.fetchone()[0]
                cursor.close()
            except Exception as e:
                answer = f"Error: {e}"
        st.markdown(answer)
        st.session_state.chat_messages.append({"role": "assistant", "content": answer})
