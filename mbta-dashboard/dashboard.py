import streamlit as st
from datetime import datetime, timedelta
from data_access import query
from tabs.live_tab_view import render as render_live_tab
from tabs.system_overview_tab import render as render_system_overview
from tabs.occupancy_route_tab import render as render_occupancy_route
from tabs.occupancy_hour_tab import render as render_occupancy_hour
from tabs.stop_events_tab import render as render_stop_events
from tabs.alerts_route_tab import render as render_alerts_route
from tabs.alerts_stop_tab import render as render_alerts_stop


st.set_page_config(
    page_title="MBTA Bus Performance Dashboard",
    page_icon="🚌",
    layout="wide"
)

st.markdown("""
<style>
    .metric-card {
        background: #f0f4f8;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }
    .stTabs [data-baseweb="tab"] {
        font-size: 15px;
        font-weight: 500;
    }
</style>
""", unsafe_allow_html=True)

# ── Header ───────────────────────────────────────────────────
st.title("CSE5114 MBTA Bus Performance Dashboard")
st.caption(f"Live data collected every 60 seconds via MBTA GTFS-RT API · Refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S ET')}")

# ── Sidebar filters ──────────────────────────────────────────
st.sidebar.header("Filters")

# Date range
date_range = st.sidebar.date_input(
    "Date range",
    value=[datetime(2026, 3, 1), datetime.now()],
    max_value=datetime.now()
)
start_date = date_range[0] if len(date_range) == 2 else datetime.now() - timedelta(days=14)
end_date = date_range[1] if len(date_range) == 2 else datetime.now()

# Route selector
routes_df = query("""
    SELECT DISTINCT ROUTE_ID, ROUTE_SHORT_NAME
    FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY
    WHERE ROUTE_SHORT_NAME IS NOT NULL
    ORDER BY ROUTE_SHORT_NAME
""")

all_routes = routes_df["ROUTE_SHORT_NAME"].dropna().tolist()
selected_routes = st.sidebar.multiselect(
    "Select routes (leave blank for all)",
    options=all_routes,
    default=all_routes[:8] if len(all_routes) >= 8 else all_routes
)

if not selected_routes:
    selected_routes = all_routes

route_filter = "', '".join(selected_routes)

# ── Tabs ─────────────────────────────────────────────────────
tab0, tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Live tab",
    "System overview",
    "Occupancy by route",
    "Occupancy by hour",
    "Stop events",
    "Alerts by route",
    "Alerts by stop"
])

# ── Tab 0: Live tab ──────────────────────────────────────────
with tab0:
    render_live_tab()

# ── Tab 1: System overview ───────────────────────────────────
with tab1:
    render_system_overview(query, start_date, end_date)

# ── Tab 2: Occupancy by route ────────────────────────────────
with tab2:
    render_occupancy_route(query, route_filter)

# ── Tab 3: Occupancy by hour ─────────────────────────────────
with tab3:
    render_occupancy_hour(query, start_date, end_date, route_filter)

# ── Tab 4: Stop events ───────────────────────────────────────
with tab4:
    render_stop_events(query, start_date, end_date, route_filter)

with tab5:
    render_alerts_route(query, start_date, end_date)

with tab6:
    render_alerts_stop(query, start_date, end_date)

# ── Footer ────────────────────────────────────────────────────
st.divider()
st.caption("MBTA Bus Performance Dashboard · Data: MBTA GTFS-RT API · Pipeline: AWS Lambda → S3 → Spark → Snowflake · Built with Streamlit")