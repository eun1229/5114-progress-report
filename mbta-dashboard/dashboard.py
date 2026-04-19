import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

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

# ── Connection ───────────────────────────────────────────────
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

@st.cache_resource
def get_conn():
    with open(st.secrets["SF_PRIVATE_KEY_PATH"], "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    return snowflake.connector.connect(
        user=st.secrets["SF_USER"],
        account=st.secrets["SF_ACCOUNT"],
        private_key=private_key_bytes,
        warehouse=st.secrets["SF_WAREHOUSE"],
        database=st.secrets["SF_DATABASE"],
        schema="FINAL_PROJECT_MART"
    )

@st.cache_data(ttl=60)
def query(sql):
    conn = get_conn()
    return pd.read_sql(sql, conn)

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
tab1, tab2, tab3, tab4 = st.tabs([
    "System overview",
    "Occupancy by route",
    "Occupancy by hour",
    "Stop events"
])

# ── Tab 1: System overview ───────────────────────────────────
with tab1:
    st.subheader("System-wide occupancy over time")

    overall_df = query(f"""
        SELECT
            SERVICE_DATE,
            AVG_OCCUPANCY_PCT,
            PCT_EMPTY,
            PCT_MANY_SEATS,
            PCT_FEW_SEATS,
            PCT_STANDING_ROOM,
            PCT_CRUSHED_STANDING,
            PCT_FULL,
            SNAPSHOT_COUNT
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_OVERALL_DAY
        WHERE SERVICE_DATE BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY SERVICE_DATE
    """)

    if not overall_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric(
            "Avg occupancy",
            f"{overall_df['AVG_OCCUPANCY_PCT'].mean():.1f}%"
        )
        col2.metric(
            "% empty seats",
            f"{overall_df['PCT_EMPTY'].mean():.1f}%"
        )
        col3.metric(
            "% standing room",
            f"{overall_df['PCT_STANDING_ROOM'].mean():.1f}%"
        )
        col4.metric(
            "Total snapshots",
            f"{overall_df['SNAPSHOT_COUNT'].sum():,}"
        )

        fig = px.line(
            overall_df,
            x="SERVICE_DATE",
            y="AVG_OCCUPANCY_PCT",
            title="Average system-wide occupancy % over time",
            labels={"AVG_OCCUPANCY_PCT": "Avg occupancy %", "SERVICE_DATE": "Date"},
            markers=True
        )
        fig.update_traces(line_color="#1a56db", line_width=2)
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Occupancy breakdown over time")
        occupancy_cols = ["SERVICE_DATE", "PCT_EMPTY", "PCT_MANY_SEATS",
                          "PCT_FEW_SEATS", "PCT_STANDING_ROOM",
                          "PCT_CRUSHED_STANDING", "PCT_FULL"]
        melted = overall_df[occupancy_cols].melt(
            id_vars="SERVICE_DATE",
            var_name="Category",
            value_name="Percentage"
        )
        melted["Category"] = melted["Category"].str.replace("PCT_", "").str.replace("_", " ").str.title()

        fig2 = px.area(
            melted,
            x="SERVICE_DATE",
            y="Percentage",
            color="Category",
            title="Occupancy category breakdown over time",
            labels={"SERVICE_DATE": "Date", "Percentage": "%"},
            color_discrete_sequence=px.colors.sequential.Blues_r
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No system-wide data for selected date range.")

# ── Tab 2: Occupancy by route ────────────────────────────────
with tab2:
    st.subheader("Occupancy by route")

    route_day_df = query(f"""
        SELECT
            ROUTE_ID,
            ROUTE_SHORT_NAME,
            AVG_OCCUPANCY_PCT,
            PCT_EMPTY,
            PCT_MANY_SEATS,
            PCT_FEW_SEATS,
            PCT_STANDING_ROOM,
            PCT_CRUSHED_STANDING,
            PCT_FULL,
            SNAPSHOT_COUNT
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY
        WHERE ROUTE_SHORT_NAME IN ('{route_filter}')
        ORDER BY AVG_OCCUPANCY_PCT DESC
    """)

    if not route_day_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                route_day_df.head(20).sort_values("AVG_OCCUPANCY_PCT"),
                x="AVG_OCCUPANCY_PCT",
                y="ROUTE_SHORT_NAME",
                orientation="h",
                title="Average occupancy % by route",
                labels={"AVG_OCCUPANCY_PCT": "Avg occupancy %", "ROUTE_SHORT_NAME": "Route"},
                color="AVG_OCCUPANCY_PCT",
                color_continuous_scale="Blues"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig2 = px.bar(
                route_day_df.head(20).sort_values("PCT_STANDING_ROOM", ascending=False),
                x="ROUTE_SHORT_NAME",
                y=["PCT_EMPTY", "PCT_MANY_SEATS", "PCT_FEW_SEATS",
                   "PCT_STANDING_ROOM", "PCT_CRUSHED_STANDING", "PCT_FULL"],
                title="Occupancy breakdown by route",
                labels={"value": "%", "ROUTE_SHORT_NAME": "Route",
                        "variable": "Category"},
                barmode="stack",
                color_discrete_sequence=px.colors.sequential.Blues_r
            )
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Most crowded routes")
        crowded = route_day_df[["ROUTE_SHORT_NAME", "AVG_OCCUPANCY_PCT",
                                 "PCT_STANDING_ROOM", "PCT_FULL",
                                 "SNAPSHOT_COUNT"]].head(10)
        crowded.columns = ["Route", "Avg occupancy %", "% standing room",
                           "% full", "Snapshots"]
        crowded = crowded.round(2)
        st.dataframe(crowded, use_container_width=True, hide_index=True)
    else:
        st.info("No route-level data for selected filters.")

# ── Tab 3: Occupancy by hour ─────────────────────────────────
with tab3:
    st.subheader("Occupancy patterns by hour of day")

    hour_df = query(f"""
        SELECT
            SERVICE_DATE,
            HOUR,
            ROUTE_ID,
            ROUTE_SHORT_NAME,
            AVG_OCCUPANCY_PCT,
            PCT_STANDING_ROOM,
            SNAPSHOT_COUNT
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_HOUR
        WHERE SERVICE_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND ROUTE_SHORT_NAME IN ('{route_filter}')
        ORDER BY SERVICE_DATE, HOUR
    """)

    if not hour_df.empty:
        avg_by_hour = hour_df.groupby("HOUR")["AVG_OCCUPANCY_PCT"].mean().reset_index()

        fig = px.line(
            avg_by_hour,
            x="HOUR",
            y="AVG_OCCUPANCY_PCT",
            title="Average occupancy by hour of day (all routes)",
            labels={"HOUR": "Hour (ET)", "AVG_OCCUPANCY_PCT": "Avg occupancy %"},
            markers=True
        )
        fig.update_traces(line_color="#1a56db", line_width=2)
        fig.add_vrect(x0=7, x1=9, fillcolor="orange", opacity=0.1,
                      annotation_text="AM peak")
        fig.add_vrect(x0=16, x1=19, fillcolor="orange", opacity=0.1,
                      annotation_text="PM peak")
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Heatmap: occupancy by route and hour")
        pivot = hour_df.groupby(["ROUTE_SHORT_NAME", "HOUR"])["AVG_OCCUPANCY_PCT"].mean().reset_index()
        pivot_wide = pivot.pivot(
            index="ROUTE_SHORT_NAME",
            columns="HOUR",
            values="AVG_OCCUPANCY_PCT"
        )

        fig2 = px.imshow(
            pivot_wide,
            title="Avg occupancy % by route and hour",
            labels={"x": "Hour (ET)", "y": "Route", "color": "Occupancy %"},
            color_continuous_scale="Blues",
            aspect="auto"
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No hourly data for selected filters.")

# ── Tab 4: Stop events ───────────────────────────────────────
with tab4:
    st.subheader("Stop events")

    stops_df = query(f"""
        SELECT
            SERVICE_DATE,
            ROUTE_ID,
            STOP_NAME,
            DIRECTION_ID,
            COUNT(*) AS event_count,
            COUNT(DISTINCT TRIP_ID) AS trip_count,
            COUNT(DISTINCT VEHICLE_ID) AS vehicle_count
        FROM LEMMING_DB.FINAL_PROJECT_MART.STOP_EVENTS
        WHERE SERVICE_DATE BETWEEN '{start_date}' AND '{end_date}'
        AND ROUTE_ID IN (
            SELECT ROUTE_ID FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY
            WHERE ROUTE_SHORT_NAME IN ('{route_filter}')
        )
        GROUP BY 1, 2, 3, 4
        ORDER BY event_count DESC
        LIMIT 500
    """)

    if not stops_df.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total stop events", f"{stops_df['EVENT_COUNT'].sum():,}")
        col2.metric("Unique trips", f"{stops_df['TRIP_COUNT'].sum():,}")
        col3.metric("Unique vehicles", f"{stops_df['VEHICLE_COUNT'].sum():,}")

        daily_events = stops_df.groupby("SERVICE_DATE")["EVENT_COUNT"].sum().reset_index()
        fig = px.bar(
            daily_events,
            x="SERVICE_DATE",
            y="EVENT_COUNT",
            title="Stop events per day",
            labels={"SERVICE_DATE": "Date", "EVENT_COUNT": "Event count"},
            color_discrete_sequence=["#1a56db"]
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Busiest stops")
        busiest = stops_df.groupby("STOP_NAME")["EVENT_COUNT"].sum() \
            .reset_index().sort_values("EVENT_COUNT", ascending=False).head(15)
        fig2 = px.bar(
            busiest.sort_values("EVENT_COUNT"),
            x="EVENT_COUNT",
            y="STOP_NAME",
            orientation="h",
            title="Top 15 busiest stops by event count",
            labels={"EVENT_COUNT": "Events", "STOP_NAME": "Stop"},
            color_discrete_sequence=["#1a56db"]
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No stop event data for selected filters.")

# ── Footer ────────────────────────────────────────────────────
st.divider()
st.caption("MBTA Bus Performance Dashboard · Data: MBTA GTFS-RT API · Pipeline: AWS Lambda → S3 → Spark → Snowflake · Built with Streamlit")