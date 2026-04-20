import streamlit as st
import plotly.express as px


def render(query, start_date, end_date, route_filter):
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
