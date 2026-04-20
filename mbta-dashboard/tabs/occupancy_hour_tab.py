import streamlit as st
import plotly.express as px


def render(query, start_date, end_date, route_filter):
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
