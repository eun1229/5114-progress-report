import streamlit as st
import plotly.express as px


def render(query, start_date, end_date, route_filter):
    st.subheader("Service delivered %")
    st.caption(
        "% of scheduled trips that were actually delivered in entirety, "
        "without being cancelled or missing from the real-time feed. "
        "Hover over a point to see the breakdown of each trip bucket."
    )

    sd_df = query(f"""
        WITH daily AS (
            SELECT
                trip_start_date                         AS bucket,
                SUM(scheduled_trips)                    AS scheduled_trips,
                SUM(delivered_trips)                    AS delivered_trips,
                SUM(canceled_trips)                     AS canceled_trips,
                SUM(no_rt_data_trips)                   AS no_rt_data_trips,
                SUM(added_trips)                        AS added_trips
            FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_SERVICE_DELIVERED
            WHERE trip_start_date BETWEEN '{start_date}' AND '{end_date}'
              AND route_name IN ('{route_filter}')
            GROUP BY trip_start_date
        )
        SELECT
            bucket,
            scheduled_trips,
            delivered_trips,
            canceled_trips,
            no_rt_data_trips,
            added_trips,
            ROUND(delivered_trips   * 100.0 / NULLIF(scheduled_trips, 0), 2) AS pct_delivered,
            ROUND(canceled_trips    * 100.0 / NULLIF(scheduled_trips, 0), 2) AS pct_canceled,
            ROUND(no_rt_data_trips  * 100.0 / NULLIF(scheduled_trips, 0), 2) AS pct_no_rt_data,
            ROUND(added_trips       * 100.0 / NULLIF(scheduled_trips, 0), 2) AS pct_added
        FROM daily
        ORDER BY bucket
    """)

    if sd_df.empty:
        st.info("No service delivered data for selected filters.")
        return

    fig = px.line(
        sd_df,
        x="BUCKET",
        y="PCT_DELIVERED",
        markers=True,
        title="Daily service delivered %",
        labels={"BUCKET": "Date", "PCT_DELIVERED": "Delivered %"},
        custom_data=[
            "PCT_DELIVERED",
            "PCT_CANCELED",
            "PCT_NO_RT_DATA",
            "PCT_ADDED",
            "SCHEDULED_TRIPS",
            "DELIVERED_TRIPS",
            "CANCELED_TRIPS",
            "NO_RT_DATA_TRIPS",
            "ADDED_TRIPS",
        ],
        color_discrete_sequence=["#1a56db"],
    )
    fig.update_traces(
        hovertemplate=(
            "<b>%{x|%Y-%m-%d}</b><br>"
            "Delivered: %{customdata[0]:.1f}% (%{customdata[5]:,} trips)<br>"
            "Canceled: %{customdata[1]:.1f}% (%{customdata[6]:,} trips)<br>"
            "No RT data: %{customdata[2]:.1f}% (%{customdata[7]:,} trips)<br>"
            "Added: %{customdata[3]:.1f}% (%{customdata[8]:,} trips)<br>"
            "Scheduled: %{customdata[4]:,} trips"
            "<extra></extra>"
        )
    )
    fig.update_yaxes(range=[0, 100], ticksuffix="%")
    fig.update_xaxes(tickformat="%Y-%m-%d", dtick="D1")
    st.plotly_chart(fig, use_container_width=True)
