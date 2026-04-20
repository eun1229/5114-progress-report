import streamlit as st
import plotly.express as px


def render(query, start_date, end_date, route_filter):
    st.subheader("Occupancy %")
    st.caption(
        "% occupancy on vehicles for the filtered routes. "
        "**Set the date range to a single date to view hourly occupancy.**"
    )

    single_day = start_date == end_date
    bucket_expr = "hour" if single_day else "service_date"

    occ_df = query(f"""
        SELECT
            {bucket_expr} AS bucket,
            SUM(snapshot_count) AS snapshot_count,
            SUM(avg_occupancy_pct * snapshot_count)
                / NULLIF(SUM(CASE WHEN avg_occupancy_pct IS NOT NULL THEN snapshot_count ELSE 0 END), 0)
                AS avg_occupancy_pct,
            SUM(pct_empty * snapshot_count)            / NULLIF(SUM(snapshot_count), 0) AS pct_empty,
            SUM(pct_many_seats * snapshot_count)       / NULLIF(SUM(snapshot_count), 0) AS pct_many_seats,
            SUM(pct_few_seats * snapshot_count)        / NULLIF(SUM(snapshot_count), 0) AS pct_few_seats,
            SUM(pct_standing_room * snapshot_count)    / NULLIF(SUM(snapshot_count), 0) AS pct_standing_room,
            SUM(pct_crushed_standing * snapshot_count) / NULLIF(SUM(snapshot_count), 0) AS pct_crushed_standing,
            SUM(pct_full * snapshot_count)             / NULLIF(SUM(snapshot_count), 0) AS pct_full,
            SUM(pct_no_data_occupancy * snapshot_count)/ NULLIF(SUM(snapshot_count), 0) AS pct_no_data_occupancy
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_HOUR
        WHERE service_date BETWEEN '{start_date}' AND '{end_date}'
          AND route_short_name IN ('{route_filter}')
        GROUP BY bucket
        ORDER BY bucket
    """)

    if occ_df.empty:
        st.info("No occupancy data for selected filters.")
        return

    x_label = "Hour of day" if single_day else "Date"
    title = (
        f"Hourly occupancy ({start_date})"
        if single_day
        else "Daily occupancy"
    )
    hover_x = "%{x}:00" if single_day else "%{x|%Y-%m-%d}"

    fig = px.line(
        occ_df,
        x="BUCKET",
        y="AVG_OCCUPANCY_PCT",
        markers=True,
        title=title,
        labels={"BUCKET": x_label, "AVG_OCCUPANCY_PCT": "Avg occupancy %"},
        custom_data=[
            "AVG_OCCUPANCY_PCT",
            "PCT_EMPTY",
            "PCT_MANY_SEATS",
            "PCT_FEW_SEATS",
            "PCT_STANDING_ROOM",
            "PCT_CRUSHED_STANDING",
            "PCT_FULL",
            "PCT_NO_DATA_OCCUPANCY",
            "SNAPSHOT_COUNT",
        ],
        color_discrete_sequence=["#1a56db"],
    )
    fig.update_traces(
        hovertemplate=(
            f"<b>{hover_x}</b><br>"
            "Avg occupancy: %{customdata[0]:.1f}%<br>"
            "<br>"
            "<b>Status mix</b><br>"
            "Empty: %{customdata[1]:.1f}%<br>"
            "Many seats: %{customdata[2]:.1f}%<br>"
            "Few seats: %{customdata[3]:.1f}%<br>"
            "Standing room: %{customdata[4]:.1f}%<br>"
            "Crushed standing: %{customdata[5]:.1f}%<br>"
            "Full: %{customdata[6]:.1f}%<br>"
            "No data: %{customdata[7]:.1f}%<br>"
            "<br>"
            "Snapshots: %{customdata[8]:,}"
            "<extra></extra>"
        )
    )
    fig.update_yaxes(range=[0, 100], ticksuffix="%")
    if single_day:
        fig.update_xaxes(
            tickmode="array",
            tickvals=list(range(24)),
            ticktext=[f"{h:02d}:00" for h in range(24)],
            range=[-0.5, 23.5],
        )
    else:
        fig.update_xaxes(tickformat="%Y-%m-%d", dtick="D1")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Top 10 most crowded routes")
    st.caption("Ranked by snapshot-weighted average occupancy across the selected date range. **Route filters do not affect this view.**")

    top_routes_df = query(f"""
        SELECT *
        FROM (
            SELECT
                route_short_name,
                SUM(snapshot_count) AS snapshot_count,
                SUM(avg_occupancy_pct * snapshot_count)
                    / NULLIF(SUM(CASE WHEN avg_occupancy_pct IS NOT NULL THEN snapshot_count ELSE 0 END), 0)
                    AS avg_occupancy_pct
            FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_HOUR
            WHERE service_date BETWEEN '{start_date}' AND '{end_date}'
              AND route_short_name IS NOT NULL
            GROUP BY route_short_name
        )
        WHERE avg_occupancy_pct IS NOT NULL
        ORDER BY avg_occupancy_pct DESC
        LIMIT 10
    """)

    if top_routes_df.empty:
        st.info("No route occupancy data for selected date range.")
        return

    display_df = top_routes_df.rename(columns={
        "ROUTE_SHORT_NAME": "Route",
        "AVG_OCCUPANCY_PCT": "Avg occupancy %",
        "SNAPSHOT_COUNT": "# of Data Points",
    })[["Route", "Avg occupancy %", "# of Data Points"]]
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Avg occupancy %": st.column_config.NumberColumn(format="%.1f%%"),
            "# of Data Points": st.column_config.NumberColumn(format="%d"),
        },
    )
