import streamlit as st
import plotly.express as px


def render(query, start_date, end_date):
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
