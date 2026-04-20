import math

import streamlit as st
import plotly.express as px


def render(query, start_date, end_date, route_filter=None):
    st.subheader("Service alerts by route")
    st.caption("Number of alerts that affect the selected routes for the selected date ranges.")

    route_clause = ""
    if route_filter:
        route_clause = f"AND route_name IN ('{route_filter}')"

    alerts_route_df = query(f"""
        SELECT
            alert_date,
            route_id,
            route_name,
            alert_count,
            severe_count,
            warning_count,
            info_count
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY
        WHERE alert_date BETWEEN '{start_date}' AND '{end_date}'
        {route_clause}
        ORDER BY alert_date DESC
    """)

    if not alerts_route_df.empty:
        daily = alerts_route_df.groupby('ALERT_DATE')[
            ['SEVERE_COUNT', 'WARNING_COUNT', 'INFO_COUNT']
        ].sum().reset_index()

        fig = px.bar(
            daily,
            x='ALERT_DATE',
            y=['SEVERE_COUNT', 'WARNING_COUNT', 'INFO_COUNT'],
            title='Daily alert counts by type',
            labels={
                'value': 'Alert count',
                'ALERT_DATE': 'Date',
                'variable': 'Alert type'
            },
            barmode='stack',
            color_discrete_map={
                'SEVERE_COUNT': '#e63946',
                'WARNING_COUNT': '#f4a261',
                'INFO_COUNT':    '#457b9d'
            }
        )
        max_daily_total = int(
            daily[['SEVERE_COUNT', 'WARNING_COUNT', 'INFO_COUNT']].sum(axis=1).max()
        )
        y_dtick = max(1, math.ceil(max_daily_total / 10))
        fig.update_yaxes(tickformat='d', tick0=0, dtick=y_dtick, rangemode='tozero')
        st.plotly_chart(fig, use_container_width=True)

        top_routes = query(f"""
            SELECT route_name, SUM(alert_count) AS total
            FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY
            WHERE alert_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 10
        """)

        top_routes_sorted = top_routes.sort_values("TOTAL")
        top_routes_sorted["ROUTE_NAME"] = top_routes_sorted["ROUTE_NAME"].astype(str)
        fig2 = px.bar(
            top_routes_sorted,
            x="TOTAL",
            y="ROUTE_NAME",
            orientation="h",
            title="Top routes by alert count",
            subtitle="The top alerts for the selected date range, unaffected by route filters",
            labels={"TOTAL": "Alert count", "ROUTE_NAME": "Route"},
            color_discrete_sequence=["#7F77DD"],
        )
        fig2.update_yaxes(
            type="category",
            categoryorder="array",
            categoryarray=top_routes_sorted["ROUTE_NAME"].tolist(),
            title=None,
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No alert route data for selected date range.")
