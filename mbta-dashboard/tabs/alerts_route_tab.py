import streamlit as st
import plotly.express as px


def render(query, start_date, end_date):
    st.subheader("Service alerts by route")
    st.caption("Historical alert data from Snowflake MART tables")

    alerts_route_df = query(f"""
        SELECT
            alert_date,
            route_id,
            alert_count,
            severe_count,
            warning_count,
            info_count
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY
        WHERE alert_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY alert_date, alert_count DESC
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
        st.plotly_chart(fig, use_container_width=True)

        top_routes = alerts_route_df.groupby('ROUTE_ID')['ALERT_COUNT'].sum() \
            .reset_index().sort_values('ALERT_COUNT', ascending=False).head(10)
        fig2 = px.bar(
            top_routes.sort_values('ALERT_COUNT'),
            x='ALERT_COUNT',
            y='ROUTE_ID',
            orientation='h',
            title='Most affected routes by alert count',
            labels={'ALERT_COUNT': 'Alerts', 'ROUTE_ID': 'Route'},
            color_discrete_sequence=['#e63946']
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No alert route data for selected date range.")
