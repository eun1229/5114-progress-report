import streamlit as st
import plotly.express as px


def render(query, start_date, end_date):
    st.subheader("Alert hotspots by stop")
    st.caption("Aggregated across the selected date range for each stop. **Route filters will not affect this view.**")

    alerts_stop_df = query(f"""
        SELECT
            stop_name,
            AVG(stop_lat) as stop_lat,
            AVG(stop_lon) as stop_lon,
            SUM(alert_count) as alert_count,
            SUM(severe_count) as severe_count,
            SUM(warning_count) as warning_count,
            SUM(info_count) as info_count,
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY_STOPS
        WHERE alert_date BETWEEN '{start_date}' AND '{end_date}'
          AND stop_lat IS NOT NULL
          AND stop_lon IS NOT NULL
        GROUP BY stop_name
    """)

    if not alerts_stop_df.empty:
        stop_agg = alerts_stop_df.groupby(['STOP_NAME','STOP_LAT','STOP_LON']).agg(
            ALERT_COUNT=('ALERT_COUNT','sum'),
            SEVERE_COUNT=('SEVERE_COUNT','sum')
        ).reset_index()

        fig_map = px.scatter_mapbox(
            stop_agg,
            lat='STOP_LAT',
            lon='STOP_LON',
            size='ALERT_COUNT',
            color='SEVERE_COUNT',
            hover_name='STOP_NAME',
            hover_data={'ALERT_COUNT': True, 'SEVERE_COUNT': True},
            color_continuous_scale='Reds',
            size_max=20,
            zoom=11,
            title='Alert hotspots',
            subtitle='Bubble size = alert count, Color = number of severe alerts'
        )
        fig_map.update_layout(
            mapbox_style='carto-positron',
            height=500,
            margin={"r":0,"t":50,"l":0,"b":0}
        )
        st.plotly_chart(fig_map, use_container_width=True)

        st.subheader("Stops most affected by alerts")
        top_stops = stop_agg.sort_values('ALERT_COUNT', ascending=False).head(10)
        top_stops = top_stops[['STOP_NAME','ALERT_COUNT','SEVERE_COUNT']].rename(columns={
            'STOP_NAME': 'Stop',
            'ALERT_COUNT': 'Total alerts',
            'SEVERE_COUNT': 'Severe alerts'
        })
        st.dataframe(top_stops, use_container_width=True, hide_index=True)
    else:
        st.info("No alert stop data for selected date range.")
