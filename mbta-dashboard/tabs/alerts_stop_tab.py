import streamlit as st
import plotly.express as px


def render(query, start_date, end_date):
    st.subheader("Alert hotspots by stop")
    st.caption("Aggregated across the selected date range")

    alerts_stop_df = query(f"""
        SELECT
            stop_name,
            stop_lat,
            stop_lon,
            alert_count,
            severe_count,
            warning_count,
            info_count,
            alert_date
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY_STOPS
        WHERE alert_date BETWEEN '{start_date}' AND '{end_date}'
          AND stop_lat IS NOT NULL
          AND stop_lon IS NOT NULL
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
            title='Alert hotspots — bubble size = alert count, color = severe alerts'
        )
        fig_map.update_layout(
            mapbox_style='open-street-map',
            height=500,
            margin={"r":0,"t":40,"l":0,"b":0}
        )
        st.plotly_chart(fig_map, use_container_width=True)

        st.subheader("Most alerted stops")
        top_stops = stop_agg.sort_values('ALERT_COUNT', ascending=False).head(10)
        top_stops = top_stops[['STOP_NAME','ALERT_COUNT','SEVERE_COUNT']].rename(columns={
            'STOP_NAME': 'Stop',
            'ALERT_COUNT': 'Total alerts',
            'SEVERE_COUNT': 'Severe alerts'
        })
        st.dataframe(top_stops, use_container_width=True, hide_index=True)
    else:
        st.info("No alert stop data for selected date range.")
