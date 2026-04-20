import streamlit as st
import plotly.express as px
from datetime import datetime
from live_tab import get_live_vehicles, get_live_alerts


def render():
    st.subheader("Live MBTA bus positions — updating every 60 seconds")
    st.caption(f"Last fetched: {datetime.now().strftime('%H:%M:%S ET')} · Refreshes automatically")

    if st.button("Refresh now"):
        st.cache_data.clear()

    vehicles_df = get_live_vehicles()
    alerts_df = get_live_alerts()

    if not vehicles_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Active buses right now", len(vehicles_df))
        col2.metric("Routes operating", vehicles_df['route_id'].nunique())
        col3.metric(
            "Avg occupancy",
            f"{vehicles_df[vehicles_df['occupancy_pct'] > 0]['occupancy_pct'].mean():.0f}%"
            if len(vehicles_df[vehicles_df['occupancy_pct'] > 0]) > 0 else "N/A"
        )
        col4.metric("Active alerts", len(alerts_df))

        st.divider()

        col_left, col_right = st.columns([2, 1])

        with col_left:
            st.subheader("Bus locations right now")
            map_df = vehicles_df[
                (vehicles_df['latitude'] != 0) &
                (vehicles_df['longitude'] != 0)
            ][['latitude', 'longitude']].copy()

            if not map_df.empty:
                st.map(map_df, zoom=11)

        with col_right:
            st.subheader("Active buses by route")
            by_route = vehicles_df.groupby('route_id').size() \
                .reset_index(name='active_buses') \
                .sort_values('active_buses', ascending=False) \
                .head(15)

            fig = px.bar(
                by_route,
                x='active_buses',
                y='route_id',
                orientation='h',
                color='active_buses',
                color_continuous_scale='Blues',
                labels={'active_buses': 'Active buses', 'route_id': 'Route'}
            )
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        st.subheader("Current occupancy status across all buses")
        occ_counts = vehicles_df['occupancy_status'].value_counts().reset_index()
        occ_counts.columns = ['Status', 'Count']

        fig2 = px.pie(
            occ_counts,
            values='Count',
            names='Status',
            color_discrete_sequence=px.colors.sequential.Blues_r
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.error("Could not fetch live data from MBTA API. Check your internet connection.")

    if not alerts_df.empty:
        st.divider()
        st.subheader(f"Active service alerts ({len(alerts_df)} right now)")
        for _, row in alerts_df.head(10).iterrows():
            with st.expander(f"{row['routes_affected']} — {row['effect']}"):
                st.write(row['header'])
                st.caption(f"Cause: {row['cause']}")
