import streamlit as st
import plotly.express as px


def render(query, route_filter):
    st.subheader("Occupancy by route")

    route_day_df = query(f"""
        SELECT
            ROUTE_ID,
            ROUTE_SHORT_NAME,
            AVG_OCCUPANCY_PCT,
            PCT_EMPTY,
            PCT_MANY_SEATS,
            PCT_FEW_SEATS,
            PCT_STANDING_ROOM,
            PCT_CRUSHED_STANDING,
            PCT_FULL,
            SNAPSHOT_COUNT
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY
        WHERE ROUTE_SHORT_NAME IN ('{route_filter}')
        ORDER BY AVG_OCCUPANCY_PCT DESC
    """)

    if not route_day_df.empty:
        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                route_day_df.head(20).sort_values("AVG_OCCUPANCY_PCT"),
                x="AVG_OCCUPANCY_PCT",
                y="ROUTE_SHORT_NAME",
                orientation="h",
                title="Average occupancy % by route",
                labels={"AVG_OCCUPANCY_PCT": "Avg occupancy %", "ROUTE_SHORT_NAME": "Route"},
                color="AVG_OCCUPANCY_PCT",
                color_continuous_scale="Blues"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig2 = px.bar(
                route_day_df.head(20).sort_values("PCT_STANDING_ROOM", ascending=False),
                x="ROUTE_SHORT_NAME",
                y=["PCT_EMPTY", "PCT_MANY_SEATS", "PCT_FEW_SEATS",
                   "PCT_STANDING_ROOM", "PCT_CRUSHED_STANDING", "PCT_FULL"],
                title="Occupancy breakdown by route",
                labels={"value": "%", "ROUTE_SHORT_NAME": "Route",
                        "variable": "Category"},
                barmode="stack",
                color_discrete_sequence=px.colors.sequential.Blues_r
            )
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Most crowded routes")
        crowded = route_day_df[["ROUTE_SHORT_NAME", "AVG_OCCUPANCY_PCT",
                                 "PCT_STANDING_ROOM", "PCT_FULL",
                                 "SNAPSHOT_COUNT"]].head(10)
        crowded.columns = ["Route", "Avg occupancy %", "% standing room",
                           "% full", "Snapshots"]
        crowded = crowded.round(2)
        st.dataframe(crowded, use_container_width=True, hide_index=True)
    else:
        st.info("No route-level data for selected filters.")
