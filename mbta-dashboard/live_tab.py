"""
live_tab.py

Fetches live MBTA data directly from the GTFS-RT API.
This bypasses the Snowflake pipeline entirely for the live tab,
giving us data that is seconds old rather than hours old.

MBTA GTFS-RT endpoints (no API key required):
- Vehicle positions: https://cdn.mbta.com/realtime/VehiclePositions.pb
- Trip updates:      https://cdn.mbta.com/realtime/TripUpdates.pb
- Alerts:            https://cdn.mbta.com/realtime/Alerts.pb

Data format: Protocol Buffers (protobuf) binary — same format
our Lambda collects and stores in S3 every 60 seconds.
"""

import streamlit as st
import requests
import pandas as pd
from datetime import datetime
from google.transit import gtfs_realtime_pb2


VEHICLE_POSITIONS_URL = "https://cdn.mbta.com/realtime/VehiclePositions.pb"
ALERTS_URL            = "https://cdn.mbta.com/realtime/Alerts.pb"
TRIP_UPDATES_URL      = "https://cdn.mbta.com/realtime/TripUpdates.pb"

REQUEST_TIMEOUT = 10


@st.cache_data(ttl=60)
def get_live_vehicles() -> pd.DataFrame:
    """
    Fetch current vehicle positions from the MBTA GTFS-RT API.

    Parses the protobuf binary response into a structured DataFrame.
    Cached for 60 seconds to avoid hammering the API on every rerun.

    Returns:
        pd.DataFrame: One row per active vehicle with columns:
            vehicle_id, route_id, latitude, longitude, status,
            occupancy_pct, occupancy_status, updated_at
    """
    try:
        response = requests.get(VEHICLE_POSITIONS_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        rows = []
        for entity in feed.entity:
            if not entity.HasField('vehicle'):
                continue

            v = entity.vehicle
            rows.append({
                'vehicle_id':       entity.id,
                'route_id':         v.trip.route_id,
                'latitude':         v.position.latitude,
                'longitude':        v.position.longitude,
                'status':           gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(
                                        v.current_status
                                    ),
                'occupancy_pct':    v.occupancy_percentage if v.occupancy_percentage else 0,
                'occupancy_status': gtfs_realtime_pb2.VehiclePosition.OccupancyStatus.Name(
                                        v.occupancy_status
                                    )
                                    if v.occupancy_status else 'NO_DATA',
                'updated_at':       datetime.fromtimestamp(v.timestamp).strftime('%H:%M:%S')
                                    if v.timestamp else 'N/A'
            })

        return pd.DataFrame(rows)

    except requests.exceptions.RequestException as e:
        st.warning(f"Could not reach MBTA vehicle positions API: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"Error parsing vehicle positions: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_live_alerts() -> pd.DataFrame:
    """
    Fetch current service alerts from the MBTA GTFS-RT API.

    Parses the protobuf binary response into a structured DataFrame.
    Cached for 60 seconds.

    Returns:
        pd.DataFrame: One row per active alert with columns:
            alert_id, header, cause, effect, routes_affected
    """
    try:
        response = requests.get(ALERTS_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        rows = []
        for entity in feed.entity:
            if not entity.HasField('alert'):
                continue

            a = entity.alert
            header = (
                a.header_text.translation[0].text
                if a.header_text.translation else ''
            )
            routes = [
                ie.route_id
                for ie in a.informed_entity
                if ie.route_id
            ]

            rows.append({
                'alert_id':       entity.id,
                'header':         header,
                'cause':          a.Cause.Name(a.cause),
                'effect':         a.Effect.Name(a.effect),
                'routes_affected': ', '.join(set(routes)) if routes else 'System-wide'
            })

        return pd.DataFrame(rows)

    except requests.exceptions.RequestException as e:
        st.warning(f"Could not reach MBTA alerts API: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"Error parsing alerts: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_live_summary() -> dict:
    """
    Return a quick summary of the current live state of the MBTA network.
    Useful for displaying top-level metrics without loading full DataFrames.

    Returns:
        dict: {
            'total_vehicles': int,
            'routes_operating': int,
            'avg_occupancy': float,
            'total_alerts': int,
            'feed_timestamp': str
        }
    """
    vehicles_df = get_live_vehicles()
    alerts_df   = get_live_alerts()

    if vehicles_df.empty:
        return {
            'total_vehicles': 0,
            'routes_operating': 0,
            'avg_occupancy': 0.0,
            'total_alerts': 0,
            'feed_timestamp': 'N/A'
        }

    active_occ = vehicles_df[vehicles_df['occupancy_pct'] > 0]['occupancy_pct']

    return {
        'total_vehicles':   len(vehicles_df),
        'routes_operating': vehicles_df['route_id'].nunique(),
        'avg_occupancy':    round(active_occ.mean(), 1) if len(active_occ) > 0 else 0.0,
        'total_alerts':     len(alerts_df),
        'feed_timestamp':   datetime.now().strftime('%H:%M:%S ET')
    }
