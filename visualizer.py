import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import geopandas as gpd
import plotly.express as px
import requests
import streamlit as st
from pygwalker.api.streamlit import StreamlitRenderer, init_streamlit_comm


def flatten_json(y):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                flatten(x[a], f"{name}{a}_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, f"{name}{i}_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def fetch_data(
    feature, url="https://api-prod.raw-data.hotosm.org/v1/stats/polygon/", max_retries=3
):
    geometry = json.dumps({"geometry": feature["geometry"]})
    headers = {"Content-Type": "application/json"}

    for attempt in range(max_retries):
        response = requests.post(url, headers=headers, data=geometry)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            if attempt < max_retries - 1:
                sleep_time = 60
                retry_msg = (
                    f"Rate limited by the API. Retrying in {sleep_time} seconds..."
                )
                st.warning(retry_msg)
                time.sleep(sleep_time)
            else:
                retry_msg = (
                    "Max retries exceeded after rate limit. Please try again later."
                )
                st.warning(retry_msg)
                return None
        else:
            return None


def process_and_update_geojson(geojson):
    features = geojson["features"]
    total_features = len(features)

    progress_bar = st.progress(0)
    progress_value = 0

    progress_increment = 1 / total_features
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_feature = {
            executor.submit(fetch_data, feature): feature for feature in features
        }

        for future in as_completed(future_to_feature):
            feature = future_to_feature[future]
            result = future.result()
            if result:
                flat_result = flatten_json(result)
                feature["properties"].update(flat_result)
            progress_value += progress_increment
            progress_bar.progress(min(int(progress_value * 100), 100))

    progress_bar.progress(100)

    return geojson


def create_geodataframe_from_updated_geojson(geojson):
    gdf = gpd.GeoDataFrame.from_features(geojson["features"])
    gdf["x"] = gdf.centroid.x
    gdf["y"] = gdf.centroid.y
    return gdf


def gen_chloropleth_map(geojson, gdf, popup_fields, field_to_plot):
    gdf["popup_text"] = gdf[popup_fields].apply(
        lambda row: "<br>".join(row.values.astype(str)), axis=1
    )

    fig = px.choropleth_mapbox(
        gdf,
        geojson=geojson,
        locations=gdf.index,
        color=field_to_plot,
        color_continuous_scale="Viridis",
        mapbox_style="carto-positron",
        zoom=3,
        center={"lat": gdf.centroid.y.mean(), "lon": gdf.centroid.x.mean()},
        opacity=0.5,
        hover_data=["popup_text"],
    )

    st.plotly_chart(fig)


st.set_page_config(page_title="Data Completeness Stats", layout="wide")

st.title("OSM Data completeness stats")

uploaded_file = st.file_uploader("Upload a GeoJSON file", type=["geojson"])

geojson_text = st.text_area("or paste GeoJSON here")

geojson = None

if uploaded_file is not None:
    geojson = json.load(uploaded_file)

elif geojson_text:
    try:
        geojson = json.loads(geojson_text)
    except json.JSONDecodeError:
        st.error("Invalid GeoJSON")

if geojson:
    with st.spinner("Fetching data..."):
        updated_geojson = process_and_update_geojson(geojson)
        gdf = create_geodataframe_from_updated_geojson(updated_geojson)
        df = gdf.drop(
            columns=[
                "geometry",
                "summary_buildings",
                "summary_roads",
                "meta_indicators",
                "meta_metrics",
            ]
        )
        st.write(df)
        geojson_data = gdf.to_json()

        st.download_button(
            label="Download GeoJSON",
            data=geojson_data,
            file_name="updated_data.geojson",
            mime="application/json",
        )
        st.subheader("Map")

        field_to_plot = st.selectbox("Select the field to plot", gdf.columns)
        popup_fields = st.multiselect(
            "Select fields for the popup", gdf.columns, default=gdf.columns[0]
        )
        if st.button("Generate Choropleth Map"):
            geojson = json.loads(geojson_data)
            gen_chloropleth_map(
                geojson=geojson,
                gdf=gdf,
                popup_fields=popup_fields,
                field_to_plot=field_to_plot,
            )
        st.subheader("Visualization")
        init_streamlit_comm()

        @st.cache_resource
        def get_pyg_renderer() -> "StreamlitRenderer":
            return StreamlitRenderer(df, use_kernel_calc=True, debug=False)

        renderer = get_pyg_renderer()

        renderer.render_explore()
