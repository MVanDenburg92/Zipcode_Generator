import pandas as pd
import geopandas as gpd
import streamlit as st
import numpy as np
import os
import requests
import tempfile
from io import StringIO
from shapely.geometry import Point
import zipfile
import base64
import pydeck as pdk
import json

def create_full_geographic_data(df, zipcode_column, groupby_column, include_columns=None):
    """
    Create a GeoDataFrame for all ZIP codes with their geographic boundaries,
    without any dissolving.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input DataFrame containing ZIP code data
    zipcode_column : str
        Name of the column containing ZIP codes
    groupby_column : str
        Name of the column to group by for visualization.
    include_columns : list of str, optional
        Additional columns from DataFrame to include in shapefile
        
    Returns:
    --------
    geopandas.GeoDataFrame
        GeoDataFrame with ZIP code geometries and associated data
    """
    
    # 1. DATA PREPARATION
    st.info("Starting data preparation...")
    
    if zipcode_column not in df.columns or groupby_column not in df.columns:
        st.error(f"Required columns '{zipcode_column}' or '{groupby_column}' not found.")
        st.stop()
    
    df_clean = df.copy()
    df_clean = df_clean.dropna(subset=[zipcode_column, groupby_column])
    df_clean[zipcode_column] = df_clean[zipcode_column].astype(str).str.zfill(5)
    
    # Ensure groupby_column is always included in aggregation
    columns_to_aggregate = [col for col in include_columns if col != groupby_column]
    if groupby_column not in columns_to_aggregate:
        columns_to_aggregate.append(groupby_column)
    
    agg_dict = {col: 'first' for col in columns_to_aggregate if col != zipcode_column}
    for col in columns_to_aggregate:
        if pd.api.types.is_numeric_dtype(df_clean[col]):
            agg_dict[col] = 'sum'
            
    if agg_dict:
        zip_data = df_clean.groupby(zipcode_column).agg(agg_dict).reset_index()
    else:
        zip_data = df_clean[[zipcode_column, groupby_column]].drop_duplicates()

    st.success(f"Aggregated to {len(zip_data)} unique ZIP codes.")
    
    # 2. GET ZIPCODE BOUNDARIES
    st.subheader("Retrieving Geographic Boundaries")
    unique_zipcodes = set(zip_data[zipcode_column])
    
    try:
        url = "https://www2.census.gov/geo/tiger/TIGER2023/ZCTA520/tl_2023_us_zcta520.zip"
        st.info("Downloading TIGER/Line shapefile from U.S. Census Bureau...")
        response = requests.get(url, stream=True, verify=False)
        response.raise_for_status()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_file:
            tmp_file.write(response.content)
            zip_file_path = tmp_file.name
        
        zcta_gdf = gpd.read_file(f"zip://{zip_file_path}")
        os.unlink(zip_file_path)
        
        st.success(f"Loaded {len(zcta_gdf)} ZIP code boundaries.")
        
    except Exception as e:
        st.error(f"Could not download or process geographic data: {e}")
        st.stop()
        
    # 3. FILTER AND MERGE
    st.subheader("Merging Data")
    zip_data_merge = zip_data.copy()
    zip_data_merge = zip_data_merge.rename(columns={zipcode_column: 'ZCTA5CE20'})

    zcta_gdf['ZCTA5CE20'] = zcta_gdf['ZCTA5CE20'].astype(str)
    zip_data_merge['ZCTA5CE20'] = zip_data_merge['ZCTA5CE20'].astype(str)

    zcta_gdf_filtered = zcta_gdf[zcta_gdf['ZCTA5CE20'].isin(unique_zipcodes)].copy()
    st.info(f"Found boundaries for {len(zcta_gdf_filtered)} of your ZIP codes.")
    
    final_gdf = zip_data_merge.merge(zcta_gdf_filtered, on='ZCTA5CE20', how='left')
    final_gdf = gpd.GeoDataFrame(final_gdf, geometry='geometry', crs='EPSG:4326')
    
    st.success(f"Final merged dataset contains {len(final_gdf)} records.")
    
    return final_gdf

def get_binary_file_downloader_html(file_data, file_label, file_name):
    """Generates a link to download a file."""
    b64 = base64.b64encode(file_data).decode()
    href = f'<a href="data:application/octet-stream;base64,{b64}" download="{file_name}">{file_label}</a>'
    return href

def create_zip_file(geodataframe, zip_file_name):
    """Creates a zip file containing the shapefile components."""
    with tempfile.TemporaryDirectory() as temp_dir:
        shapefile_path = os.path.join(temp_dir, "output.shp")
        geodataframe.to_file(shapefile_path)
        
        zip_path = os.path.join(temp_dir, zip_file_name)
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for file in os.listdir(temp_dir):
                if file.startswith("output."):
                    zf.write(os.path.join(temp_dir, file), arcname=file)
        
        with open(zip_path, 'rb') as f:
            zip_content = f.read()
    return zip_content

# =========================================================================
# STREAMLIT APP CODE
# =========================================================================

st.set_page_config(page_title="Geographic File Generator", layout="wide")
st.title("Geographic File Generator")
st.markdown("This app converts your ZIP code data into geographic files (Shapefile, GeoJSON) and visualizes the results.")

uploaded_file = st.file_uploader("📂 **Upload your CSV file**", type=["csv"])

if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file)
        st.success("✅ File uploaded successfully!")
        
        st.write("---")
        st.subheader("1. Configure Analysis")
        
        cols = list(df.columns)
        zip_col = st.selectbox("Select the column containing ZIP codes:", cols)
        
        groupby_col = st.selectbox(
            "Select a column to group/dissolve the ZIP codes by for visualization:", 
            cols,
            index=0
        )
        
        metric_cols = st.multiselect(
            "Select columns to include in the output files:", 
            cols,
            default=[c for c in cols if c not in [zip_col, groupby_col]]
        )

        if st.button("🚀 Generate Geographic Files"):
            with st.spinner("Generating files... This may take a moment."):
                final_gdf = create_full_geographic_data(df, zip_col, groupby_col, metric_cols)
                
                if final_gdf is not None and not final_gdf.empty:
                    st.success("✅ Files Generated Successfully!")
                    st.write("---")

                    st.subheader("2. Download Results (Full Dataset)")
                    
                    geojson_string = final_gdf.to_json()
                    csv_string = final_gdf.drop(columns=['geometry']).to_csv(index=False)
                    zip_file_name = "output_shapefile.zip"
                    shapefile_zip = create_zip_file(final_gdf, zip_file_name)

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.markdown(get_binary_file_downloader_html(shapefile_zip, "Download Shapefile (.zip)", zip_file_name), unsafe_allow_html=True)
                    with col2:
                        st.download_button(
                            label="Download GeoJSON (.geojson)",
                            data=geojson_string,
                            file_name="output_data.geojson",
                            mime="application/json"
                        )
                    with col3:
                        st.download_button(
                            label="Download CSV (.csv)",
                            data=csv_string,
                            file_name="output_attributes.csv",
                            mime="text/csv"
                        )

                    st.write("---")
                    st.subheader("3. Geographic Visualization (Sampled and Dissolved)")
                    st.markdown(f"This map shows a **dissolved sample** of the ZIP codes to ensure a smooth, interactive experience. The downloadable files above contain the complete, undissolved dataset.")
                    
                    # Create a sampled GeoDataFrame for visualization only
                    unique_groups = final_gdf[groupby_col].unique()
                    if len(unique_groups) > 5:
                        sampled_groups = np.random.choice(unique_groups, size=5, replace=False)
                    else:
                        sampled_groups = unique_groups
                    
                    sampled_gdf = final_gdf[final_gdf[groupby_col].isin(sampled_groups)]
                    
                    # Dissolve the sampled geometries
                    if sampled_gdf.empty:
                         st.warning("No data found for the selected groups to visualize.")
                    else:
                        visual_gdf = sampled_gdf.dissolve(by=groupby_col, aggfunc='sum')
                        visual_gdf = visual_gdf.reset_index()

                        st.info(f"Visualizing a dissolved sample of the following groups from the `{groupby_col}` column: {', '.join(map(str, sampled_groups))}")
    
                        # Corrected: Convert to GeoJSON string, then parse to a dictionary
                        visual_json_data = json.loads(visual_gdf.to_json())
                        
                        st.pydeck_chart(pdk.Deck(
                            map_style='mapbox://styles/mapbox/light-v9',
                            initial_view_state=pdk.ViewState(
                                latitude=visual_gdf.geometry.centroid.y.mean(),
                                longitude=visual_gdf.geometry.centroid.x.mean(),
                                zoom=3.5,
                                pitch=45,
                            ),
                            layers=[
                                pdk.Layer(
                                    'GeoJsonLayer',
                                    data=visual_json_data,
                                    filled=True,
                                    get_fill_color=[200, 30, 0, 160],
                                    stroked=True,
                                    get_line_color=[0, 0, 0],
                                    get_line_width=200,
                                    pickable=True,
                                    auto_highlight=True
                                )
                            ],
                        ))
                    
    except pd.errors.ParserError:
        st.error("Invalid CSV file. Please ensure the file is in a correct CSV format.")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")