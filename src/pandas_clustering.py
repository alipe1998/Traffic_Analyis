import pandas as pd
import sys
from pathlib import Path
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
from shapely.geometry import Point
from sklearn.cluster import DBSCAN
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import pdist
from typing import Tuple
from io import StringIO
from urllib.parse import urlparse
import boto3

# get root directory of project
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

import config

DATA_DIR = ROOT_DIR / 'data' # get data directory of project

BUCKET_NAME = 'txdot-crash-data' # bucket name for the location where crash data is stored in my s3 account

def read_crash_data(s3_url: str) -> pd.DataFrame:
    '''
    Loads in crash data from an s3 bucket and outputs a pyspark rdd. This requires key and secret to
    s3 bucket containing the crash data. The key and secret can be changed in the config.py file
    
    Args:
    - s3 url (str): s3 url to file object

    Returns:
    - df (pd.Dataframe): dataframe containing crash data
    '''
    # helper function to read in s3 files
    def parse_s3_url(s3_url):
        parsed_url = urlparse(s3_url)
        bucket_name = parsed_url.netloc
        object_key = parsed_url.path.lstrip('/')
        return bucket_name, object_key
    
    #s3_url = 's3://txdot-crash-data-rice/raw_data/combined_cleaned_group_crash.csv'
    bucket_name, object_key = parse_s3_url(s3_url)
    # set key_id and secret_access_key in config file
    s3 = boto3.client('s3',
                      aws_access_key_id=config.aws_access_key_id,
                      aws_secret_access_key=config.aws_secret_access_key)
    # read in s3 object and return a pandas dataframe
    s3_object = s3.get_object(Bucket=bucket_name, Key=object_key) 
    file_content = s3_object['Body'].read().decode('utf-8') 
    csv_buffer = StringIO(file_content) 
    return  pd.read_csv(csv_buffer)

def create_region_csv(crash_data_df: pd.DataFrame, lat_bounds: Tuple[float, float], long_bounds: Tuple[float, float], region_filename: str) -> pd.DataFrame:
    '''
    Uses max and min latitude and longitude to create regions within the crash dataframe. The regions are saved as a csv file in data/processed_pandas_data/ driectory.

    Args:
    - crash_data_df (pd.DataFrame): DataFrame of the crash data.
    - lat_bounds (Tuple[float, float]): Tuple containing min and max latitude of the region (lat_min, lat_max).
    - long_bounds (Tuple[float, float]): Tuple containing min and max longitude of the region (long_min, long_max).
    - region_filename (str): The name of the CSV file containing the crash data for the region subset.

    Returns:
    - pd.DataFrame: DataFrame containing the crash data for the region subset.
    '''
    # unpack the lat and long tuple
    lat_min, lat_max = lat_bounds[0], lat_bounds[1]
    lon_min, lon_max = long_bounds[0], long_bounds[1]
    # keep only specific columns
    col_to_keep = ['Crash_ID', 'Crash_Fatal_Fl', 'Crash_Date', 'Latitude', 'Longitude',
                   'Sus_Serious_Injry_Cnt', 'Nonincap_Injry_Cnt', 'Poss_Injry_Cnt',
                   'Non_Injry_Cnt', 'Unkn_Injry_Cnt', 'Tot_Injry_Cnt', 'Death_Cnt']
    crash_data_short = crash_data_df[col_to_keep]
    # filter rows based on region
    region_df = crash_data_short[(crash_data_df['Latitude'] >= lat_min) & (crash_data_short['Latitude'] <= lat_max) & 
                (crash_data_short['Longitude'] >= lon_min) & (crash_data_short['Longitude'] <= lon_max)]
    csv_buffer = StringIO()
    region_df.to_csv(csv_buffer, index=False)
    
    # set key_id and secret_access_key in config file
    s3 = boto3.client('s3',
                      aws_access_key_id=config.aws_access_key_id,
                      aws_secret_access_key=config.aws_secret_access_key)
    bucket_name = 'txdot-crash-data-rice'
    bucket_key = f"processed_pandas_data/{region_filename}"
    # save file in s3 bucket
    s3.put_object(Bucket=bucket_name, Key=bucket_key, Body=csv_buffer.getvalue())
    print(f"DataFrame saved to {bucket_name}/{bucket_key} in s3")
    # save file locally
    region_df.to_csv(DATA_DIR / 'processed_pandas_data' / region_filename, index=False)
    return region_df

def filter_by_year(df, year):
    """
    Filters the DataFrame for rows where the Crash_Date is in the specified year
    and saves the result to an S3 bucket.

    Parameters:
    - df: The input DataFrame with a 'Crash_Date' column in 'mm/dd/yyyy' format.
    - year: The year to filter the DataFrame by.
    """
    df['Crash_Date'] = pd.to_datetime(df['Crash_Date'], format='%m/%d/%Y')
    filtered_df = df[df['Crash_Date'].dt.year == year]
    
    csv_buffer = StringIO()
    filtered_df.to_csv(csv_buffer, index=False)
    # Set up the S3 client with provided credentials
    s3 = boto3.client('s3',
                      aws_access_key_id=config.aws_access_key_id,
                      aws_secret_access_key=config.aws_secret_access_key)
    bucket_name = 'txdot-crash-data-rice'
    bucket_key = f"processed_pandas_data/crash_data_{year}"

    # Save the CSV data to the S3 bucket
    s3.put_object(Bucket=bucket_name, Key=bucket_key, Body=csv_buffer.getvalue())

    print(f"DataFrame saved to {bucket_name}/{bucket_key} in s3")
    return filtered_df

##################################### Class for Models and Plotting #####################################

class CrashDataProcessor:
    def __init__(self, regions_df: pd.DataFrame):
        '''
        Initializes the CrashDataProcessor with the given DataFrame.

        Args:
        - regions_df (pd.DataFrame): DataFrame containing the regions subset data.
        '''
        self.regions_df = regions_df

    def DBSCAN_model(self, eps: float = 0.003, min_samples: int = 100) -> pd.DataFrame:
        '''
        Clusters the data by latitude and longitude from the regions dataframe using the DBSCAN algorithm. DBSCAN is very useful when clustering crash data because it uses density based clustering and is optimized for clustering irregular connected shapes. Crash clusters tend to follow irregular but connected shapes near intersections or along roadway segments.

        Args:
        - eps (float): The maximum distance between two samples for them to be considered as in the same neighborhood (default is 0.003).
        - min_samples (int): The number of samples (or total weight) in a neighborhood for a point to be considered as a core point (default is 100).
        
        Returns:
        - pd.DataFrame: Regions DataFrame containing a new column with DBSCAN clusters.
        '''
        # check if latitude and longitude columns exist
        if 'Latitude' not in self.regions_df.columns or 'Longitude' not in self.regions_df.columns:
            raise ValueError("DataFrame must contain 'Latitude' and 'Longitude' columns")

        # perform DBSCAN and save results in a column called 'DBSCAN_clusters'
        coords = self.regions_df[['Latitude', 'Longitude']].values
        db = DBSCAN(eps=eps, min_samples=min_samples).fit(coords)
        self.regions_df['DBSCAN_cluster'] = db.labels_
        return self

    def hier_model(self, linkage_type: str = 'ward') -> pd.DataFrame:
        '''
        Clusters by latitude and longitude from the regions dataframe using the hierarchical clustering algorithm.
        The number of clusters is determined by dividing the number of crashes by 250 to ensure and average of 250 crashes per cluster. 
        Args:
        - linkage_type (str): The linkage criterion to use. One of 'ward', 'complete', 'average', 'single' (default is 'ward').

        Returns:
        - pd.DataFrame: Regions DataFrame containing a new column with hierarchical clusters.
        '''
        # check if linkage type is correct
        valid_linkages = ['ward', 'complete', 'average', 'single']
        if linkage_type not in valid_linkages:
            raise ValueError(f"Invalid linkage_type. Expected one of {valid_linkages}")
        # check if latitude and longitude columns exist
        if 'Latitude' not in self.regions_df.columns or 'Longitude' not in self.regions_df.columns:
            raise ValueError("DataFrame must contain 'Latitude' and 'Longitude' columns")
        # perform hierarchical clustering algorithm and save results in hier_cluster column
        coords = self.regions_df[['Latitude', 'Longitude']].values
        distance_matrix = pdist(coords)
        Z = linkage(distance_matrix, method=linkage_type)
        avg_crashes_per_cluster = 250 # determine an average number of crashes per cluster
        num_clusters = len(self.regions_df) / avg_crashes_per_cluster
        self.regions_df['hier_cluster'] = fcluster(Z, num_clusters, criterion='maxclust')
        return self
    
    def plot_clusters(self, cluster_column: str, zoom: int = 12, save_path: str = None) -> None:
        '''
        Plots regional clusters on a basemap and optionally saves the plot to a file.
    
        Args:
        - cluster_column (str): The name of the column containing cluster labels.
        - zoom (int): The zoom level for the basemap (default is 12).
        - save_path (str, optional): The file path to save the plot. If None, the plot is not saved.
        '''
        # check if latitude and longitude columns exist
        if not {'Latitude', 'Longitude', cluster_column}.issubset(self.regions_df.columns):
            raise ValueError("DataFrame must contain 'Latitude', 'Longitude', and the specified cluster column")
        # call geopandas object
        gdf = gpd.GeoDataFrame(self.regions_df, geometry=gpd.points_from_xy(self.regions_df.Longitude, self.regions_df.Latitude))
        gdf.set_crs(epsg=4326, inplace=True) # coord reference system (CRS) to WGS84 (latitude and longitude)
        gdf_web_mercator = gdf.to_crs(epsg=3857)
        fig, ax = plt.subplots(figsize=(10, 10))
        # plot the crash locations with cluster labels
        gdf_web_mercator.plot(ax=ax, column=cluster_column, categorical=True, legend=False, markersize=10, cmap='tab10', alpha=0.6)
        # basemap using OpenStreetMap.Mapnik with adjustable zoom
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom=zoom)
        minx, miny, maxx, maxy = gdf_web_mercator.total_bounds
        ax.set_xlim(minx, maxx)
        ax.set_ylim(miny, maxy)
        plt.title('Crash Locations Clusters')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        if save_path:
            plt.savefig(save_path, bbox_inches='tight')
        plt.show()
