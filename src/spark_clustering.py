import os
import matplotlib.pyplot as plt
import contextily as ctx
from shapely.geometry import Point
from typing import Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as sql_sum, count as sql_count
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans, GaussianMixture
import geopandas as gpd
import pyspark

# Configuration for AWS access keys
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

def read_crash_data(spark: SparkSession, s3_url: str):
    '''
    Loads crash data from an S3 bucket into a Spark DataFrame.

    Args:
    - spark (SparkSession): the spark session
    - s3_url (str): S3 filepath to CSV object in the S3 bucket
    Returns:
    - df (pyspark.sql.DataFrame): DataFrame containing crash data
    '''
    # Read CSV from S3 directly into a DataFrame
    df = spark.read.csv(s3_url, header=True, inferSchema=True)
    # Select necessary columns
    columns = [
        'Crash_ID', 'Crash_Fatal_Fl', 'Crash_Date', 'Latitude', 'Longitude',
        'Sus_Serious_Injry_Cnt', 'Nonincap_Injry_Cnt', 'Poss_Injry_Cnt',
        'Non_Injry_Cnt', 'Unkn_Injry_Cnt', 'Tot_Injry_Cnt', 'Death_Cnt'
    ]
    df = df.select(*columns)
    return df

def create_region_dataframe(crash_data_df, lat_bounds: Tuple[float, float], long_bounds: Tuple[float, float]):
    '''
    Filters crash data DataFrame to create regions based on latitude and longitude.

    Args:
    - crash_data_df (DataFrame): DataFrame of the crash data.
    - lat_bounds (Tuple[float, float]): Tuple containing min and max latitude of the region.
    - long_bounds (Tuple[float, float]): Tuple containing min and max longitude of the region.

    Returns: 
    - DataFrame: Spark DataFrame containing the crash data for the region.
    '''
    lat_min, lat_max = lat_bounds
    lon_min, lon_max = long_bounds

    # Filter DataFrame by latitude and longitude
    region_df = crash_data_df.filter(
        (col("Latitude").cast("double") >= lat_min) &
        (col("Latitude").cast("double") <= lat_max) &
        (col("Longitude").cast("double") >= lon_min) &
        (col("Longitude").cast("double") <= lon_max)
    )
    return region_df

# Class for modeling the crash data
class CrashDataProcessor:
    def __init__(self, regions_df):
        '''
        Initializes the CrashDataProcessor with the DataFrame.

        Args:
        - regions_df (pyspark.sql.DataFrame): DataFrame containing the regions subset data.
        '''
        self.regions_df = regions_df

    def assemble_features(self, feature_col="features") -> 'CrashDataProcessor':
        '''
        Assembles the feature vector using latitude and longitude as features.

        Args:
        - feature_col (str): The name of the feature column (default is "features").

        Returns:
        - CrashDataProcessor: self with the DataFrame containing the assembled feature vector.
        '''
        self.regions_df = self.regions_df.withColumn("Latitude", col("Latitude").cast("double")).withColumn("Longitude", col("Longitude").cast("double"))
        self.regions_df = self.regions_df.dropna(subset=["Latitude", "Longitude"])
        
        if feature_col not in self.regions_df.columns:
            assembler = VectorAssembler(inputCols=["Latitude", "Longitude"], outputCol=feature_col)
            self.regions_df = assembler.transform(self.regions_df)
        
        return self

    def KMeans_model(self, seed: int = 42, feature_col="features") -> 'CrashDataProcessor':
        '''
        Clusters the data using the KMeans algorithm.

        Args:
        - seed (int): Random seed (default is 42).
        - feature_col (str): The name of the feature column (default is "features").

        Returns:
        - CrashDataProcessor: self with the updated DataFrame containing a column with KMeans clusters.
        '''
        self.assemble_features(feature_col=feature_col)
        num_clusters = self.estimate_optimal_clusters()
        
        kmeans = KMeans(k=num_clusters, seed=seed, featuresCol=feature_col, predictionCol="kmeans_cluster")
        model = kmeans.fit(self.regions_df)
        self.regions_df = model.transform(self.regions_df)

        return self

    def GMM_model(self, seed: int = 1, feature_col="features") -> 'CrashDataProcessor':
        '''
        Clusters the data using the Gaussian Mixture Model algorithm.

        Args:
        - seed (int): Random seed (default is 1).
        - feature_col (str): The name of the feature column (default is "features").

        Returns:
        - CrashDataProcessor: Self with the updated DataFrame containing GMM clusters.
        '''
        self.assemble_features(feature_col=feature_col)
        num_clusters = self.estimate_optimal_clusters()
        
        gmm = GaussianMixture(k=num_clusters, seed=seed, featuresCol=feature_col, predictionCol="gmm_cluster")
        model = gmm.fit(self.regions_df)
        self.regions_df = model.transform(self.regions_df)
        
        return self

    def estimate_optimal_clusters(self, avg_crashes_per_cluster: int = 250) -> int:
        '''
        Estimate the optimal number of clusters based on the average crashes per cluster.

        Args:
        - avg_crashes_per_cluster (int): The average number of crashes per cluster (default is 250).

        Returns:
        - int: Estimated number of clusters.
        '''
        total_records = self.regions_df.count()
        num_clusters = max(int(total_records / avg_crashes_per_cluster), 1)
        return num_clusters

    def compute_fatality_rate(self, cluster_col: str, save_to_s3: bool = False, s3_path: str = "") -> pyspark.sql.DataFrame:
        '''
        Calculates the cluster centroids and fatality rate for each cluster.

        Args:
        - cluster_col (str): The name of the column containing cluster labels.
        - save_to_s3 (bool): If True, saves the resulting DataFrame as a CSV file to the specified S3 path.
        - s3_path (str): The S3 path where the CSV file should be saved.

        Returns:
        - pyspark.sql.DataFrame: A DataFrame containing the cluster centroid latitude, cluster centroid longitude, and fatality rate.
        '''
        centroid_df = self.regions_df.groupBy(cluster_col).agg(
            avg("Latitude").alias("centroid_latitude"),
            avg("Longitude").alias("centroid_longitude"),
            (sql_sum("Death_Cnt") / sql_count("Death_Cnt")).alias("fatality_rate")
        )
        
        if save_to_s3 and s3_path:
            centroid_df.write.csv(s3_path, header=True, mode='overwrite')
    
        return centroid_df

    def plot_clusters(self, cluster_column: str, zoom: int = 12, save_path: str = None) -> None:
        '''
        Plots regional clusters on a basemap using geopandas.

        Args:
        - cluster_column (str): The name of the column containing cluster labels.
        - zoom (int): The zoom level for the basemap.
        - save_path (str, optional): The file path to save the plot. If None, the plot is not saved.

        Raises:
        - ValueError: If the specified columns are not present in the DataFrame.
        '''
        pandas_df = self.regions_df.select("Latitude", "Longitude", cluster_column).toPandas()
        pandas_df['geometry'] = pandas_df.apply(lambda row: Point(float(row["Longitude"]), float(row["Latitude"])), axis=1)
        
        if not {'Latitude', 'Longitude', cluster_column}.issubset(pandas_df.columns):
            raise ValueError("DataFrame must contain 'Latitude', 'Longitude', and the specified cluster column")
        
        gdf = gpd.GeoDataFrame(pandas_df, geometry='geometry')
        gdf.set_crs(epsg=4326, inplace=True)
        gdf_web_mercator = gdf.to_crs(epsg=3857)

        fig, ax = plt.subplots(figsize=(10, 10))
        gdf_web_mercator.plot(ax=ax, column=cluster_column, categorical=True, legend=False, markersize=8, cmap='tab10', alpha=0.6)
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

def load_and_process_crash_data(spark: SparkSession, s3_url: str) -> CrashDataProcessor:
    """
    Loads crash data from a CSV file on S3 into a Spark DataFrame and converts it into a CrashDataProcessor object.

    Args:
    - spark (SparkSession): The active Spark session.
    - s3_url (str): URL to the CSV file in S3.

    Returns:
    - CrashDataProcessor: An instance of the CrashDataProcessor class with the processed DataFrame.
    """
    df = read_crash_data(spark, s3_url)
    crash_data_processor = CrashDataProcessor(df)
    return crash_data_processor

# Class for modeling and computing metrics on the clustered data
class CentroidClusterData:
    def __init__(self, centroid_df):
        '''
        Initializes the CentroidClusterData with the centroid DataFrame.

        Args:
        - centroid_df (pyspark.sql.DataFrame): DataFrame containing the cluster centroids and fatality rates.
        '''
        self.centroid_df = centroid_df

    def plot_fatality_rate(self, zoom: int = 12, buffer_ratio: float = 0.1, save_path: str = None) -> None:
        '''
        Plots the cluster centroids and colors them based on the fatality rate.

        Args:
        - zoom (int): The zoom level for the basemap.
        - buffer_ratio (float): The ratio of the buffer zone around the plot bounds (default is 0.1).
        - save_path (str, optional): The file path to save the plot. If None, the plot is not saved.

        Raises:
        - ValueError: If the specified columns are not present in the DataFrame.
        '''
        pandas_df = self.centroid_df.toPandas()
        pandas_df['geometry'] = pandas_df.apply(lambda row: Point(float(row["centroid_longitude"]), float(row["centroid_latitude"])), axis=1)
        
        if not {'centroid_latitude', 'centroid_longitude', 'fatality_rate'}.issubset(pandas_df.columns):
            raise ValueError("DataFrame must contain 'centroid_latitude', 'centroid_longitude', and 'fatality_rate' columns")
        
        gdf = gpd.GeoDataFrame(pandas_df, geometry='geometry')
        gdf.set_crs(epsg=4326, inplace=True)
        gdf_web_mercator = gdf.to_crs(epsg=3857)

        fig, ax = plt.subplots(figsize=(10, 10))
        gdf_web_mercator.plot(ax=ax, column='fatality_rate', cmap='RdYlGn_r', legend=False, markersize=100, alpha=0.6)
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom=zoom)
        minx, miny, maxx, maxy = gdf_web_mercator.total_bounds
        buffer_x = (maxx - minx) * buffer_ratio
        buffer_y = (maxy - miny) * buffer_ratio
        ax.set_xlim(minx - buffer_x, maxx + buffer_x)
        ax.set_ylim(miny - buffer_y, maxy + buffer_y)
        plt.title('Cluster Centroids and Fatality Rates')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        
        if save_path:
            plt.savefig(save_path, bbox_inches='tight')

        plt.show()
