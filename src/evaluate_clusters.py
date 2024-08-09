import pandas as pd
import boto3
from io import StringIO
from folium import Map, CircleMarker
import config  # Import the config file containing AWS credentials

def read_csv_from_s3(s3_url: str) -> pd.DataFrame:
    '''
    Reads in the CSV files with the cluster centroids and their fatality rates from an S3 bucket,
    then combines all CSV files in the S3 directory into a single pandas DataFrame.

    Inputs:
    - s3_url (str): Directory to an S3 bucket with CSV files in it

    Returns:
    - pandas DataFrame object
    '''
    # Parse the S3 URL
    s3_components = s3_url.replace("s3://", "").split("/")
    bucket_name = s3_components[0]
    prefix = "/".join(s3_components[1:])

    # Initialize S3 client using credentials from config.py
    s3 = boto3.client(
        's3',
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
    )

    # List objects within the specified S3 bucket and prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Combine all CSV files into a single DataFrame
    all_dataframes = []

    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            csv_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
            csv_content = csv_obj['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            all_dataframes.append(df)

    # Concatenate all DataFrames
    combined_df = pd.concat(all_dataframes, ignore_index=True)

    return combined_df

def plot_highest_fatality(crash_data_df: pd.DataFrame, rank: int):
    '''
    Finds the nth ranked fatality_rate cluster and uses the cluster_latitude and cluster_longitude
    to plot all clusters within one mile of the cluster centroid on a map.

    Inputs:
    - crash_data_df (pandas DataFrame): DataFrame containing crash data with centroid and fatality rates.
    - rank (int): The rank of the fatality_rate cluster to visualize.
    '''
    # Sort clusters by fatality_rate in descending order
    sorted_df = crash_data_df.sort_values(by='fatality_rate', ascending=False).reset_index(drop=True)

    # Get the nth ranked cluster
    nth_cluster = sorted_df.iloc[rank - 1]
    centroid_lat = nth_cluster['centroid_latitude']
    centroid_lon = nth_cluster['centroid_longitude']

    # Calculate distance from nth cluster to all clusters
    crash_data_df['distance'] = ((crash_data_df['centroid_latitude'] - centroid_lat)**2 +
                                 (crash_data_df['centroid_longitude'] - centroid_lon)**2).pow(0.5) * 69.0  # 69 miles per degree

    # Filter clusters within one mile of the centroid
    nearby_clusters = crash_data_df[crash_data_df['distance'] <= 1.0]

    # Plot using Folium
    folium_map = Map(location=[centroid_lat, centroid_lon], zoom_start=14)

    for _, row in nearby_clusters.iterrows():
        CircleMarker(
            location=(row['centroid_latitude'], row['centroid_longitude']),
            radius=5,
            color='red',
            fill=True,
            fill_opacity=0.7
        ).add_to(folium_map)

    return folium_map


