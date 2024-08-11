from pathlib import Path
import os
import sys
import warnings
import pandas as pd
import boto3
from io import StringIO
from folium import Map, CircleMarker
import matplotlib.colors as mcolors
from IPython.display import display

# get root directory of project
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

DATA_DIR = ROOT_DIR / 'data'

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

def read_csv_from_s3(s3_url: str) -> pd.DataFrame:
    '''
    Reads in the CSV files with the cluster centroids and their fatality rates from an S3 bucket,
    then combines all CSV files in the S3 directory into a single pandas DataFrame.

    Inputs:
    - s3_url (str): Directory to an S3 bucket with CSV files in it

    Returns:
    - pandas DataFrame object
    '''
    # parse the S3 URL
    s3_components = s3_url.replace("s3://", "").split("/")
    bucket_name = s3_components[0]
    prefix = "/".join(s3_components[1:])
    # call s3 object
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # concat csv files
    all_dataframes = []
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            csv_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
            csv_content = csv_obj['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            all_dataframes.append(df)
    combined_df = pd.concat(all_dataframes, ignore_index=True)

    return combined_df

def plot_highest_fatality(crash_data_df: pd.DataFrame, rank: int, distance: int, save_path: str = None):
    '''
    Finds the nth ranked fatality_rate cluster and uses the cluster_latitude and cluster_longitude
    to plot all clusters within one mile of the cluster centroid on a map, with colors ranging from
    green to red based on the fatality_rate.

    Inputs:
    - crash_data_df (pandas DataFrame): DataFrame containing crash data with centroid and fatality rates.
    - rank (int): The rank of the fatality_rate cluster to visualize.
    - distance (int): Select clusters that are located within n mile(s) of the specified cluster centroid
    - save_plot (str): If a path is specified then the plot is saved in the specified directory.
    '''
    # sort clusters by fatality_rate in descending order
    sorted_df = crash_data_df.sort_values(by='fatality_rate', ascending=False).reset_index(drop=True)
    nth_cluster = sorted_df.iloc[rank - 1] # retrieve nth ranked cluster
    centroid_lat = nth_cluster['centroid_latitude']
    centroid_lon = nth_cluster['centroid_longitude']
    print(f"Fatality Rate: {round(nth_cluster['fatality_rate'], 2)}")
    # get distance from nth cluster
    crash_data_df['distance'] = ((crash_data_df['centroid_latitude'] - centroid_lat)**2 +
                                 (crash_data_df['centroid_longitude'] - centroid_lon)**2).pow(0.5) * 69.0  # 69 miles per degree

    nearby_clusters = crash_data_df[crash_data_df['distance'] <= distance]
    # get range of fatality rates and normalize
    min_fatality = nearby_clusters['fatality_rate'].min()
    max_fatality = nearby_clusters['fatality_rate'].max()
    nearby_clusters['color_value'] = (nearby_clusters['fatality_rate'] - min_fatality) / (max_fatality - min_fatality)

    # create plot using folium
    colormap = mcolors.LinearSegmentedColormap.from_list("green_red", ["green", "red"])
    folium_map = Map(location=[centroid_lat, centroid_lon], zoom_start=14)

    for _, row in nearby_clusters.iterrows():
        color = colormap(row['color_value'])
        CircleMarker(
            location=(row['centroid_latitude'], row['centroid_longitude']),
            radius=5,
            color=mcolors.to_hex(color),
            fill=True,
            fill_opacity=0.7
        ).add_to(folium_map)
    
    if save_path:
        file_path = Path(save_path)
        folium_map.save(file_path / f"high_fatality_clusters_{rank}.html")  # save the map as an HTML file
    display(folium_map)

    return folium_map


