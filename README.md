# TXDOT Fatality Rate Calculations

## Project Description

The objective of this analysis is to analyze crash data collected by TXDOT, focusing on a clustering analysis based on crash locations. This analysis could highlight more hazardous road sections. However, urban clusters will inherently have higher crash counts than rural clusters. Therefore, calculating a fatality rate for each cluster is necessary to normalize the data to a standard metric. By identifying clusters with the highest fatality rates per crash, it is possible to pinpoint the most dangerous intersections and roadway segments in Texas. Civil engineers can then utilize this data to identify roadway segments with potential design flaws that may require maintenance or redesign.

## Installations

Since it is difficult maintaining all installations and dependencies to run pyspark on a local machine, a jupyter/pyspark Docker container image was used to locally develop and test the code for this project. The container image used to run pyspark in jupyter notebook is called `jupyter/pyspark-notebook` and can be found on the Docker Hub. Installation directions below:

Docker Pull Command

`docker pull jupyter/pyspark-notebook`

## Data

The data was collected from the TXDOT CRIS database that contains records crashes in the state of Texas from 2014-2024. The data used for this project contains all crashes from 2024 to 2019. The data is stored in private s3 buckets as csv files. You may request the TXDOT crash data from the [CRIS Bulk Request Page](https://www.txdot.gov/apps-cg/crash_records/form.htm).

## File Structure
```
FINAL_PROJECT/
├── docs/
│   ├── Part 1 Proposal.docx              <-- Part 1 proposal Word document
│   ├── Part 1 Proposal.pdf               <-- Part 1 proposal PDF document
│   ├── Part 2 Proposal.docx              <-- Part 2 proposal Word document
│   └── Part 2 Proposal.pdf               <-- Part 2 proposal PDF document
├── data/                                 <-- Folder contains all data used for local testing and development
│   ├── plots/                            <-- Folder contains plots produced from the analysis
│   ├── processed_pandas_data/            <-- Folder contains all CSV files used for local testing and development in pandas DataFrames
│   └── processed_spark_data/             <-- Folder contains all CSV files used for local testing and development in spark.sql.DataFrames
├── notebooks/                            <-- Folder contains all Jupyter notebooks run in the project
│   ├── create_regions_pandas.ipynb       <-- Notebook divides the crash_data into subsets for easier local testing and development using pandas
│   ├── create_regions_spark.ipynb        <-- Notebook divides the crash_data into subsets for easier local testing and development using PySpark
│   ├── test_pandas_clustering.ipynb      <-- Notebook tests the functions and classes developed in pandas_clustering.py
│   └── test_spark_clustering.ipynb       <-- Notebook tests the functions and classes developed in spark_clustering.py
├── spark_batch_files/                    <-- Folder for all PySpark batch files used in the project
│   ├── spark_batch_cluster_points.py     <-- Notebook divides the crash_data into subsets for easier local testing and development using pandas
│   ├── test_spark_batch_cluster.ipynb    <-- Notebook divides the crash_data into subsets for easier local testing and development using PySpark
├── src/                                  <-- Folder contains all modules developed for the project
│   ├── pandas_clustering.py              <-- Contains functions and classes used to get clusters using pandas DataFrame
│   └── spark_clustering.py               <-- Contains functions and classes to group crashes into clusters using Spark RDD
├── requirements.txt                      <-- The requirements.txt file
└── README.md                             <-- README.md
```
