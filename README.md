```
FINAL_PROJECT/
├── docs/
│   ├── Part 1 Proposal.docx              <-- Part 1 proposal Word document
│   ├── Part 1 Proposal.pdf               <-- Part 1 proposal PDF document
│   ├── Part 2 Proposal.docx              <-- Part 2 proposal Word document
│   └── Part 2 Proposal.pdf               <-- Part 2 proposal PDF document
├── data/                                 <-- Folder contains all data used for local testing and development 
│   ├── processed_pandas_data/            <-- Folder contains all CSV files used for local testing and development in pandas DataFrames
│   └── processed_spark_data/             <-- Folder contains all CSV files used for local testing and development in spark.sql.DataFrames
├── plots/ 
│   ├── fatality_plots/                   <-- Folder contains plots of the top five most dangerous cluster determined from KMeans algorithm
│   └── small_clustering_plots            <-- Folder contains plots produced from clustering analysis of small regions
├── notebooks/                            <-- Folder contains all Jupyter notebooks run in the project
│   ├── create_regions_pandas.ipynb       <-- Notebook divides the crash_data into subsets for easier local testing and development using pandas
│   ├── create_regions_spark.ipynb        <-- Notebook divides the crash_data into subsets for easier local testing and development using PySpark
│   ├── test_evaluate_clusters.ipynb      <-- Notebook test the evaluate_clusters module using the CSV file produced from the EMR Cluster Analysis
│   ├── test_pandas_clustering.ipynb      <-- Notebook tests the functions and classes developed in pandas_clustering.py
│   └── test_spark_clustering.ipynb       <-- Notebook tests the functions and classes developed in spark_clustering.py
├── spark_batch_files/                    <-- Folder for all PySpark batch files used in the project
│   ├── spark_batch_cluster_points.py     <-- spark batch file that was submitted to the EMR cluster 
│   ├── test_spark_batch_cluster.ipynb    <-- Notebook that test the spark batch file on a small subset of the larger dataset
├── src/                                  <-- Folder contains all modules developed for the project
│   ├── pandas_clustering.py              <-- Contains functions and classes used to get clusters using pandas DataFrame
│   ├── evaluate_clusters.py              <-- Contains functions used to evaluate the results of the Big Data Analysis in the EMR cluster
│   └── spark_clustering.py               <-- Contains functions and classes to group crashes into clusters using Spark RDD
├── requirements.txt                      <-- Inlcudes necessary dependencies to run file. Pyspark dependencies not inlcuded!
├── install_requirements.sh               <-- This sh file was submitted to the EMR cluster to install necessary python libraries. Pyspark dependencies are not included in this file.
├── emr_cluster.sh                        <-- This sh file is used to create the EMR cluster 
└── README.md                             <-- README.md
```