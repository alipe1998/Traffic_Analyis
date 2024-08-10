#!/bin/bash

# This script creates an EMR cluster for crash data analysis
# Make sure to upload the install_requirements.sh file to your s3 bucket and update the s3 path in the bootstrap-actions below
# before running the script.

aws emr create-cluster \
--name "crash-data-cluster" \
--log-uri "s3://aws-logs-058264074507-us-east-1/elasticmapreduce" \
--release-label "emr-7.2.0" \
--service-role "arn:aws:iam::058264074507:role/service-role/AmazonEMR-ServiceRole-20240731T103547" \
--managed-scaling-policy '{"ComputeLimits":{"UnitType":"Instances","MinimumCapacityUnits":2,"MaximumCapacityUnits":16,"MaximumOnDemandCapacityUnits":16,"MaximumCoreCapacityUnits":16}}' \
--unhealthy-node-replacement \
--ec2-attributes '{"InstanceProfile":"AmazonEMR-InstanceProfile-20240731T103530","EmrManagedMasterSecurityGroup":"sg-0ddf683581a1a16c4","EmrManagedSlaveSecurityGroup":"sg-06c56ba9499317137","KeyName":"crash-data-key","AdditionalMasterSecurityGroups":[],"AdditionalSlaveSecurityGroups":[],"SubnetId":"subnet-03c930d8d08890152"}' \
--tags 'for-use-with-amazon-emr-managed-policies=true' \
--applications Name=Hadoop Name=Hive Name=JupyterEnterpriseGateway Name=Livy Name=Spark \
--instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","Name":"Primary","InstanceType":"r5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}],"EbsOptimized":true}},{"InstanceCount":2,"InstanceGroupType":"CORE","Name":"Core","InstanceType":"r5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}],"EbsOptimized":true}}]' \
--bootstrap-actions '[{"Args":[],"Name":"install-requirements","Path":"s3://path_to/install_requirements.sh"}]' \
--scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
--auto-termination-policy '{"IdleTimeout":3600}' \
--region "us-east-1"