#!/bin/bash -xe
# Deploy Airflow DAGs/tasks
# Sync nydus code and all airflow dags/tasks with AWS Elastic File System mount

sudo apt-get update
sudo apt-get install -y nfs-common dnsutils rsync

echo "Deploying to ${ENVIRONMENT}..."

# Make sure efs temp directory exists
if [[ ! -d /tmp/efs ]] ; then
    mkdir /tmp/efs
fi

# Mount EFS to jenkins worker
export EFS_URI=`dig +short $(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone).${EFS_HOST} @169.254.169.253 | awk "{ print ; exit }"`
sudo mount -v -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 $EFS_URI:/ /tmp/efs

for thing in dags nydus tasks; do
    rsync -rav --delete ${thing}/ /tmp/efs/${thing}
done

cp version /tmp/efs

sudo umount /tmp/efs
