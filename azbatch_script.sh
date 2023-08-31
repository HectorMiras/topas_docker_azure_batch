#!/bin/bash

# Arg 1: Simulation config txt file path
# Arg 2: Number of computing nodes 

# Variables
POOL_ID="topas-pool"
#JOB_ID="topas-job"
SIM_CONFIG_FILE=$1
NTotalNodes=$2  # Change this to your desired number of nodes
DOCKER_IMAGE="hectormiras/topascellsnp:latest"
DOCKER_HUB_USERNAME="hectormiras"
DOCKER_HUB_PASSWORD="yourpassword"
STORAGE_ACCOUNT_NAME="hmstoragebatch"
#STORAGE_CONTAINER_NAME="yourcontainername"
OUTPUT_DIR_PATH="./work/RunDir/run"
OUTPUT_FILENAME1="your_output_file1.txt"
OUTPUT_FILENAME2="your_output_file2.txt"

# Authenticate with Azure
#az login  # This will prompt for credentials or use existing sessions

az account set --subscription "98248176-745c-41ca-8584-f1691f840083"

# Connect to Azure Batch account (replace placeholders with your account and resource group)
az batch account set --name 'cuentaazurebatchhector' --resource-group 'BatchHM'

# Generate the date in yyyymmddhhmmss format
CURRENT_DATE=$(date +"%Y%m%d%H%M%S")
# Define Job ID with the current date
JOB_ID="topas-job-$CURRENT_DATE"
# Use job name as the name for the output container
STORAGE_CONTAINER_NAME=$JOB_ID

# Create a new pool
az batch pool create \
    --id $POOL_ID \
    --vm-size "Standard_A1" \
    --target-dedicated-nodes $NTotalNodes \
    --image "microsoft-azure-batch:ubuntu-server-container:16-04-lts" \
    --node-agent-sku-id "batch.node.ubuntu 16.04" \
    --start-task-command-line "/bin/bash -c 'printenv | grep AZ_BATCH; sleep 90'" \
    --container-configuration containerImageName=$DOCKER_IMAGE registryServer=docker.io


# Wait for pool to be in steady state
until [ "$(az batch pool show --pool-id $POOL_ID --query "allocationState" -o tsv)" == "steady" ]; do
    echo "Waiting for pool to be steady..."
    sleep 30
done

# Create a job to run the tasks
az batch job create --id $JOB_ID --pool-id $POOL_ID

# Creates the storage container
if az storage container exists --name $STORAGE_CONTAINER_NAME --account-name $STORAGE_ACCOUNT_NAME | grep -q '"exists": false'; then
  az storage container create --name $STORAGE_CONTAINER_NAME --account-name $STORAGE_ACCOUNT_NAME
fi
# Upload the Python parameter file to a blob container
az storage blob upload --account-name $STORAGE_ACCOUNT_NAME --container-name $STORAGE_CONTAINER_NAME --type text/plain --file ~/AzureBatch/SimulationConfigFile.py --name SimulationConfigFile.py


# Loop to create tasks
for ((i=1; i<=$NTotalNodes; i++)); do
  az batch task create \
    --job-id $JOB_ID \
    --task-id task-$i \
    --command-line "/bin/bash -c 'git pull origin master && az storage blob download --account-name $STORAGE_ACCOUNT_NAME --container-name $STORAGE_CONTAINER_NAME --name SimulationConfigFile.txt --file ./$SIM_CONFIG_FILE && ./main_run_simulation_docker.sh $i $STORAGE_CONTAINER_NAME'" \
    --image-name $DOCKER_IMAGE \
    --output $OUTPUT_DIR_PATH/$OUTPUT_FILENAME1=raw/$OUTPUT_FILENAME1 \
    --output $OUTPUT_DIR_PATH/$OUTPUT_FILENAME2=raw/$OUTPUT_FILENAME2
done



# Wait for tasks to complete
ALL_TASKS_COMPLETED=0
while [ $ALL_TASKS_COMPLETED -eq 0 ]; do
    COMPLETED_TASKS=$(az batch task list --job-id $JOB_ID --filter "state eq 'completed'" --query "length([])" -o tsv)
    if [ $COMPLETED_TASKS -eq $NTotalNodes ]; then
        ALL_TASKS_COMPLETED=1
    else
        echo "Waiting for tasks to complete. Completed tasks: $COMPLETED_TASKS/$NTotalNodes"
        sleep 30
    fi
done

# Upload output files to storage container
for ((i=1; i<=$NTotalNodes; i++)); do
  OUTPUT_DIR_PATH="./RunDir/run$i"
  az storage blob upload \
    --account-name $STORAGE_ACCOUNT_NAME \
    --container-name $STORAGE_CONTAINER_NAME \
    --type text/plain \
    --name "run$i-$OUTPUT_FILENAME1" \
    --type text/plain \
    --file $OUTPUT_DIR_PATH/$OUTPUT_FILENAME1

  az storage blob upload \
    --account-name $STORAGE_ACCOUNT_NAME \
    --container-name $STORAGE_CONTAINER_NAME \
    --type text/plain \
    --name "run$i-$OUTPUT_FILENAME2" \
    --file $OUTPUT_DIR_PATH/$OUTPUT_FILENAME2
done

# Once all tasks are done, delete the pool to avoid extra costs
az batch pool delete --pool-id $POOL_ID

echo "All tasks completed and pool deleted!"

