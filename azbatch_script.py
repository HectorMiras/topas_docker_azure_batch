#!/usr/bin/env python3
import sys

# Azure Batch SDK imports
from azure.batch import BatchServiceClient
from azure.batch.models import (
    PoolAddParameter,
    VirtualMachineConfiguration,
    ImageReference,
    ContainerConfiguration,
    ContainerType,
    TaskAddParameter,
    CloudTask,
    OutputFile,
    OutputFileBlobContainerDestination,
    TaskContainerSettings,
    OutputFileUploadOptions,
    ComputeNodeState,
    OutputFileUploadCondition,
    UserAccount
)
from azure.batch.batch_auth import SharedKeyCredentials

# Azure Storage SDK imports
from azure.storage.blob import (
    BlobServiceClient, 
    BlobClient, 
    ContainerClient,
    BlobType
)

# Standard library and other necessary imports
import datetime
import time
from datetime import datetime, timedelta
import os
import json

# Load the configuration from 'config.json'
with open('config.json', 'r') as file:
    config_data = json.load(file)

# Assign the values from the JSON to your Python variables
BATCH_ACCOUNT_NAME = config_data["BATCH_ACCOUNT_NAME"]
BATCH_ACCOUNT_KEY = config_data["BATCH_ACCOUNT_KEY"]
BATCH_ACCOUNT_URL = config_data["BATCH_ACCOUNT_URL"]
STORAGE_ACCOUNT_NAME = config_data["STORAGE_ACCOUNT_NAME"]
STORAGE_ACCOUNT_KEY = config_data["STORAGE_ACCOUNT_KEY"]
POOL_ID = config_data["POOL_ID"]
SIM_CONFIG_FILE = config_data["SIM_CONFIG_FILE"]
NTotalNodes = config_data["NTotalNodes"]
DOCKER_IMAGE = config_data["DOCKER_IMAGE"]
DOCKER_HUB_USERNAME = config_data["DOCKER_HUB_USERNAME"]
OUTPUT_FILE_NAMES = config_data["OUTPUT_FILE_NAMES"]
OUTPUT_DIR_PATH = config_data["OUTPUT_DIR_PATH"]

# Initialize the Batch client
shared_key_credentials = SharedKeyCredentials(BATCH_ACCOUNT_NAME, BATCH_ACCOUNT_KEY)
batch_client = BatchServiceClient(shared_key_credentials, batch_url=BATCH_ACCOUNT_URL)

# Define your Azure Storage account details
STORAGE_CONNECTION_STRING = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"

# Initialize the Blob service client
blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)

#batch_client = BatchServiceClient(account_url=BATCH_ACCOUNT_URL, credential=BATCH_ACCOUNT_KEY)
#blob_client = BlobServiceClient(account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", credential=STORAGE_ACCOUNT_KEY)

# The ImageReference provided below is a generic one for Ubuntu 16.04 with Docker support. 
# Modify as needed.
IMAGE_REFERENCE = ImageReference(publisher="microsoft-azure-batch", offer="ubuntu-server-container", sku="16-04-lts", version="latest")
# Container configuration
container_config = ContainerConfiguration(
    type=ContainerType.docker_compatible,
    container_image_names=[DOCKER_IMAGE],
    container_registries=[{"registry_server": "docker.io"}])

user_account = UserAccount(
    name="batchuser",  # This is a sample username. You can use any name you like.
    password="YourSecurePassword123!",  # Choose a secure password.
    elevation_level="admin"  # Or "nonadmin", based on your requirements.
)

pool = PoolAddParameter(
    id=POOL_ID,
    vm_size="Standard_A1",
    target_dedicated_nodes=NTotalNodes,
    user_accounts=[user_account],
    virtual_machine_configuration=VirtualMachineConfiguration(
        image_reference=IMAGE_REFERENCE,
        node_agent_sku_id="batch.node.ubuntu 16.04",
        container_configuration=container_config
    )
)
batch_client.pool.add(pool)

# Wait for all VMs in the pool to be allocated
timeout = timedelta(minutes=30)  # For example, wait up to 30 minutes
timeout_expiration = datetime.now() + timeout

while datetime.now() < timeout_expiration:
    pool = batch_client.pool.get(pool.id)

    # Check if the number of dedicated nodesbatch_client.task.add(job_id=JOB_ID, task=cloud_task) in 'idle' state equals the total number of VMs
    if pool.current_dedicated_nodes == NTotalNodes and all(
            node.state == ComputeNodeState.idle for node in batch_client.compute_node.list(pool.id)):
        print(f"All {NTotalNodes} VMs in the pool are ready!")
        break

    print("Waiting for VMs to be ready...")
    time.sleep(30)  # Wait for 60 seconds before checking again

else:
    print(f"Timeout reached after waiting {timeout} minutes. Not all VMs are ready.")

# Generate the date in yyyymmddhhmmss format
CURRENT_DATE=datetime.now().strftime('%Y%m%d%H%M%S')
# Define Job ID with the current date
JOB_ID=f"topas-job-{CURRENT_DATE}"
# Use job name as the name for the output container
STORAGE_CONTAINER_NAME=JOB_ID

# Check if the container exists# Generate the date in yyyymmddhhmmss format
CURRENT_DATE=datetime.now().strftime('%Y%m%d%H%M%S')
# Define Job ID with the current date
JOB_ID=f"topas-job-{CURRENT_DATE}"
# Use job name as the name for the output container
STORAGE_CONTAINER_NAME=JOB_ID
container_client = blob_service_client.get_container_client(STORAGE_CONTAINER_NAME)
if not container_client.exists():
    # If the container doesn't exist, create it
    blob_service_client.create_container(STORAGE_CONTAINER_NAME)

# Upload the file to the container
blob_client = blob_service_client.get_blob_client(container=STORAGE_CONTAINER_NAME, blob=SIM_CONFIG_FILE)
with open(os.path.expanduser(f"./{SIM_CONFIG_FILE}"), "rb") as data:
    blob_client.upload_blob(data, blob_type=BlobType.BlockBlob)


# The command line for each task
COMMAND_TEMPLATE = "/bin/bash -c 'git pull origin master && az storage blob download --account-name {account_name} --container-name {container_name} --name {config_file} --file ./{config_file} && ./main_run_simulation_docker.sh {node_number} {container_name}'"

for i in range(1, NTotalNodes + 1):
    # Construct the task command
    task_command = COMMAND_TEMPLATE.format(
        account_name=STORAGE_ACCOUNT_NAME,
        container_name=STORAGE_CONTAINER_NAME,
        config_file=SIM_CONFIG_FILE,
        node_number=i
    )

    # Define output file destinations for this specific task
    output_file_destinations = [
        OutputFile(
            file_pattern=f"{OUTPUT_DIR_PATH}/{filename}",
            destination=OutputFileBlobContainerDestination(
                container_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{STORAGE_CONTAINER_NAME}",
                path=f"raw/run{i}-{filename}"  # Prefixing with task number
            ),
            upload_options=OutputFileUploadOptions(upload_condition=OutputFileUploadCondition.task_success)
        ) for filename in OUTPUT_FILE_NAMES
    ]

    # Define the task
    cloud_task = CloudTask(
        id=f"task-{i}",
        command_line=task_command,
        container_settings=TaskContainerSettings(image_name=DOCKER_IMAGE),
        output_files=output_file_destinations
    )
    batch_client.task.add(job_id=JOB_ID, task=cloud_task)

# Initialize all_tasks_completed flag
all_tasks_completed = False

while not all_tasks_completed:
    # Get a list of all tasks with a state of 'completed'
    completed_tasks = [task for task in batch_client.task.list(job_id=JOB_ID) if task.state == 'completed']

    # Check if the number of completed tasks is equal to the total nodes
    if len(completed_tasks) == NTotalNodes:
        all_tasks_completed = True
    else:
        print(f"Waiting for tasks to complete. Completed tasks: {len(completed_tasks)}/{NTotalNodes}")
        time.sleep(30)

# Using the previously initialized 'batch_client'
batch_client.pool.delete(pool_id=POOL_ID)
print("All tasks completed and pool deleted!")

sys.exit()