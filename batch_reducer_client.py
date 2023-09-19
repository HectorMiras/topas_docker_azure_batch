"""
Create a pool of 1 node to reduce files from storage.
"""

import datetime
import io
import os
import shutil
import sys
import time

import azure.batch.models as batchmodels
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.batch.models import OutputFile, OutputFileBlobContainerDestination, OutputFileUploadOptions, \
    OutputFileUploadCondition, CloudTask, TaskContainerSettings, OutputFileDestination, ResourceFile
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas,
    generate_container_sas,
    ContainerSasPermissions
)

import appconfig
import simconfig
from auxiliar_methods import query_yes_no
from azure_batch_methods import generate_sas_for_container, upload_file_to_container, create_pool, create_job, \
    add_tasks, wait_for_tasks_to_complete, print_task_output, print_batch_exception, generate_sas_url

if __name__ == '__main__':

    start_time = datetime.datetime.now().replace(microsecond=0)
    print(f'Sample start: {start_time}')
    print()

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    blob_service_client = BlobServiceClient(
        account_url=f"https://{appconfig.STORAGE_ACCOUNT_NAME}.{appconfig.STORAGE_ACCOUNT_DOMAIN}/",
        credential=appconfig.STORAGE_ACCOUNT_KEY
    )

    # Generate the date in yyyymmddhhmmss format
    CURRENT_DATE = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    # Define Job ID with the current date
    JOB_ID_WORKERS = f"{simconfig.SIM_ID}-workers-{CURRENT_DATE}"
    # Use job name as the name for the output container
    STORAGE_CONTAINER_NAME = f"{simconfig.SIM_ID}-{CURRENT_DATE}"
    POOL_ID_WORKERS = f'{simconfig.SIM_ID}-workers'

    # Use the blob client to create the containers in Azure Storage if they
    # don't yet exist.
    input_container_name = STORAGE_CONTAINER_NAME  # pylint: disable=invalid-name
    try:
        blob_service_client.create_container(input_container_name)
    except ResourceExistsError:
        pass

    # Generate sas token for the container
    container_sas_token = generate_sas_for_container(STORAGE_CONTAINER_NAME,
                                                     appconfig.STORAGE_ACCOUNT_NAME,
                                                     appconfig.STORAGE_ACCOUNT_KEY)
    container_url = f"https://{appconfig.STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{input_container_name}?{container_sas_token}"


    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = SharedKeyCredentials(appconfig.BATCH_ACCOUNT_NAME,
                                       appconfig.BATCH_ACCOUNT_KEY)

    batch_client = BatchServiceClient(
        credentials,
        batch_url=appconfig.BATCH_ACCOUNT_URL)

    # reducer job
    # Define Job ID with the current date
    JOB_ID_REDUCER = f"{simconfig.SIM_ID}-reducer-{CURRENT_DATE}"
    POOL_ID_REDUCER = f'{simconfig.SIM_ID}-reducer'
    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        create_pool(batch_service_client=batch_client,
                    pool_id=POOL_ID_WORKERS,
                    node_count=simconfig.POOL_NODE_COUNT,
                    docker_image=appconfig.REDUCER_DOCKER_IMAGE)

        # Create the job that will run the tasks.
        create_job(batch_service_client=batch_client,
                   job_id=JOB_ID_REDUCER,
                   pool_id=POOL_ID_REDUCER)

        # Add the tasks to the job.
        git_clone_command = f'git clone https://{appconfig.GIT_TOKEN}@github.com/{appconfig.GIT_USER}/{appconfig.GIT_REPO}.git'
        COMMAND_TEMPLATE = (
            "/bin/bash -c \"current_dir=$(pwd) && "
            "unzip SIM_DIR.zip || (echo 'Failed to unzip' && exit 1) && "
            "{git_command} && "
            "ls -la && $current_dir/{run_script}\"")
        command = COMMAND_TEMPLATE.format(git_command=git_clone_command, run_script=simconfig.REDUCER_SCRIPT)

        # resource files is now the blob container with the results
        blob_name = f'{STORAGE_CONTAINER_NAME}/nodes_output'
        # resource_files = get_results_dir_from_storage()

        add_tasks(batch_service_client=batch_client,
                  job_id=JOB_ID_REDUCER,
                  total_nodes=1,
                  resource_file=resource_files,
                  container_url=container_url,
                  docker_image=appconfig.REDUCER_DOCKER_IMAGE,
                  command=command)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client, JOB_ID_REDUCER, datetime.timedelta(minutes=30))

        print("  Success! All tasks reached the 'Completed' state within the "
              "specified timeout period.")

        # Print the stdout.txt and stderr.txt files for each task to the console
        print_task_output(batch_client, JOB_ID_REDUCER)

        # Print out some timing info
        end_time = datetime.datetime.now().replace(microsecond=0)
        print()
        print(f'Sample end: {end_time}')
        elapsed_time = end_time - start_time
        print(f'Elapsed time: {elapsed_time}')
        print()
        input('Press ENTER to exit...')

    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise

    finally:
        # Clean up storage resources
        if query_yes_no('Delete container?') == 'yes':
            print(f'Deleting reducer container [{input_container_name}]...')
            blob_service_client.delete_container(input_container_name)

        # Clean up Batch resources (if the user so chooses).
        if query_yes_no('Delete reducer job?') == 'yes':
            batch_client.job.delete(JOB_ID_REDUCER)

        if query_yes_no('Delete reducer pool?') == 'yes':
            batch_client.pool.delete(POOL_ID_REDUCER)
