"""
Batch Workers Script

Description:
    This script manages and orchestrates the execution of simulation tasks using Azure Batch Service. It is designed
    to automate the process of setting up Azure resources, submitting tasks, and handling the outputs.

    Key Operations:
    1. Configuration loading: Load settings from 'appconfig.json' and 'simconfig.json'.
    2. Azure Blob Storage:
        - Set up Blob Service Client for interactions with Azure's Blob Storage.
        - Create storage containers and generate SAS tokens for authentication.
        - Upload necessary files to the Blob Storage for use in the tasks.
    3. Azure Batch:
        - Set up a connection to the Azure Batch service.
        - Create a pool of VMs using specified configurations (e.g., VM size, Docker image).
        - Create and submit a job that contains tasks (simulations) to be run on the VMs in the pool.
        - Each task clones a specified GitHub repository and runs a simulation script within.
    4. Monitoring:
        - Monitor the status of the tasks and ensure they complete successfully.
    5. Cleanup (Optional):
        - Provide options to delete the storage container, job, and pool in Azure, aiding in resource management.

Usage:
    Run this script directly to initiate the worker tasks in Azure Batch:
        python batch_simulation_runner.py /path/to/simconfig.json

Author: Hector Miras del Rio
Date: September 2023
"""


import datetime
import time
import os
import shutil
import sys

import azure.batch.models as batchmodels
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

from auxiliar_methods import query_yes_no, ConfigClass
from azure_batch_methods import generate_sas_for_container, upload_file_to_container, create_pool, create_job, \
    add_tasks, wait_for_tasks_to_complete, print_task_output, print_batch_exception, download_output_files
#import logging
#logging.basicConfig(level=logging.DEBUG)


if __name__ == '__main__':

    start_time = datetime.datetime.now().replace(microsecond=0)
    print(f'Sample start: {start_time}')
    print()

    appconfig = ConfigClass('appconfig.json')
    #simconfig = ConfigClass('simconfig.json')
    sim_config_file = sys.argv[1]
    #sim_config_file = "/home/hector/mytopassimulations/MGHsimulations/TOPAS_CellsNPs/simconfig.json"
    #sim_config_file = "./example/simconfig.json"
    simconfig = ConfigClass(sim_config_file)

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
    STORAGE_CONTAINER_NAME = f"{simconfig.SIM_ID}".lower()
    POOL_ID_WORKERS = f'{simconfig.SIM_ID}-workers'

    # Use the blob client to create the containers in Azure Storage if they
    # don't yet exist.
    retry_count = 0
    while retry_count < 5:
        input_container_name = STORAGE_CONTAINER_NAME # pylint: disable=invalid-name
        try:
            blob_service_client.create_container(input_container_name)
        except ResourceExistsError:
            print(f"Container {input_container_name} already exists")
            break
        except Exception as e:
            print(f"Failed to create container: {e}")
            retry_count += 1
            time.sleep(2 ** retry_count)  # exponential backoff

    # Generate sas token for the container
    container_sas_token = generate_sas_for_container(STORAGE_CONTAINER_NAME,
                                                     appconfig.STORAGE_ACCOUNT_NAME,
                                                     appconfig.STORAGE_ACCOUNT_KEY)
    container_url = f"https://{appconfig.STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{input_container_name}?{container_sas_token}"

    # Set the path to the directory you want to zip
    directory_to_zip = simconfig.LOCAL_SIM_PATH

    # Set the path for the output zip file (without extension)
    output_zip_path = os.path.join(sys.path[0], 'SIM_DIR')

    # Create the zip archive
    shutil.make_archive(output_zip_path, 'zip', directory_to_zip)

    # Upload and generate ResourceFiles
    files_to_upload = [f"{output_zip_path}.zip", sim_config_file]
    resource_files = []
    for file in files_to_upload:
        resource_file = upload_file_to_container(
            appconfig=appconfig,
            blob_storage_service_client=blob_service_client,
            container_name=STORAGE_CONTAINER_NAME,
            file_path=file
        )
        resource_files.append(resource_file)

    # Optionally, you can remove the zip file after uploading if you don't need it
    os.remove(f"{output_zip_path}.zip")

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = SharedKeyCredentials(appconfig.BATCH_ACCOUNT_NAME,
                                       appconfig.BATCH_ACCOUNT_KEY)

    batch_client = BatchServiceClient(
        credentials,
        batch_url=appconfig.BATCH_ACCOUNT_URL)

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        create_pool(appconfig=appconfig,
                    batch_service_client=batch_client,
                    pool_id=POOL_ID_WORKERS,
                    node_count=simconfig.POOL_NODE_COUNT,
                    vm_size=simconfig.POOL_VM_SIZE,
                    docker_image=appconfig.WORKER_DOCKER_IMAGE)

        # Create the job that will run the tasks.
        create_job(batch_service_client=batch_client,
                   job_id=JOB_ID_WORKERS,
                   pool_id=POOL_ID_WORKERS)

        # git_clone_command = f'git clone https://{appconfig.GIT_TOKEN}@github.com/{appconfig.GIT_USER}/{appconfig.GIT_REPO}.git'
        COMMAND_TEMPLATE = (
            "/bin/bash -c \"current_dir=$(pwd) && "
            "unzip -n SIM_DIR.zip || (echo 'Failed to unzip' && exit 1) && "
            "ls -la && $current_dir/{run_script} {node_id}\"")

        #command = COMMAND_TEMPLATE.format(git_command=git_clone_command, run_script=simconfig.RUN_SCRIPT)

        #add_tasks(batch_service_client=batch_client,
        #          job_id=JOB_ID_WORKERS,
        #          total_nodes=simconfig.POOL_NODE_COUNT,
        #          resource_files=resource_files,
        #          container_url=container_url,
        #          docker_image=appconfig.WORKER_DOCKER_IMAGE,
        #          command_template=COMMAND_TEMPLATE,
        #          file_patterns=simconfig.OUTPUT_FILE_PATTERNS)

        #command = COMMAND_TEMPLATE.format(run_script=simconfig.RUN_SCRIPT)
        add_tasks(batch_service_client=batch_client,
                  job_id=JOB_ID_WORKERS,
                  simconfig=simconfig,
                  resource_files=resource_files,
                  container_url=container_url,
                  docker_image=appconfig.WORKER_DOCKER_IMAGE,
                  command_template=COMMAND_TEMPLATE)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client, JOB_ID_WORKERS, datetime.timedelta(minutes=30))

        print("  Success! All worker tasks reached the 'Completed' state within the "
              "specified timeout period.")

        # Print the stdout.txt and stderr.txt files for each task to the console
        print_task_output(batch_client, JOB_ID_WORKERS, "stdout.txt")

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
        # Download nodes simulation outputs?
        if query_yes_no('Download simulation results:') == 'yes':
            download_output_files(
                appconfig=appconfig,
                container_name=STORAGE_CONTAINER_NAME,
                local_dir=f'{simconfig.LOCAL_SIM_PATH}'
            )
        # Clean up storage resources
        if query_yes_no('Delete simulation container from storage?') == 'yes':
            print(f'Deleting container [{input_container_name}]...')
            blob_service_client.delete_container(input_container_name)

        # Clean up Batch resources (if the user so chooses).
        if query_yes_no('Delete job/pool?') == 'yes':
            batch_client.pool.delete(POOL_ID_WORKERS)
            batch_client.job.delete(JOB_ID_WORKERS)
