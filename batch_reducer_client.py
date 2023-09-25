"""
Create a pool of 1 node to reduce files from storage.
"""

import datetime

import azure.batch.models as batchmodels
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.storage.blob import BlobServiceClient

from auxiliar_methods import query_yes_no, ConfigClass
from azure_batch_methods import (generate_sas_for_container, upload_file_to_container, create_pool, create_job, \
                                 add_tasks, wait_for_tasks_to_complete, print_task_output, print_batch_exception,
                                 delete_blob_from_container, download_output_files)

if __name__ == '__main__':

    start_time = datetime.datetime.now().replace(microsecond=0)
    print(f'Sample start: {start_time}')
    print()

    appconfig = ConfigClass('appconfig.json')
    simconfig = ConfigClass('simconfig.json')

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.
    blob_service_client = BlobServiceClient(
        account_url=f"https://{appconfig.STORAGE_ACCOUNT_NAME}.{appconfig.STORAGE_ACCOUNT_DOMAIN}/",
        credential=appconfig.STORAGE_ACCOUNT_KEY
    )

    # Generate the date in yyyymmddhhmmss format
    CURRENT_DATE = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

    STORAGE_CONTAINER_NAME = f'{simconfig.SIM_ID}'

    # Generate sas token for the container
    container_sas_token = generate_sas_for_container(STORAGE_CONTAINER_NAME,
                                                     appconfig.STORAGE_ACCOUNT_NAME,
                                                     appconfig.STORAGE_ACCOUNT_KEY)
    container_url = f"https://{appconfig.STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{STORAGE_CONTAINER_NAME}?{container_sas_token}"

    # Upload files and generate ResourceFiles
    files_to_upload = ["download_files.py",
                       "auxiliar_methods.py",
                       "azure_batch_methods.py",
                       "appconfig.json",
                       "simconfig.json"]
    resource_files = []
    for file in files_to_upload:
        resource_file = upload_file_to_container(
            appconfig=appconfig,
            blob_storage_service_client=blob_service_client,
            container_name=STORAGE_CONTAINER_NAME,
            file_path=file
        )
        resource_files.append(resource_file)

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
        create_pool(appconfig=appconfig,
                    batch_service_client=batch_client,
                    pool_id=POOL_ID_REDUCER,
                    node_count=1,
                    vm_size=simconfig.POOL_VM_SIZE,
                    docker_image=appconfig.REDUCER_DOCKER_IMAGE)

        # Create the job that will run the tasks.
        create_job(batch_service_client=batch_client,
                   job_id=JOB_ID_REDUCER,
                   pool_id=POOL_ID_REDUCER)

        # Add the tasks to the job.
        git_clone_command = f'git clone https://{appconfig.GIT_TOKEN}@github.com/{appconfig.GIT_USER}/{appconfig.GIT_REPO}.git'
        COMMAND_TEMPLATE = (
            "/bin/bash -c \"current_dir=$(pwd) && "
            "python download_files.py || (echo 'Failed to downloading files' && exit 1) &&"
            "{git_command} && "
            "ls -la && cd ./{git_repo} && "
            "$current_dir/{git_repo}/{run_script} $current_dir/nodes_output && cd ..\"")
        command = COMMAND_TEMPLATE.format(
            git_command=git_clone_command,
            git_repo=appconfig.GIT_REPO,
            run_script=simconfig.REDUCER_SCRIPT)

        add_tasks(batch_service_client=batch_client,
                  job_id=JOB_ID_REDUCER,
                  total_nodes=1,
                  resource_files=resource_files,
                  container_url=container_url,
                  docker_image=appconfig.REDUCER_DOCKER_IMAGE,
                  command=command,
                  file_patterns=[simconfig.OUTPUT_FILE_PATTERNS])

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client, JOB_ID_REDUCER, datetime.timedelta(minutes=30))

        print("  Success! All tasks reached the 'Completed' state within the "
              "specified timeout period.")

        # Print the stdout.txt and stderr.txt files for each task to the console
        print_task_output(batch_client, JOB_ID_REDUCER, "stdout.txt")

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
        for file in files_to_upload:
            delete_blob_from_container(
                blob_storage_service_client=blob_service_client,
                container_name=STORAGE_CONTAINER_NAME,
                blob_name=file
            )
        if query_yes_no('Download simulation results:') == 'yes':
            download_output_files(
                appconfig=appconfig,
                container_name=STORAGE_CONTAINER_NAME,
                local_dir=f'{simconfig.LOCAL_SIM_PATH}'
            )
        if query_yes_no('Delete container?') == 'yes':
            print(f'Deleting reducer container [{STORAGE_CONTAINER_NAME}]...')
            blob_service_client.delete_container(STORAGE_CONTAINER_NAME)

        # Clean up Batch resources (if the user so chooses).
        if query_yes_no('Delete reducer job?') == 'yes':
            batch_client.job.delete(JOB_ID_REDUCER)

        if query_yes_no('Delete reducer pool?') == 'yes':
            batch_client.pool.delete(POOL_ID_REDUCER)
