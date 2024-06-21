import datetime
import io
import os
import sys
import time

import azure.batch.models as batchmodels
from azure.batch import BatchServiceClient
from azure.batch.models import OutputFile, OutputFileBlobContainerDestination, OutputFileUploadOptions, \
    OutputFileUploadCondition, CloudTask, TaskContainerSettings, OutputFileDestination, ResourceFile
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas,
    generate_container_sas,
    ContainerSasPermissions
)
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

from auxiliar_methods import _read_stream_as_string, DEFAULT_ENCODING, ConfigClass
#import logging
#logging.basicConfig(level=logging.DEBUG)


def print_batch_exception(batch_exception: batchmodels.BatchErrorException):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print(f'{mesg.key}:\t{mesg.value}')
    print('-------------------------------------------')


def upload_file_to_container(appconfig: ConfigClass, blob_storage_service_client: BlobServiceClient,
                             container_name: str, file_path: str) -> batchmodels.ResourceFile:
    """
    Uploads a local file to an Azure Blob storage container.

    :param blob_storage_service_client: A blob service client.
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """
    blob_name = os.path.basename(file_path)
    blob_client = blob_storage_service_client.get_blob_client(container_name, blob_name)

    print(f'Uploading file {file_path} to container [{container_name}]...')

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    sas_token = generate_blob_sas(
        appconfig.STORAGE_ACCOUNT_NAME,
        container_name,
        blob_name,
        account_key=appconfig.STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2)
    )

    sas_url = generate_sas_url(
        appconfig.STORAGE_ACCOUNT_NAME,
        appconfig.STORAGE_ACCOUNT_DOMAIN,
        container_name,
        blob_name,
        sas_token
    )

    return batchmodels.ResourceFile(
        http_url=sas_url,
        file_path=blob_name
    )


def generate_sas_url(
        account_name: str,
        account_domain: str,
        container_name: str,
        blob_name: str,
        sas_token: str
) -> str:
    """
    Generates and returns a sas url for accessing blob storage
    """
    return f"https://{account_name}.{account_domain}/{container_name}/{blob_name}?{sas_token}"


def generate_sas_for_container(container_name, account_name, account_key, expiry_time=24):
    """
    Generate a SAS token for a container in the storage account.
    :param container_name: Name of the container.
    :param account_name: Name of the storage account.
    :param account_key: Access key for the storage account.
    :param expiry_time: Number of hours the SAS token should remain valid.
    :return: SAS token as a string.
    """

    sas_token = generate_container_sas(
        account_name=account_name,
        container_name=container_name,
        account_key=account_key,
        permission=ContainerSasPermissions(read=True, write=True, list=True, delete=True),
        # Set permissions as required.
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=expiry_time)
    )

    return sas_token


def delete_blob_from_container(blob_storage_service_client: BlobServiceClient,
                               container_name: str, blob_name: str) -> None:
    """
    Deletes a blob from an Azure Blob storage container.

    :param blob_storage_service_client: A blob service client.
    :param str container_name: The name of the Azure Blob storage container.
    :param str blob_name: The name of the blob (file) to delete.
    """
    blob_client = blob_storage_service_client.get_blob_client(container_name, blob_name)

    try:

        blob_client.delete_blob()

        print(f'Blob {blob_name} deleted successfully from container [{container_name}].')
    except ResourceNotFoundError:
        print(f"Blob {blob_name} not found in container {container_name}.")
    except HttpResponseError as e:
        print(f"Failed to delete blob {blob_name} from container {container_name}. Error: {e.message}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def download_output_files(appconfig: ConfigClass, container_name: str, local_dir: str):
    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient(
        account_url=f"https://{appconfig.STORAGE_ACCOUNT_NAME}.{appconfig.STORAGE_ACCOUNT_DOMAIN}/",
        credential=appconfig.STORAGE_ACCOUNT_KEY
    )

    # Create the local directory if it does not exist
    os.makedirs(local_dir, exist_ok=True)

    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    # List all blobs in the directory and download them one by one
    blob_list = container_client.list_blobs(name_starts_with="nodes_output/")
    for blob in blob_list:
        blob_name = blob.name
        blob_dir, blob_file = os.path.split(blob_name)

        # Create local directories as needed
        os.makedirs(os.path.join(local_dir, blob_dir), exist_ok=True)

        # Define the local file path to download to
        download_file_path = os.path.join(local_dir, blob_name)

        # Download the blob to local storage
        with open(download_file_path, "wb") as my_blob:
            blob_data = container_client.get_blob_client(blob_name)
            data_stream = blob_data.download_blob()
            data_stream.readinto(my_blob)

    print("Download completed!")


def create_pool(appconfig: ConfigClass, batch_service_client: BatchServiceClient,
                pool_id: str, node_count: int, vm_size: str, docker_image: str):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param docker_image:
    :param vm_size:
    :param node_count:
    :param appconfig:
    :param batch_service_client: A Batch service client.
    :param str pool_id: An ID for the new pool.
    """
    print(f'Creating pool [{pool_id}]...')

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
    IMAGE_REFERENCE = batchmodels.ImageReference(
        publisher="microsoft-azure-batch",
        offer="ubuntu-server-container",
        sku="20-04-lts",
        version="latest"
    )
    container_config = batchmodels.ContainerConfiguration(
        type=batchmodels.ContainerType.docker_compatible,
        container_image_names=[docker_image],
        container_registries=[{"registry_server": "docker.io",
                               "user_name": f"{appconfig.DOCKER_USER}",
                               "password": f"{appconfig.DOCKER_TOKEN}"}])

    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=IMAGE_REFERENCE,
            # image_reference=batchmodels.ImageReference(
            #    publisher="canonical",
            #    offer="0001-com-ubuntu-server-focal",
            #    sku="20_04-lts",
            #    version="latest"
            # ),
            container_configuration=container_config,
            node_agent_sku_id="batch.node.ubuntu 20.04"),
        vm_size=vm_size,
        target_dedicated_nodes=node_count
    )
    batch_service_client.pool.add(new_pool)

    # Wait for all VMs in the pool to be allocated
    timeout = datetime.timedelta(minutes=30)  # For example, wait up to 30 minutes
    timeout_expiration = datetime.datetime.now() + timeout
    while datetime.datetime.now() < timeout_expiration:
        pool = batch_service_client.pool.get(pool_id)
        # Check if the number of dedicated nodesbatch_client.task.add(job_id=JOB_ID, task=cloud_task) in 'idle' state equals the total number of VMs
        if pool.current_dedicated_nodes == node_count and all(
                node.state == batchmodels.ComputeNodeState.idle for node in
                batch_service_client.compute_node.list(pool.id)):
            print(f"All {node_count} VMs in the pool are ready!")
            break
        print("Waiting for VMs to be ready...")
        time.sleep(30)  # Wait for 60 seconds before checking again
    else:
        print(f"Timeout reached after waiting {timeout} minutes. Not all VMs are ready.")


def create_job(batch_service_client: BatchServiceClient, job_id: str, pool_id: str):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print(f'Creating job [{job_id}]...')

    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)


def add_tasks(batch_service_client: BatchServiceClient, job_id: str, simconfig: ConfigClass,
              resource_files: list[ResourceFile], container_url: str, docker_image: str,
              command_template: str):
    """
    Adds a task for each input file in the collection to the specified job.

    :param command_template:
    :param resource_files:
    :param simconfig:
    :param container_url:
    :param docker_image:
    :param batch_service_client: A Batch service client.
    :param str job_id: The ID of the job to which to add the tasks.
     created for each input file.
    """
    total_nodes=simconfig.POOL_NODE_COUNT
    file_patterns = simconfig.OUTPUT_FILE_PATTERNS

    # Get the task type: workers or reducer
    task_type = ""
    if "workers" in job_id:
        task_type = "workers"

    if "reducer" in job_id:
        task_type = "reducer"
        total_nodes=1

    for i in range(1, total_nodes + 1):

        if task_type=="workers":
            # Add the node index to the task command as argument
            task_command = command_template.format(run_script=simconfig.RUN_SCRIPT, node_id=i)
        else:
            task_command = command_template

        print(f'task_command: {task_command}')
        print(" ")

        # Define output file destinations for this specific task
        output_file_destinations = [
            OutputFile(
                file_pattern=f"./{fpattern}" if (task_type == "workers") else f"./nodes_output/results/*",
                destination=OutputFileDestination(
                    container=OutputFileBlobContainerDestination(
                        container_url=container_url,
                        path=f"nodes_output/run{i}" if (task_type == "workers") else "nodes_output/results"
                    )
                ),
                upload_options=OutputFileUploadOptions(upload_condition=OutputFileUploadCondition.task_success)
            ) for fpattern in file_patterns
        ]

        # Define the task
        cloud_task = CloudTask(
            id=f"task-{i}",
            command_line=task_command,
            container_settings=TaskContainerSettings(image_name=docker_image),
            output_files=output_file_destinations,
            resource_files=resource_files

        )
        batch_service_client.task.add(job_id=job_id, task=cloud_task)


def wait_for_tasks_to_complete(batch_service_client: BatchServiceClient, job_id: str,
                               timeout: datetime.timedelta):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :param job_id: The id of the job whose tasks should be to monitored.
    :param timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    print(f"Monitoring all tasks for 'Completed' state, timeout in {timeout}...", end='')

    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True

        time.sleep(1)

    print()
    raise RuntimeError("ERROR: Tasks did not reach 'Completed' state within "
                       "timeout period of " + str(timeout))


def print_task_output(batch_service_client: BatchServiceClient, job_id: str,
                      output_filename: str, text_encoding: str = None):
    """
    Prints the stdout.txt file for each task in the job.

    :param batch_client: The batch client to use.
    :param str job_id: The id of the job with task output files to print.
    """

    print('Printing task output...')

    tasks = batch_service_client.task.list(job_id)

    for task in tasks:

        node_id = batch_service_client.task.get(
            job_id, task.id).node_info.node_id
        print(f"Task: {task.id}")
        print(f"Node: {node_id}")

        stream = batch_service_client.file.get_from_task(
            job_id, task.id, output_filename)

        file_text = _read_stream_as_string(
            stream,
            text_encoding)

        if text_encoding is None:
            text_encoding = DEFAULT_ENCODING

        sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding=text_encoding)
        sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding=text_encoding)

        print("Standard output:")
        print(file_text)
