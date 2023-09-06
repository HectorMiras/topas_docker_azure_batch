# python quickstart client Code Sample
#
# Copyright (c) Microsoft Corporation
#
# All rights reserved.
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

"""
Create a pool of nodes to output text files from azure blob storage.
"""

import datetime
import io
import os
import sys
import time
import zipfile
import shutil

from azure.batch.models import OutputFile, OutputFileBlobContainerDestination, OutputFileUploadOptions, \
    OutputFileUploadCondition, CloudTask, TaskContainerSettings, OutputFileDestination, ResourceFile
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas,
    generate_container_sas,
    ContainerSasPermissions,
    AccountSasPermissions
)
from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels
from azure.core.exceptions import ResourceExistsError

import appconfig
import simconfig

DEFAULT_ENCODING = "utf-8"


# Update the Batch and Storage account credential strings in config.py with values
# unique to your accounts. These are used when constructing connection strings
# for the Batch and Storage client objects.

def query_yes_no(question: str, default: str = "yes") -> str:
    """
    Prompts the user for yes/no input, displaying the specified question text.

    :param str question: The text of the prompt for input.
    :param str default: The default if the user hits <ENTER>. Acceptable values
    are 'yes', 'no', and None.
    :return: 'yes' or 'no'
    """
    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError(f"Invalid default answer: '{default}'")

    choice = default

    while 1:
        user_input = input(question + prompt).lower()
        if not user_input:
            break
        try:
            choice = valid[user_input[0]]
            break
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")

    return choice


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


def upload_file_to_container(blob_storage_service_client: BlobServiceClient,
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


def create_pool(batch_service_client: BatchServiceClient = None, pool_id: str = None):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sku
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
        container_image_names=[appconfig.DOCKER_IMAGE],
        container_registries=[{"registry_server": "docker.io",
                               "user_name": f"{appconfig.DOCKER_USER}",
                               "password": f"{appconfig.DOCKER_PASS}"}])

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
        vm_size=simconfig.POOL_VM_SIZE,
        target_dedicated_nodes=simconfig.POOL_NODE_COUNT
    )
    batch_service_client.pool.add(new_pool)

    # Wait for all VMs in the pool to be allocated
    timeout = datetime.timedelta(minutes=30)  # For example, wait up to 30 minutes
    timeout_expiration = datetime.datetime.now() + timeout
    while datetime.datetime.now() < timeout_expiration:
        pool = batch_client.pool.get(pool_id)
        # Check if the number of dedicated nodesbatch_client.task.add(job_id=JOB_ID, task=cloud_task) in 'idle' state equals the total number of VMs
        if pool.current_dedicated_nodes == simconfig.POOL_NODE_COUNT and all(
                node.state == batchmodels.ComputeNodeState.idle for node in batch_client.compute_node.list(pool.id)):
            print(f"All {simconfig.POOL_NODE_COUNT} VMs in the pool are ready!")
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


def add_tasks_test(batch_service_client: BatchServiceClient, job_id: str, config_file: str, total_nodes: int):
    """
    Adds a task for each input file in the collection to the specified job.

    :param config_file:
    :param total_nodes:
    :param batch_service_client: A Batch service client.
    :param str job_id: The ID of the job to which to add the tasks.
     created for each input file.
    """

    for i in range(1, total_nodes + 1):
        # Construct the task command
        task_command = "/bin/bash -c 'pwd && cd /topas/mytopassimulations && pwd && ls -la'"

        # Define the task
        cloud_task = CloudTask(
            id=f"task-{i}",
            command_line=task_command,
            container_settings=TaskContainerSettings(image_name=appconfig.DOCKER_IMAGE),
        )
        batch_client.task.add(job_id=JOB_ID, task=cloud_task)


def add_tasks(batch_service_client: BatchServiceClient, job_id: str, total_nodes: int, resource_file: ResourceFile,
              container_token):
    """
    Adds a task for each input file in the collection to the specified job.

    :param container_token:
    :param resource_file:
    :param total_nodes:
    :param batch_service_client: A Batch service client.
    :param str job_id: The ID of the job to which to add the tasks.
     created for each input file.
    """

    tasks = []
    # docker_wkdir="/topas/mytopassimulations"

    #COMMAND_TEMPLATE = ("/bin/bash -c \"current_dir=$(pwd) && wget -O $current_dir/SIM_DIR.zip '{sas_url}' || (echo 'Failed to download zip' && exit 1) && ls -la && unzip SIM_DIR.zip || (echo 'Failed to unzip' && exit 1) && ls -la && $current_dir/{run_script}\"")

    COMMAND_TEMPLATE = (
        "/bin/bash -c \"current_dir=$(pwd) && "
        "wget -O $current_dir/SIM_DIR.zip '{sas_url}' || (echo 'Failed to download zip' && exit 1) && "
        "unzip SIM_DIR.zip || (echo 'Failed to unzip' && exit 1) && ls -la && "
        "$current_dir/{run_script}\"")

    container_name = job_id
    for i in range(1, total_nodes + 1):
        # Construct the task command
        task_command = COMMAND_TEMPLATE.format(
            # workingdir=docker_wkdir,
            sas_url=resource_file.http_url,
            run_script=simconfig.RUN_SCRIPT
        )

        print(f'task_command: {task_command}')
        print(" ")

        # Define output file destinations for this specific task
        output_file_destinations = [
            OutputFile(
                file_pattern=f"./{fname}",
                destination=OutputFileDestination(
                    container=OutputFileBlobContainerDestination(
                        container_url=f"https://{appconfig.STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{container_name}?{container_token}",
                        path=f"nodes_output/run{i}/{fname}"
                    )
                ),
                upload_options=OutputFileUploadOptions(upload_condition=OutputFileUploadCondition.task_success)
            ) for fname in simconfig.OUTPUT_FILE_NAMES
        ]

        # Define the task
        cloud_task = CloudTask(
            id=f"task-{i}",
            command_line=task_command,
            container_settings=TaskContainerSettings(image_name=appconfig.DOCKER_IMAGE),
            output_files=output_file_destinations
            #resource_files=[resource_file]

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
                      text_encoding: str = None):
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
            job_id, task.id, simconfig.STANDARD_OUT_FILE_NAME)

        file_text = _read_stream_as_string(
            stream,
            text_encoding)

        if text_encoding is None:
            text_encoding = DEFAULT_ENCODING

        sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding=text_encoding)
        sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding=text_encoding)

        print("Standard output:")
        print(file_text)


def _read_stream_as_string(stream, encoding) -> str:
    """
    Read stream as string

    :param stream: input stream generator
    :param str encoding: The encoding of the file. The default is utf-8.
    :return: The file content.
    """
    output = io.BytesIO()
    try:
        for data in stream:
            output.write(data)
        if encoding is None:
            encoding = DEFAULT_ENCODING
        return output.getvalue().decode(encoding)
    finally:
        output.close()


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
    JOB_ID = f"topas-job-{CURRENT_DATE}"
    # Use job name as the name for the output container
    STORAGE_CONTAINER_NAME = JOB_ID

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

    # The collection of data files that are to be processed by the tasks.
    # input_file_paths = [os.path.join(sys.path[0], config.SIM_CONFIG_FILE)]

    # Upload the data files.
    # input_files = [
    #    upload_file_to_container(blob_service_client, input_container_name, file_path)
    #    for file_path in input_file_paths]

    # Set the path to the directory you want to zip
    directory_to_zip = simconfig.LOCAL_SIM_PATH

    # Set the path for the output zip file (without extension)
    output_filename = os.path.join(sys.path[0], 'SIM_DIR')

    # Create the zip archive
    shutil.make_archive(output_filename, 'zip', directory_to_zip)

    # Upload the zipped file
    resource_zip_file = upload_file_to_container(blob_service_client, input_container_name, f"{output_filename}.zip")

    # Optionally, you can remove the zip file after uploading if you don't need it
    # os.remove(zip_filename)

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
        create_pool(batch_client, simconfig.POOL_ID)

        # Create the job that will run the tasks.
        create_job(batch_client, JOB_ID, simconfig.POOL_ID)

        # Add the tasks to the job.
        add_tasks(batch_client, JOB_ID, simconfig.POOL_NODE_COUNT, resource_zip_file, container_sas_token)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client, JOB_ID, datetime.timedelta(minutes=30))

        print("  Success! All tasks reached the 'Completed' state within the "
              "specified timeout period.")

        # Print the stdout.txt and stderr.txt files for each task to the console
        print_task_output(batch_client, JOB_ID)

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
            print(f'Deleting container [{input_container_name}]...')
            blob_service_client.delete_container(input_container_name)

        # Clean up Batch resources (if the user so chooses).
        if query_yes_no('Delete job?') == 'yes':
            batch_client.job.delete(JOB_ID)

        if query_yes_no('Delete pool?') == 'yes':
            batch_client.pool.delete(simconfig.POOL_ID)
