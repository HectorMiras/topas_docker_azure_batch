# Azure Batch Processing System

This system leverages Azure Batch for efficient distributed processing. It is designed to run simulation programs on worker VMs and subsequently aggregate or process results using a reducer VM. Both VM types utilize Docker containers configured with specific images from DockerHub. It is structured into two main components: a script to distribute tasks to multiple workers (`batch_simulation_runner.py`) and a reducer script to aggregate or process the results (`batch_data_reducer.py`).

## Prerequisites

- Python 3.x
- Azure Batch and Azure Storage accounts.
- Azure Python SDK

## Setting Up

1. Set up your Azure Batch and Azure Storage account. Make sure to note down your account names, account keys, and associated URLs.

2. Clone the repository:
    ```
    git clone https://github.com/HectorMiras/topas_docker_azure_batch.git
    cd topas_docker_azure_batch
    ```

3. Install the required Python packages:
    ```
    pip install azure-batch azure-storage-blob azure-core azure-commons
    ```

## Configuration

1. Update the `appconfig.json` with your Azure Batch, Azure Storage details, DockerHub repository details for the VM images, and your GitHub repository details for the reducer code.
2. Modify the `simconfig.json` for specific details of the task or simulation you're performing, such as simulation ID, path, and associated scripts.


This system is designed to execute batch processing tasks on Azure using worker VMs for simulation programs and a reducer VM for post-processing.

### `appconfig.json`

This file contains configuration details for the Azure Batch, Azure Storage, Docker images, and the GitHub repository for the reducer code.

- **STORAGE_ACCOUNT_NAME**: Your Azure Storage account name.
- **STORAGE_ACCOUNT_KEY**: Your Azure Storage account key.
- **STORAGE_ACCOUNT_DOMAIN**: Domain for your Azure Storage (typically `core.windows.net`).
- **BATCH_ACCOUNT_NAME**: Your Azure Batch account name.
- **BATCH_ACCOUNT_KEY**: Your Azure Batch account key.
- **BATCH_ACCOUNT_URL**: URL for your Azure Batch account.
- **WORKER_DOCKER_IMAGE**: DockerHub image for the worker VMs. This image should be set up to run simulation programs.
- **REDUCER_DOCKER_IMAGE**: DockerHub image for the reducer VM. This image should be configured to execute Python code.
- **GIT_USER**: GitHub username where the reducer code is hosted.
- **GIT_REPO**: GitHub repository name where the reducer code is located.
- **GIT_TOKEN**: Your GitHub token for authentication (Ensure to keep this confidential).

### `simconfig.json`

This file contains specific details for the simulation task you're performing.

- **SIM_ID**: A unique identifier for your simulation.
- **LOCAL_SIM_PATH**: Path to the simulation data or code on your local machine.
- **RUN_SCRIPT**: The script (or command) that should be executed on the worker VMs for the simulation.
- **REDUCER_SCRIPT**: The bash script that will invoke the reducer Python script(s) fetched from GitHub.
- **POOL_VM_SIZE**: VM size for both worker and reducer VMs.
- **POOL_NODE_COUNT**: Number of nodes (VMs) in the worker pool.
- **OUTPUT_FILE_PATTERNS**: Patterns to recognize output files, typically used in file searches or filters.



### Docker Images

- **Worker VMs**: These VMs utilize a Docker image designed to run the simulation programs. The image should be available on DockerHub and specified in the `appconfig.json`.
  
- **Reducer VM**: This VM uses a Docker container configured specifically to execute Python code. The image for this VM is also specified in the `appconfig.json`.

### Reducer Code

The reducer code, which should be a Python script or a set of Python scripts, is fetched from a GitHub repository specified in the `appconfig.json`. This code is executed via a bash script which also needs to be specified in the `appconfig.json`.

## Usage

### Distributing Tasks to Workers

Run 
```
python batch_simulation_runner.py /path_to_simconfig/simconfig.json
```
to distribute tasks among multiple workers.

This script does the following:

- Zips and uploads your specified simulation local directory to Azure Blob Storage.
- Creates a pool of VMs.
- Distributes tasks among the VMs in the pool.

### Reducing or Post-Processing the Results

After all tasks are completed, you can aggregate or post-process the results using the command 
```
python batch_data_reducer.py /path_to_simconfig/simconfig.json
```

This script:

- Creates a new pool with one reducer node using the docker image specified in the `appconfig.json` file.
- Downloads results from worker nodes to the reducer.
- Processes or aggregates the results using the specified reducer script fetched from GitHub.


## Cleanup and Post-Processing

### Workers

At the conclusion of the worker tasks, the script offers options to perform the following clean-up operations:

1. **Delete the Azure Storage Container**: After the tasks, you'll have the option to delete the Azure Storage container where simulation data and outputs were temporarily stored.

2. **Download Simulation Results**: You'll be prompted if you wish to download the output simulation files from the computing nodes to your local machine.

3. **Delete the Batch Job and Pool**: Once the tasks are completed, and you don't plan to submit more, you can delete the pool and the records of the tasks from Azure Batch service to stop incurring charges.


### Reducer

Once the reducer has completed its tasks, it performs the following operations:

1. **Delete Uploaded Files from Storage**: It automatically deletes the files it initially uploaded to Azure Storage.

2. **Download Simulation Results**: You'll be prompted if you wish to download the aggregated or reduced results from the simulation to your local machine.

3. **Delete the Azure Storage Container**: After downloading the results, or if you choose not to, you'll have the option to delete the Azure Storage container where the reducer outputs were stored.

4. **Delete the Batch Job and Pool**: As with the worker tasks, you can choose to delete the records of the reducer tasks and the VMs pool in the Azure Batch service to prevent further charges.


## Example Usage

The `example` directory contains the necessary files to test the application:

1. Dockerfiles for setting up the worker and reducer containers.

2. A `simulation` subdirectory with TOPAS parameter files to run a simple simulation.

3. A pre-configured `simconfig.json` file for running the simulation in the `example_simulation` directory.

To run the example, please follow these steps:

1. Build the Docker container images using the provided Dockerfiles and publish them to a container registry such as Docker Hub.

2. Update the `simconfig.json` file, replacing `LOCAL_SIM_PATH` with the absolute path to your `AzureBatch/example/example_simulation` directory.

3. Modify the `appconfig.json` file with your specific Azure Batch and Azure Storage details, Docker image references, Git repository information, etc.

4. Place the updated `appconfig.json` file in the directory that contains the `batch_simulation_runner.py` script.

5. Open a shell terminal, navigate to the directory containing the batch Python scripts, and execute the following command:
   ```
   python batch_simulation_runner.py ./example/simconfig.json
   ```

Respond 'yes' when prompted to download files. A new subdirectory will be created at `.../example/example_simulation/nodes_output` containing the output files from each compute node.

To test the reducer script, create Python scripts to process the node outputs and push them to a GitHub repository. Then, execute the following command to run the reducer:
```
python batch_data_reducer.py ./example/simconfig.json
```

When answering 'yes' to the download files prompt, a new directory `.../example/example_simulation/nodes_output/results` will appear with the consolidated result files.


## Contribution

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT](https://choosealicense.com/licenses/mit/)




