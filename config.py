# -------------------------------------------------------------------------
#
# THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
# EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
# ----------------------------------------------------------------------------------
# The example companies, organizations, products, domain names,
# e-mail addresses, logos, people, places, and events depicted
# herein are fictitious. No association with any real company,
# organization, product, domain name, email address, logo, person,
# places, or events is intended or should be inferred.
# --------------------------------------------------------------------------

# Global constant variables (Azure Storage account/Batch details)

# import "config.py" in "python_quickstart_client.py "
# Please note that storing the batch and storage account keys in Azure Key Vault
# is a better practice for Production usage.

"""
Configure Batch and Storage Account credentials
"""

BATCH_ACCOUNT_NAME = 'cuentaazurebatchhector'  # Your batch account name
BATCH_ACCOUNT_KEY = 'zZlg8xHGePKOmGUcpV/N2Pf/VofIlRQaSSglmAWjCXFsQ9g+zvuj5xjO8UeXQeicUOO//5EtSrAJ+ABa8Ylxsw=='  # Your batch account key
BATCH_ACCOUNT_URL = 'https://cuentaazurebatchhector.westeurope.batch.azure.com'  # Your batch account URL
STORAGE_ACCOUNT_NAME = 'hmstoragebatch'
STORAGE_ACCOUNT_KEY = '2w0dB19iO7mgHqy3AmUQUlYq+UiFjwAMr8Y0j1lO67ORFE2OfuDrewlfxtQRoA9skT/y7CbjhInx+AStnd4+Qg=='
STORAGE_ACCOUNT_DOMAIN = 'blob.core.windows.net' # Your storage account blob service domain

DOCKER_IMAGE = "hectormiras/topascellsnp:v2"

POOL_ID = 'topas-pool'  # Your Pool ID
POOL_NODE_COUNT = 2  # Pool node count
POOL_VM_SIZE = 'STANDARD_A1_v2'  # VM Type/Size
JOB_ID = 'testjob'  # Job ID
STANDARD_OUT_FILE_NAME = 'stdout.txt'  # Standard Output file

SIM_CONFIG_FILE = "SimulationConfigFile.txt"
OUTPUT_DIR_PATH = "./work/RunDir/run"
OUTPUT_FILE_NAMES_EXTENSIONS = ["csv", "header", "phsp"]
