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
Simulation parameters
"""

POOL_ID = 'topas-pool2'  # Your Pool ID
POOL_NODE_COUNT = 2  # Pool node count
POOL_VM_SIZE = 'STANDARD_A1_v2'  # VM Type/Size
#JOB_ID = 'testjob'  # Job ID
STANDARD_OUT_FILE_NAME = 'stdout.txt'  # Standard Output file

SIM_CONFIG_FILE = "SimulationConfigFile.txt"
LOCAL_SIM_PATH = "/home/hector/mytopassimulations/test_sim_az_batch"
RUN_SCRIPT = "main_run_script.sh"
OUTPUT_DIR_PATH = "./"
OUTPUT_FILE_NAMES_EXTENSIONS = ["csv", "header", "phsp"]
OUTPUT_FILE_NAMES = ["DoseToNucleus_1p0mgml_1_AuNP15_electrons.csv",
                     "nucleus_PHSP_1p0mgml_1_AuNP15_electrons.header",
                     "nucleus_PHSP_1p0mgml_1_AuNP15_electrons.phsp"]
