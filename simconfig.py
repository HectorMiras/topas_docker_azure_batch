"""
Simulation parameters
"""

SIM_ID = 'topas-simulation'  # Your Pool ID
POOL_NODE_COUNT = 2  # Pool node count
POOL_VM_SIZE = 'STANDARD_A1_v2'  # VM Type/Size
#POOL_VM_SIZE = 'STANDARD_D1_v2'  # VM Type/Size
STANDARD_OUT_FILE_NAME = 'stdout.txt'  # Standard Output file

#SIM_CONFIG_FILE = "SimulationConfigFile.txt"
LOCAL_SIM_PATH = "/home/hector/mytopassimulations/test_sim_az_batch"
RUN_SCRIPT = "main_run_script.sh"
REDUCER_SCRIPT = "main_batch_reducer_script.sh"
OUTPUT_FILE_PATTERNS = ["*.csv",
                     "*.header",
                     "*.phsp"]
