from azure_batch_methods import download_output_files

from auxiliar_methods import ConfigClass

appconfig = ConfigClass('appconfig.json')
simconfig = ConfigClass('simconfig.json')


container_name = f"{simconfig.SIM_ID}"

# Specify the local directory to download files to
local_dir = "./"

download_output_files(appconfig=appconfig,container_name=container_name, local_dir=local_dir)