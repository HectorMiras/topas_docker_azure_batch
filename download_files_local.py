import os
import sys
from azure_batch_methods import download_output_files
from auxiliar_methods import ConfigClass

container_name = sys.argv[1]
local_dir = sys.argv[2]
local_dir = local_dir +'/'+ container_name
only_reduced=False
if sys.argv[3].startswith('Y'): only_reduced = True
appconfig = ConfigClass('appconfig.json')



#container_name = 'topas-cellsnp-nonps'
#local_dir = '/home/hector/mytopassimulations/MGHsimulations/TOPAS_CellsNPs/azure_batch_files/' + container_name
#only_reduced = False



print(f'Download container: {container_name}')
print(f'to local directory: {local_dir}')
print(f'only reduced: {only_reduced}')

download_output_files(appconfig=appconfig,container_name=container_name, local_dir=local_dir, only_reduced=only_reduced)