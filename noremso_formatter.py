from netCDF4 import Dataset
import json
import re
import pandas as pd

from importlib import reload  # Python 3.4+
from read_StM import create_StM_data_3d_array
from create_dataset_json import create_metadata_json

# read_StM = reload(read_StM)

# files names
deployments_file = "testdeployments.csv"

deployments = pd.read_csv(deployments_file, sep="\t", dtype='str',
                          converters={'DEPLOY_LAT': float, 'DEPLOY_LON': float})

deployment_info = deployments.iloc[0]  # substitute with for loop


# Read input data files and create 3d array
(dimensions_variables, variables_list, data_3d_array) \
    = create_StM_data_3d_array(deployment_info)
# Create the metadata json file
(json_filename) \
    = create_metadata_json(deployment_info, variables_list, dimensions_variables)

# Read metadata into dictionary
json_filename_full = json_filename + ".json"
with open(json_filename_full) as json_file:
    attributes = json.load(json_file)

# Split between global, dimension and variables' attributes
typesofatt = list(attributes.keys())

# Global attributes dictionary
globals = [g for g in typesofatt if "global" in g]
globattDict = attributes[globals[0]][0]

# List of dimensions
# dimensions = [dim for dim in typesofatt if "dim" in dim]
#dimensions = ['LATITUDE_varatt', 'LONGITUDE_varatt', 'TIME_varatt', 'DEPTH_varatt']


# List of variables
variables = [var for var in typesofatt if "var" in var]

# Create NetCDF file
nc_filename_full = json_filename + ".nc"
nc = Dataset(nc_filename_full, format="NETCDF4_CLASSIC", mode="w")

# Assign
# based on Maren's script
for d in dimensions_variables.keys():
#for d in dimensions:
    #    dimname=re.split('dimatt',d)[0]
    dimname = str.upper(re.split('_variable', d)[0])

    # Create dimension
    dimnamesize = len(dimensions_variables[d])
    dim = nc.createDimension(dimname, dimnamesize)
    otheratts = attributes[dimname+"_dimatt"][0]

    # Create dimension variable
    dimvar = nc.createVariable(dimname, otheratts["datatype"], otheratts["dimensions"],
                               fill_value=otheratts["_FillValue"])
    dimattributes = attributes[dimname+"_dimatt"][1]
    for key, value in dimattributes.items():
        dimvar.setncattr(key, value)

    dimvar[:] = dimensions_variables[d]

# for ind, v in enumerate(variables):
for ind, v in enumerate(variables_list):

    # vname=re.split('varatt',v)[0]
    vname = v

    # otheratts=attributes[v][0]
    otheratts = attributes[v + "_varatt"][0]

    var = nc.createVariable(vname, otheratts["datatype"], otheratts["dimensions"], fill_value=otheratts["_FillValue"])

    # varattributes=attributes[v][1]
    varattributes = attributes[v + "_varatt"][1]
    for key, value in varattributes.items():
        var.setncattr(key, value)

    var[:] = data_3d_array[:, :, ind]

# Set global attributes
nc.setncatts(globattDict)
print(nc)
nc.close()
