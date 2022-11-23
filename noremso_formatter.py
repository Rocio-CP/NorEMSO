from netCDF4 import Dataset
import json
import re
import pandas as pd
import numpy as np
# custom functions (instead of calling the scripts
from read_StM import read_StM_files
from create_dataset_json import create_metadata_json

# Pick whether it'll be for archiving (only Station M for now) or ERDDAP; "archive" vs "emso_erddap"
file_format="archive" #"archive" # or "emso_erddap"
variables_list_emso_erddap=['TEMP','TEMP_QC','PSAL','PSAL_QC','CNDC','CNDC_QC','PRES','PRES_QC','FCOW','FCOW_QC']

if file_format == 'archive':
    deployments_file = "deployments_info_archive.tsv"
elif file_format == 'emso_erddap':
    deployments_file = "deployments_info_emso_erddap.tsv"

# Read and loop through the deployments information file
deployments = pd.read_csv(deployments_file, sep="\t", dtype='str',
                          converters={'DEPLOY_LAT': float, 'DEPLOY_LON': float})

for ind, deployment_info in deployments.iterrows():
    if deployment_info['create_nc']=='Y':
        deployment_info = deployment_info.replace(np.nan, '', regex=True)  # Replace nan with empty text
        # Create the data array and dimension variables
        (dimensions_variables, data_array_dict) = read_StM_files(deployment_info)

        # Create the metadata json file
        variables_list=data_array_dict.keys()
        if file_format == 'emso_erddap':
            variables_list=list(set(variables_list)&set(variables_list_emso_erddap))
        (json_filename) \
            = create_metadata_json(deployment_info, data_array_dict, dimensions_variables)

        # Read metadata json back into a dictionary
        with open(json_filename) as json_file:
            attributes = json.load(json_file)

        # Create NetCDF file
        nc_filename = deployment_info["INTERNAL_ID"] + ".nc"
        nc = Dataset(nc_filename, format="NETCDF4_CLASSIC", mode="w")

        # Generate dimensions and their variables
        # dimensions_variables is a DICTIONARY (with names and values)
        for d in dimensions_variables.keys():
            dimension_name = str.upper(re.split('_variable', d)[0])

            # Create dimension
            dimension_length = len(dimensions_variables[d])
            dim = nc.createDimension(dimension_name, dimension_length)
            # Attributes needed to create the variable
            create_attributes = attributes[dimension_name + "_dimatt"][0]

            # Create dimension variable
            dimension_variable = nc.createVariable(dimension_name,
                                                   create_attributes["datatype"],
                                                   create_attributes["dimensions"],
                                                   fill_value=create_attributes["_FillValue"])
            # Dimension variable attributes
            dimension_variable_attributes = attributes[dimension_name + "_dimatt"][1]
            for key, value in dimension_variable_attributes.items():
                dimension_variable.setncattr(key, value)
            # Dimension variable values
            dimension_variable[:] = dimensions_variables[d]

        # Generate variables
        # variables_list is a list of variables' names (values are in data_array)
        for variable_ind, variable_name in enumerate(variables_list):
            # Attributes needed to create the variable
            create_attributes = attributes[variable_name + "_varatt"][0]

            # Create variable
            variable = nc.createVariable(variable_name,
                                         create_attributes["datatype"],
                                         create_attributes["dimensions"],
                                         fill_value=create_attributes["_FillValue"])
            # Variable attributes
            variable_attributes = attributes[variable_name + "_varatt"][1]
            for key, value in variable_attributes.items():
                variable.setncattr(key, value)
            # Variable values
            variable[:] = data_array_dict[variable_name]
            #variable[:] = data_array[:, :, variable_ind]

        # Set global attributes
        global_attributes = [g for g in attributes.keys() if "global" in g]
        global_attributes_dict = attributes[global_attributes[0]][0]
        nc.setncatts(global_attributes_dict)

        # Close NetCDF file
        # print(nc)
        nc.close()
