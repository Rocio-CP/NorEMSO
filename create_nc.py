# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 15:23:45 2023

@author: a40753
"""
import os
import pandas as pd
from netCDF4 import Dataset
import numpy as np

def create_nc_erddap(attributes, one_sensor_dataframe, nc_fullpath):
    if os.path.exists(nc_fullpath):
        os.remove(nc_fullpath)

    nc = Dataset(nc_fullpath, format="NETCDF4_CLASSIC", mode="w")

    # Create the days-from-1950 time variable
    one_sensor_dataframe['TIME_obj'] = one_sensor_dataframe['TIME']
    datetime_diff_1950 = one_sensor_dataframe['TIME_obj'] - \
        pd.Timestamp('1950-01-01T00:00:00', tz='UTC')
    datetime_numeric_1950 = datetime_diff_1950.dt.total_seconds() / 86400
    one_sensor_dataframe['TIME'] = datetime_numeric_1950

    # check the time is monotonically increasing
    if not one_sensor_dataframe['TIME'].is_monotonic_increasing:
        raise Exception("time is not monotonically increasing")

    # Generate dimensions and their variables
    dim_variables = [a.split("_")[0]
                     for a in attributes.keys() if "dimatt" in a]
    variables_list = [a.rsplit('_', 1)[0]
                      for a in attributes.keys() if "varatt" in a]

    for d in dim_variables:
        dimension_name = d

        # Create dimension
        dimension_length = len(one_sensor_dataframe[dimension_name].unique())
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
        dimension_variable.setncatts(dimension_variable_attributes)
        # Correct valid_min/max attributes to float32 instead of float64
        dimension_variable.setncattr('valid_min', np.array(dimension_variable.getncattr('valid_min'),'f4'))
        dimension_variable.setncattr('valid_max', np.array(dimension_variable.getncattr('valid_max'),'f4'))

        # Dimension variable values
        dimension_variable[:] = one_sensor_dataframe[dimension_name].unique()

    # Generate variables
    # variables_list is a list of variables' names (values are in data_array)

    for variable_ind, variable_name in enumerate(variables_list):
        # print(variable_name)
        # Attributes needed to create the variable
        create_attributes = attributes[variable_name + "_varatt"][0]

        # Create variable
        variable = nc.createVariable(variable_name,
                                     create_attributes["datatype"],
                                     create_attributes["dimensions"],
                                     fill_value=create_attributes["_FillValue"])
        # Variable attributes
        variable_attributes = attributes[variable_name + "_varatt"][1]
        variable.setncatts(variable_attributes)
        # Correct valid_min/max attributes to float32 instead of float64
        try: # QC variables don't have min/max
                variable.setncattr('valid_min', np.array(variable.getncattr('valid_min'),'f4'))
                variable.setncattr('valid_max', np.array(variable.getncattr('valid_max'),'f4'))
        except:
                pass

        # QC variable values not in the input files (but exist in the json)
        # create new series with same flag (1 good_data)
        if (variable_name not in one_sensor_dataframe) and (variable_name.__contains__('_QC')): # QC flags added for EMSO::
                one_sensor_dataframe[variable_name] = 1

        # Variable values
        variable[:] = one_sensor_dataframe[variable_name]
        #variable[:] = data_array[:, :, variable_ind]


    # Set global attributes
    global_attributes = [g for g in attributes.keys() if "global" in g]
    global_attributes_dict = attributes[global_attributes[0]][0]
    # For ERDDAP, the feature type is different! (one file per sensor)
    global_attributes_dict['featureType'] = 'TimeSeries'
    global_attributes_dict['cdm_data_type'] = 'timeSeries'
    global_attributes_dict['data_type'] = 'OceanSITES time-series data'
    global_attributes_dict['Metadata_Conventions'] = ''
    global_attributes_dict['standard_name_vocabulary'] = 'CF Standard Name Table v80'
    global_attributes_dict['creator_name'] = 'Havforskningsinstituttet'
    global_attributes_dict['creator_email'] = 'datahjelp@hi.no'
    global_attributes_dict['creator_url'] = 'https://www.hi.no'
    global_attributes_dict['cf_role'] = 'timeseries_id'

    nc.setncatts(global_attributes_dict)

    # Close NetCDF file
    nc.close()

    # Reopen the file and remove the empty attributes
    # nc = Dataset(nc_fullpath, format="NETCDF4_CLASSIC", mode="r+")

    # for gat in nc.ncattrs():
    #         if nc.getncattr(gat) =='': nc.delncattr(gat)

    # for var in nc.variables.keys():
    #     for at in nc[var].ncattrs():
    #         if nc[var].getncattr(at) == '': nc[var].delncattr(at)

    # nc.close()

    # Python can't store flag_values as bytes. Use ncatted to correct
    nc=Dataset(nc_fullpath, mode='r')
    qcvars=[v for v in nc.variables.keys() if v.__contains__('_QC')]
    nc.close()
    for qc_var in qcvars:
            os.system('ncatted -O -h -a flag_values,' + qc_var + ',m,b,"0, 1, 2, 3, 4, 5, 6, 7, 8, 9" ' + nc_fullpath)
