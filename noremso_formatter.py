# -*- coding: utf-8 -*-
"""
Created on Mon Aug  7 13:18:00 2023

@author: a40753
"""

import os
# custom functions (instead of calling the scripts)
import read_input_files as rif
import create_dataset_json as cmj
import common_info as ci
import create_nc as cn

NorEMSO_DB = ci.NorEMSO_DB
variables_dict = ci.variables_dict
erddap_files_path = ci.erddap_files_path
archive_files_path = ci.archive_files_path

# Pick whether it'll be for archiving (one file per deployment) or ERDDAP (one file per sensor and deployment)
# "archive" vs "emso_erddap"
file_format = "emso_erddap"
#variables_list_emso_erddap = ['TEMP', 'TEMP_QC', 'PSAL', 'PSAL_QC',
#                              'CNDC', 'CNDC_QC', 'PRES', 'PRES_QC', 'FCOW', 'FCOW_QC']

deployments_list = [#'F10_2020',
                    #'StM_2020', 'StM_2021',
                    #'S1S_2020', 'S1N_2020',
                    'S1S_2021'#, 'S1N_2021',
                    #'S1S_2022', 'S1N_2022',
                    #'SD1_2021', 'SD2_2021', 'SD3_2021', 'SD4_2021'
                    ]


for depl in deployments_list:
    current_deployment_code = depl

    filt = NorEMSO_DB['input_files']['deployment_code'] == current_deployment_code
    current_deployment_files = NorEMSO_DB['input_files'][filt]

    print(depl)

    # Function that reads the file and creates a dataframe (one per sensor) with harmonized column names
    for ind, cfile in enumerate(current_deployment_files['input_files']):

        if cfile.__contains__('ADCP'): continue # ADCPs require different treatment; they're timeseriesprofiles

        current_file_fullpath = os.path.join(ci.input_files_path,
                                             current_deployment_files['input_files_path'].iloc[ind],
                                             cfile)

        print(cfile)

        one_sensor_dataframe = rif.read_input(current_file_fullpath)

        # Function to generate metadata json
        metadata = cmj.create_metadata_json(
            current_file_fullpath, one_sensor_dataframe)

        # Create NetCDF file name = Deployment + L22 + SN + nominal depth
        instrument_id = ci.NorEMSO_DB['input_files'].loc[(NorEMSO_DB['input_files']['input_files']==cfile) &
                                                         (NorEMSO_DB['input_files']['deployment_code']==depl), 'sensor_ID'].item()
        nomdepth= ci.NorEMSO_DB['sensors_deployed'].loc[(NorEMSO_DB['sensors_deployed']['sensor_ID']==instrument_id) &
                                                        (NorEMSO_DB['sensors_deployed']['deployment_code']==depl), 'sensor_depth'].item()
        nc_filename = depl + '_' + instrument_id + '_' + nomdepth + 'm.nc'

        # Function to create netcdf from dataframe + json
        nc_fullpath = os.path.join(erddap_files_path, nc_filename)
        cn.create_nc_erddap(metadata, one_sensor_dataframe, nc_fullpath)
