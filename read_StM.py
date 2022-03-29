import pandas as pd
import dateparser
import datetime
import re
import numpy as np

# Use P02 for names (recommended by OceanSites)
variables_dict = {'time': 'TIME', 'lat': 'LATITUDE', 'lon': 'LONGITUDE', 'depth': 'DEPTH',
                  'press': 'PRES', 'temp': 'TEMP', 'sal': 'PSAL', 'cond': 'CNDC'}

def create_StM_data_3d_array(deployment_info):
    latitude_variable = [deployment_info["DEPLOY_LAT"]]
    longitude_variable = [deployment_info["DEPLOY_LON"]]
    data_files = deployment_info["FILES"].split(",")  # List of data_files
    # Depth information is in the data files names
    depth_variable = [float(re.findall("\d+m", current_file)[0][0:-1]) for current_file in data_files]

    # Put all data_files in the same pandas dataframe (time, depth, values), THEN rearrange in matrix.
    tic = datetime.datetime.now()

    full_dataframe = pd.DataFrame()
    for current_file in data_files:
        one_file_dataframe = pd.read_csv(current_file,
                                         sep="\t")  # do not reindex; will use the index for creating the matrix
        full_dataframe = pd.concat([full_dataframe, one_file_dataframe])

    toc = datetime.datetime.now() - tic
    print("Reading the files took " + str(toc))

    # Rename columns
    full_dataframe.rename(columns={"p_dbar": variables_dict['press'], "p_qf": variables_dict['press'] + "_QC",
                                   "p_raw_dbar": variables_dict['press'] + "_UNCALIBRATED",
                                   "p_raw_qf": variables_dict['press'] + "_UNCALIBRATED_QC",
                                   "T_degC": variables_dict['temp'], "T_qf": variables_dict['temp'] + "_QC",
                                   "T_raw_degC": variables_dict['temp'] + "_UNCALIBRATED",
                                   "T_raw_qf": variables_dict['temp'] + "_UNCALIBRATED_QC",
                                   "C_S/m": variables_dict['cond'], "C_qf": variables_dict['cond'] + "_QC",
                                   "C_raw_S/m": variables_dict['cond'] + "_UNCALIBRATED",
                                   "C_raw_qf": variables_dict['cond'] + "_UNCALIBRATED_QC",
                                   "S": variables_dict['sal'], "S_qf": variables_dict['sal'] + "_QC",
                                   "S_raw": variables_dict['sal'] + "_UNCALIBRATED",
                                   "S_raw_qf": variables_dict['sal'] + "_UNCALIBRATED_QC"
                                   }, inplace=True)


    # Create timestamp pandas series (as datetime64 object) not list! Need dateparser because Norwegian
    datetime_obj = full_dataframe.apply(
        lambda x: dateparser.parse(x['Date'] + x['Time'],
                                   settings={'TIMEZONE': 'UTC', 'RETURN_AS_TIMEZONE_AWARE': True},
                                   languages=['nb']), axis=1)
    # Timestamps have different SECONDS. Have all the seconds to zero
    datetime_obj = datetime_obj.apply(lambda x: x.replace(second=0))
    # Create days-from-1950 numeric date
    datetime_diff_1950 = datetime_obj - pd.Timestamp('1950-01-01T00:00:00', tz='UTC')
    datetime_numeric_1950 = datetime_diff_1950.dt.total_seconds() / 86400
    time_variable = datetime_numeric_1950[0:datetime_numeric_1950.idxmax() + 1]
    # Remove Date and Time from the full_dataframe
    full_dataframe.drop(['Date', 'Time'], inplace=True, axis=1)

    variables_list = full_dataframe.columns

    # Create variable DEPTH/TIME/variables 3-d numpy array
    # Here I assume all files and variables have values at the same timestamps
    # (slow, and means to iterate twice, but can't think of anything else)

    tic = datetime.datetime.now()

    if all([(len(set(datetime_obj[i])) == 1) for i in datetime_obj.index]):
        data_3d_matrix = np.empty([len(depth_variable), len(time_variable), len(variables_list)])
        for c, v in enumerate(variables_list):
            for i in datetime_numeric_1950.index:
                data_3d_matrix[:, i, c] = full_dataframe[v][i]
    else:
        print("datetimes are different across the data_files, sorry")

    toc = datetime.datetime.now() - tic
    print("Creating the 3d array took " + str(toc))

    return (latitude_variable, longitude_variable, time_variable, depth_variable,
           variables_list, data_3d_matrix)
