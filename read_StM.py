import pandas as pd
import dateparser
import datetime
import re
import numpy as np

# Use P02 for names (recommended by OceanSites)
variables_dict = {'time': 'TIME', 'lat': 'LATITUDE', 'lon': 'LONGITUDE', 'depth': 'DEPTH',
                  'press': 'PRES', 'temp': 'TEMP', 'sal': 'PSAL', 'cond': 'CNDC',
                  'xco2': 'XCOW', 'pco2': 'PCOW', 'fco2': 'FCOW'}

def read_StM_files(deployment_info):
    # Read the file(s) and create the full_dataframe outside.
    if deployment_info["FILES"].__contains__(";"): # then there is more than one file
        data_files=deployment_info["FILES"].split(";")
    else:
        data_files = [deployment_info["FILES"]]

    full_dataframe = pd.DataFrame()
    for current_file in data_files:
        print(current_file)
        one_file_dataframe = pd.read_csv(current_file,
                                         sep="\t")  # DO NOT REINDEX; will use the index for creating the matrix
        full_dataframe = pd.concat([full_dataframe, one_file_dataframe])

    # Rename columns and parse the date column
    # Switch for physics vs carbon (different column headers)
    if deployment_info['FILES'].__contains__('CO2') or deployment_info['FILES'].__contains__('carbon'):
        # Rename columns
        full_dataframe.rename(columns={"p_in_mbar": "PREM",
                                       "p_dbar": variables_dict['press'], "p_qf": variables_dict['press'] + "_QC",
                                       "T_degC": variables_dict['temp'], "T_qf": variables_dict['temp'] + "_QC",
                                       "S": variables_dict['sal'], "S_qf": variables_dict['sal'] + "_QC",
                                       "xCO2_pp_raw_ppm": variables_dict['xco2'] + "_UNCALIBRATED",
                                       "xCO2_pp_raw_qf": variables_dict['xco2'] + "_UNCALIBRATED_QC",
                                       "pCO2_pp_raw_uatm": variables_dict['pco2'] + "_UNCALIBRATED",
                                       "pCO2_pp_raw_qf": variables_dict['pco2'] + "_UNCALIBRATED_QC",
                                       "fCO2_sensor_raw_uatm": variables_dict['fco2'] + "_UNCALIBRATED",
                                       "fCO2_sensor_raw_qf": variables_dict['fco2'] + "_UNCALIBRATED_QC",
                                       "fCO2_sensor_corr_uatm": variables_dict['fco2'],
                                       "fCO2_sensor_corr_qf": variables_dict['fco2'] + "_QC"
                                       }, inplace=True)

        # Create timestamp pandas series, timezone aware. pd.Timestamp guesses the format (and get's it wrong with month/day)
        datetime_obj = full_dataframe.apply(
            lambda x: datetime.datetime.strptime(x['Date_Time'], '%d.%m.%Y %H:%M'), axis=1)
        datetime_obj = datetime_obj.apply(
            lambda x: x.replace(tzinfo=datetime.timezone.utc))
        # Remove Date and Time from the full_dataframe
        full_dataframe.drop(['Date_Time'], inplace=True, axis=1)

    elif deployment_info['FILES'].__contains__('SBE'):
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
        # Remove Date and Time from the full_dataframe
        full_dataframe.drop(['Date', 'Time'], inplace=True, axis=1)

    # Check appropriateness of the timestamp series datetime_obj
    # if multiple files, check that the "co-located" timestamps  are identical (that's why the timestamps seconds were set to zero), in order to create the depth x time x variable array
    # "co-located" timestamps are identical. (that's why the timestamps seconds were set to zero)
    # the set(datetime_obj[i] will return a set of unique values to datetime_obj with index i (indices were kept, so if 5 files, the set may potentially have up to 5 values, if all the dates are different)
    if deployment_info["FILES"].__contains__(";"): # then there is more than one file
        if not all([(len(set(datetime_obj[i])) == 1) for i in datetime_obj.index]):
            raise Exception("datetimes are different across the data_files, sorry")

    time_variable_datetime_obj = datetime_obj[0:datetime_obj.idxmax() + 1]

    # Create days-from-1950 numeric date
    datetime_diff_1950 = time_variable_datetime_obj - pd.Timestamp('1950-01-01T00:00:00', tz='UTC')
    datetime_numeric_1950 = datetime_diff_1950.dt.total_seconds() / 86400
    time_variable = datetime_numeric_1950

    # check the time is monotonically increasing
    if not time_variable.is_monotonic_increasing:
        raise Exception("time is not monotonically increasing")

    # Dimensions values dictionary
    latitude_variable = [deployment_info["DEPLOY_LAT"]]
    longitude_variable = [deployment_info["DEPLOY_LON"]]
    # Depth information is in the data files names
    depth_variable = [float(re.findall("\d+m", current_file)[0][0:-1]) for current_file in data_files]
    dimensions_variables = {'latitude_variable': latitude_variable,
                            'longitude_variable': longitude_variable,
                            'depth_variable': depth_variable,
                            'time_variable': time_variable}

    # Create the depth x time x variables array
    variables_list = full_dataframe.columns
    data_array = np.empty([len(dimensions_variables['depth_variable']),
                           len(dimensions_variables['time_variable']),
                           len(full_dataframe.columns)])
    data_array_dict=dict()
    for c, v in enumerate(variables_list):
        for i in full_dataframe.index: # per time step
            data_array[:, i, c] = full_dataframe[v][i]
            #array_to_dict=full_dataframe[v][i]
        data_array_dict[v] = data_array[:,:,c]

    return(dimensions_variables, data_array_dict)