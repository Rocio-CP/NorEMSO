# -*- coding: utf-8 -*-
"""
Created on Mon Aug  7 14:49:27 2023

@author: a40753
"""
import os
import datetime
import dateparser
import pandas as pd
import common_info as ci
import xarray as xr

NorEMSO_DB = ci.NorEMSO_DB
variables_dict = ci.variables_dict

def read_input(current_file_fullpath):
    if current_file_fullpath.__contains__('S1') or \
            current_file_fullpath.__contains__('SD') or \
            current_file_fullpath.__contains__('Svinoy'):
        output_df = read_Svinoy(
            current_file_fullpath, variables_dict)

    elif current_file_fullpath.__contains__('StaM'):
        output_df = read_StationM(current_file_fullpath, variables_dict)

    elif current_file_fullpath.__contains__('F10'):
        output_df = read_F10(current_file_fullpath, variables_dict)

    output_df = lat_lon_depth_instrument_series(output_df, current_file_fullpath)

    return output_df


def lat_lon_depth_instrument_series(output_df, current_file_fullpath):
    current_deployment_code = NorEMSO_DB['input_files'].loc[
        (NorEMSO_DB['input_files']['input_files'] == current_file_fullpath.split(os.sep)[-1]) &
        (NorEMSO_DB['input_files']['input_files_path'] == current_file_fullpath.split(os.sep)[-2]),
        'deployment_code'].item()
    deployment_position = NorEMSO_DB['deployments'].loc[
        NorEMSO_DB['deployments']['deployment_code'] == current_deployment_code,
        "deployment_position_wkt"]  # keep the DF object, since it has the str.extract method
    sensor_deployed_ID = NorEMSO_DB['input_files'].loc[
        (NorEMSO_DB['input_files']['input_files'] == current_file_fullpath.split(os.sep)[-1]) &
        (NorEMSO_DB['input_files']['input_files_path'] == current_file_fullpath.split(os.sep)[-2]),
        "sensor_ID"].item()

    output_df['LATITUDE'] = float(deployment_position.str.extract(
        r'POINT \(-?\d*\.\d* (-?\d*\.\d*)')[0].item())
    output_df['LONGITUDE'] = float(
        deployment_position.str.extract(r'POINT \((-?\d*\.\d*)')[0].item())
    output_df['DEPTH'] = float(NorEMSO_DB['sensors_deployed'].loc[
        (NorEMSO_DB['sensors_deployed']['sensor_ID'] == sensor_deployed_ID) &
        (NorEMSO_DB['sensors_deployed']
         ['deployment_code'] == current_deployment_code),
        "sensor_depth"].item())
    output_df['INSTRUMENT_ID'] = sensor_deployed_ID

    return output_df

def read_F10(current_file_fullpath, variables_dict):

        ds = xr.open_dataset(current_file_fullpath)

        # It's already a nc file per sensor, with the names equal to our standar variables
        # Remove some of the variables we don't need (e.g. timeseries or position, which is taken from the DB)
        remove_vars=ci.dim_variables + ['TIMESERIES', 'DEPTH_SEAFLOOR', 'PLATFORM', 'INSTRUMENT','CRS']
        remove_vars.remove('TIME')
        allncvars=[ds[v].name for v in ds.variables]
        keepncvars=list(set(allncvars) - set(remove_vars))

        df = ds.to_dataframe()
        df=df.reset_index()
        df2=df[keepncvars]

        # Round to the closest minute (some rounding discrepancies in the nanoseconds)
        df2.loc[:,'TIME']=df['TIME'].round(datetime.timedelta(minutes=1))

        # # Round to the closest hour/ 15 min / 30 min...
        # roundminutes=[datetime.timedelta(minutes=mins) for mins in [90, 60, 30, 15]]
        # timeinterval=df2['TIME'].diff().mean()
        # closestdiff=[roundminute for roundminute in roundminutes if abs(timeinterval - roundminute)<=datetime.timedelta(minutes=1) ]
        # df2.loc[:,'TIME']= df2['TIME'].round(closestdiff[0])
        datetime_obj=df2['TIME']
        datetime_obj = datetime_obj.apply(
            lambda x: x.replace(tzinfo=datetime.timezone.utc))
        df2.loc[:,'TIME']=datetime_obj

        output_df = df2

        return output_df


def read_Svinoy(current_file_fullpath, variables_dict):
    # For Svinoy!!
    # Separator is space, but current direction has spaces in its name.
    # Read column headers separate and sanitize before creating the dataframe

    with open(current_file_fullpath) as f:
        header_line = f.readline()
    f.close()

    column_headers = header_line.replace(
        'deg rel N', 'deg.rel.N').replace('\n', '').split(sep=' ')

    one_file_dataframe = pd.read_csv(current_file_fullpath, delim_whitespace=True,
                                     skiprows=1, header=0, names=column_headers,
                                     dtype=dict.fromkeys(['yyyy', 'mm', 'dd', 'hh'], 'str'))  # or sep="\s+")  # DO NOT REINDEX; will use the index for creating the matrix
    # full_dataframe = pd.concat([full_dataframe, one_file_dataframe])

    # Rename columns
    one_file_dataframe.rename(columns={"Pres(dbar)": variables_dict['press'],
                                       "Temp(oC)": variables_dict['temp'],
                                       "Salt": variables_dict['sal'],
                                       "speed/cm/s)": variables_dict['currvel'],
                                       "dir(deg.rel.N)": variables_dict['currdir'],
                                       }, inplace=True)

    # Current speeds are given as cm/s, change units to m/s (the common unit)
    one_file_dataframe[variables_dict['currvel']] = \
        one_file_dataframe[variables_dict['currvel']]*0.01

    # Create timestamp pandas series, timezone aware.
    # pd.Timestamp guesses the format (and gets it wrong with month/day)
    datetime_obj = one_file_dataframe.apply(
        lambda x: datetime.datetime.strptime(
            '-'.join([x['yyyy'], x['mm'], x['dd'], x['hh']]),
            '%Y-%m-%d-%H'), axis=1)
    datetime_obj = datetime_obj.apply(
        lambda x: x.replace(tzinfo=datetime.timezone.utc))
    # ANOTHER METHOD, using pandas:'
    datetime_obj = pd.to_datetime({'year': one_file_dataframe['yyyy'],
                                   'month': one_file_dataframe['mm'],
                                   'day': one_file_dataframe['dd'],
                                   'hour': one_file_dataframe['hh']}, utc=True)
    # Remove Date and Time from the full_dataframe
    one_file_dataframe.drop(
        ['yyyy', 'mm', 'dd', 'hh'], inplace=True, axis=1)
    one_file_dataframe['TIME'] = datetime_obj

    return one_file_dataframe


def read_StationM(current_file_fullpath, variables_dict):
    # Read the file(s) and create the full_dataframe outside.

    one_file_dataframe = pd.read_csv(current_file_fullpath, sep="\t")

    # Rename columns and parse the date column
    # Switch for physics vs carbon (different column headers)
    if current_file_fullpath.__contains__('CO2') or current_file_fullpath.__contains__('carbon'):
        # Rename columns
        one_file_dataframe.rename(
            columns={"p_in_mbar": "PREM",
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
        datetime_obj = one_file_dataframe.apply(
            lambda x: datetime.datetime.strptime(x['Date_Time'], '%d.%m.%Y %H:%M'), axis=1)
        datetime_obj = datetime_obj.apply(
            lambda x: x.replace(tzinfo=datetime.timezone.utc))
        # Remove Date and Time from the full_dataframe
        one_file_dataframe.drop(['Date_Time'], inplace=True, axis=1)

    elif current_file_fullpath.__contains__('SBE'):
        # Rename columns
        one_file_dataframe.rename(
            columns={"p_dbar": variables_dict['press'], "p_qf": variables_dict['press'] + "_QC",
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
        datetime_obj = one_file_dataframe.apply(
            lambda x: dateparser.parse(x['Date'] + x['Time'],
                                       settings={'TIMEZONE': 'UTC',
                                                 'RETURN_AS_TIMEZONE_AWARE': True},
                                       languages=['nb']), axis=1)
        # Timestamps have different SECONDS. Have all the seconds to zero
        datetime_obj = datetime_obj.apply(lambda x: x.replace(second=0))
        # Remove Date and Time from the full_dataframe
        one_file_dataframe.drop(['Date', 'Time'], inplace=True, axis=1)

    one_file_dataframe['TIME'] = datetime_obj

    return one_file_dataframe
