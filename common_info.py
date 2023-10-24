# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 09:59:59 2023

@author: a40753
"""

import os
import pandas as pd
import requests
import json
import re
import toml

# Import paths from toml file
with open('NorEMSO.toml') as f:
    config = toml.load(f)

root_path = os.path.join(config['root_path'])
info_path = os.path.join(config['info_path'])
input_files_path = os.path.join(config['input_files_path'])
temp_files_path = os.path.join(config['temp_files_path'])
erddap_files_path = os.path.join(config['erddap_files_path'])
archive_files_path = os.path.join(config['archive_files_path'])


variables_dict = {'time': 'TIME', 'lat': 'LATITUDE', 'lon': 'LONGITUDE', 'depth': 'DEPTH',
                  'press': 'PRES', 'temp': 'TEMP', 'sal': 'PSAL', 'cond': 'CNDC',
                  'currvel': 'CSPD', 'currdir': 'CDIR', 'curru': 'UCUR', 'currv': 'VCUR',
                  'o2vol': 'DOXY', 'o2mass': 'DOX2', 'chla': 'CPWC', 'turb': 'TURB',
                  'xco2': 'XCOW', 'pco2': 'PCOW', 'fco2': 'FCOW', 'pch4': 'PCH4', 'pressmemb': 'PREM'}
dim_variables = ['DEPTH', 'TIME', 'LATITUDE', 'LONGITUDE']


# Read all NorEMSO_DB tables as dictionary
NorEMSO_DB = {}
for file in os.listdir(info_path):
    filename, ext = os.path.splitext(file)
    # print(filename, ext)
    if ext != '.csv':
        continue
    NorEMSO_DB[filename] = pd.read_csv(
        os.path.join(info_path, file), dtype=str, keep_default_na=False)
NorEMSO_DB['variables_attributes']['valid_min'] = pd.to_numeric(
    NorEMSO_DB['variables_attributes']['valid_min'])
NorEMSO_DB['variables_attributes']['valid_max'] = pd.to_numeric(
    NorEMSO_DB['variables_attributes']['valid_max'])


def NVS_uri_from_urn(NVS_urn):
        NVS_uri = 'https://vocab.nerc.ac.uk/collection/' \
               + re.findall('SDN:(.{3})::.*',NVS_urn)[0] + '/current/' \
                       + re.findall('SDN:.{3}::(.*)',NVS_urn)[0] + '/'
        return(NVS_uri)

def NVS_ship_name_from_C17(c17code):
    response_API = requests.get("https://vocab.nerc.ac.uk/collection/C17/current/"
                                + c17code
                                + "/?_mediatype=application%2Fld%2Bjson&_profile=nvs").text
    parse_json = json.loads(response_API)
    ship_name = parse_json['prefLabel']['@value']

    return(ship_name)


def NVS_sensor_name_from_L22(L22code):
    if not L22code.__contains__('SDN:'): # if it's only the code, not the URN
            L22code = 'SDN:L22::' + L22code

    response_API = requests.get(NVS_uri_from_urn(L22code) \
                                + "/?_mediatype=application%2Fld%2Bjson&_profile=nvs").text
    parse_json = json.loads(response_API)
    sensor_name = parse_json['prefLabel']['@value']

    return(sensor_name)


def NVS_sensor_manufacturer_from_L22(L22code):
    if not L22code.__contains__('SDN:'): # if it's only the code, not the URN
            L22code = 'SDN:L22::' + L22code

    response_API_sensor = requests.get(NVS_uri_from_urn(L22code) \
                                       + "/?_mediatype=application%2Fld%2Bjson&_profile=nvs").text
    parse_json_sensor = json.loads(response_API_sensor)

    manufacturer_code = [
        man.split("/")[-2] for man in parse_json_sensor['related'] if 'L35' in man][0]
    manufacturer_urn= 'SDN:L35::' + manufacturer_code
    manufacturer_uri= NVS_uri_from_urn(manufacturer_urn)

    response_API_manufacturer = requests.get(manufacturer_uri \
                                             + "/?_mediatype=application%2Fld%2Bjson&_profile=nvs").text
    parse_json_manufacturer = json.loads(response_API_manufacturer)
    manufacturer = parse_json_manufacturer['prefLabel']['@value']

    return(manufacturer, manufacturer_urn, manufacturer_uri)
