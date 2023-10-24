# -*- coding: utf-8 -*-
"""
Created on Mon Aug  7 15:52:22 2023

@author: a40753
"""
import datetime
import json
import copy
import os
import common_info as ci
import re


def clean_empty_entries(d):
    if isinstance(d, dict):
        return {
            k: v
            for k, v in ((k, clean_empty_entries(v)) for k, v in d.items())
            if v
        }
    if isinstance(d, list):
        return [v for v in map(clean_empty_entries, d) if v]
    return d


def create_metadata_json(current_file_fullpath, one_sensor_dataframe):

    json_filename = current_file_fullpath.split(os.sep)[-2] + "_" +\
        current_file_fullpath.split(os.sep)[-1] + ".json"
    json_fullpath = os.path.join(ci.temp_files_path, json_filename)
    if os.path.exists(json_fullpath):
        os.remove(json_fullpath)

    # Create dataset json metadata file from template
    with open("NorEMSO_new_metadata.json", "r") as template:
        metadata = json.load(template)
    template.close()

    # Fill global and variable metadata json.
    # Dimension attributes remain as they were in the template
    metadata = fill_global_attributes(
        current_file_fullpath, one_sensor_dataframe, metadata)
    metadata = create_variable_attributes(
        current_file_fullpath, one_sensor_dataframe, metadata)

    # Change from nan to empty strings (eventually, if needed)
    # Remove empty entries (?) (buggy; something about any)
    # metadata=clean_empty_entries(metadata)

    with open(json_fullpath, 'w') as fp:
        json.dump(metadata, fp, indent=4)
    fp.close()

    print(json_filename)

    return metadata


def fill_global_attributes(current_file_fullpath, one_sensor_dataframe, metadata):
    NorEMSO_DB = ci.NorEMSO_DB

    current_deployment_code = NorEMSO_DB['input_files'].loc[
        (NorEMSO_DB['input_files']['input_files'] == current_file_fullpath.split(os.sep)[-1]) &
        (NorEMSO_DB['input_files']['input_files_path']
         == current_file_fullpath.split(os.sep)[-2]),
        'deployment_code'].item()
    current_platform_code = NorEMSO_DB['deployments'].loc[
        NorEMSO_DB['deployments']['deployment_code'] == current_deployment_code,
        'platform_code'].item()
    current_location_code = NorEMSO_DB['platforms'].loc[
        NorEMSO_DB['platforms']['platform_code'] == current_platform_code,
        'location_code'].item()
    current_site_code = NorEMSO_DB['locations'].loc[
        NorEMSO_DB['locations']['location_code'] == current_location_code,
        'site_code'].item()
    current_sensor_ID = NorEMSO_DB['input_files'].loc[
        (NorEMSO_DB['input_files']['input_files'] == current_file_fullpath.split(os.sep)[-1]) &
        (NorEMSO_DB['input_files']['input_files_path']
         == current_file_fullpath.split(os.sep)[-2]),
        'sensor_ID'].item()
    keywords = NorEMSO_DB['sensors'].loc[
        NorEMSO_DB['sensors']['sensor_ID'] == current_sensor_ID,
        'sensor_variables'].item()

    # no OceanSites code for any of the Nordic Seas platforms
    metadata['globalatt'][0]["platform_code"] = ""
    metadata['globalatt'][0]["summary"] = ""
    # leave empty for now, only gliders have WMO
    metadata['globalatt'][0]["wmo_platform_code"] = ""
    metadata['globalatt'][0]["source"] = NorEMSO_DB['platforms'].loc[
        NorEMSO_DB['platforms']['platform_code'] == current_platform_code,
        'platform_type_L06'].item()

    # PI and institution
    metadata['globalatt'][0]["principal_investigator"] = NorEMSO_DB['sites'].loc[
        NorEMSO_DB['sites']['site_code'] == current_site_code,
        'principal_investigator'].item()
    metadata['globalatt'][0]["principal_investigator_email"] = NorEMSO_DB['sites'].loc[
        NorEMSO_DB['sites']['site_code'] == current_site_code,
        'principal_investigator_email'].item()
    metadata['globalatt'][0]["principal_investigator_id"] = NorEMSO_DB['sites'].loc[
        NorEMSO_DB['sites']['site_code'] == current_site_code,
        'principal_investigator_orcid'].item()
    metadata['globalatt'][0]["institution"] = NorEMSO_DB['sites'].loc[
        NorEMSO_DB['sites']['site_code'] == current_site_code,
        'institution_acronym'].item()
    metadata['globalatt'][0]["institution_edmo_code"] = NorEMSO_DB['sites'].loc[
        NorEMSO_DB['sites']['site_code'] == current_site_code,
        'institution_edmo_code'].item()
    metadata['globalatt'][0]["institution_edmo_uri"] = NorEMSO_DB['sites'].loc[
        NorEMSO_DB['sites']['site_code'] == current_site_code,
        'institution_edmo_uri'].item()
    metadata['globalatt'][0]["institution_ror_uri"] = NorEMSO_DB['sites'].loc[
        NorEMSO_DB['sites']['site_code'] == current_site_code,
        'institution_ror_uri'].item()

    metadata['globalatt'][0]["array"] = ""  # later
    metadata['globalatt'][0]["keywords_vocabulary"] = "SeaDataNet parameter discovery vocabulary"
    metadata['globalatt'][0]["keywords"] = keywords
    metadata['globalatt'][0]["sea_area"] = NorEMSO_DB['locations'].loc[
        NorEMSO_DB['locations']['location_code'] == current_location_code,
        'sea_area_C19'].item()
    metadata['globalatt'][0]["time_coverage_resolution"] = ""

    metadata['globalatt'][0]["platform_deployment_date"] = NorEMSO_DB['deployments'].loc[
        NorEMSO_DB['deployments']['deployment_code'] == current_deployment_code,
        "deployment_date"].item()
    metadata['globalatt'][0]["platform_deployment_cruise_name"] = NorEMSO_DB['deployments'].loc[
        NorEMSO_DB['deployments']['deployment_code'] == current_deployment_code,
        "deployment_cruise_name"].item()
    metadata['globalatt'][0]["platform_deployment_cruise_expocode"] = NorEMSO_DB['cruises'].loc[
        NorEMSO_DB['cruises']['cruise_name'] == metadata['globalatt'][0]["platform_deployment_cruise_name"],
        'expocode'].item()
    metadata['globalatt'][0]["platform_deployment_ship_ICES_code"] = \
        metadata['globalatt'][0]["platform_deployment_cruise_expocode"][0:4]
    metadata['globalatt'][0]["platform_deployment_ship_name"] = \
        ci.NVS_ship_name_from_C17(
            metadata['globalatt'][0]["platform_deployment_ship_ICES_code"])

    # Don't always have recovery info
    try:
        metadata['globalatt'][0]["platform_recovery_date"] = NorEMSO_DB['deployments'].loc[
            NorEMSO_DB['deployments']['deployment_code'] == current_deployment_code,
            "recovery_date"].item()
        metadata['globalatt'][0]["platform_recovery_cruise_name"] = NorEMSO_DB['deployments'].loc[
            NorEMSO_DB['deployments']['deployment_code'] == current_deployment_code,
            "recovery_cruise_name"].item()
        metadata['globalatt'][0]["platform_recovery_cruise_expocode"] = NorEMSO_DB['cruises'].loc[
            NorEMSO_DB['cruises']['cruise_name'] == metadata['globalatt'][0]["platform_recovery_cruise_name"],
            'expocode'].item()
        metadata['globalatt'][0]["platform_recovery_ship_ICES_code"] = \
            metadata['globalatt'][0]["platform_recovery_cruise_expocode"][0:4]
        metadata['globalatt'][0]["platform_recovery_ship_name"] = \
            ci.NVS_ship_name_from_C17(
                metadata['globalatt'][0]["platform_recovery_ship_ICES_code"])

    except:
        metadata['globalatt'][0]["platform_recovery_date"] = ""
        metadata['globalatt'][0]["platform_recovery_cruise_name"] = ""
        metadata['globalatt'][0]["platform_recovery_cruise_expocode"] = ""
        metadata['globalatt'][0]["platform_recovery_ship_ICES_code"] = ""
        metadata['globalatt'][0]["platform_recovery_ship_name"] = ""

    metadata['globalatt'][0]["site_code"] = current_site_code
    metadata['globalatt'][0]["nominal_position"] = NorEMSO_DB['locations'].loc[
        NorEMSO_DB['locations']['location_code'] == current_location_code,
        'nominal_position_wkt'].item()
    metadata['globalatt'][0]["nominal_bottom_depth"] = NorEMSO_DB['locations'].loc[
        NorEMSO_DB['locations']['location_code'] == current_location_code,
        'nominal_bottom_depth'].item()
    metadata['globalatt'][0]["DOI"] = ""
    metadata['globalatt'][0]["in_situ_samples_deployment"] = ""
    metadata['globalatt'][0]["in_situ_samples_recovery"] = ""

    # Global attributes calculated from dataset values
    metadata['globalatt'][0]["geospatial_lat_min"] = \
        min(one_sensor_dataframe['LATITUDE'])
    metadata['globalatt'][0]["geospatial_lat_max"] = \
        max(one_sensor_dataframe['LATITUDE'])
    metadata['globalatt'][0]["geospatial_lon_min"] = \
        min(one_sensor_dataframe['LONGITUDE'])
    metadata['globalatt'][0]["geospatial_lon_max"] = \
        max(one_sensor_dataframe['LONGITUDE'])
    metadata['globalatt'][0]["geospatial_vertical_min"] = \
        min(one_sensor_dataframe['DEPTH'])
    metadata['globalatt'][0]["geospatial_vertical_max"] = \
        max(one_sensor_dataframe['DEPTH'])
    metadata['globalatt'][0]["time_coverage_start"] = \
        min(one_sensor_dataframe['TIME']).strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata['globalatt'][0]["time_coverage_end"] = \
        max(one_sensor_dataframe['TIME']).strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata['globalatt'][0]["time_coverage_resolution"] = ""
    # metadata['globalatt'][0]["time_coverage_resolution"] = \
    #     isodate.duration_isoformat(
    #         datetime.timedelta(
    #             one_sensor_dataframe['TIME'].diff().mode().item())) #pick the mode
    metadata['globalatt'][0]["date_created"] = \
        datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata['globalatt'][0]["history"] = \
        datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ") + ": Creation"

    # Create title (and summary?)
    # Type of data hydrography, currents, oxygen, carbon,
    datatype = []
    if metadata['globalatt'][0]['keywords'].__contains__('TEMP'):
        datatype.append('hydrography')
    if metadata['globalatt'][0]['keywords'].__contains__('CUR') or \
            metadata['globalatt'][0]['keywords'].__contains__('CDIR'):
        datatype.append('currents')
    if metadata['globalatt'][0]['keywords'].__contains__('COW') or \
            metadata['globalatt'][0]['keywords'].__contains__('CH4'):
        datatype.append('carbon')
    if metadata['globalatt'][0]['keywords'].__contains__('OXY'):
        datatype.append('oxygen')

    metadata['globalatt'][0]["title"] = metadata['globalatt'][0]["site_code"] + \
        " " + ", ".join(datatype) + " data from a single depth."

    nominal_depth = NorEMSO_DB['sensors_deployed'].loc[
        (NorEMSO_DB['sensors_deployed']['sensor_ID'] == current_sensor_ID) &
        (NorEMSO_DB['sensors_deployed']
         ['deployment_code'] == current_deployment_code),
        'sensor_depth']

    summary_text = "This file contains " + \
        ", ".join(datatype) + " data from a " + \
        ci.NVS_sensor_name_from_L22(current_sensor_ID.split("_")[0]) + \
        " sensor located at " + nominal_depth + "mbsl, " + \
        str(metadata['globalatt'][0]["geospatial_lat_min"]) + "N, " + \
        str(metadata['globalatt'][0]["geospatial_lat_min"]) + "E, starting on " +\
        metadata['globalatt'][0]["time_coverage_start"] + \
        ". It is part of the NorEMSO project and EMSO-ERIC."
    metadata['globalatt'][0]["summary"] = summary_text.item()

    return metadata


def create_variable_attributes(current_file_fullpath, one_sensor_dataframe, metadata):

    NorEMSO_DB = ci.NorEMSO_DB
    current_deployment_code = NorEMSO_DB['input_files'].loc[
        (NorEMSO_DB['input_files']['input_files'] == current_file_fullpath.split(os.sep)[-1]) &
        (NorEMSO_DB['input_files']['input_files_path']
         == current_file_fullpath.split(os.sep)[-2]),
        'deployment_code'].item()

    # list of variables without dimension variables
    dim_variables = ci.dim_variables
    all_potential_variables = NorEMSO_DB['variables_attributes']['variable_name']
    variables_list = one_sensor_dataframe.columns.drop(dim_variables)
    base_variables_list = list(
        set(all_potential_variables).intersection(variables_list))

    # Create variables' entries and fill attributes
    for vn in base_variables_list:

        # Create "base" variables entries
        metadata[vn + "_varatt"] = copy.deepcopy(metadata["VARS_varatt"])

        for varatt in NorEMSO_DB['variables_attributes'].columns.drop('variable_name'):
            metadata[vn + "_varatt"][1][varatt] = \
                NorEMSO_DB['variables_attributes'].loc[
                NorEMSO_DB['variables_attributes']['variable_name'] == vn,
                varatt].item()

        # Parameters and units URI
        metadata[vn + "_varatt"][1]["sdn_parameter_uri"] = \
                ci.NVS_uri_from_urn(metadata[vn + "_varatt"][1]["sdn_parameter_urn"])
        metadata[vn + "_varatt"][1]["sdn_uom_uri"] = \
                ci.NVS_uri_from_urn(metadata[vn + "_varatt"][1]["sdn_uom_urn"])

        # sensor_specific attributes
        current_sensor_ID = NorEMSO_DB['input_files'].loc[
            (NorEMSO_DB['input_files']['input_files'] == current_file_fullpath.split(os.sep)[-1]) &
            (NorEMSO_DB['input_files']['input_files_path']
             == current_file_fullpath.split(os.sep)[-2]),
            'sensor_ID'].item()

        metadata[vn + "_varatt"][1]["sensor_serial_number"] = re.findall(
            'TOOL.*_(.*)', current_sensor_ID)[0]

        metadata[vn + "_varatt"][1]["sensor_SeaVoX_L22_code"] = 'SDN:L22::' + re.findall(
            '(TOOL.*)_.*', current_sensor_ID)[0]
        metadata[vn + "_varatt"][1]["sensor_reference"] = \
                ci.NVS_uri_from_urn(metadata[vn + "_varatt"][1]["sensor_SeaVoX_L22_code"])


        # get model name via NVS API (2 calls; not super efficient, but here we go)
        metadata[vn + "_varatt"][1]["sensor_model"] =\
            ci.NVS_sensor_name_from_L22(
                metadata[vn + "_varatt"][1]["sensor_SeaVoX_L22_code"])

        # Not all instruments have recorded manufacturers:
        try:
           manufacturer, manufacturer_urn, manufacturer_uri = \
                   ci.NVS_sensor_manufacturer_from_L22(
                           metadata[vn + "_varatt"][1]["sensor_SeaVoX_L22_code"])

           metadata[vn + "_varatt"][1]["sensor_manufacturer"] = manufacturer
           metadata[vn + "_varatt"][1]["sensor_manufacturer_urn"] = manufacturer_urn
           metadata[vn + "_varatt"][1]["sensor_manufacturer_uri"] = manufacturer_uri
        except:
           metadata[vn + "_varatt"][1]["sensor_manufacturer"] = ""
           metadata[vn + "_varatt"][1]["sensor_manufacturer_urn"] = ""
           metadata[vn + "_varatt"][1]["sensor_manufacturer_uri"] = ""

        # Not all sensors have the corrections info
        try:
            all_sensor_corrections = NorEMSO_DB['sensors_deployed'].loc[
                (NorEMSO_DB['sensors_deployed']['sensor_ID'] == current_sensor_ID) &
                (NorEMSO_DB['sensors_deployed']
                 ['deployment_code'] == current_deployment_code),
                'sensor_correction']
            sensor_corrections = "Corrections "+all_sensor_corrections.str.extract(
                fr'.*({vn} (?:\+|-)\d*.\d*).*').squeeze()  # to use only the value, not the full dataframe
        except:
            sensor_corrections = ""
        metadata[vn + "_varatt"][1]["comment"] = sensor_corrections

        metadata[vn + "_varatt"][1]["ancillary_variables"] = vn + "_QC"


        # Create QC entries. Required regardless. Add POSITION and DEPTH
        #if vn+"_QC" in variables_list:
        base_variable_for_QC = vn[0:-1 - 2]
        metadata[vn + "_QC_varatt"] = copy.deepcopy(metadata["QC_varatt"])
        metadata[vn + "_QC_varatt"][1]["long_name"] = \
            metadata[vn + "_varatt"][1]["long_name"] + " quality flag"

        # Create UNCALIBRATED entries if they exist
        if vn+"_UNCALIBRATED" in variables_list:
            metadata[vn + "_UNCALIBRATED_varatt"] = copy.deepcopy(
                metadata[vn + "_varatt"])  # DEEPCOPY. Otherwise, it's a link more than a copy
            metadata[vn + "_UNCALIBRATED_varatt"][1]["standard_name"] = ""
            metadata[vn + "_UNCALIBRATED_varatt"][1]["long_name"] = \
                metadata[vn + "_UNCALIBRATED_varatt"][1]["long_name"] \
                + " uncalibrated, not quality-controlled."
            metadata[vn + "_UNCALIBRATED_varatt"][1]["QC_indicator"] = "No QC was performed"
            metadata[vn + "_UNCALIBRATED_varatt"][1]["processing_level"] = \
                "Instrument data that has been converted to geophysical values"
            # for carbon, factory calibration was performed
            if vn.__contains__('COW'):
                metadata[vn + "_UNCALIBRATED_varatt"][1]["processing_level"] = \
                    "Post-recovery calibrations have been applied"
                metadata[vn + "_UNCALIBRATED_varatt"][1]["comment"] = \
                    "Factory calibration and postprocessing was done, but not QC"
            metadata[vn + "_UNCALIBRATED_varatt"][1]["DM_indicator"] = "P"
            metadata[vn + "_UNCALIBRATED_varatt"][1]["comment"] = ""

        if vn+"_UNCALIBRATED_QC" in variables_list:
            base_variable_for_QC = vn[0:-1 - 2]
            metadata[vn +
                     "_UNCALIBRATED_QC_varatt"] = copy.deepcopy(metadata["QC_varatt"])
            metadata[vn + "_UNCALIBRATED_QC_varatt"][1]["long_name"] = \
                metadata[vn + "_UNCALIBRATED_varatt"][1]["long_name"] + \
                " quality flag"


    # POSITION_QC and DEPTH_QC are required by EMSO (and OceanSites)
    metadata["POSITION_QC_varatt"] = copy.deepcopy(metadata["QC_varatt"])
    metadata["POSITION_QC_varatt"][1]["long_name"] = "Position quality flag"
    metadata["DEPTH_QC_varatt"] = copy.deepcopy(metadata["QC_varatt"])
    metadata["DEPTH_QC_varatt"][1]["long_name"] = "Depth quality flag"

    # Remove generic _varatt entries
    metadata.pop("QC_varatt")
    metadata.pop("VARS_varatt")

    # # Remove empty attributes
    # metadata2 = dict()
    # for metakey in metadata.keys():
    #     if metakey == 'globalatt':
    #         metadata2['globalatt'][0] = {
    #             k: v for k, v in metadata['globalatt'][0].items() if v or v == 0}
    #     else:
    #         metadata2[metakey][0] = metadata[metakey][0]
    #         metadata2[metakey][1] = {
    #             k: v for k, v in metadata[metakey][1].items() if v or v == 0}

    return metadata
