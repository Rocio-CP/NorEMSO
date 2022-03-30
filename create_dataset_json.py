import datetime
import isodate
import json
import numpy as np


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


def create_metadata_json(deployment_info, variables_list, dimensions_variables):
    json_filename = deployment_info["INTERNAL_ID"]
    deployment_info = deployment_info.replace(np.nan, '', regex=True)  # Replace nan with empty text

    ### Create dataset json metadata file from template
    with open("NorEMSO_metadata.json", "r") as template:
        metadata = json.load(template)
    template.close()

    # Loop through the variables. Separate loops for dimensions and variables
    # Dimensions

    # Variables' attributes
    for vn in variables_list:
        # Create UNCALIBRATED entries
        if "UNCALIBRATED" in vn and "QC" not in vn:
            fundamental_variable = vn[0:4]  # Assuming variable names will be 4-letters
            metadata[vn + "_varatt"] = metadata[fundamental_variable + "_varatt"]
            metadata[vn + "_varatt"][1]["standard_name"] = ""
            metadata[vn + "_varatt"][1]["long_name"] = \
                metadata[fundamental_variable + "_varatt"][1]["long_name"] \
                + " uncalibrated, not quality-controlled."
            metadata[vn + "_varatt"][1]["QC_indicator"] = "No QC was performed"
            metadata[vn + "_varatt"][1]["processing_level"] = \
                "Instrument data that has been converted to geophysical values"
            metadata[vn + "_varatt"][1]["DM_indicator"] = "P"

        # Create QC entries
        if "QC" in vn:
            fundamental_variable = vn[0:-1 - 2]
            metadata[vn + "_varatt"] = metadata["QC_varatt"]
            metadata[vn + "_varatt"][1]["long_name"] = \
                metadata[fundamental_variable + "_varatt"][1]["long_name"] \
                + " quality flag"

        # Add sensor attributes from deployment_info
        if "QC" not in vn:
            # Same sensor for conductivity and salinity
            if 'CNDC' in vn:
                fundamental_variable = 'PSAL'
            else:
                fundamental_variable = vn[0:4]  # Assuming variable names will be 4-letters

            sensor_info = deployment_info.loc[deployment_info.index.str.contains(fundamental_variable)]
            for s in sensor_info.index:
                print(s)
                metadata[vn + "_varatt"][1][s[5:]] = deployment_info[s]

        # remove non-used entries (e.g. PCO2 in physics file)

        '''
# below it was another way of doing it (based on variables_dict instead of variables_list
        
        if vn not in variables_list and vn!='LATITUDE' and vn!='LONGITUDE' and vn!='TIME' and vn!='DEPTH':
            print('no need for '+ vn+' attributes')
            if vn+"_varatt" in metadata:
                print('something to delete! '+ vn)
                del metadata[vn+"_varatt"]
                del metadata[vn+"_QC_varatt"]
        else:
            # keep the variable attributes + customize +
            # keep QC variable attributes
            metadata[vn + "_QC_varatt"] = metadata["QC_varatt"]
            metadata[vn + "_QC_varatt"][1]["long_name"] =metadata[vn + "_varatt"][1]["long_name"]+" quality flag"

        if vn+"_UNCALIBRATED" in variables_list:
            # copy the vn attributes and the QC, with modifications
            metadata[vn+"_UNCALIBRATED_varatt"]=metadata[vn+"_varatt"]
            metadata[vn + "_UNCALIBRATED_varatt"][1]["standard_name"]=""
            metadata[vn + "_UNCALIBRATED_varatt"][1]["long_name"]=\
                metadata[vn + "_varatt"][1]["long_name"] + " uncalibrated, not quality-controlled."
            metadata[vn + "_UNCALIBRATED_varatt"][1]["QC_indicator"]="No QC was performed"
            metadata[vn + "_UNCALIBRATED_varatt"][1]["processing_level"]="Instrument data that has been converted to geophysical values"
            metadata[vn + "_UNCALIBRATED_varatt"][1]["DM_indicator"]="P"

            metadata[vn+"_UNCALIBRATED_QC_varatt"]=metadata[vn+"_QC_varatt"]
            metadata[vn + "_UNCALIBRATED_QC_varatt"][1]["long_name"] = \
                metadata[vn + "_QC_varatt"][1]["long_name"]+ " uncalibrated"
                
        '''

        # Remove generic QC_varatt entry
    metadata.pop("QC_varatt")

    #print(metadata)
    # Global attributes
    # Set global attributes values in the deployments file
    globalatt_fromfile = deployment_info.index[deployment_info.index.str.islower()]
    for gf in globalatt_fromfile:
        metadata['globalatt'][0][gf] = deployment_info[gf]


    # Global attributes calculated from dataset values
    base_date=datetime.datetime(1950,1,1,0,0,0)
    first_datedelta=datetime.timedelta(min(dimensions_variables['time_variable']))
    last_datedelta=datetime.timedelta(max(dimensions_variables['time_variable']))

    metadata['globalatt'][0]["geospatial_lat_min"] = \
        min(dimensions_variables['latitude_variable'])
    metadata['globalatt'][0]["geospatial_lat_max"]= \
        max(dimensions_variables['latitude_variable'])
    metadata['globalatt'][0]["geospatial_lon_min"]= \
        min(dimensions_variables['longitude_variable'])
    metadata['globalatt'][0]["geospatial_lon_max"]= \
        max(dimensions_variables['longitude_variable'])
    metadata['globalatt'][0]["geospatial_vertical_min"]= \
        min(dimensions_variables['depth_variable'])
    metadata['globalatt'][0]["geospatial_vertical_max"]= \
        max(dimensions_variables['depth_variable'])

    metadata['globalatt'][0]["time_coverage_start"] = \
        (first_datedelta + base_date).strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata['globalatt'][0]["time_coverage_end"] = \
        (last_datedelta + base_date).strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata['globalatt'][0]["time_coverage_resolution"] = \
        isodate.duration_isoformat(
            datetime.timedelta(
                dimensions_variables['time_variable'].diff().mode().item()))  # pick the mode


    # clean_empty_entries(metadata)

    with open(json_filename + ".json", 'w') as fp:
        json.dump(metadata, fp, indent=4)
    fp.close()

    return json_filename