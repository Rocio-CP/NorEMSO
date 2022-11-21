import datetime
import isodate
import json
import copy

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
    json_filename = deployment_info["INTERNAL_ID"]+".json"

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
            base_variable = vn[0:4]  # Assuming variable names will be 4-letters
            metadata[vn + "_varatt"] = copy.deepcopy(
                metadata[base_variable + "_varatt"])  # DEEPCOPY. Otherwise, it's a link more than a copy
            metadata[vn + "_varatt"][1]["standard_name"] = ""
            metadata[vn + "_varatt"][1]["long_name"] = \
                metadata[base_variable + "_varatt"][1]["long_name"] \
                + " uncalibrated, not quality-controlled."
            metadata[vn + "_varatt"][1]["QC_indicator"] = "No QC was performed"
            metadata[vn + "_varatt"][1]["processing_level"] = \
                "Instrument data that has been converted to geophysical values"
            # for carbon, factory calibration was performed
            if vn.__contains__('COW'):
                metadata[vn + "_varatt"][1]["processing_level"] = \
                    "Post-recovery calibrations have been applied"
                metadata[vn + "_varatt"][1]["comment"] = \
                    "Factory calibration and postprocessing was done, but not QC"

            metadata[vn + "_varatt"][1]["DM_indicator"] = "P"

        # Create QC entries
        if "QC" in vn:
            base_variable_for_QC = vn[0:-1 - 2]
            metadata[vn + "_varatt"] = copy.deepcopy(metadata["QC_varatt"])
            metadata[vn + "_varatt"][1]["long_name"] = \
                metadata[base_variable_for_QC + "_varatt"][1]["long_name"] + " quality flag"

        # Add sensor attributes from deployment_info
        if "QC" not in vn:
            # Same sensor for conductivity and salinity
            if 'CNDC' in vn:
                base_variable = 'PSAL'
            else:
                base_variable = vn[0:4]  # Assuming variable names will be 4-letters

            sensor_info = deployment_info.loc[deployment_info.index.str.contains(base_variable)]
            for s in sensor_info.index:
                metadata[vn + "_varatt"][1][s[5:]] = deployment_info[s]

        # Comments to salinity do not apply to conductivity
        metadata["CNDC_varatt"][1]["comment"] = ""

        # remove non-used entries (e.g. PCOW in physics file)

    # Remove generic QC_varatt entry
    metadata.pop("QC_varatt")

    # Global attributes
    # Set global attributes values from the deployments file
    globalatt_fromfile = deployment_info.index[deployment_info.index.str.contains("GLOBAL_")]
    globalatt = [x.strip('GLOBAL_') for x in globalatt_fromfile]
    for gf in globalatt:
        metadata['globalatt'][0][gf] = deployment_info["GLOBAL_"+gf] # Remove initial "GLOBAL_"]

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
    metadata['globalatt'][0]["date_created"] = \
        datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata['globalatt'][0]["history"] = \
        datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ") + ": Creation"

    # Remove empty attributes. It's buggy, will figure out later
    # clean_empty_entries(metadata)

    with open(json_filename, 'w') as fp:
        json.dump(metadata, fp, indent=4)
    fp.close()

    return json_filename