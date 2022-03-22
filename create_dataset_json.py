import json

outputjsonfilename='StMtest'
deployments=pd.read_csv("testdeployments.csv", sep="\t")
deployment=deployments.iloc[0]

# Create the json file
# csv file-specific + general EMSO json
#with open("NorEMSO_metadata.json", "r") as template, open("StMtest.json", "w") as stm:
#    stm.write(template.read())
#template.close()
#stm.close()


### Create dataset json metadata file from template
with open("NorEMSO_metadata.json", "r") as template:
    metadata=json.load(template)
template.close()



for vn in cvn.values():
    if vn not in records and vn!='LATITUDE' and vn!='LONGITUDE':
        print('no need for '+ vn+' attributes')
        if vn+"_varatt" in metadata:
            print('something to delete! '+ vn)
            del metadata[vn+"_varatt"]
            del metadata[vn+"_QC_varatt"]
    else:
        # keep the variable attributes + customize +
        # keep QC variable attributes
        if vn+"_sensor_L22" in deployment:
            metadata[vn+"_varatt"][1]["sensor_SeaVox_L22_code"]=deployment[vn+"_sensor_SeaVox_L22_code"].item()
        # QC variables
        metadata[vn + "_QC_varatt"] = metadata["QC_varatt"]
        metadata[vn + "_QC_varatt"][1]["long_name"] =metadata[vn + "_varatt"][1]["long_name"]+" quality flag"

    if vn+"_UNCALIBRATED" in records:
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


print(metadata)
# Global attributes
metadata['globalatt'][0]['site_code'] = "StationM"

metadata['globalatt'][0]['principal_investigator'] = deployment["PI"].item()

metadata['globalatt'][0]["time_coverage_start"]=dtobj.iloc[0].strftime("%Y-%m-%dT%H:%M:%SZ")
metadata['globalatt'][0]["time_coverage_end"]=dtobj.iloc[-1].strftime("%Y-%m-%dT%H:%M:%SZ")
metadata['globalatt'][0]["time_coverage_resolution"]=isodate.duration_isoformat(dtobj.diff().shift(-1).mode().item()) # pick the mode


clean_empty_entries(metadata)

with open(outputjsonfilename+".json", 'w') as fp:
    json.dump(metadata, fp, indent=4)
fp.close()


def clean_empty_entries(d):
    if isinstance(d, dict):
        return {
            k: v
            for k, v in ((k, clean_empty(v)) for k, v in d.items())
            if v
        }
    if isinstance(d, list):
        return [v for v in map(clean_empty, d) if v]
    return d