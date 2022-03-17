import pandas as pd
import dateparser
import datetime
import isodate
import json

filename="StaM_SBE_20200825.txt"
records=pd.read_csv(filename, sep="\t")

variablenames=records.columns

# Date and time separated: create 1950-date
# Assume UTC
# Create the ISO timestring
# Python doesn't understand norwegian per se; dateparser does
#dateparser.parse(records.Date[1000]+records.Time[1000], settings={'TIMEZONE':'UTC'}, languages=['nb'])
dtobj=records.apply(lambda x: dateparser.parse(x['Date']+x['Time'],settings={'TIMEZONE': 'UTC','RETURN_AS_TIMEZONE_AWARE': True}, languages=['nb']), axis=1)
# Create function
diff = dtobj - datetime.datetime(1950, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
datetimevalues=diff.dt.days + diff.dt.seconds / 86400

# Use P02 for names (recommended by OceanSites)
cvn= {'time': 'TIME', 'lat': 'LATITUDE', 'lon': 'LONGITUDE', 'depth': 'DEPTH',
                 'press': 'PRES', 'temp': 'TEMP','sal': 'PSAL', 'cond': 'CNDC'}

records.rename(columns={"Depth_m":cvn['depth'],"Depth_qf":cvn['depth']+"_QC",
                        "p_dbar": cvn['press'], "p_qf": cvn['press'] + "_QC", "p_raw_dbar": cvn['press'] + "_UNCALIBRATED","p_raw_qf": cvn['press'] + "_UNCALIBRATED_QC",
                        "T_degC":cvn['temp'],"T_qf":cvn['temp']+"_QC","T_raw_degC":cvn['temp']+"_UNCALIBRATED","T_raw_qf":cvn['temp']+"_UNCALIBRATED_QC",
                        "C_S/m":cvn['cond'],"C_qf":cvn['cond']+"_QC","C_raw_S/m":cvn['cond']+"_UNCALIBRATED","C_raw_qf":cvn['cond']+"_UNCALIBRATED_QC",
                        "S": cvn['sal'], "S_qf": cvn['sal'] + "_QC", "S_raw": cvn['sal'] + "_UNCALIBRATED","S_raw_qf": cvn['sal'] + "_UNCALIBRATED_QC"
                        }, inplace=True)
# Create the json file
# csv file-specific + general EMSO json
#with open("NorEMSO_metadata.json", "r") as template, open("StMtest.json", "w") as stm:
#    stm.write(template.read())
#template.close()
#stm.close()


with open("NorEMSO_metadata.json", "r") as template:
    metadata=json.load(template)
template.close()

deployments=pd.read_csv("deployments.csv")
deployment=deployments[deployments['file']==filename]


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

with open('StMtest.json', 'w') as fp:
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