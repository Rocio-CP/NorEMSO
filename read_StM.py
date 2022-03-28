import pandas as pd
import dateparser
import datetime
import re
import numpy as np
import isodate
import json

# Use P02 for names (recommended by OceanSites)
cvn= {'time': 'TIME', 'lat': 'LATITUDE', 'lon': 'LONGITUDE', 'depth': 'DEPTH',
                 'press': 'PRES', 'temp': 'TEMP','sal': 'PSAL', 'cond': 'CNDC'}

deployments=pd.read_csv("testdeployments.csv", sep="\t")
deployment=deployments.iloc[0]
files=deployment["FILES"].split(",") # List of files
depthvalues=[float(re.findall("\d+m",currentfile)[0][0:-1]) for currentfile in files]

# Put all files in the same dataframe (time, depth, values), THEN rearrange in matrix.
fulldataframe=pd.DataFrame()
for currentfile in files:
    records=pd.read_csv(currentfile, sep="\t")
    #records["DEPTH"]=float(re.findall("\d+m",currentfile)[0][0:-1])
    fulldataframe=pd.concat([fulldataframe,records])

# Rename columns
fulldataframe.rename(columns={"Depth_m":cvn['depth'],"Depth_qf":cvn['depth']+"_QC",
                        "p_dbar": cvn['press'], "p_qf": cvn['press'] + "_QC", "p_raw_dbar": cvn['press'] + "_UNCALIBRATED","p_raw_qf": cvn['press'] + "_UNCALIBRATED_QC",
                        "T_degC":cvn['temp'],"T_qf":cvn['temp']+"_QC","T_raw_degC":cvn['temp']+"_UNCALIBRATED","T_raw_qf":cvn['temp']+"_UNCALIBRATED_QC",
                        "C_S/m":cvn['cond'],"C_qf":cvn['cond']+"_QC","C_raw_S/m":cvn['cond']+"_UNCALIBRATED","C_raw_qf":cvn['cond']+"_UNCALIBRATED_QC",
                        "S": cvn['sal'], "S_qf": cvn['sal'] + "_QC", "S_raw": cvn['sal'] + "_UNCALIBRATED","S_raw_qf": cvn['sal'] + "_UNCALIBRATED_QC"
                        }, inplace=True)

# Create timestamp series (as datetime64 object). Need dateparser because Norwegian
dtobj = fulldataframe.apply(
    lambda x: dateparser.parse(x['Date'] + x['Time'], settings={'TIMEZONE': 'UTC', 'RETURN_AS_TIMEZONE_AWARE': True},
                               languages=['nb']), axis=1)
# Dates have different SECONDS. Have all the seconds to zero
dtobj=[x.replace(second=10) for x in dtobj] # this is something syntax
# Create days-from-1950 numeric date
diff = dtobj - datetime.datetime(1950, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
timenumeric = diff.dt.total_seconds() / 86400
# Remove Date and Time from the dataframe
fulldataframe.drop(['Date','Time'], inplace=True, axis=1)

# Create dimension variables: latitude/longitude values, depth and time arrays
LATITUDEvar=deployment["DEPLOY_LAT"]
LONGITUDEvar=deployment["DEPLOY_LON"]
DEPTHvar= depthvalues
TIMEvar=timenumeric

# Create variable DEPTH/TIME/variables 3-d matrix
# If all dates are equal across the files (slow, and means to iterate twice, but can't think of anything else)
# time it
tic=datetime.datetime.now()
if all([(len(set(dtobj[i]))==1) for i in dtobj.index]):
    allvars_fulldataframe=np.empty([len(files), timenumeric.idxmax() + 1,len(fulldataframe.columns)])
    for c, v in enumerate(fulldataframe.columns):
        for i in timenumeric.index:
            allvars_fulldataframe[:,i,c]=fulldataframe[v][i]
else:
    print("datetimes are different across the files, sorry")

toc=datetime.datetime.now()-tic
print(toc)







