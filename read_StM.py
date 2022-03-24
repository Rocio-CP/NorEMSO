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
# deployment=deployments[deployments['file']==filename]

# Depth info is in filename
alldepths=[float(re.findall("\d+m",f)[0][0:-1]) for f in files]
# Create rank of alldepths; they may not be in order
temp = np.array(alldepths).argsort()
depthranks = np.empty_like(temp)
depthranks[temp] = np.arange(len(alldepths))

fulldataframe=pd.DataFrame()
# Put all files in the same array (time, depth, values), THEN rearrange in matrix.
for currentfile in files:
#    currentfile=files[0]
    records=pd.read_csv(currentfile, sep="\t")
    records["DEPTH"]=float(re.findall("\d+m",currentfile)[0][0:-1])
    fulldataframe=pd.concat([fulldataframe,records])
    #fulldataframe=fulldataframe.append(records)

dtobj = fulldataframe.apply(
    lambda x: dateparser.parse(x['Date'] + x['Time'], settings={'TIMEZONE': 'UTC', 'RETURN_AS_TIMEZONE_AWARE': True},
                               languages=['nb']), axis=1)
# Create function
diff = dtobj - datetime.datetime(1950, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
datetimevalues = diff.dt.days + diff.dt.seconds / 86400



# Date and time separated: create 1950-date
# Assume UTC
# Create the ISO timestring
# Python doesn't understand norwegian per se; dateparser does
#dateparser.parse(records.Date[1000]+records.Time[1000], settings={'TIMEZONE':'UTC'}, languages=['nb'])

# Check dates from different files??

# Create dimension variables: latitude/longitude values, depth and time arrays
LATITUDEvar=deployment["DEPLOY_LAT"]
LONGITUDEvar=deployment["DEPLOY_LON"]
DEPTHvar[0]= depthvalue# Grows per iteration
TIMEvar=datetimevalues
# Create variable DEPTH/TIME matrices

# Rename columns
records.rename(columns={"Depth_m":cvn['depth'],"Depth_qf":cvn['depth']+"_QC",
                        "p_dbar": cvn['press'], "p_qf": cvn['press'] + "_QC", "p_raw_dbar": cvn['press'] + "_UNCALIBRATED","p_raw_qf": cvn['press'] + "_UNCALIBRATED_QC",
                        "T_degC":cvn['temp'],"T_qf":cvn['temp']+"_QC","T_raw_degC":cvn['temp']+"_UNCALIBRATED","T_raw_qf":cvn['temp']+"_UNCALIBRATED_QC",
                        "C_S/m":cvn['cond'],"C_qf":cvn['cond']+"_QC","C_raw_S/m":cvn['cond']+"_UNCALIBRATED","C_raw_qf":cvn['cond']+"_UNCALIBRATED_QC",
                        "S": cvn['sal'], "S_qf": cvn['sal'] + "_QC", "S_raw": cvn['sal'] + "_UNCALIBRATED","S_raw_qf": cvn['sal'] + "_UNCALIBRATED_QC"
                        }, inplace=True)





