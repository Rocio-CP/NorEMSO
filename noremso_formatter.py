import netCDF4
from netCDF4 import Dataset
import json
import re

exec(open("read_StM.py").read())
exec(open("create_dataset_json.py").read())

with open("StMtest.json") as json_file:
    attributes = json.load(json_file)

typesofatt=list(attributes.keys())

# Global attributes dictionary
globals=[g for g in typesofatt if "global" in g]
globattDict=attributes[globals[0]][0]
# List of dimensions
#dimensions = [dim for dim in typesofatt if "dim" in dim]
dimensions = ['LATITUDE_varatt', 'LONGITUDE_varatt','TIME_varatt','DEPTH_varatt']
# List of variables
variables = [var for var in typesofatt if "var" in var]

nc = Dataset("testStM.nc", format="NETCDF4_CLASSIC", mode="w")

# based on Maren's script
for d in dimensions:
#    dimname=re.split('dimatt',d)[0]
    dimname=re.split('_varatt',d)[0]


    # Create dimension
    dimnamesize = eval(dimname+"var")
    dim = nc.createDimension(dimname, len(dimnamesize))
    otheratts = attributes[d][0]

    # Create dimension variable
    dimvar = nc.createVariable(dimname, otheratts["datatype"],otheratts["dimensions"], fill_value=otheratts["_FillValue"])
    dimattributes=attributes[d][1]
    for key, value in dimattributes.items():
      dimvar.setncattr(key, value)

    dimvar[:]=dimnamesize

#for ind, v in enumerate(variables):
for ind, v in enumerate(fulldataframe.columns):

    #vname=re.split('varatt',v)[0]
    vname=v

    #otheratts=attributes[v][0]
    otheratts=attributes[v+"_varatt"][0]

    var = nc.createVariable(vname, otheratts["datatype"],otheratts["dimensions"], fill_value=otheratts["_FillValue"])

    #varattributes=attributes[v][1]
    varattributes=attributes[v+"_varatt"][1]
    for key, value in varattributes.items():
      var.setncattr(key, value)

    var[:] = allvars_fulldataframe[:,:,ind]

####

#####

# Set global attributes
nc.setncatts(globattDict)
print(nc)
nc.close()

