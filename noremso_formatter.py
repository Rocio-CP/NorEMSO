import netCDF4
from netCDF4 import Dataset
import json
import re

with open("StMtest.json") as json_file:
    attributes = json.load(json_file)

typesofatt=list(attributes.keys())

# Global attributes dictionary
globals=[g for g in typesofatt if "global" in g]
globattDict=attributes[globals[0]][0]
# List of dimensions
dimensions = [dim for dim in typesofatt if "dim" in dim]
# List of variables
variables = [var for var in typesofatt if "var" in var]

nc = Dataset("testStM.nc", format="NETCDF4_CLASSIC", mode="w")

# based on Maren's script
for d in dimensions:
    dimname=re.split('dimatt',d)[0]

    # Create dimension
    dim = nc.createDimension(dimname, 1)
    otheratts = attributes[v][0]

    # Create dimension variable
    dimvar = nc.createVariable(dimname, otheratts["datatype"],otheratts["dimensions"], fill_value=otheratts["_FillValue"])
    dimattributes=attributes[d][1]
    for key, value in dimattributes.items():
      dimvar.setncattr(key, value)

for ind, v in enumerate(variables):
    vname=re.split('varatt',v)[0]

    otheratts=attributes[v][0]
    var = nc.createVariable(vname, otheratts["datatype"],otheratts["dimensions"], fill_value=otheratts["_FillValue"])


    var[:,:] = allvars_fulldataframe[:,:,ind]

    varattributes=attributes[v][1]
    for key, value in varattributes.items():
      var.setncattr(key, value)
####

#####

# Set global attributes
nc.setncatts(globattDict)
print(nc)
nc.close()

