from urllib import request
import json
import urllib
from urllib.parse import urlparse, urljoin

#parsed=urlparse("https://www.doi.org/10.21334/npolar.2021.c4d80b64")
#parsed2=urlparse("https://data.npolar.no/dataset/c4d80b64-25f6-4afd-b392-696430c3fd14")
# Find out how to get the UUID from the DOI page

urllib.request.urlretrieve("https://api.npolar.no/dataset/c4d80b64-25f6-4afd-b392-696430c3fd14", 'sample_NPI.json')

# Get the dataset through the API, save as json(?)
# Search in the json file
with open("sample_NPI.json", "r") as npi_dataset_json:
    dataset_api_info = json.load(npi_dataset_json)
npi_dataset_json.close()


# attachments have the name of the files and the link. Get where the SBE files are
# Produces a list of lists
check_filenames_SBE=[['SBE' in s for s in subList.values()] for subList in dataset_api_info['attachments']]
filename_list=list()
link_list=list()
# Better in loop form?
for attach in dataset_api_info['attachments']:
    if 'SBE' in attach['filename']:
        filename_list.append(attach['filename'])
        link_list.append(attach['href'])


# Read attributes from the file into a NorEMSO metadata json
from netCDF4 import Dataset
urllib.request.urlretrieve(link_list[1], filename_list[1])
with open("NorEMSO_metadata.json", "r") as template:
    metadata_NPI = json.load(template)
template.close()

nc = Dataset(filename_list[0], mode="r")
npi_global_atts=nc.__dict__






