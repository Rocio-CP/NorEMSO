


import requests
import json
import pandas as pd

# All OceanGliders from Norway
response_API=requests.get("https://www.ocean-ops.org/api/1/data/platform"
                          +"?exp=[\"networkPtfs.network.nameShort='OceanGliders'"
                          +" and program.country.name='Norway'\"]"
                          +"&include=["
                          +"\"id\""
                          +", \"name\""
                          +", \"ref\""
                          +", {\"path\":\"ptfDepl\""
                          +"}"
                          +", {\"ptfDepl\":[\"ship.name\""
                          +", \"deplDate\""
                          +", \"lat\""
                          +", \"lon\""
                          +", \"cruiseName\"]}"
                          ", {\"path\":\"retrieval\""
                          +"}"
                          +", {\"retrieval\":[\"ship.name\""
                          +", \"endDate\""
                          +", \"cruiseName\"]}"
                          +", {\"path\":\"ptfModel\""
                          +"}"
                          +", {\"ptfModel\":[\"name\""
                          +"]}"
                          +", {\"path\":\"ptfVariables\"}"
                          +", {\"ptfVariables\":[\"serialNo\""
                          +", \"calibDate\""
                          +", \"variable.name\""
                          +", \"sensorModel.name\"]}"
                          +", {\"path\":\"wmos\"}"
                          +", {\"wmos\":[\"wmo\""
                          +"]}"
                          +", {\"path\":\"ptfIdentifiers\"}"
                          +", {\"ptfIdentifiers\":[\"internalRef\""
                          +"]}"
                          +"]"
                          ).text

parse_json=json.loads(response_API)
oceanops_Norway_gliders_df=pd.json_normalize(parse_json['data'])
print(oceanops_Norway_gliders_df.iloc[10,1])

oceanops_Norway_gliders_df['wmos.wmo']=oceanops_Norway_gliders_df.apply( lambda row: row['wmos'][0].get('wmo'), axis=1)

# further flatten the datafram
for ind in range(oceanops_Norway_gliders_df.shape[0]):
        row=oceanops_Norway_gliders_df.iloc[[ind],:]
        var_rows=pd.json_normalize(row['ptfVariables'].item()).reset_index(drop=True)
        var_rows=var_rows.add_prefix('ptfVariables.') # keep OceanOPS API column name structure
        for var in var_rows:
#                oceanops_Norway_gliders_df[var]=''
                oceanops_Norway_gliders_df.loc[ind,var]= \
                        var_rows[var].astype(str).mask(pd.isna(var_rows[var])).str.cat(sep=' ')

oceanops_Norway_gliders_df=oceanops_Norway_gliders_df.drop(['wmos','ptfVariables','retrieval.ship','ptfDepl.ship'], axis=1)

oceanops_Norway_gliders_df.to_csv("Norwegian_gliders_OceanOPS.tsv", sep="\t", index=False)
