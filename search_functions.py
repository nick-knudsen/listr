import pandas as pd
import dask.dataframe as dd

search_results = unique_complete_hotspot_sightings_df[
    (unique_complete_hotspot_sightings_df['LOCALITY ID'] == 165354) & 
    (unique_complete_hotspot_sightings_df['OBSERVATION DATE'] == '2020-11-03')
    ].groupby('COMMON NAME')['SAMPLING EVENT IDENTIFIER'].nunique().compute().sort_values(ascending=False)


print(search_results)