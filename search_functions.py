import pandas as pd
import dask.dataframe as dd

search_results = unique_complete_hotspot_sightings_df[
    (unique_complete_hotspot_sightings_df['LOCALITY ID'] == 165354) & 
    (unique_complete_hotspot_sightings_df['OBSERVATION DATE'] == '2020-11-03')
    ].groupby('COMMON NAME')['SAMPLING EVENT IDENTIFIER'].nunique().compute().sort_values(ascending=False)


print(search_results)


yellowthroat_df = sightings_df[sightings_df['COMMON NAME'] == 'Common Yellowthroat']
yellowthroat_df['DAY OF YEAR'] = yellowthroat_df['OBSERVATION DATE'].dt.dayofyear
late_yellowthroat_df = yellowthroat_df[yellowthroat_df['DAY OF YEAR'] >= pd.to_datetime('1970-11-01').day_of_year ].sort_values('DAY OF YEAR', ascending=False)
late_yellowthroat_df = pd.DataFrame(late_yellowthroat_df.compute().reset_index(drop=True))
print(late_yellowthroat_df)