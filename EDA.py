import pandas as pd
from pandas.core.reshape.merge import merge

path = "/home/juan/Documents/Data_engineering/"

# pd.set_option('display.max_columns', 20)

print('\n\nPerforming EDA (exploratory data analysis): \n\n ')

df = pd.read_csv(path + 'scooter.csv')

print('\nColumns:\n')
print(df.columns)

print('\nTypes: \n')
print(df.dtypes)

print('\nFirst Lines\n')
print(df.head())

print('\nSingle column:\n')
print(df['DURATION'])

print('\nPull sample:\n')
print(df.sample(5))

print('\nSlice data, rows 5 to 9: \n')
print(df[5:10])

print('\nSlice with loc:\n')
print(df.loc[34221])

print('\nValue in specific column and line:\n')
print(df.at[2, 'DURATION'])

print('\nSlicing using condition:\n')
print(df.where(df['user_id'] == 8417864))

print('\nSlicing by combining dfs:\n')
one = df['user_id'] == 8417864

two = df['trip_ledger_id'] == 1488838
print(df.where(one & two))

print('\nAnalysing data:\n')
print(df['DURATION'].describe())

print('\nGet unique values:\n')
print(df['DURATION'].value_counts(normalize=True, dropna=False))

print('\nCount missing values:\n')
print(df.isnull().sum())

print('\nDroping columns: \n')
df.drop(columns=['region_id'], inplace=True)
df.drop(index=[34225], inplace=True)

print('\nDrop null values:\n')
df.dropna(subset=['start_location_name'], inplace=True)

print('\nFill null values:\n')
df_null_sart_location = df['start_location_name'].isnull()
df_null_end_location = df['end_location_name'].isnull()

df_null = df.where(df_null_end_location & df_null_sart_location)

print(df_null[['start_location_name', 'end_location_name']])

values_to_map = {
    'start_location_name': 'Start St.',
    'end_location_name': 'Stop St.'
}

df_null.fillna(value=values_to_map, inplace=True)

print('\nFill with map values:\n')
print(df_null[['start_location_name', 'end_location_name']])

print('\nDrop with advanced filter:\n')
df_may = df[(df['month'] == 'May')]
df.drop(index=df_may.index, inplace=True)

print('\nCreating and modifying columns:\n')
print('lower case columns:\n')

df.columns = [x.lower() for x in df.columns]

print(df.columns)

print('\nChanging columns name:\n')
df.rename(columns={'region_id': 'region'}, inplace=True)

print('\n Interact through dataframe:\n')

df.loc[df['trip_id'] == 1737416, 'new_column'] = '1737416'
df.loc[df['trip_id'] != 1737416, 'new_column'] = 'No'


print(df[['trip_id', 'new_column']].head())
print(df)

print('\nSpliting data:\n')

print(df['started_at'])

print('\nExpanded series:\n')
new_split_serie = df['started_at'].str.split(expand=True)
print(new_split_serie)

df['started_at_date'] = new_split_serie[0]
df['started_at_time'] = new_split_serie[1]

print(df)

print('\nModifying data types:\n')

print(df.dtypes)

df['started_at'] = pd.to_datetime(
        df['started_at'], 
        format='%m/%d/%Y %H:%M'
    )

print(df.dtypes)

print('\nEnriching data:\n')

top_counts_df = pd.DataFrame(df['start_location_name'].value_counts().head(5))

top_counts_df.reset_index(inplace=True)
top_counts_df.columns = ['address', 'count']

print(top_counts_df)

top_counts_clean_serie = top_counts_df['address'].str.split(
    pat=',',
    n=1,
    expand=True
)
print(top_counts_clean_serie)

top_counts_df['street'] = top_counts_clean_serie[0]
top_counts_df['street'] = top_counts_clean_serie[0].str.replace('@', "and")

print('\nOnly street name without @:\n')
print(top_counts_df['street'])


print('\nImport geocode data:\n')
geocode_df = pd.read_csv(path + 'geocodedstreet.csv')
print(geocode_df)

join_df = top_counts_df.join(
        other=geocode_df,
        how='left',
        lsuffix='_new',
        rsuffix='_geo'
    )

print('\njoined DF with geocode data\n')
print(join_df[['street_new', 'street_geo', 'x', 'y']])

print('\nMerging DFs:\n')

merge_df = pd.merge(top_counts_df, geocode_df, on='street')

print(merge_df[['street', 'x', 'y']])