import itertools
import mysql.connector
from mysql.connector import errorcode
from secret import mySQL_password # Note: secret.py file

import numpy as np
import pandas as pd

# Create or get a MySQL connection object
cnx = mysql.connector.connect(user='root', password=mySQL_password,
                              host='127.0.0.1',
                              database='trails')
print(cnx)

# Instantiates and returns a cursor using C Extension
cursor = cnx.cursor()

# ### Retrieve the `trail_info` table from the database and create Pandas DataFrame
# Read SQL query or database table into a DataFrame
state_trails_df = pd.read_sql('SELECT * FROM trails.trail_info', con=cnx)
state_trails_df.head()


# ### Clean and Manipulate DataFrame to Create Final Trail Info Table
# #### Drop non-California observations
# Return elements, either from `x` or `y`, depending on `condition`
state_trails_df['non_california'] = np.where(state_trails_df.state.isin(['California', '']), 0, 1)

# Categorical column frequency
# Returns counts of unique values in descending order (first element is the most frequently-occurring element)
# Note: Excludes NA values by default
state_trails_df.non_california.value_counts(dropna=False)

# Excluding non-california values
state_trails_df = state_trails_df[state_trails_df.non_california == 0]

# Remove columns by specifying directly column names
# Note: inplace=True changes the original DataFrame
state_trails_df.drop(columns=['non_california'], inplace=True)


# #### Assign `state` to 'California' for the following `trail_id`s that were confirmed to be California trails
state_trails_df['trail_id'][state_trails_df.state != 'California']

values = state_trails_df['trail_id'][state_trails_df.state != 'California'].values.tolist()
values

# Return elements, either from `x` or `y`, depending on `condition`
state_trails_df['state'] = np.where(state_trails_df.trail_id.isin(values), 'California', state_trails_df.state)

# Categorical column frequency
# Returns counts of unique values in descending order (first element is the most frequently-occurring element)
# Note: Excludes NA values by default
state_trails_df.state.value_counts(dropna=False)

# Return the number of rows and columns (dimensionality) of the DataFrame
print('Rows: {}, Cols: {}'.format(state_trails_df.shape[0], state_trails_df.shape[1]))


# #### Find missing values for `city` and include in data if found
# Detect missing values
state_trails_df[state_trails_df.city.isnull()]

# Note: The city names were found for the following trails after searching the internet for the trail name
state_trails_df['city'] = np.where(state_trails_df.trail_id == 10541176, 'La Jolla', state_trails_df.city)
state_trails_df['city'] = np.where(state_trails_df.trail_id == 10577579, 'Big Bear Lake', state_trails_df.city)


# #### `distance` in kilometers is contained in the data
# Return elements, either from `x` or `y`, depending on `condition`
state_trails_df['distance_km_flag'] = np.where(state_trails_df['distance'].str.contains('km'), 1, 0)

# Categorical column frequency
# Returns counts of unique values in descending order (first element is the most frequently-occurring element)
# Note: Excludes NA values by default
state_trails_df.distance_km_flag.value_counts(dropna=False)


# #### Converting `distance` to numeric measurement as `distance_numeric`
state_trails_df['distance_numeric'] = state_trails_df.distance.str.replace(',', '').str.extract('(\d*\.\d+|\d+)', expand=False).astype(float)


# #### Converting `distance_numeric` to numeric measurement as `distance_miles`
# Note: Records flagged as distance in kilometers will be converted to miles.

# Return elements, either from `x` or `y`, depending on `condition`
# Kilometer to mile conversion factor
conv_fac = 0.621371
state_trails_df['distance_miles'] = np.where(state_trails_df.distance_km_flag == 1, round(state_trails_df.distance_numeric * conv_fac, 1), state_trails_df.distance_numeric)

# Remove columns by specifying directly column names
# Note: inplace=True changes the original DataFrame
state_trails_df.drop(columns=['distance', 'distance_km_flag', 'distance_numeric'], inplace=True)


# #### `elevation_gain` in meters is contained in the data
# Return elements, either from `x` or `y`, depending on `condition`
state_trails_df['elevation_gain_m_flag'] = np.where(state_trails_df['elevation_gain'].str.contains('m'), 1, 0)

# Categorical column frequency
# Returns counts of unique values in descending order (first element is the most frequently-occurring element)
# Note: Excludes NA values by default
state_trails_df.elevation_gain_m_flag.value_counts(dropna=False)


# #### Converting `elevation_gain` to numeric measurement as `elevation_gain_numeric`
state_trails_df['elevation_gain_numeric'] = state_trails_df.elevation_gain.str.replace(',', '').str.extract('(\d*\.\d+|\d+)', expand=False).astype(int)


# #### Converting `elevation_gain_numeric` to numeric measurement as `elevation_gain_ft`
# Note: Records flagged as elevation gain in meters will be converted to feet.

# Return elements, either from `x` or `y`, depending on `condition`
# Meter to feet conversion factor
conv_fac = 0.3048
state_trails_df['elevation_gain_ft'] = np.where(state_trails_df.elevation_gain_m_flag == 1, round(state_trails_df.elevation_gain_numeric / conv_fac).astype(int), state_trails_df.elevation_gain_numeric)

# Remove columns by specifying directly column names
# Note: inplace=True changes the original DataFrame
state_trails_df.drop(columns=['elevation_gain', 'elevation_gain_m_flag', 'elevation_gain_numeric'], inplace=True)


# #### Create dummy variables for `trail_difficulty`
# Convert categorical variable into dummy/indicator variables
# Note: prefix: string to append DataFrame column names
#       drop_first=True removes the first level to get k-1 dummies out of k categorical events
# Join columns with other DataFrame either on index or on a key
state_trails_df = state_trails_df.join(pd.get_dummies(state_trails_df.trail_difficulty, prefix='trail_difficulty', drop_first=False))
state_trails_df.head()


# #### Create dummy variables for `route_type`
# Convert categorical variable into dummy/indicator variables
# Note: prefix: string to append DataFrame column names
#       drop_first=True removes the first level to get k-1 dummies out of k categorical events
# Join columns with other DataFrame either on index or on a key
state_trails_df = state_trails_df.join(pd.get_dummies(state_trails_df.route_type, prefix='route_type', drop_first=False))
state_trails_df.head()

# Rename one or more columns in the original DataFrame rather than returning a new view
# Note: inplace: whether to return a new DataFrame; if True then value of copy is ignored
state_trails_df.rename(columns={'route_type_Out & Back': 'route_type_Out_and_Back', 'route_type_Point to Point': 'route_type_Point_to_Point'}, inplace=True)


# #### Create binary variables for Trail Tags
state_trails_df['trail_tags_str_lst'] = state_trails_df.trail_tags_str.str.split(' ')
state_trails_df.head()

# Identifying unique trail tags
unique_tags = set(itertools.chain.from_iterable(state_trails_df.trail_tags_str_lst))
#unique_tags = state_trails_df.trail_tags_str_lst.explode().unique()
unique_tags

# Creating trail tag flags
tag_flags = state_trails_df['trail_tags_str_lst'].str.join(sep='*').str.get_dummies(sep='*')
tag_flags.head()

# Rename one or more columns in the original DataFrame rather than returning a new view
# Note: inplace: whether to return a new DataFrame; if True then value of copy is ignored
tag_flags.rename(columns={'Ohv/OffRoadDriving': 'OhvOffRoadDriving'}, inplace=True)

# Concatenate pandas objects along a particular axis with optional set logic along the other axes
# Note: axis: the axis to concatenate along; {0/'index', 1/'columns'}, default 0
state_trails_df = pd.concat([state_trails_df, tag_flags], axis=1)
state_trails_df.head()

# Remove columns by specifying directly column names
# Note: inplace=True changes the original DataFrame
state_trails_df.drop(columns=['trail_tags_str', 'trail_tags_str_lst'], inplace=True)

# Print a concise summary of a DataFrame including the index dtype and column dtypes, non-null values, and memory usage
# Note: Useful to quickly see if null values exist
state_trails_df.info()


# ### Create City DataFrame of California Trails
# #### Identifying important columns to include in the DataFrame
# Select multiple columns
include_cols = ['city']
cities_df = state_trails_df[include_cols]
cities_df

# Return DataFrame with duplicate rows removed, optionally only considering certain columns
cities_df = cities_df.drop_duplicates()

# Sort a DataFrame by multiple columns
# Note: ascending: sort ascending vs. descending; ascending=True is default
#       inplace=True changes the original DataFrame
cities_df = cities_df.sort_values(by=['city'], ascending=True, inplace=False)
cities_df


# ### Create California City-County Crosswalk DataFrame
# [SimpleMaps: United States Cities Database](https://simplemaps.com/data/us-cities)
# ### Reading a File

# Create data path variable for loading data
data_path = '/Users/yangweichle/Documents/Employment/TRAINING/DATA SCIENCE/SharpestMinds/Project/SimpleMaps/'

# Read file into DataFrame
# Note: header=0 disables the header from the file
#       dtype: type name or dict of column -> type, default None; data type for data or columns;
#              e.g. {'a': np.float64, 'b': np.int32}; use 'str' or 'object' together with suitable 'na_values' settings
#              to preserve and not interpret dtype
us_cities_df = pd.read_csv(data_path + 'uscities.csv', header=0) # Comma-separated values file


# ### Examine the DataFrame
# Return the number of rows and columns (dimensionality) of the DataFrame
print('Rows: {}, Cols: {}'.format(us_cities_df.shape[0], us_cities_df.shape[1]))

# Print a concise summary of a DataFrame including the index dtype and column dtypes, non-null values, and memory usage
# Note: Useful to quickly see if null values exist
us_cities_df.info()

# Column names (which is "an index")
us_cities_df.columns

# Print the first 5 rows of the DataFrame
us_cities_df.head()


# #### Identifying important columns to include in the DataFrame
# Select multiple columns
include_cols = ['city', 'state_id', 'county_name']
us_cities_df = us_cities_df[include_cols]

# Return the number of rows and columns (dimensionality) of the DataFrame
print('Rows: {}, Cols: {}'.format(us_cities_df.shape[0], us_cities_df.shape[1]))

# Print the first 5 rows of the DataFrame
us_cities_df.head()

# Rename one or more columns in the original DataFrame rather than returning a new view
# Note: inplace: whether to return a new DataFrame; if True then value of copy is ignored
us_cities_df.rename(columns={'county_name': 'county'}, inplace=True)

# Print a concise summary of a DataFrame including the index dtype and column dtypes, non-null values, and memory usage
# Note: Useful to quickly see if null values exist
us_cities_df.info()


# #### Select only California records for crosswalk
ca_city_county_xwalk_df = us_cities_df[us_cities_df.state_id == 'CA']

# Return the number of rows and columns (dimensionality) of the DataFrame
print('Rows: {}, Cols: {}'.format(ca_city_county_xwalk_df.shape[0], ca_city_county_xwalk_df.shape[1]))

# Print the first 5 rows of the DataFrame
ca_city_county_xwalk_df.head()

# Return DataFrame with duplicate rows removed, optionally only considering certain columns
ca_city_county_xwalk_df = ca_city_county_xwalk_df.drop_duplicates()

# Return the number of rows and columns (dimensionality) of the DataFrame
print('Rows: {}, Cols: {}'.format(ca_city_county_xwalk_df.shape[0], ca_city_county_xwalk_df.shape[1]))


# ### Merge City DataFrame with California City-County Crosswalk DataFrame
# Merge DataFrame or named Series objects with a database-style join
cities_df = cities_df.merge(ca_city_county_xwalk_df, how='left', on='city')
cities_df

# Determine how many counties are missing for all cities after merging with California city-county crosswalk
print(cities_df.county.isnull().sum())
cities_df[cities_df.county.isnull()]

# Create City-County dictionary where county is missing
city_county_dict = dict(zip(cities_df[cities_df.county.isnull()].city, cities_df[cities_df.county.isnull()].county))
city_county_dict

len(city_county_dict)

# Search for city name on internet to find county
# Update dictionary values
city_county_dict['Alviso'] = 'Santa Clara'
city_county_dict['Amboy'] = 'San Bernardino'
city_county_dict['Angels Camp'] = 'Calaveras'
city_county_dict['Angelus Oaks'] = 'San Bernardino'
city_county_dict['Applegate'] = 'Placer'
city_county_dict['Badger'] = 'Tulare'
city_county_dict['Bard'] = 'Imperial'
city_county_dict['Beale Air Force Base'] = 'Yuba'
city_county_dict['Belvedere Tiburon'] = 'Marin'
city_county_dict['Big Bar'] = 'Trinity'
city_county_dict['Big Sur'] = 'Monterey'
city_county_dict['Blairsden Graeagle'] = 'Plumas'
city_county_dict['Blocksburg'] = 'Humboldt'
city_county_dict['Blue Jay'] = 'San Bernardino'
city_county_dict['Browns Valley'] = 'Yuba'
city_county_dict['Caliente'] = 'Kern'
city_county_dict['Callahan'] = 'Siskiyou'
city_county_dict['Campo Seco'] = 'Calaveras'
city_county_dict['Canoga Park'] = 'Los Angeles'
city_county_dict['Cantil'] = 'Kern'
city_county_dict['Canyon'] = 'Contra Costa'
city_county_dict['Canyon Country'] = 'Los Angeles'
city_county_dict['Canyon Dam'] = 'Plumas'
city_county_dict['Capistrano Beach'] = 'Orange'
city_county_dict['Cardiff-by-the-Sea'] = 'San Diego'
city_county_dict['Carlotta'] = 'Humboldt'
city_county_dict['Carmel'] = 'Monterey'
city_county_dict['Castella'] = 'Shasta'
city_county_dict['Cedar Glen'] = 'San Bernardino'
city_county_dict['Cedar Grove'] = 'Fresno'
city_county_dict['Cedarpines Park'] = 'San Bernardino'
city_county_dict['Chatsworth'] = 'Los Angeles'
city_county_dict['Cima'] = 'San Bernardino'
city_county_dict['Cool'] = 'El Dorado'
city_county_dict['Coolidge Springs'] = 'Imperial'
city_county_dict['Coyote'] = 'Santa Clara'
city_county_dict['Daggett'] = 'San Bernardino'
city_county_dict['Death Valley'] = 'Inyo'
city_county_dict['Dodgertown'] = 'Los Angeles'
city_county_dict['Dulzura'] = 'San Diego'
city_county_dict['Dunlap'] = 'Fresno'
city_county_dict['Earp'] = 'San Bernardino'
city_county_dict['Echo Lake'] = 'El Dorado'
city_county_dict['Edison'] = 'Kern'
city_county_dict['Edwards'] = 'Kern'
city_county_dict['Elk'] = 'Mendocino'
city_county_dict['Emigrant Gap'] = 'Placer'
city_county_dict['Encino'] = 'Los Angeles'
city_county_dict['Essex'] = 'San Bernardino'
city_county_dict['Fawnskin'] = 'San Bernardino'
city_county_dict['Foothill Ranch'] = 'Orange'
city_county_dict['Forest Falls'] = 'San Bernardino'
city_county_dict['Forest Knolls'] = 'Marin'
city_county_dict['Forks of Salmon'] = 'Siskiyou'
city_county_dict['Fort Irwin National Training Center'] = 'San Bernardino'
city_county_dict['Garden Valley'] = 'El Dorado'
city_county_dict['Glenn'] = 'Glenn'
city_county_dict['Glennville'] = 'Kern'
city_county_dict['Gold Run'] = 'Placer'
city_county_dict['Granada Hills'] = 'Los Angeles'
city_county_dict['Green Valley Lake'] = 'San Bernardino'
city_county_dict['Greenbrae'] = 'Marin'
city_county_dict['Gualala'] = 'Mendocino'
city_county_dict['Guatay'] = 'San Diego'
city_county_dict['Hathaway Pines'] = 'Calaveras'
city_county_dict['Helendale'] = 'San Bernardino'
city_county_dict['Hinkley'] = 'San Bernardino'
city_county_dict['Homewood'] = 'Placer'
city_county_dict['Hume'] = 'Fresno'
city_county_dict['Igo'] = 'Shasta'
city_county_dict['Johnsondale'] = 'Tulare'
city_county_dict['Jolon'] = 'Monterey'
city_county_dict['Kit Carson'] = 'Amador'
city_county_dict['Klamath River'] = 'Siskiyou'
city_county_dict['Kyburz'] = 'El Dorado'
city_county_dict['La Canada Flintridge'] = 'Los Angeles'
city_county_dict['La Crescenta'] = 'Los Angeles'
city_county_dict['La Grange'] = 'Stanislaus'
city_county_dict['La Jolla'] = 'San Diego'
city_county_dict['Lagunitas'] = 'Marin'
city_county_dict['Lakeshore'] = 'Fresno'
city_county_dict['Landers'] = 'San Bernardino'
city_county_dict['Little Lake'] = 'Inyo'
city_county_dict['Llano'] = 'Los Angeles'
city_county_dict['Lotus'] = 'El Dorado'
city_county_dict['Lucia'] = 'Monterey'
city_county_dict['Marshall'] = 'Marin'
city_county_dict['Mc Kittrick'] = 'Kern'
city_county_dict['Mccloud'] = 'Siskiyou'
city_county_dict['Mi Wuk Village'] = 'Tuolumne'
city_county_dict['Mill Creek'] = 'Tehama'
city_county_dict['Mono Hot Springs'] = 'Fresno'
city_county_dict['Mount Baldy'] = 'San Bernardino'
city_county_dict['Mount Hamilton'] = 'Santa Clara'
city_county_dict['Mount Wilson'] = 'Los Angeles'
city_county_dict['Navarro'] = 'Mendocino'
city_county_dict['New Almaden'] = 'Santa Clara'
city_county_dict['Newberry Springs'] = 'San Bernardino'
city_county_dict['Newbury Park'] = 'Ventura'
city_county_dict['Newhall'] = 'Los Angeles'
city_county_dict['Newport Coast'] = 'Orange'
city_county_dict['Nipton'] = 'San Bernardino'
city_county_dict['Norden'] = 'Nevada'
city_county_dict['North Fork'] = 'Madera'
city_county_dict['North Hills'] = 'Los Angeles'
city_county_dict['North Hollywood'] = 'Los Angeles'
city_county_dict['Northridge'] = 'Los Angeles'
city_county_dict['Olema'] = 'Marin'
city_county_dict['Olympic Valley'] = 'Placer'
city_county_dict['Oro Grande'] = 'San Bernardino'
city_county_dict['Pacific Palisades'] = 'Los Angeles'
city_county_dict['Pacoima'] = 'Los Angeles'
city_county_dict['Paicines'] = 'San Benito'
city_county_dict['Palomar Mountain'] = 'San Diego'
city_county_dict['Palos Verdes Peninsula'] = 'Los Angeles'
city_county_dict['Parker Dam'] = 'San Bernardino'
city_county_dict['Paso Robles'] = 'San Luis Obispo'
city_county_dict['Pauma Valley'] = 'San Diego'
city_county_dict['Pearblossom'] = 'Los Angeles'
city_county_dict['Pebble Beach'] = 'Monterey'
city_county_dict['Petrolia'] = 'Humboldt'
city_county_dict['Piercy'] = 'Mendocino'
city_county_dict['Pilot Hill'] = 'El Dorado'
city_county_dict['Pinecrest'] = 'Tuolumne'
city_county_dict['Pioneertown'] = 'San Bernardino'
city_county_dict['Platina'] = 'Shasta'
city_county_dict['Playa Del Rey'] = 'Los Angeles'
city_county_dict['Pope Valley'] = 'Napa'
city_county_dict['Porter Ranch'] = 'Los Angeles'
city_county_dict['Quail Valley'] = 'Riverside'
city_county_dict['Ranchita'] = 'San Diego'
city_county_dict['Raymond'] = 'Madera'
city_county_dict['Red Mountain'] = 'San Bernardino'
city_county_dict['Rumsey'] = 'Yolo'
city_county_dict['San Gregorio'] = 'San Mateo'
city_county_dict['San Pedro'] = 'Los Angeles'
city_county_dict['San Ysidro'] = 'San Diego'
city_county_dict['Santa Ysabel'] = 'San Diego'
city_county_dict['Scott Bar'] = 'Siskiyou'
city_county_dict['Seiad Valley'] = 'Siskiyou'
city_county_dict['Sequoia National Park'] = 'Tulare'
city_county_dict['Seven Pines'] = 'Inyo'
city_county_dict['Sherman Oaks'] = 'Los Angeles'
city_county_dict['Silverado'] = 'Orange'
city_county_dict['Skyforest'] = 'San Bernardino'
city_county_dict['Sloughhouse'] = 'Sacramento'
city_county_dict['Somerset'] = 'El Dorado'
city_county_dict['Somes Bar'] = 'Siskiyou'
city_county_dict['Stewarts Point'] = 'Sonoma'
city_county_dict['Stony Creek Village'] = 'Tulare'
city_county_dict['Strawberry Valley'] = 'Yuba'
city_county_dict['Studio City'] = 'Los Angeles'
city_county_dict['Sugarloaf'] = 'San Bernardino'
city_county_dict['Sun City'] = 'Riverside'
city_county_dict['Sun Valley'] = 'Los Angeles'
city_county_dict['Sunland'] = 'Los Angeles'
city_county_dict['Sunland-Tujunga'] = 'Los Angeles'
city_county_dict['Sylmar'] = 'Los Angeles'
city_county_dict['Tahoe City'] = 'Placer'
city_county_dict['Tarzana'] = 'Los Angeles'
city_county_dict['The Sea Ranch'] = 'Sonoma'
city_county_dict['Tollhouse'] = 'Fresno'
city_county_dict['Trabuco Canyon'] = 'Orange'
city_county_dict['Tuolumne'] = 'Tuolumne'
city_county_dict['Twin Bridges'] = 'El Dorado'
city_county_dict['Twin Peaks'] = 'San Bernardino'
city_county_dict['Two Harbors'] = 'Los Angeles'
city_county_dict['Universal City'] = 'Los Angeles'
city_county_dict['Valencia'] = 'Los Angeles'
city_county_dict['Valyermo'] = 'Los Angeles'
city_county_dict['Van Nuys'] = 'Los Angeles'
city_county_dict['Venice'] = 'Los Angeles'
city_county_dict['Ventura'] = 'Ventura'
city_county_dict['Vernalis'] = 'San Joaquin'
city_county_dict['Warner Springs'] = 'San Diego'
city_county_dict['Weimar'] = 'Placer'
city_county_dict['West Hills'] = 'Los Angeles'
city_county_dict['Westport'] = 'Mendocino'
city_county_dict['Whiskeytown'] = 'Shasta'
city_county_dict['Whitethorn'] = 'Humboldt'
city_county_dict['Whitmore'] = 'Shasta'
city_county_dict['Wilmington'] = 'Los Angeles'
city_county_dict['Wishon'] = 'Madera'
city_county_dict['Woodland Hills'] = 'Los Angeles'
city_county_dict['Yermo'] = 'San Bernardino'

len(city_county_dict)
city_county_dict

# Create DataFrame from list of dictionary items
city_county_df = pd.DataFrame(list(city_county_dict.items()), columns=['city', 'county'])
city_county_df

# Excluding missing city because county can't be mapped
city_county_df = city_county_df[city_county_df.county.notna()]
city_county_df


# #### Update California City-County Crosswalk to include cities/counties from City-County DataFrame where county was missing
# Concatenate pandas objects along a particular axis with optional set logic along the other axes
# Note: axis: the axis to concatenate along; {0/'index', 1/'columns'}, default 0
ca_city_county_xwalk_df2 = pd.concat([ca_city_county_xwalk_df.drop(columns=['state_id'], inplace=False),
                                      city_county_df], axis=0)

# Return DataFrame with duplicate rows removed, optionally only considering certain columns
ca_city_county_xwalk_df2 = ca_city_county_xwalk_df2.drop_duplicates()


# ### Join State Trails DataFrame with California City-County Crosswalk to obtain `county`
# Merge DataFrame or named Series objects with a database-style join
state_trails_df2 = state_trails_df.merge(ca_city_county_xwalk_df2, how='left', on='city')
state_trails_df2

# Note: The crosswalk contains cities with the same name but in different counties.  After performing the merge on city, 46 rows were added in error (wrong county added to city).
mask = state_trails_df2.trail_id.groupby(state_trails_df2.trail_id).transform('value_counts') > 1
state_trails_df2_dups = state_trails_df2[mask]

pd.set_option('max_rows', None, 'max_columns', None)
state_trails_df2_dups[['city', 'trail_id', 'trail_name', 'county']]

pd.reset_option('display.max_rows')
pd.reset_option('display.max_columns')

# Note: After examining the duplicate rows, the following criteria will be used to flag and remove the rows that were found to be errors in 1-many merging (i.e., 1 city name to many counties).
# Return elements, either from `x` or `y`, depending on `condition`
state_trails_df2['dup_city'] = np.where(((state_trails_df2.city == 'Greenfield') & (state_trails_df2.county == 'Kern')) |
                                        ((state_trails_df2.city == 'El Sobrante') & (state_trails_df2.county == 'Riverside')) |
                                        ((state_trails_df2.city == 'Spring Valley') & (state_trails_df2.county == 'Lake')) |
                                        ((state_trails_df2.city == 'Paradise') & (state_trails_df2.county == 'Mono')) |
                                        ((state_trails_df2.city == 'Burbank') & (state_trails_df2.county == 'Santa Clara')) |
                                        ((state_trails_df2.city == 'El Cerrito') & (state_trails_df2.county == 'Riverside')) |
                                        ((state_trails_df2.city == 'Mountain View') & (state_trails_df2.county == 'Contra Costa')) |
                                        ((state_trails_df2.city == 'Strawberry') & (state_trails_df2.county == 'Marin')), 1, 0)

# Categorical column frequency
# Returns counts of unique values in descending order (first element is the most frequently-occurring element)
# Note: Excludes NA values by default
state_trails_df2.dup_city.value_counts(dropna=False)

# Excluding duplicate row values that were errors from 1-many merging (i.e., 1 city name to many counties)
state_trails_df_final = state_trails_df2.loc[state_trails_df2['dup_city'] == 0]

# Remove columns by specifying directly column names
# Note: inplace=True changes the original DataFrame
state_trails_df_final = state_trails_df_final.drop(columns=['dup_city'], inplace=False)

# Note: After investigating the following trail, there does not exist a city that can be assigned to the row. However, the county can be assigned based on the location.
state_trails_df_final[state_trails_df_final.county.isnull()]

# Return elements, either from `x` or `y`, depending on `condition`
state_trails_df_final['county'] = np.where(state_trails_df_final.trail_id == 10581562, 'Santa Barbara', state_trails_df_final.county)

include_cols = ['state', 'city', 'county', 'trail_name', 'trail_id', 'trail_difficulty', 'trail_difficulty_easy','trail_difficulty_hard', 'trail_difficulty_moderate',
                'stars', 'num_reviews', 'trail_region', 'duration_mins', 'route_type', 'route_type_Loop', 'route_type_Out_and_Back', 'route_type_Point_to_Point',
                'distance_miles', 'elevation_gain_ft', 'Backpacking', 'Beach', 'BikeTouring', 'BirdWatching', 'Blowdown',
                'BridgeOut', 'Bugs', 'Camping', 'Cave', 'CityWalk', 'Closed', 'CrossCountrySkiing', 'DogFriendly', 'DogsOnLeash',
                'Event', 'Fee', 'Fishing', 'Forest', 'Hiking', 'HistoricSite', 'HorsebackRiding', 'HotSprings', 'KidFriendly',
                'Lake', 'MountainBiking', 'Muddy', 'NatureTrips', 'NoDogs', 'NoShade', 'OffTrail', 'OhvOffRoadDriving', 'OverGrown',
                'PaddleSports', 'PartiallyPaved', 'Paved', 'PrivateProperty', 'PubWalk', 'RailsTrails', 'River', 'RoadBiking',
                'RockClimbing', 'Rocky', 'Running', 'ScenicDriving', 'Scramble', 'Skiing', 'Snow', 'Snowshoeing', 'StrollerFriendly',
                'Views', 'Walking', 'WashedOut', 'Waterfall', 'WheelchairFriendly', 'WildFlowers', 'Wildlife']
state_trails_df_final = state_trails_df_final[include_cols]


# ### Create Final Trail Info Table in MySQL Database
# Note: In order to upload to mySQL database, `NaN` values need to be converted to `None`.  However, in doing so, this changes the dtype of converted columns to object.
# Replace values in a column
# Note: inplace=True changes the original DataFrame
state_trails_df_final.duration_mins.replace({np.nan: None}, inplace=True)

# Drop table if exists
sql = 'DROP TABLE IF EXISTS trail_info_final'
cursor.execute(sql)

# Store CREATE statements in Python dictionary TABLES
TABLES = {}

TABLES['trail_info_final'] = (
    "CREATE TABLE `trail_info_final` ("
    "  `state` VARCHAR(21) NOT NULL,"
    "  `city` VARCHAR(50) NULL,"
    "  `county` VARCHAR(50) NULL,"
    "  `trail_name` VARCHAR(120) NOT NULL,"
    "  `trail_id` INT(11) NOT NULL,"
    "  `trail_difficulty` VARCHAR(9) NOT NULL,"
    "  `trail_difficulty_easy` INT(1) NOT NULL,"
    "  `trail_difficulty_hard` INT(1) NOT NULL,"
    "  `trail_difficulty_moderate` INT(1) NOT NULL,"
    "  `stars` DECIMAL(2,1) NOT NULL,"
    "  `num_reviews` INT(11) NOT NULL,"
    "  `trail_region` VARCHAR(100) NOT NULL,"
    "  `duration_mins` VARCHAR(10) NULL,"
    "  `route_type` VARCHAR(15) NOT NULL,"
    "  `route_type_Loop` INT(1) NOT NULL,"
    "  `route_type_Out_and_Back` INT(1) NOT NULL,"
    "  `route_type_Point_to_Point` INT(1) NOT NULL,"
    "  `distance_miles` FLOAT NOT NULL,"
    "  `elevation_gain_ft` INT(11) NOT NULL,"
    "  `Backpacking` INT(1) NOT NULL,"
    "  `Beach` INT(1) NOT NULL,"
    "  `BikeTouring` INT(1) NOT NULL,"
    "  `BirdWatching` INT(1) NOT NULL,"
    "  `Blowdown` INT(1) NOT NULL,"
    "  `BridgeOut` INT(1) NOT NULL,"
    "  `Bugs` INT(1) NOT NULL,"
    "  `Camping` INT(1) NOT NULL,"
    "  `Cave` INT(1) NOT NULL,"
    "  `CityWalk` INT(1) NOT NULL,"
    "  `Closed` INT(1) NOT NULL,"
    "  `CrossCountrySkiing` INT(1) NOT NULL,"
    "  `DogFriendly` INT(1) NOT NULL,"
    "  `DogsOnLeash` INT(1) NOT NULL,"
    "  `Event` INT(1) NOT NULL,"
    "  `Fee` INT(1) NOT NULL,"
    "  `Fishing` INT(1) NOT NULL,"
    "  `Forest` INT(1) NOT NULL,"
    "  `Hiking` INT(1) NOT NULL,"
    "  `HistoricSite` INT(1) NOT NULL,"
    "  `HorsebackRiding` INT(1) NOT NULL,"
    "  `HotSprings` INT(1) NOT NULL,"
    "  `KidFriendly` INT(1) NOT NULL,"
    "  `Lake` INT(1) NOT NULL,"
    "  `MountainBiking` INT(1) NOT NULL,"
    "  `Muddy` INT(1) NOT NULL,"
    "  `NatureTrips` INT(1) NOT NULL,"
    "  `NoDogs` INT(1) NOT NULL,"
    "  `NoShade` INT(1) NOT NULL,"
    "  `OffTrail` INT(1) NOT NULL,"
    "  `OhvOffRoadDriving` INT(1) NOT NULL,"
    "  `OverGrown` INT(1) NOT NULL,"
    "  `PaddleSports` INT(1) NOT NULL,"
    "  `PartiallyPaved` INT(1) NOT NULL,"
    "  `Paved` INT(1) NOT NULL,"
    "  `PrivateProperty` INT(1) NOT NULL,"
    "  `PubWalk` INT(1) NOT NULL,"
    "  `RailsTrails` INT(1) NOT NULL,"
    "  `River` INT(1) NOT NULL,"
    "  `RoadBiking` INT(1) NOT NULL,"
    "  `RockClimbing` INT(1) NOT NULL,"
    "  `Rocky` INT(1) NOT NULL,"
    "  `Running` INT(1) NOT NULL,"
    "  `ScenicDriving` INT(1) NOT NULL,"
    "  `Scramble` INT(1) NOT NULL,"
    "  `Skiing` INT(1) NOT NULL,"
    "  `Snow` INT(1) NOT NULL,"
    "  `Snowshoeing` INT(1) NOT NULL,"
    "  `StrollerFriendly` INT(1) NOT NULL,"
    "  `Views` INT(1) NOT NULL,"
    "  `Walking` INT(1) NOT NULL,"
    "  `WashedOut` INT(1) NOT NULL,"
    "  `Waterfall` INT(1) NOT NULL,"
    "  `WheelchairFriendly` INT(1) NOT NULL,"
    "  `WildFlowers` INT(1) NOT NULL,"
    "  `Wildlife` INT(1) NOT NULL,"
    "  PRIMARY KEY (`trail_id`)"
    ") ENGINE=InnoDB")

# Create tables by iterating over the items of the TABLES dictionary
for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print('Creating table {}: '.format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print('already exists')
        else:
            print(err.msg)
    else:
        print('OK')

#cursor.close()
#cnx.close()

values = [tuple(x) for x in state_trails_df_final.values]
#print(values)

# Execute the given operation multiple times
# Note: If the record is a duplicate, then the IGNORE keyword tells MySQL to discard it silently without generating an error
cursor.executemany('''
    INSERT IGNORE INTO trail_info_final (state, city, county, trail_name, trail_id, trail_difficulty, trail_difficulty_easy, trail_difficulty_hard, trail_difficulty_moderate, stars, num_reviews, trail_region, duration_mins, route_type, route_type_Loop, route_type_Out_and_Back, route_type_Point_to_Point, distance_miles, elevation_gain_ft, Backpacking, Beach, BikeTouring, BirdWatching, Blowdown, BridgeOut, Bugs, Camping, Cave, CityWalk, Closed, CrossCountrySkiing, DogFriendly, DogsOnLeash, Event, Fee, Fishing, Forest, Hiking, HistoricSite, HorsebackRiding, HotSprings, KidFriendly, Lake, MountainBiking, Muddy, NatureTrips, NoDogs, NoShade, OffTrail, OhvOffRoadDriving, OverGrown, PaddleSports, PartiallyPaved, Paved, PrivateProperty, PubWalk, RailsTrails, River, RoadBiking, RockClimbing, Rocky, Running, ScenicDriving, Scramble, Skiing, Snow, Snowshoeing, StrollerFriendly, Views, Walking, WashedOut, Waterfall, WheelchairFriendly, WildFlowers, Wildlife)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', values)

# Commit current transaction
cnx.commit()

sql = "SELECT COUNT(*) FROM trails.trail_info_final"
cursor.execute(sql)
result = cursor.fetchall()

for row in result:
    print(row[0])

# Read SQL query or database table into a DataFrame
trail_info_final_df = pd.read_sql('SELECT * FROM trails.trail_info_final', con=cnx)
trail_info_final_df.head()

# Print a concise summary of a DataFrame including the index dtype and column dtypes, non-null values, and memory usage
# Note: Useful to quickly see if null values exist
trail_info_final_df.info()
