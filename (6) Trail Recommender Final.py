import itertools
import mysql.connector
from mysql.connector import errorcode
from secret import mySQL_password # Note: secret.py file

import numpy as np
import pandas as pd

import time
import json

from sklearn.preprocessing import PolynomialFeatures, StandardScaler, MinMaxScaler
import pickle

# Create or get a MySQL connection object
cnx = mysql.connector.connect(user='root', password=mySQL_password,
                              host='127.0.0.1',
                              database='trails')
print(cnx)

# Instantiates and returns a cursor using C Extension
cursor = cnx.cursor()


# ### Retrieve the `trail_info_final` table from the database and create Pandas DataFrame
# Read SQL query or database table into a DataFrame
trail_info_final_df = pd.read_sql('SELECT * FROM trails.trail_info_final', con=cnx)
trail_info_final_df.head()


# ### Examine the DataFrame
# Return the number of rows and columns (dimensionality) of the DataFrame
print('Rows: {}, Cols: {}'.format(trail_info_final_df.shape[0], trail_info_final_df.shape[1]))

# Print a concise summary of a DataFrame including the index dtype and column dtypes, non-null values, and memory usage
# Note: Useful to quickly see if null values exist
trail_info_final_df.info()

# Column names (which is "an index")
trail_info_final_df.columns

# Sort a DataFrame by multiple columns
# Note: ascending: sort ascending vs. descending; ascending=True is default
#       inplace=True changes the original DataFrame
#trail_info_final_df.sort_values(by=['X', 'Y'], ascending=True, inplace=True)

# Print the first 5 rows and the last 5 rows of the DataFrame
trail_info_final_df.head().append(trail_info_final_df.tail())


# ### [Turi Create](https://github.com/apple/turicreate)
#
#     pip install turicreate
#
# - [User Guide](https://apple.github.io/turicreate/docs/userguide/)
# - [Turi Create API Documentation](https://apple.github.io/turicreate/docs/api/index.html)
import turicreate as tc


# ### Reading a File
# SFrame means scalable data frame. A tabular, column-mutable dataframe object that can scale to big data
# Note: data: array | pandas.DataFrame | string | dict, optional;
#             the actual interpretation of this field is dependent on the 'format' parameter
#       format: format of the data; the default, 'auto' will automatically infer the input data format
trail_info_final_sf = tc.SFrame(data=trail_info_final_df, format='dataframe')


# ### Examine the SFrame
# The shape of the SFrame, in a tuple. The first entry is the number of rows, the second is the number of columns
print('Rows: {}, Cols: {}'.format(trail_info_final_sf.shape[0], trail_info_final_sf.shape[1]))

# Print a concise summary of a DataFrame including the index dtype and column dtypes, non-null values, and memory usage
# Note: Useful to quickly see if null values exist
#trail_info_final_sf.info()

# Column names and types of the SFrame
column_info = dict(zip(trail_info_final_sf.column_names(), trail_info_final_sf.column_types()))
column_info

# The first n rows of the SFrame
trail_info_final_sf.head(5)


# ### [Recommender Systems](https://apple.github.io/turicreate/docs/userguide/recommender/)
#
# ### [Item Content Recommender](https://apple.github.io/turicreate/docs/api/generated/turicreate.recommender.item_content_recommender.ItemContentRecommender.html)
#
# Create a content-based recommender model in which the similarity between the items recommended is determined by the content of those items rather than learned from user interaction data.
#
# The similarity score between two items is calculated by first computing the similarity between the item data for each column, then taking a weighted average of the per-column similarities to get the final similarity. The recommendations are generated according to the average similarity of a candidate item to all the items in a user’s set of rated items.

# **Creating an `ItemContentRecommender`**
# - A recommender based on the similarity between item content rather using user interaction patterns to compute similarity.

# ### Model Variables: Elevation Gain, Distance, Trail Difficulty, Route Type, and Trail Tags
# Describe numeric columns
# Generates descriptive summary statistics of the central tendency, dispersion, and shape of the distribution
# Note: By default only numeric (int64) fields are returned
#       Excludes "NaN" (missing) values
# Remove columns by specifying directly column names
# Note: inplace=True changes the original DataFrame
pd.set_option('max_rows', None, 'max_columns', None)
round(trail_info_final_df.drop(columns=['trail_id', 'stars', 'num_reviews', 'duration_mins', 'elevation_gain_ft', 'distance_miles', 'trail_difficulty_easy', 'trail_difficulty_hard', 'trail_difficulty_moderate', 'route_type_Loop', 'route_type_Out_and_Back', 'route_type_Point_to_Point']).describe(), 3)

pd.reset_option('display.max_rows')
pd.reset_option('display.max_columns')

# Trail tags to include in model
trail_tags = ['Backpacking', 'Beach', 'BirdWatching', 'Blowdown', 'Bugs', 'Camping', 'Cave', 'CityWalk', 'Closed', 'DogFriendly', 'DogsOnLeash', 'Fee', 'Fishing', 'Forest', 'Hiking', 'HistoricSite', 'HorsebackRiding', 'KidFriendly', 'Lake',
              'MountainBiking', 'Muddy', 'NatureTrips', 'NoDogs', 'NoShade', 'OffTrail', 'OhvOffRoadDriving', 'OverGrown', 'PartiallyPaved', 'Paved', 'PrivateProperty', 'River', 'RoadBiking', 'RockClimbing', 'Rocky', 'Running', 'ScenicDriving',
              'Scramble', 'Snow', 'Snowshoeing', 'StrollerFriendly', 'Views', 'Walking', 'WashedOut', 'Waterfall', 'WheelchairFriendly', 'WildFlowers', 'Wildlife']

# Create a list of model columns
model_cols = ['elevation_gain_ft', 'distance_miles', 'trail_difficulty_easy', 'trail_difficulty_moderate', 'trail_difficulty_hard', 'route_type_Loop', 'route_type_Out_and_Back', 'route_type_Point_to_Point']
model_cols = model_cols + trail_tags

# Create X
X = trail_info_final_sf[model_cols]

# Convert this SFrame to pandas.DataFrame
X_df = X.to_dataframe()

# Use MinMaxScaler when you do not assume that the shape of all your features follows a normal distribution.

# Instantiate a MinMaxScaler object and compute the minimum and maximum to be used for later scaling
# --> Transform features by scaling each feature to a given range, e.g. between zero and one (default)
scaler = MinMaxScaler()
scaler.fit(X_df)

# Scale features of X according to feature_range
X_scaled = scaler.transform(X_df)
X_scaled

# Create pandas dataframe from numpy array
X_scaled_df = pd.DataFrame(X_scaled, columns=model_cols)

# SFrame means scalable data frame. A tabular, column-mutable dataframe object that can scale to big data
# Note: data: array | pandas.DataFrame | string | dict, optional;
#             the actual interpretation of this field is dependent on the 'format' parameter
#       format: format of the data; the default, 'auto' will automatically infer the input data format
trail_info_final_item_data_sf = tc.SFrame(data=X_scaled_df, format='dataframe')

# Returns an SFrame with a new column.
# The number of elements in the data given must match the length of every other column of the SFrame
# Note: column_name: the name of the column
#       inplace: whether the SFrame is modified in place; defaults to False
trail_info_final_item_data_sf = trail_info_final_item_data_sf.add_column(trail_info_final_sf['trail_id'], column_name='trail_id')

# Note: `ItemContentRecommender` shows a parameter for `similarity_metrics` in the documentation.  However, the default option is `auto` which produces cosine similarity and this is the only similarity metric type available. `ItemSimilarityRecommender` allows ‘jaccard’, ‘cosine’, ‘pearson’ as similarity types.
#
# [Cosine Similarity](https://www.sciencedirect.com/topics/computer-science/cosine-similarity)

# Setting parameter `max_item_neighborhood_size` equal to the total number of trail items in order to get a similarity score and ranking for each trail.
k = len(trail_info_final_item_data_sf)

# Create a content-based recommender model in which the similarity between the items recommended is determined by
#   the content of those items rather than learned from user interaction data
# Note: item_data: an SFrame giving the content of the items to use to learn the structure of similar items;
#                  the SFrame must have one column that matches the name of the item_id; this gives a unique identifier
#                     that can then be used to make recommendations;
#                  the rest of the columns are then used in the distance calculations below
#       item_id: the name of the column in item_data (and observation_data, if given) that represents the item ID
#       weights: if given, then weights must be a dictionary of column names present in 'item_data' to weights between
#                the column names; if 'auto' (default) is given, the all columns are weighted equally
#       similarity_metrics: similarity metric to use; 'auto' defaults to cosine
#       max_item_neighborhood_size: for each item, we hold this many similar items to use when aggregating models for
#                                   predictions; dcreasing this value decreases the memory required by the model and
#                                   decreases the time required to generate recommendations, but it may also
#                                   decrease recommendation accuracy
model_cosine = tc.recommender.item_content_recommender.create(item_data=trail_info_final_item_data_sf, item_id='trail_id', weights='auto', similarity_metrics='cosine', max_item_neighborhood_size=k)
model_cosine

# # Save the model. The model is saved as a directory which can then be loaded using the 'turicreate.load_model' method
# location = '/Users/yangweichle/Documents/Employment/TRAINING/DATA SCIENCE/SharpestMinds/Project/CaliforniaTrailFinder_app/TuriCreate_model'
# model_cosine.save(location)

# # Load any Turi Create model that was previously saved
# loaded_model_cosine = tc.load_model(location)


# ### Recommend Similar Items
# Drop table if exists
sql = 'DROP TABLE IF EXISTS recommender'
cursor.execute(sql)

# Store CREATE statements in Python dictionary TABLES
TABLES = {}

TABLES['recommender'] = (
    "CREATE TABLE `recommender` ("
    "  `trail_id` INT(11) NOT NULL,"
    "  `similiar_trails` MEDIUMBLOB,"
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


def recommender(trail_id_list):

    recommender_list = []

    trails_added = 0
    for trail_i in range(len(trail_id_list)):

        # Recommend the 'k' highest scored items based on the interactions given in 'observed_items'
        # Note: observed_items: a list/SArray of items to use to make recommendations, or an SFrame of items and optionally ratings and/or other interaction data;
        #                       **the model will then recommend the most similar items to those given
        #       k: the number of recommendations to generate
        item_recommender = model_cosine.recommend_from_interactions(observed_items=trail_id_list[trail_i:trail_i+1], k=k)

        item_recommender_trail_id_list = item_recommender['trail_id'].to_numpy().tolist()
        #print(item_recommender_trail_id_list)

        item_recommender_score_list = item_recommender['score'].to_numpy().tolist()
        #print(item_recommender_score_list)

        similiar_trails = [list(a) for a in zip(item_recommender_trail_id_list, item_recommender_score_list)]
        #print('\nSimiliar Trails:', similiar_trails)

        trail_id = int(trail_id_list[trail_i:trail_i+1].to_numpy()[0])
        #print('\nTrail ID:', trail_id)

        list_item = [trail_id, similiar_trails]

        # Return the pickled representation of the object as a bytes object
        pickled_list = pickle.dumps(similiar_trails)

        recommender_list.append(list_item)

        values = (trail_id, pickled_list)
        #print('Values:', values)

        # Execute given statement using given parameters
        # Note: If the record is a duplicate, then the IGNORE keyword tells MySQL to discard it silently without generating an error
        cursor.execute('''
            INSERT IGNORE INTO recommender (trail_id, similiar_trails)
            VALUES (%s, %s)''', values)
        rowcount = cursor.rowcount

        if rowcount == 1:
            trails_added += 1
    print('Trails added:', trails_added)

    # Commit current transaction
    cnx.commit()

    return recommender_list

trail_id_list = trail_info_final_sf['trail_id']

# Get Recommended Similar Items
get_time_start = time.time()

# Recommend the 'k' highest scored items based on the interactions given in 'observed_items'
# Note: observed_items: a list/SArray of items to use to make recommendations, or an SFrame of items and optionally ratings and/or other interaction data;
#                       **the model will then recommend the most similar items to those given
#       k: the number of recommendations to generate
recommender_list = recommender(trail_id_list)
minutes = (time.time() - get_time_start)/60
print('Get Recommended Similar Items time: {} hrs: {} mins'.format(int(minutes // 60), round(minutes % 60)))
