from flask import Flask, render_template, request, redirect, url_for
import mysql.connector
from mysql.connector import errorcode
from secret import mySQL_password # Note: secret.py file
import turicreate as tc

app = Flask(__name__)

# Create or get a MySQL connection object
cnx = mysql.connector.connect(user='root', password=mySQL_password,
                              host='127.0.0.1',
                              database='alltrails')

# Local: http://127.0.0.1:5000/index

@app.route('/index', methods=['GET', 'POST'])
def index():

    if request.method == 'GET':
        category_selected = request.args.get('category')
        county_selected = request.args.get('county')
        trail_name_selected = request.args.get('trail_name')
        county_similar_trails_selected = request.args.get('county_similar_trails')

        categories = [('Stroller Friendly',), ('Kid Friendly',), ('Dog Friendly',), ('Wheelchair Friendly',)]
        counties = read_counties()
        trails = read_trails(category_selected, county_selected)

        should_redirect = False
        if trail_name_selected is not None and trail_name_selected not in map(lambda x: x[0], trails):
            trail_name_selected = None
            should_redirect = True

        # numbers = [('10',), ('20',), ('30',), ('40',), ('50',)]

        selected_trail, similar_trails = recommend_similar(category_selected, trail_name_selected, county_similar_trails_selected)

        if should_redirect:
            return redirect(url_for('index', category=category_selected, county=county_selected, trail_name=trail_name_selected, county_similar_trails=county_similar_trails_selected))

        return render_template('index.html', categories=categories, counties=counties, trails=trails, selected_trail=selected_trail, similar_trails=similar_trails)


def read_counties():
    # Instantiates and returns a cursor using C Extension
    cursor = cnx.cursor()

    # Note: California is divided into 58 counties
    sql = """
    SELECT DISTINCT county
    FROM alltrails.trail_info_final
    ORDER BY county
    """
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


def read_trails(category, county):
    if category is None or county is None:
        return []

    category_selected = ''.join(category.split())
    print('\nCategory Selected:', category_selected)

    # Instantiates and returns a cursor using C Extension
    cursor = cnx.cursor()

    sql = """
    SELECT TRIM(trail_name) AS trail_name_trimmed
    FROM alltrails.trail_info_final
    WHERE county = %s
      AND {} = 1
    ORDER BY trail_name_trimmed
    """.format(category_selected)
    cursor.execute(sql, (county,))
    result = cursor.fetchall()
    return result


def recommend_similar(category, trail_name, county):
    if category is None or trail_name is None or county is None:
        return [], []

    # Instantiates and returns a cursor using C Extension
    cursor = cnx.cursor()

    sql = "SELECT COUNT(*) FROM alltrails.trail_info_final"
    cursor.execute(sql)
    result = cursor.fetchall()
    for row in result:
        k = row[0]

    sql = """
    SELECT info.trail_id,
           info.trail_name,
           info.city,
           county,
           trail_difficulty,
           distance_miles,
           elevation_gain_ft,
           route_type,
           duration_mins,
           urls.url
    FROM alltrails.trail_info_final AS info
    LEFT JOIN alltrails.trail_urls_ca AS urls
      ON info.trail_id = urls.trail_id
    WHERE info.trail_name = %s
    """
    cursor.execute(sql, (trail_name,))
    selected_result = cursor.fetchall()

    observed_item = None
    for row in selected_result:
        observed_item = row[0]
    print('\nSelected Trail ID:', observed_item)

    # Recommend the 'k' highest scored items based on the interactions given in 'observed_items'
    # Note: observed_items: a list/SArray of items to use to make recommendations, or an SFrame of items and optionally ratings and/or other interaction data;
    #                       **the model will then recommend the most similar items to those given
    #       k: the number of recommendations to generate

    # Load any Turi Create model that was previously saved
    location = '/Users/yangweichle/Documents/Employment/TRAINING/DATA SCIENCE/SharpestMinds/Project/TrailSuitabilityRecommender_app/TuriCreate_model'
    loaded_model_cosine = tc.load_model(location)

    item_recommender = loaded_model_cosine.recommend_from_interactions(observed_items=[observed_item], k=k)
    #item_recommender.print_rows(num_rows=k)
    print('\nItem Recommender Number:', len(item_recommender))

    if category is not None:
        category_selected = ''.join(category.split())

    sql = """
    SELECT info.trail_id,
           info.trail_name,
           info.city,
           county,
           trail_difficulty,
           distance_miles,
           elevation_gain_ft,
           route_type,
           duration_mins,
           urls.url,
           {}
    FROM alltrails.trail_info_final AS info
    LEFT JOIN alltrails.trail_urls_ca AS urls
      ON info.trail_id = urls.trail_id
    WHERE county = %s
    """.format(category_selected)
    cursor.execute(sql, (county,))
    result_from_county_selected = cursor.fetchall()
    print('\nTrails from County Selected:', result_from_county_selected[0:5])

    # Convert list of tuples to list of dictionaries
    keys = ('trail_id', 'trail_name', 'city', 'county', 'trail_difficulty', 'distance_miles', 'elevation_gain_ft', 'route_type', 'duration_mins', 'url', 'category_selected')
    result_from_county_selected_list_of_dict = [dict(zip(keys, values)) for values in result_from_county_selected]

    # Convert 'result_from_county_selected_list_of_dict' to a dictionary with trail_id as keys
    trail_info_by_id = dict(map(lambda row: (row['trail_id'], row), result_from_county_selected_list_of_dict))

    # Join similar result info dictionary with trail_id as keys ('trail_info_by_id') with similar trails scores/ranks info from all trails ('item_recommender')
    scores_joined_with_trail_info = list(map(lambda score_row: {**score_row, **trail_info_by_id.get(score_row['trail_id'], {})}, item_recommender))
    print('\nTrail Info with Scores/Ranks (all counties):', scores_joined_with_trail_info[0:5])

    # Filtering list of dictionaries by trail_id values from county selected ('trail_info_by_id')
    similar_result_with_scores = [d for d in scores_joined_with_trail_info if d['trail_id'] in trail_info_by_id]
    print('\nSimilar Trails from County Selected:', similar_result_with_scores[0:5])

    return selected_result, similar_result_with_scores

if __name__ == '__main__': # Remove debug=True for final version
    app.run(debug=True)
