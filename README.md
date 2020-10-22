# California-Trail-Finder

Summary and goal

List files and what each one does


## Files:
(0) Create MySQL Database.py
- This program creates the MySQL database for managing data tables that will be created.

(1) Get City URLs from States.py
- This program accesses the California cities URL, webscrapes the URL associated with each city, and inserts the data into a table in the database.

(2) Get Trail URLs from Cities.py
- This program accesses each of the California city URLs, webscrapes the URL associated with each trail in the city, and inserts the data into a table in the database.

(3) Get Trail Info from Trail URLs.py
- This program accesses each of the California trail URLs, webscrapes the trail information associated with each trail, and inserts the data into a table in the database.

(4) Trail Info EDA and Create Pandas DataFrame.ipynb
- This program:
  - Performs exploratory data analysis on the trail information table.  
  - Examines trail info missingness and distributions, data visualizations, and performs data processing in preparation for creating the final table.

(5) Create Trail Info Final Table.py
- This program cleans and processes the trail info table and inserts the data into a final table in the database.

(6) Trail Recommender Final.py
- This program:
  - Reads the final trail info table and implements the trail similarity recommender system.
  - Calculates the similarity between each trail in the data and inserts the data into a table in the database.




### Web Application: [CaliforniaTrailFinder](http://50.18.56.244/index)

![w9e3jeghhrqtew5tlgcn](https://user-images.githubusercontent.com/41403941/96290086-e8337a00-0f9a-11eb-8e76-e0dfcb28ae84.jpg)
