# California-Trail-Finder
### Summary
This repository includes the steps to build [CaliforniaTrailFinder.com](http://CaliforniaTrailFinder.com/), a full-stack hiking trail recommender system. Incorporates user-specific trail preferences such as kid, dog, stroller, or wheelchair friendly, logs previously liked trails, and finds similar trails.

Blog Post on Medium: [Building CaliforniaTrailFinder](https://tinyurl.com/y4aqby6a)

![w9e3jeghhrqtew5tlgcn](https://user-images.githubusercontent.com/41403941/96290086-e8337a00-0f9a-11eb-8e76-e0dfcb28ae84.jpg)

**Goals**
- Implement a web-based recommender system for finding similar hiking trails in California
- Describe the process including:
    - Data Collection
    - Database Management
    - Exploratory Data Analysis
    - Model Development
    - Web Development/Deployment

**Files**

(0) Create MySQL Database.py
- Create the MySQL database for managing data tables that will be created.

(1) Get City URLs from States.py
- Access the California cities URL, webscrape the URL and other info associated with each city, and insert the data into a table in the database.

(2) Get Trail URLs from Cities.py
- Access each of the California city URLs, webscrape the URL and other info associated with each trail in the city, and insert the data into a table in the database.

(3) Get Trail Info from Trail URLs.py
- Access each of the California trail URLs, webscrape the trail information associated with each trail, and insert the data into a table in the database.

(4) Trail Info EDA and Create Pandas DataFrame.ipynb
- Perform exploratory data analysis on the trail information table.  
- Examine trail info missingness and distributions, data visualizations, and perform data processing in preparation for creating the final table.

(5) Create Trail Info Final Table.py
- Clean and process the trail info table and insert the data into a final table in the database.

(6) Trail Recommender Final.py
- Read the final trail info table and implement the trail similarity recommender system.
- Calculate the similarity between each trail in the data and insert the data into a table in the database.
