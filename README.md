# YJI : the BI service for the hosts (190708 ~ 190712)
- Stack : Python, Pyspark, SQL, (React, Meteor, Docker)
- Service : AWS EMR, Glue, S3, (lambda, API Gateway)

- TODO:
    - 1 : Data Preprocessing with Pyspark(HDBScan, H3), AWS EMR Command, Save to DB
        - H3 : Add 100m, 500m, 1000m, 2000m Count
        - ~DBScan : Try DBScan and Label, add region competitors Count~ 
        - Set up DB
    - 2 : Set up API Service with Python, Docker

    - 3 : Web Service (Customized Market Analysis, Broad Overview - https://github.com/keplergl/kepler.gl)
    - 4 : Wrapup & Add more

### Dataset
    - [Inside Airbnb - New York City, New York, United States](http://insideairbnb.com/get-the-data.html)

### How to use
1. Download file through ./download_data.sh

