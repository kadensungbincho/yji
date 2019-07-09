from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType


spark = SparkSession.builder.getOrCreate()


SCHEMA = {
    "host_id": IntegerType,
    "host_name": StringType,
    "host_since": StringType,
    "host_response_rate": StringType,
    "host_listings_count": IntegerType,
    "host_total_listings_count": IntegerType,
    "neighbourhood": StringType,
    "neighbourhood_group_cleansed": StringType,
    "latitude": DoubleType,
    "longitude": DoubleType,
    "price": StringType,
    "weekly_price": StringType,
    "security_deposit": StringType,
    "cleaning_fee": StringType,
    "extra_people": StringType,
    "property_type": StringType,
    "room_type": StringType,
    "beds": IntegerType,
    "bathrooms": IntegerType,
    "accommodates": IntegerType,
    "amenities": StringType,
    "minimum_nights": IntegerType,
    "number_of_reviews_ltm":  IntegerType,
    "review_scores_rating": IntegerType,
    "reviews_per_month": DoubleType
}

df = spark.sql("""
SELECT host_id
    , host_name
    , host_since
    , host_response_rate
    , host_listings_count
    , host_total_listings_count
    , neighbourhood
    , neighbourhood_group_cleansed
    , latitude
    , longitude
    , price
    , weekly_price
    , security_deposit
    , cleaning_fee
    , property_type
    , room_type
    , beds
    , bathrooms
    , accommodates
    , amenities
    , extra_people
    , minimum_nights
    , number_of_reviews_ltm
    , review_scores_rating
    , reviews_per_month
FROM yji.listings
""")

import re
import pyspark.sql.functions as F

def remove_dollar_sign(x):
    if isinstance(x, str):
        sub = re.sub('[$,]+', '', x)
        reg = re.compile('\d+.\d')
        matched = reg.findall(sub)
        if matched:
            return matched[0]
    return ''
    
udf_remove_dollar_sign = F.udf(remove_dollar_sign, StringType())
    
price_columns = ['price', 'weekly_price', 'security_deposit', 'cleaning_fee', 'extra_people']
for col in price_columns:
    df = df.withColumn(col, udf_remove_dollar_sign(df[col]))

for k, v in SCHEMA.items():
    listings_filtered = listings_filtered.withColumn(k, listings_filtered[k].cast(v()))



