import re

import pyspark.sql.functions as F
from h3 import h3
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


def remove_sign(x):
    if isinstance(x, str):
        sub = re.sub('[%$,]+', '', x)
        reg = re.compile('\d+.\d')
        matched = reg.findall(sub)
        if matched:
            return matched[0]
    return ''
    
  
udf_remove_dollar_sign = F.udf(remove_sign, StringType())
    
prep_columns = ['host_response_rate', 'price', 'weekly_price', 'security_deposit', 'cleaning_fee', 'extra_people']
for col in prep_columns:
    df = df.withColumn(col, udf_remove_dollar_sign(df[col]))

for k, v in SCHEMA.items():
    df = df.withColumn(k, df[k].cast(v()))


def geo_to_h3(lat, lon, resolution):
    if  isinstance(lat, float) & isinstance(lon, float):
        return h3.geo_to_h3(lat, lon, resolution)
    return None


udf_geo_to_h3 = F.udf(geo_to_h3, StringType())

# 4, 22
# 6, 3
# 7, 1.2
# 8, 0.5
for i in [4, 6, 7, 8]:
    df = df.withColumn(f'h3_r{i}', udf_geo_to_h3(df.latitude, df.longitude, F.lit(i)))


df.createOrReplaceTempView('source')


STATS_COLS = ['host_response_rate', 'host_listings_count', 'host_total_listings_count', 'price', 'weekly_price', 'security_deposit', 'cleaning_fee', 'beds', 'bathrooms', 'accommodates', 'extra_people', 'minimum_nights', 'number_of_reviews_ltm', 'review_scores_rating', 'reviews_per_month']
def get_columns(func, delimeter, key):
    sentences = [f"{func}({col}) as {col}_{func}_{key}" for col in STATS_COLS]
    return delimeter.join(sentences)


def get_agg_df(key, suffix):
    df = spark.sql(f"""
    SELECT
        {key}
        , property_type
        , room_type
        , {get_columns('avg', ', ', suffix)}
    FROM source
    GROUP BY 1
    """)
    return df

ngbg_df = get_agg_df('neighbourhood_group_cleansed', 'ngbg')
ngb_df = get_agg_df('neighbourhood', 'ngb')
h3_r3_df = get_agg_df('h3_r3', 'h3_r3')
h3_r6_df = get_agg_df('h3_r6', 'h3_r6')
h3_r7_df = get_agg_df('h3_r7', 'h3_r7')
h3_r7_df = get_agg_df('h3_r8', 'h3_r8')