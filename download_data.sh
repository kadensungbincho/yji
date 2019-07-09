
listings="http://data.insideairbnb.com/united-states/ny/new-york-city/2019-06-02/data/listings.csv.gz"
calendar="http://data.insideairbnb.com/united-states/ny/new-york-city/2019-06-02/data/calendar.csv.gz"
review="http://data.insideairbnb.com/united-states/ny/new-york-city/2019-06-02/data/reviews.csv.gz"
neighbor="http://data.insideairbnb.com/united-states/ny/new-york-city/2019-06-02/visualisations/neighbourhoods.csv"
neighbor_geo="http://data.insideairbnb.com/united-states/ny/new-york-city/2019-06-02/visualisations/neighbourhoods.geojson"

data_dir="data"
targets=($listings $calendar $review $neighbor $neighbor_geo)

mkdir -p $data_dir

# for i in "${targets[@]}";
# do
#     name=$(echo $i | rev | cut -d/ -f1 | rev)
#     echo downloading $name ...
#     wget $i -O $data_dir/$name
# done

# echo gunzipping...
# for file in "$data_dir"/*; do
#   if [ ${file: -3} == ".gz" ]; then
#     gunzip $file
#   fi
# done


# echo parqueting...
# for file in "$data_dir"/*; do
#   if [ ${file: -4} == ".csv" ]; then
#     python ./make_parquet.py $file
#   fi
# done