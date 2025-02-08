URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

python3 pipeline_gabriel.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --url=${URL} \
    --table_name=yellow_taxi_trips

docker build -t taxi_ingest:v001 .


#Para o docker rodar, a network precisa apontar para o host do container com o PG
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet" # main data

docker run -it --network=pg-network \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --url=${URL} \
    --table_name=yellow_taxi_trips

#lookup table
URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv" 
docker run -it --network=pg-network \
    taxi_ingest_orig:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --url=${URL} \
    --table_name=zones