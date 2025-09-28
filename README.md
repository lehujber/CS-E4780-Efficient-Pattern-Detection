# CS-E4780-Efficient-Pattern-Detection
Course project for CS-E4780 at Aalto university

## Downloading the dataset
To download the dataset, run the following commands:

```
DATASET=202508
mkdir -p data/$DATASET
curl -L -o data/$DATASET/data.zip https://s3.amazonaws.com/tripdata/$DATASET-citibike-tripdata.zip
unzip data/$DATASET/data.zip
rm data/$DATASET/data.zip
```

## Running the application
In the root folder of the project, use docker compose:
```
docker compose up
```
Note that if you changed the default value of the downloaded DATASET, you also have to ensure that you mount the correct folder to the data-inserter.

## Running a dummy consumer
You can run a dummmy consumer via the following command:
```
docker run --rm --network cs-e4780-efficient-pattern-detection_kafka-network confluentinc/cp-kafka:7.5.0 kafka-console-consumer --bootstrap-server kafka:9092 --topic citibike-trips --group my-consumer-group --from-beginning
```
