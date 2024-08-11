# Real-Time Activity Prediction

This project uses various sensor data to instantly predict whether there is any movement in a room and displays the result on the screen.

## Dataset

The dataset used in this project can be found on Kaggle: [Smart Building System Dataset](https://www.kaggle.com/datasets/ranakrc/smart-building-system).

## Architecture

The architecture of the system is depicted below:

![architecture](https://github.com/user-attachments/assets/dd938cb1-4571-4fda-9b4d-8e1c02183fa1)

## Initialize Docker Environment

To start the Docker environment, run the following command:

```bash
docker-compose up -d
```

## Manipulating Data and Saving The Model

Sensor data from different rooms is merged and stored as parquet files. The entire dataset is split into training and testing data. The test data is sent to Kafka using [this data generator repository](https://github.com/erkansirin78/data-generator).

Run the following scripts to preprocess the data and train the model:

python scripts/preprocess.py
then
python scripts/model.py

preprocess.py and model.py run just one time. 

Predicting and Consuming 

Spark Streaming is used in order to read test data from kafka and make predictions. Predictions are saved into a postgresql table. 

spark-submit --master local \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0 \
scripts/consumer.py

Web UI

Predictions are sent to flask web server and printed on the browser.

python scripts/server/app.py









