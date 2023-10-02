# streaming-06-smart-smoker2
This repository is a continuation of streaming-05-smart-smoker and contains the consumers for the previously created bbq_producer in streaming-05-smart-smoker. 

## Prerequisites

1. Git
1. Python 3.7+ (3.11+ preferred)
1. VS Code Editor
1. VS Code Extension: Python
1. RabbitMQ installed and running locally

## Description - Producer
The producer bbq_producer.py reads the file smoker-temps.csv every 30 seconds and sends a message to each of 3 queues, "01-smoker", "02-food-A", and "03-food-B" with the timestamp and temperature of the respective smoker or food. In order to make it easier to read the temperatures, I used the print function rather than the log function. I also added the text "temp not recorded" if there was not a temperature for that element at that particular timestamp.

## Description - Consumers
There are a total of 3 consumers, one for each of the three queues - "01-smoker", "02-food-A", and "03-food-B". Smoker_consumer consumes the messages sent to the "01-smoker" queue and prints an alert if there is more than a 15 degree decrease in temperature in 2.5 minutes. FoodA_consumer and foodB_consumer consume the messages sent to "02-food-A" and "03-food-B" queues, respectively. Both print an alert if their respective temperatures change less than 1 degree in a 10 minute time period. 

## Screenshot of producer sending messages

![Alt text](https://github.com/bkargel/streaming-06-smart-smoker2/blob/main/bbq_producer.png?raw=true "bbq_producer messages sent")

## Screenshots of consumers receiving messages and printing alerts

![Alt text](https://github.com/bkargel/streaming-06-smart-smoker2/blob/main/smoker_consumer.png?raw=true "Smoker messages consumed, with alert")

![Alt text](https://github.com/bkargel/streaming-06-smart-smoker2/blob/main/foodA_consumer.png?raw=true "FoodA messages consumed, with alert")

![Alt text](

## Screenshot of queue admin

![Alt text](


