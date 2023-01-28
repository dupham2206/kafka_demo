# Kafka demo
### Our kafka demo for lab's seminar
## Prerequisites
* pip
* npm
* nodejs
* [apache kafka](https://kafka.apache.org/quickstart)
 (**Note**: Author install kafka version 2.13-3.3.1)
## Install
Install requirement for python demo
```
cd kafka_video_demo/kafka_python/
pip install -r requirements.txt
```
Install node_modules for js demo
```
cd kafka_video_demo/kafka_js/
npm install
```
## Run simple python demo
#### To see topics with many partitions, you need to change number of partitions config in file **/kafka_2.13-3.3.1/config/server.properties** (default num.partitions=1 -> change to a number > 1)
### First we need to run kafka server and zookeeper
#### Terminal 1, run server
```
cd kafka_2.13-3.3.1/
bin/kafka-server-start.sh config/server.properties
```
#### Terminal 2, run zookeeper
```
cd kafka_2.13-3.3.1/
bin/zookeeper-server-start.sh config/zookeeper.properties
```
### Second, we run consumers and producers. You can run many consumers and producers to see how partition in Kafka and event streaming system work.
#### In terminal, run consumer
```
cd kafka_simple_python_demo/
python consumer_test.py
```
#### Run producer without key
```
cd kafka_simple_python_demo/
python producer_test.py
```
#### Run producer with key (you can change key in code and see the results)
```
cd kafka_simple_python_demo/
python producer_test_keymes.py 
``` 
## Run video demo python
### System: receive_image -> model_detection -> server nodejs
*  **receive image**: producer, write in python, read frame from video and send to topic *detection*
* **model_detection**: consumer of topic *detection*, producer of topic *represent*, read image from topic *detection* and extract information through model yolov5s and send it to topic *represent*
* **server nodejs**: read information from topic *represent*. Note that backend of nodejs is in pipeline, frontend is req-res model so not in pipeline.
####  First, you need to run kafka server and zookeeper
After that, run server nodejs:
```
cd kafka_video_demo/kafka_js/
node server.js
```
run model detection:
```
cd kafka_video_demo/kafka_python/
python model_detection.py
```
run receive image:
```
cd kafka_video_demo/kafka_python/
python receive_image.py
```
