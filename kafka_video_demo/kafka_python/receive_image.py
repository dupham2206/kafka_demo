from kafka import KafkaProducer
from kafka.errors import KafkaError
import cv2
from datetime import datetime

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

vidcap = cv2.VideoCapture('nguoianhHoangTung.mp4')
success,image = vidcap.read()
count = 0

start_date = datetime.now()

while success:
    success,image = vidcap.read()
    if success == False:
        break
    count += 1
    ret, buffer = cv2.imencode('.jpeg', image)
    future = producer.send('detection', buffer.tobytes())
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        log.exception()
        pass
    print ("frame: " + str(count) + ",topic name: " + str(record_metadata.topic) + ",partition: " + str(record_metadata.partition) + ",offset: " + str(record_metadata.offset))

end_date = datetime.now()
print(end_date - start_date)