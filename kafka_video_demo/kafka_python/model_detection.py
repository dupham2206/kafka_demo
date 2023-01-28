from PIL import Image, ImageDraw 
from io import BytesIO
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError
import numpy as np
import cv2
import torch

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

consumer = KafkaConsumer('detection',
                        group_id='detection',
                        bootstrap_servers=['localhost:9092'])

model = torch.hub.load('ultralytics/yolov5', 'yolov5s', pretrained=True)

for message in consumer:
    stream = BytesIO(message.value)
    image_cv2 = cv2.imdecode(np.frombuffer(message.value,'u1') , cv2.IMREAD_UNCHANGED)
    stream.close()
    # image.show()
    results = model(image_cv2)
    result_string = str(results.pandas())
    print(result_string)
    future = producer.send('represent', bytes(result_string, "utf-8"))
    
