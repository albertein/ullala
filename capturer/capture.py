from __future__ import print_function

import io
import sys
import logging
import calendar
from os import getenv
from time import sleep
from datetime import datetime

import avro.io
import avro.schema
from SimpleCV import Camera
from kafka import KafkaProducer

def get_timestamp():
    timestamp = datetime.now()
    return '{0}:{1}:{2}'.format(timestamp.hour, timestamp.minute, timestamp.second)

def run_capturer(kafka_hosts, fps=24):
    producer = KafkaProducer(bootstrap_servers=kafka_hosts)
    cam = Camera()
    while True:
        img = cam.getImage()
        img.drawText(get_timestamp(), fontsize=160)
        img.save('tmp.jpg')
        with open('tmp.jpg', mode='rb') as file:
            content = file.read()
        producer.send('CAMERA_FEED', pack_image(content))
        print('Got an image')
        sleep(0.4)

def pack_image(image):
    schema_string = """
    {
        "namespace": "ullala",
        "type": "record",
        "name": "Image",
        "fields": [
            {"name": "image", "type": "bytes"},
            {"name": "capture_timestamp",  "type": "int"}
        ]
    }
    """
    schema = avro.schema.parse(schema_string)
    capture_timestamp = calendar.timegm(datetime.utcnow().utctimetuple())

    writer = avro.io.DatumWriter(schema)

    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    
    writer.write({"image": image, "capture_timestamp": capture_timestamp}, encoder)

    return bytes_writer.getvalue()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    kafka_hosts = getenv('KAFKA_HOSTS')
    if not kafka_hosts:
        print('You need to set $KAFKA_HOSTS environment variable', file=sys.stderr)
        exit(1)
    run_capturer(kafka_hosts, fps=24)
