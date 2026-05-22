import json
import time
import random
import boto3

from generar_post import generar_post

STREAM_NAME = "social-trends-stream"

kinesis = boto3.client(
    "kinesis",
    region_name="us-east-1"
)

REDES = ["X", "Reddit", "Instagram"]

while True:
    red = random.choice(REDES)

    post = generar_post(red)

    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(post),
        PartitionKey=post["usuario"]
    )

    print(f"Sent -> {red}")

    time.sleep(1)