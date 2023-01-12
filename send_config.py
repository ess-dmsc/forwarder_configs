import argparse
import json
import time

from confluent_kafka import Producer
from streaming_data_types.forwarder_config_update_rf5k import (
    serialise_rf5k,
    StreamInfo,
    Protocol,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)

parser = argparse.ArgumentParser()
required_args = parser.add_argument_group("required arguments")
required_args.add_argument(
    "-b", "--broker", type=str, help="the broker address", required=True
)

required_args.add_argument(
    "-t", "--topic", type=str, help="the configuration topic", required=True
)

required_args.add_argument(
    "-j",
    "--jsonfile",
    type=str,
    help="the json file containing the configuration",
    required=True,
)

args = parser.parse_args()

kafka_config = {"bootstrap.servers": args.broker}

with open(args.jsonfile) as file:
    raw_config = json.load(file)["streams"]

streams = []

for pv, schema, topic, access_type in raw_config:
    streams.append(
        StreamInfo(
            pv,
            schema,
            topic,
            Protocol.Protocol.CA if access_type == "ca" else Protocol.Protocol.PVA,
        )
    )

producer = Producer(**kafka_config)
producer.produce(args.topic, serialise_rf5k(UpdateType.REMOVEALL, []))
time.sleep(1)
producer.produce(args.topic, serialise_rf5k(UpdateType.ADD, streams))
producer.flush()
