import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic  # type: ignore
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError, TopicAlreadyExistsError # type: ignore
import json
import traceback
import os

# Topics used by the pipeline (producer -> flink-job -> prink job -> consumer)
PIPELINE_TOPICS = ['raw-topic', 'processed-topic', 'prink-topic']


def ensure_topics(bootstrap, topics):
    """Create the pipeline topics if they do not exist yet, so that downstream
    jobs can subscribe before the first record arrives. Replaces the former
    dummy '{}' messages, which polluted processed-topic and prink-topic."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    except KafkaError as e:
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Could not create admin client: {e}', flush=True)
        return
    try:
        existing = set(admin.list_topics())
        for name in topics:
            if name in existing:
                continue
            try:
                admin.create_topics([NewTopic(name=name, num_partitions=1, replication_factor=1)])
                print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Created topic {name}', flush=True)
            except TopicAlreadyExistsError:
                pass
            except KafkaError as e:
                print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Could not create topic {name}: {e}', flush=True)
    finally:
        admin.close()

# SLEEP_TIME = 0.002    # 500Hz frequency (1/500 seconds)
# SLEEP_TIME = 0.004    # 250Hz frequency (1/250 seconds)
# SLEEP_TIME = 0.01     # 100Hz frequency (1/100 seconds)
# SLEEP_TIME = 0.02     # 50Hz frequency (1/50 seconds)   !!! THIS IS WHERE THE PREDICTION MODEL IMPACTS LATENCY !!!
# SLEEP_TIME = 0.05     # 20Hz frequency (1/20 seconds)
# SLEEP_TIME = 0.1      # 10Hz frequency (1/10 seconds)
# SLEEP_TIME = 0.2      # 5Hz frequency (1/5 seconds)
# SLEEP_TIME = 0.5      # 2Hz frequency (1/2 seconds)
SLEEP_TIME = 1        # 1Hz frequency (1 second)  
# Adjust SLEEP_TIME as needed for your use case

def main():
    folder = 'output_json_files/'
    files = [f for f in os.listdir(folder) if f.endswith('.json')]

    # load news2 data from data.json files
    datasets = []
    message_count = 0
    for file in files:
        try:
            with open(os.path.join(folder, file), 'r') as f:
                data = json.load(f)
                datasets.append(data)
        except FileNotFoundError:
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} data.json file not found.', flush=True)
            return
        except json.JSONDecodeError as e:
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Error decoding JSON: {e}', flush=True)
            return
        except Exception as e:
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Unexpected error: {e}', flush=True)
            return

        
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:29092',
                max_block_ms=10000,  # Fail if unable to send after 10 seconds
                retries=0  # Do not retry indefinitely
            )
            topic = 'raw-topic'
            ensure_topics('kafka:29092', PIPELINE_TOPICS)

            while True:
                print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Sending messages...', flush=True)
                if message_count >= len(data):
                    message_count = 0
                for data in datasets:
                    # message = #message_count entry in data.json
                    message = data[message_count]
                    # Add a precise timestamp to the message
                    message_with_timestamp = dict(message)
                    message_with_timestamp['timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%S.') + f"{int(time.time() * 1000) % 1000:03d}Z"
                    loaded_message = json.dumps(message_with_timestamp).encode('utf-8')
                    print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Loaded message: {loaded_message}', flush=True)
                    try:
                        producer.send(topic, value=loaded_message)
                        print(f'Sent: {loaded_message} to Topic: {topic}', flush=True)
                    except KafkaError as e:
                        print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Failed to send message: {e}', flush=True)
                message_count += 1
                time.sleep(SLEEP_TIME)
        except NoBrokersAvailable:
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} No brokers available. Retrying...', flush=True)
            retries -= 1
            time.sleep(5)  # Wait before retrying
        except KafkaTimeoutError as e:
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Kafka timeout error: {e}. Retrying...', flush=True)
            retries -= 1
            time.sleep(5)  # Wait before retrying
        except Exception as e:
            print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Error in producer: {e}', flush=True)
            print(traceback.format_exc(), flush=True)
            break
    else:
        print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} Failed to connect to Kafka broker after multiple attempts.', flush=True)


if __name__ == '__main__':
    main()