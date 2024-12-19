from kafka import KafkaConsumer
import json
import pandas as pd
import os

CSV_FILE_PATH = "data/crypto_data.csv"

def ensure_data_directory():
    os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)

def save_to_csv(data):

    if not isinstance(data, list):
        data = [data]

    df = pd.DataFrame(data)

    ensure_data_directory()
  
    file_exists = os.path.isfile(CSV_FILE_PATH)

    df.to_csv(CSV_FILE_PATH, mode="a", index=False, header=not file_exists)
    print(f"Saved {len(data)} records to CSV at {CSV_FILE_PATH}")

def run_consumer():
    consumer = KafkaConsumer(
        "crypto-topic",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset='earliest'  
    )
    
    print("Consumer started. Waiting for messages...")
    
    try:
        for message in consumer:
            crypto_data = message.value
            if crypto_data: 
                save_to_csv(crypto_data)
    except KeyboardInterrupt:
        print("\nConsumer stopped by user")
    finally:
        consumer.close()
        print("Consumer closed")

if __name__ == "__main__":
    run_consumer()