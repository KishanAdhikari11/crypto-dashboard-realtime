# from kafka import KafkaConsumer
# import json
# import pandas as pd
# import os

# CSV_FILE_PATH = "data/crypto_data.csv"

# def save_to_csv(data):
#     file_exists = os.path.isfile(CSV_FILE_PATH)
#     df = pd.DataFrame(data)
#     df.to_csv(CSV_FILE_PATH, mode="a", index=False, header=not file_exists)
#     print(f"Saved {len(data)} records to CSV.")

# def run_consumer():
#     consumer = KafkaConsumer(
#         "crypto-topic",
#         bootstrap_servers="kafka:9092",
#         value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#     )
#     for message in consumer:
#         crypto_data = message.value
#         save_to_csv([crypto_data])

# if __name__ == "__main__":
#     run_consumer()
