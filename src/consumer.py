from kafka import KafkaConsumer
import json
import pandas as pd
from transform import CryptoDataTransformer
from utils.database import store_data_in_postgres


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
                df=pd.DataFrame(crypto_data)
                transformer=CryptoDataTransformer(df)
                transformed_data=transformer.transform_all()
                table_name = "cryptocurrency_data"
                db_url = "postgresql+psycopg2://postgres:postgres@localhost:5432/crypto"
                store_data_in_postgres(transformed_data, table_name, db_url)
                print(f"Processed and stored {len(transformed_data)} records into PostgreSQL.")
    except KeyboardInterrupt:
        print("\nConsumer stopped by user")
    finally:
        consumer.close()
        print("Consumer closed")

if __name__ == "__main__":
    run_consumer()