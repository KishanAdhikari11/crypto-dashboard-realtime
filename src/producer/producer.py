from kafka import KafkaProducer
import json
import time
import requests



def fetch_crypto_data():
    url = "https://api.coinpaprika.com/v1/tickers"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return [
            {
                "id": coin["id"],
                "name": coin["name"],
                "symbol": coin["symbol"],
                "rank": coin["rank"],
                "total_supply": coin["total_supply"],
                "max_supply": coin["max_supply"],
                "price": coin["quotes"]["USD"]["price"],
                "volume_24h": coin["quotes"]["USD"]["volume_24h"],
                "market_cap": coin["quotes"]["USD"]["market_cap"],
                "percent_change_24h": coin["quotes"]["USD"]["percent_change_24h"],
                "ath_price": coin["quotes"]["USD"]["ath_price"],
                "percent_from_price_ath": coin["quotes"]["USD"]["percent_from_price_ath"],
                "last_updated": coin["last_updated"],
            }
            for coin in data
        ]
    else:
        print(f"Error fetching data: {response.status_code}")
        return []

def run_producer():
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
        crypto_data = fetch_crypto_data()
        producer.send('crypto-topic', crypto_data)
        print(f"Sent: {crypto_data}")
        time.sleep(5)

if __name__ == "__main__":
    run_producer()

