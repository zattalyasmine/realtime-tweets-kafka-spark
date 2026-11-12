# producer.py
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Tweets simulés réalistes
fake_tweets = [
    "I love this new product, it's awesome and great!",
    "This service is terrible, I hate it so much",
    "The weather today is good and beautiful",
    "Slow internet connection, this is bad",
    "Amazing experience, I love this app",
    "Worst day ever, everything is terrible",
    "Great customer support, really awesome team",
    "This update is bad, hate the new design",
    "Loving the new features, absolutely great",
    "The product broke after one day, terrible quality",
    "So happy today, everything is awesome",
    "Bad experience at the store, hate waiting",
]

print("🚀 Producteur Kafka démarré — envoi de tweets simulés...")

while True:
    tweet = {
        "id": random.randint(1000, 9999),
        "text": random.choice(fake_tweets),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.send('tweets', tweet)
    print(f"✅ Tweet envoyé : {tweet['text'][:50]}...")
    time.sleep(2)  # 1 tweet toutes les 2 secondes