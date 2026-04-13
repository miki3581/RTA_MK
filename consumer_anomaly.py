from kafka import KafkaConsumer
import json
from collections import defaultdict
import time

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='anomaly_detector_group',
    auto_offset_reset='latest'
)

user_history = defaultdict(list)

for message in consumer:
    tx = message.value
    user_id = tx.get('user_id')
    current_time = time.time() 
    
    if not user_id:
        continue

    user_history[user_id].append(current_time)
    
    user_history[user_id] = [t for t in user_history[user_id] if current_time - t <= 60]
    
    if len(user_history[user_id]) > 3:
        print(f"Alert: Wykryto anomalię prędkości")
        print(f"Użytkownik: {user_id}")
        print(f"Liczba transakcji w ostatniej minucie: {len(user_history[user_id])}")
        print(f"Ostatnia kwota: {tx.get('amount')}")
        print("-" * 40)