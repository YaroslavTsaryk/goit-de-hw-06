from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
my_name = "YT"
bs_topic_name = f'{my_name}_building_sensors'
sensor_id=random.randint(1, 50)

for i in range(30):
    # Відправлення повідомлення в топік
    try:
        data = {
            "timestamp": time.time(),  # Часова мітка
            "sensor": sensor_id,
            "temperature": random.randint(20, 200),  # Випадкове значення,
            "humidity": random.randint(15, 85)  # Випадкове значення
        }
        producer.send(bs_topic_name, key=str(uuid.uuid4()), value=data)
        producer.flush()  # Очікування, поки всі повідомлення будуть відправлені
        print(f"Message {i} sent to topic '{bs_topic_name}' successfully.")
        print(f"{data = }")
        time.sleep(5)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Закриття producer