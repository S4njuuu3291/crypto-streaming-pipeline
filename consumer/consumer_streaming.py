import logging

logging.basicConfig(level=logging.INFO)
import json

from confluent_kafka import Consumer
from sqlalchemy.orm import sessionmaker

from db import INSERT_TRADE_SQL, create_engine_table

if __name__ == "__main__":
    # Create database engine and session
    engine = create_engine_table()
    Session = sessionmaker(bind=engine)

    consumer_confs = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "crypto_consumer_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(consumer_confs)
    consumer.subscribe(["crypto_prices"])

    try:
        logging.info("Starting consumer...")
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue
            event = json.loads(msg.value().decode("utf-8"))
            session = Session()

            try:
                session.execute(
                    INSERT_TRADE_SQL,
                    {
                        "symbol": event["symbol"],
                        "price": event["price"],
                        "event_time": event["event_time"],
                        "processed_at": event["processed_at"],
                    },
                )
                session.commit()
                consumer.commit(asynchronous=False)
            except Exception as e:
                session.rollback()
                logging.error(f"Error inserting trade: {e}")
            finally:
                session.close()
    except KeyboardInterrupt:
        logging.INFO("Stopping consumer...")
    finally:
        consumer.close()
