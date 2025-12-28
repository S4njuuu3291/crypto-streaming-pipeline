from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://kafka_user:kafka_pass@localhost:5433/kafka_streaming"

create_table = text("""
    CREATE TABLE IF NOT EXISTS crypto_prices (
        symbol TEXT NOT NULL,
        price NUMERIC NOT NULL,
        event_time BIGINT NOT NULL,
        processed_at BIGINT NOT NULL,
        PRIMARY KEY (symbol, event_time)
    );
""")


def create_engine_table():
    engine = create_engine(
        DB_URL,
        pool_size=20,
        max_overflow=0,
        pool_timeout=30,
    )
    # how to create table if not exist and skip if exist using sqlalchemy
    with engine.connect() as connection:
        connection.execute(create_table)
        connection.commit()
        connection.close()
    return engine


INSERT_TRADE_SQL = text("""
        INSERT INTO crypto_prices (symbol,price,event_time,processed_at)
        VALUES (:symbol, :price, :event_time, :processed_at)
        ON CONFLICT DO NOTHING
    """)
