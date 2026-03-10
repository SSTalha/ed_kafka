from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):

    KAFKA_BROKER_URL: str
    KAFKA_CONSUMER_GROUP_ID: str
    KAFKA_TOPIC: str
    KAFKA_DEAD_LETTER_TOPIC: str = "raw-ip-events-failed"

    MONGO_URI: str
    MONGO_DB_NAME: str

    REDIS_URL: str

    BLOOM_FILTER_KEY: str = "processed_ips"
    BLOOM_FILTER_CAPACITY: int = 10_000_000   # 10M unique IPs (~12MB RAM)
    BLOOM_FILTER_ERROR_RATE: float = 0.01     # 1% false positive rate

    BATCH_SIZE: int = 500
    BATCH_TIMEOUT_SECONDS: int = 60

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True
    )


settings = Settings()