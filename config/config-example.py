config = {
    "openai": {
        "groq": "groq"
    },
    "kafka": {
        "sasl.username": "KAFKA_CLUSTER_API_KEY",
        "sasl.password": "KAFKA_CLUSTER_API_SECRET",
        "bootstrap.servers": "KAFKA_CLUSTER_BOOTSTRAP_SERVER_URL:PORT",
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'session.timeout.ms': 50000
    }
}