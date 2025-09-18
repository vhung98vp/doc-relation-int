# Document Relation Integration

This project links documents and entities (person, company) using Kafka and Elasticsearch. It consumes messages from Kafka, processes document and entity relations, and outputs results to another Kafka topic.

## Project Structure

```
requirements.txt
Dockerfile
.env.example
src/
    app.py
    config.json
    config.py
    es.py
    kafka.py
    pattern.py
    utils.py
```

## Features

- Kafka consumer/producer for document processing
- Elasticsearch integration for entity lookup and document indexing
- Pattern-based ID classification and filtering
- Relation message construction for linking entities and documents

## Setup

1. **Install dependencies:**

    ```sh
    pip install -r requirements.txt
    ```

2. **Configure environment variables:**

    - Copy [`src/.env`](src/.env) or [`.env.example`](.env.example) and update values as needed.

3. **Start the application:**

    ```sh
    python src/app.py
    ```

## Configuration

- Kafka and Elasticsearch settings are managed in [`src/config.py`](src/config.py).
- Search fields and relation properties are defined in [`src/config.json`](src/config.json).

## License

MIT License

---

**Note:** Ensure your Kafka and Elasticsearch services are running and accessible with the configured credentials.