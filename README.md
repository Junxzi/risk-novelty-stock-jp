# Risk Novelty Stock JP

## Setup

1. Copy `.env.example` to `.env` in the project root.

```bash
cp .env.example .env
```

2. Open `.env` and fill in your actual credentials for the variables listed below:

- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_PORT`
- `JQUANTS_USER`
- `JQUANTS_PASS`
- `JPX_API_KEY`

3. After populating `.env`, you can start the services or run the scripts.

```bash
docker-compose up
```

Or execute the Python scripts directly after activating your environment.

