# Risk Novelty Stock JP

## Setup

1. Copy `.env.example` to `.env` in the project root.

```bash
cp .env.example .env
```

2. Edit `.env` and replace the placeholder values with your real credentials:

- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_PORT`
- `JQUANTS_USER`
- `JQUANTS_PASS`
- `JPX_API_KEY`

3. After updating `.env`, start the services or run the scripts.

```bash
docker-compose up
```

Or run the Python scripts directly after activating your environment.

