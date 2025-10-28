# MQTT + Node Consumer + MinIO

Services:
- Mosquitto (MQTT) with username/password auth
- MinIO object storage with bucket init
- Node.js consumer that subscribes to `trucks/+/metrics` and appends JSONL to `bucket/<truck_id>/<YYYY-MM-DD>/data.jsonl`

## Added: Airflow + PostGIS
- PostGIS (PostgreSQL with PostGIS extension) for Airflow metadata and optional geospatial storage
- Apache Airflow (LocalExecutor) with webserver and scheduler
- Prewired S3-compatible connection (`minio_s3`) to MinIO

## Prereqs
- Docker & Docker Compose

## Configure
Edit `.env` in project root (already created with sensible defaults).

## Run
```bash
docker compose up -d --build
```

## Test publish
```bash
docker compose exec mosquitto mosquitto_pub \
  -h mosquitto -p 1883 \
  -t trucks/TRUCK-001/metrics \
  -u "$MQTT_USERNAME" -P "$MQTT_PASSWORD" \
  -m '{"truck_id":"TRUCK-001","timestamp":"2025-10-24T12:34:56Z","gps":{"lat":-6.2002,"lon":106.8487},"metrics":{"co2_ppm":402.5,"no2_ppm":14.7,"so2_ppm":0.6,"pm25_ug_m3":22.1}}'
```

Objects will appear under:
```
minio://${MINIO_BUCKET}/TRUCK-001/2025-10-24/data.jsonl
```

Access MinIO Console at http://localhost:${MINIO_CONSOLE_PORT}

## Airflow
- Web UI: http://localhost:8080 (user/pass from `.env`)
- First-time init is handled by `airflow-init` service
- Example DAG: `airflow/dags/train_from_minio.py`

### Airflow connections and DB
- Airflow DB: `${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN}` (Postgres on `postgis` service)
- MinIO connection: `${AIRFLOW_CONN_MINIO_S3}` (S3-compatible)

### Notes
- Ensure `AIRFLOW__CORE__FERNET_KEY` is set to a real base64 key before production.


