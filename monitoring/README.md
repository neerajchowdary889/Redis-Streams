# Redis Streams Monitoring Setup

This directory contains the complete monitoring setup for Redis Streams MQ using Prometheus and Grafana.

## Quick Start

1. **Start all services:**
   ```bash
   ./RDS.sh start
   ```

2. **Start only monitoring services:**
   ```bash
   ./RDS.sh monitoring
   ```

3. **Open Grafana dashboard:**
   ```bash
   ./RDS.sh grafana
   ```
   - URL: http://localhost:3000
   - Login: admin / admin123

4. **Open Prometheus metrics:**
   ```bash
   ./RDS.sh prometheus
   ```
   - URL: http://localhost:9090

## Services

### Prometheus (Port 9090)
- Collects metrics from Redis Streams MQ health server
- Scrapes metrics every 10 seconds
- Stores data for 200 hours
- Access: http://localhost:9090

### Grafana (Port 3000)
- Pre-configured dashboard for Redis Streams monitoring
- Auto-provisioned with Prometheus datasource
- Login: admin / admin123
- Access: http://localhost:3000

### Redis Insight (Port 5540)
- Redis database management and monitoring
- Access: http://localhost:5540

## Dashboard Features

The Grafana dashboard includes:

1. **Message Throughput** - Real-time publish/consume rates
2. **Stream Lengths** - Current length of each Redis stream
3. **Processing Performance** - Average processing times per topic
4. **Error Rates** - Publish and processing error rates
5. **Connection Health** - Connection errors and reconnections
6. **Service Uptime** - Overall service availability
7. **Total Message Counts** - Cumulative message statistics

## Configuration Files

- `prometheus.yml` - Prometheus scraping configuration
- `grafana/provisioning/datasources/prometheus.yml` - Grafana datasource config
- `grafana/provisioning/dashboards/dashboard.yml` - Dashboard provisioning
- `grafana/dashboards/redis-streams-dashboard.json` - Main dashboard definition

## Metrics Exposed

The Redis Streams MQ service exposes the following Prometheus metrics:

- `redis_streams_messages_published_total` - Total messages published per topic
- `redis_streams_messages_processed_total` - Total messages processed per topic
- `redis_streams_publish_errors_total` - Total publish errors per topic
- `redis_streams_processing_errors_total` - Total processing errors per topic
- `redis_streams_stream_length` - Current stream length per topic
- `redis_streams_avg_processing_time_ms` - Average processing time per topic
- `redis_streams_uptime_seconds` - Service uptime
- `redis_streams_connection_errors_total` - Total connection errors
- `redis_streams_reconnections_total` - Total reconnections

## Troubleshooting

1. **Grafana not loading dashboard:**
   - Check if Prometheus is running: `docker ps | grep prometheus`
   - Verify datasource connection in Grafana

2. **No metrics in Prometheus:**
   - Ensure Redis Streams MQ is running on port 8083
   - Check Prometheus targets: http://localhost:9090/targets

3. **Dashboard not auto-loaded:**
   - Check Grafana logs: `docker logs grafana`
   - Verify dashboard file permissions

## Customization

To modify the dashboard:
1. Edit `grafana/dashboards/redis-streams-dashboard.json`
2. Restart Grafana: `docker restart grafana`
3. Or import the updated JSON in Grafana UI

To add new metrics:
1. Update the health server in `Health/Health.go`
2. Add corresponding panels in the dashboard JSON
3. Restart services
