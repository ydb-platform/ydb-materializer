## Monitoring stack (Prometheus + Grafana)

1) Enable metrics in the Materializer config:
    ```
    <entry key="metrics.enabled">true</entry>
    <entry key="metrics.port">9090</entry>
    ```

2) Start the stack:
    ```
    cd monitoring
    docker compose up -d
    ```

3) Open Grafana:

`http://localhost:3000` (admin / admin)

Notes:
- Prometheus scrapes `host.docker.internal:9090` by default. If the
  Materializer runs in a container, update `monitoring/prometheus.yml`
  to point to that container service name and port.
