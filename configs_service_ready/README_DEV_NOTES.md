DEV NOTES:
- The project is a scaffold. To make it production-ready:
  * Implement persistent DB connection (async engine) and migrations.
  * Implement robust parsers for each protocol (vmess/vless/trojan/hysteria/etc).
  * Implement careful retry/backoff and exponential backoff when calling external services.
  * Add tests and CI pipeline.
  * Add monitoring and observability (metrics endpoint).
