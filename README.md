# MongoDB Atlas Liveness Monitoring

A synthetic MongoDB client application designed for liveness monitoring and performance testing of MongoDB Atlas clusters. This application generates realistic database workloads to help monitor cluster health, connectivity, and performance characteristics.

## Features

- **Configurable Workloads**: Supports mixed operations (find, insert, update) with customizable ratios
- **Rate Limiting**: Precise operations-per-second control across multiple worker threads
- **Sentry Integration**: Comprehensive error monitoring and alerting for database exceptions
- **Connection Pool Management**: Configurable connection pooling for optimal resource utilization
- **Cluster Topology Detection**: Automatic detection and logging of sharded vs replica set clusters
- **Graceful Shutdown**: Proper cleanup on SIGINT/SIGTERM signals
- **Environment Configuration**: Support for `.env` files and environment variables

## Installation

1. Clone the repository:

```bash
git clone https://github.com/meticulous-dft/liveness-monitoring.git
cd liveness-monitoring
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

### Environment Variables

Create a `.env` file or set the following environment variables:

```bash
# Required
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/

# Optional (with defaults)
MONGO_DB=liveness                    # Database name
MONGO_COLL=probe                     # Collection name
TOTAL_DOCS=1000                      # Initial documents to load
OPS_PER_SEC=50                       # Operations per second
WORKERS=4                            # Number of worker threads
MAX_POOL_SIZE=50                     # MongoDB connection pool size
OP_MIX=find=70,insert=20,update=10   # Operation mix percentages
SENTRY_DSN=                          # Sentry DSN for error monitoring
LOG_LEVEL=INFO                       # Logging level
```

### Command Line Arguments

All environment variables can be overridden via command line:

```bash
python main.py --help
```

## Usage

### Basic Usage

```bash
# Using environment variables
python main.py

# Using command line arguments
python main.py --uri "mongodb+srv://user:pass@cluster.mongodb.net/" \
               --total-docs 5000 \
               --ops-per-sec 100 \
               --workers 8
```

### Advanced Configuration

```bash
# Custom operation mix with Sentry monitoring
python main.py --uri "mongodb+srv://user:pass@cluster.mongodb.net/" \
               --op-mix "find=60,insert=30,update=10" \
               --sentry-dsn "https://your-sentry-dsn@sentry.io/project-id" \
               --max-pool-size 100
```

## Operation Types

The application supports three types of database operations:

1. **Find Operations**: Query documents by various criteria (ID, indexed fields, range queries)
2. **Insert Operations**: Insert new documents with realistic data using Faker
3. **Update Operations**: Update existing documents with new field values

## Monitoring and Observability

### Sentry Integration

When configured with a Sentry DSN, the application automatically captures:

- Database connection errors
- Query execution failures
- Network timeouts and connectivity issues
- Application crashes and exceptions

### Logging

Comprehensive logging includes:

- Cluster topology information
- Operation statistics and performance metrics
- Error details and stack traces
- Worker thread activity

### Heartbeat Monitoring

The application includes a heartbeat mechanism that detects:

- Connection pool exhaustion
- Network connectivity issues
- Database server unavailability

## Architecture

```
├── main.py                 # Application entry point and configuration
├── liveness/
│   ├── __init__.py
│   ├── monitoring.py       # Sentry integration and cluster info logging
│   ├── rate_limiter.py     # Token bucket rate limiting implementation
│   └── workload.py         # Workload generation and execution
├── requirements.txt        # Python dependencies
└── README.md              # This file
```

## Performance Considerations

- **Connection Pooling**: Adjust `MAX_POOL_SIZE` based on your cluster's connection limits
- **Worker Threads**: More workers can increase throughput but may cause connection pool contention
- **Rate Limiting**: The token bucket algorithm ensures smooth operation distribution
- **Memory Usage**: Large `TOTAL_DOCS` values will consume more memory for document caching

## Troubleshooting

### Common Issues

1. **Connection Timeouts**: Increase `serverSelectionTimeoutMS` or check network connectivity
2. **Authentication Errors**: Verify MongoDB URI credentials and database permissions
3. **Rate Limiting**: If operations seem slow, check the `OPS_PER_SEC` setting
4. **Memory Usage**: Reduce `TOTAL_DOCS` if experiencing high memory consumption

### Debug Mode

Enable debug logging for detailed operation information:

```bash
LOG_LEVEL=DEBUG python main.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
