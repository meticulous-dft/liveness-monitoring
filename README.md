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
MONGODB_URI=<your-mongodb-connection-string>

# Optional (with defaults)
MONGO_DB=liveness                    # Database name
MONGO_COLL=probe                     # Collection name
TOTAL_DOCS=1000                      # Initial documents to load
OPS_PER_SEC=50                       # Operations per second
WORKERS=4                            # Number of worker threads
MAX_POOL_SIZE=50                     # MongoDB connection pool size
OP_MIX=find=70,insert=20,update=10   # Operation mix percentages
CLUSTER_TYPE=replica_set             # Cluster type (replica_set, sharded, geosharded)
SENTRY_DSN=<your-sentry-dsn>         # Sentry DSN for error monitoring
LOG_LEVEL=INFO                       # Logging level
```

### Command Line Arguments

All environment variables can be overridden via command line:

```bash
python main.py --help
```

## Cluster Types

The tool supports different MongoDB cluster configurations with appropriate sharding strategies:

### Replica Set (`replica_set`)

- **Default behavior**: No sharding is applied
- **Use case**: Single replica set deployments
- **Data distribution**: Documents are stored normally without sharding considerations

### Sharded (`sharded`)

- **Sharding strategy**: Uses `{_id: "hashed"}` shard key
- **Use case**: Regular sharded clusters where even distribution across shards is desired
- **Data distribution**: Documents are distributed evenly across shards based on hashed `_id`

### Geosharded (`geosharded`)

- **Sharding strategy**: Uses `{location: 1, _id: "hashed"}` compound shard key
- **Use case**: MongoDB Atlas Global Clusters with zone-based sharding
- **Data distribution**: Documents are routed by location field first, then distributed within zones
- **Location handling**: Uses deterministic location assignment for predictable shard targeting

You can specify the cluster type using:

```bash
# For replica set (default)
python main.py --cluster-type replica_set

# For regular sharded cluster
python main.py --cluster-type sharded

# For geosharded/global cluster
python main.py --cluster-type geosharded
```

## Usage

### Basic Usage

```bash
# Using environment variables
python main.py

# Using command line arguments
python main.py --uri "<your-mongodb-connection-string>" \
               --total-docs 5000 \
               --ops-per-sec 100 \
               --workers 8
```

### Advanced Configuration

```bash
# Custom operation mix with Sentry monitoring
python main.py --uri "<your-mongodb-connection-string>" \
               --op-mix "find=60,insert=30,update=10" \
               --sentry-dsn "<your-sentry-dsn>" \
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
