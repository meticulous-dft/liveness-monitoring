#!/usr/bin/env python3
import argparse
import logging
import os
import signal
import sys
import threading

from pymongo import MongoClient

from liveness.monitoring import log_cluster_info, register_sentry_heartbeat_listener
from liveness.workload import ClusterType, WorkloadRunner

try:
    import sentry_sdk  # type: ignore

    HAS_SENTRY = True
except Exception:
    HAS_SENTRY = False


# Lightweight .env loader (env vars in real environment take precedence)
def load_env_file(path: str = ".env") -> None:
    try:
        if not os.path.exists(path):
            return
        with open(path, "r") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.lower().startswith("export "):
                    line = line[7:].lstrip()
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip()
                if (v.startswith('"') and v.endswith('"')) or (
                    v.startswith("'") and v.endswith("'")
                ):
                    v = v[1:-1]
                # do not override existing real env
                if k and k not in os.environ:
                    os.environ[k] = v
    except Exception as e:
        print(f"Warning: failed to load {path}: {e}", file=sys.stderr)


def configure_logging():
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s [%(threadName)s]: %(message)s",
        stream=sys.stdout,
    )


logger = logging.getLogger("atlas_liveness")


def parse_args():
    p = argparse.ArgumentParser(
        description="MongoDB Atlas liveness monitoring synthetic client"
    )
    p.add_argument(
        "--uri",
        default=os.getenv("MONGODB_URI"),
        help="MongoDB connection URI (or env MONGODB_URI)",
    )
    p.add_argument(
        "--db", default=os.getenv("MONGO_DB", "liveness"), help="Database name"
    )
    p.add_argument(
        "--coll", default=os.getenv("MONGO_COLL", "probe"), help="Collection name"
    )
    p.add_argument(
        "--total-docs",
        type=int,
        default=int(os.getenv("TOTAL_DOCS", "1000")),
        help="Target total docs to pre-load",
    )
    p.add_argument(
        "--ops-per-sec",
        type=float,
        default=float(os.getenv("OPS_PER_SEC", "50")),
        help="Total operations per second across all workers",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=int(os.getenv("WORKERS", "4")),
        help="Number of worker threads",
    )
    p.add_argument(
        "--max-pool-size",
        type=int,
        default=int(os.getenv("MAX_POOL_SIZE", "50")),
        help="MongoDB client maxPoolSize",
    )
    p.add_argument(
        "--op-mix",
        default=os.getenv("OP_MIX", "find=70,insert=20,update=10"),
        help="Operation mix percentages, e.g. find=70,insert=20,update=10",
    )
    p.add_argument(
        "--cluster-type",
        choices=["replica_set", "sharded", "geosharded"],
        default=os.getenv("CLUSTER_TYPE", "replica_set"),
        help="Cluster type for sharding strategy (replica_set, sharded, geosharded)",
    )
    p.add_argument(
        "--sentry-dsn",
        default=os.getenv("SENTRY_DSN", ""),
        help="Sentry DSN (optional)",
    )
    return p.parse_args()


def parse_op_mix(spec: str):
    parts = [kv.strip() for kv in spec.split(",") if kv.strip()]
    mix = {}
    for kv in parts:
        if "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        try:
            mix[k.strip()] = float(v)
        except ValueError:
            pass
    total = sum(mix.values()) or 1.0
    # normalize to weights
    return {k: v / total for k, v in mix.items()}


def init_sentry(dsn: str):
    if not dsn:
        return False
    if not HAS_SENTRY:
        print("sentry-sdk not installed; skipping Sentry init", file=sys.stderr)
        return False
    sentry_sdk.init(dsn=dsn, traces_sample_rate=0.0)
    return True


def main():
    # Load variables from .env before parsing args so env-based defaults are available
    load_env_file()
    configure_logging()
    args = parse_args()
    if not args.uri:
        print("--uri or MONGODB_URI must be provided", file=sys.stderr)
        return 2

    sentry_enabled = init_sentry(args.sentry_dsn)
    if sentry_enabled:
        logger.info("Sentry initialized")

    # Create Mongo client
    client = MongoClient(
        args.uri,
        appname="atlas-liveness-monitor",
        maxPoolSize=args.max_pool_size,
        serverSelectionTimeoutMS=10000,
        retryWrites=True,
    )

    # Log topology and collection sharding info (works for sharded clusters)
    try:
        log_cluster_info(client, args.db, args.coll)
    except Exception:
        logger.debug("Unable to log cluster info")

    # Workload
    op_mix = parse_op_mix(args.op_mix)
    if not op_mix:
        op_mix = {"find": 0.7, "insert": 0.2, "update": 0.1}

    # Convert cluster type string to enum
    cluster_type = ClusterType(args.cluster_type)

    # Register Sentry heartbeat listener to capture connectivity issues not surfaced as exceptions
    register_sentry_heartbeat_listener(sentry_enabled)

    runner = WorkloadRunner(
        client=client,
        db_name=args.db,
        coll_name=args.coll,
        total_docs=args.total_docs,
        ops_per_sec=args.ops_per_sec,
        workers=args.workers,
        op_mix=op_mix,
        cluster_type=cluster_type,
        sentry_enabled=sentry_enabled,
    )

    # Graceful shutdown
    stop_evt = threading.Event()

    def handle_sigterm(signum, frame):
        stop_evt.set()
        runner.stop()

    signal.signal(signal.SIGINT, handle_sigterm)
    signal.signal(signal.SIGTERM, handle_sigterm)

    runner.start()
    logger.info("Workload started. Press Ctrl+C to stop.")

    try:
        while not stop_evt.is_set():
            stop_evt.wait(1.0)
    finally:
        runner.stop()
        client.close()
        logger.info("Shutdown complete.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
