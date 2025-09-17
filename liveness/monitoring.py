import logging

try:
    from pymongo.monitoring import ServerHeartbeatListener, register
except Exception:
    # If monitoring API isn't available, provide no-op fallbacks
    ServerHeartbeatListener = object  # type: ignore

    def register(listener):  # type: ignore
        return None


try:
    import sentry_sdk  # type: ignore

    HAS_SENTRY = True
except Exception:
    HAS_SENTRY = False

logger = logging.getLogger("atlas_liveness.monitoring")


class _SentryHeartbeat(ServerHeartbeatListener):  # type: ignore
    def heartbeat_failed(self, event):
        # event has attributes: connection_id (host, port), reply, duration_micros, await_time_micros
        host = None
        try:
            host = getattr(event, "connection_id", None)
        except Exception:
            pass
        msg = f"MongoDB server heartbeat failed host={host} duration_us={getattr(event, 'duration_micros', None)}"
        logger.error(msg)
        if HAS_SENTRY:
            try:
                sentry_sdk.capture_message(msg, level="error")  # type: ignore
            except Exception:
                pass


def register_sentry_heartbeat_listener(enable: bool):
    if not enable:
        return
    try:
        register(_SentryHeartbeat())
        logger.info("Registered PyMongo heartbeat failure listener")
    except Exception:
        # Swallow; listener is optional
        logger.debug("Heartbeat listener registration not available")
        pass


def log_cluster_info(client, db_name: str, coll_name: str) -> None:
    """Log high-level cluster and collection info (works for sharded or replica set)."""
    try:
        # hello is preferred over isMaster
        hello = client.admin.command("hello")
    except Exception:
        try:
            hello = client.admin.command("isMaster")
        except Exception:
            hello = {}
    try:
        is_mongos = (
            bool(getattr(client, "is_mongos", False)) or hello.get("msg") == "isdbgrid"
        )
    except Exception:
        is_mongos = False

    if is_mongos:
        logger.info("Topology: Sharded (mongos)")
        try:
            shard_list = client.admin.command("listShards")
            shards = shard_list.get("shards", [])
            logger.info("Sharded cluster has %d shards", len(shards))
            for shard in shards:
                logger.info("  Shard: %s", shard.get("_id"))
        except Exception:
            pass
    elif "setName" in hello:
        logger.info("Topology: ReplicaSet name=%s", hello.get("setName"))
    else:
        logger.info("Topology: Standalone or unknown")

    # Best-effort collection stats to see if it's sharded and per-shard counts
    try:
        stats = client[db_name].command("collStats", coll_name)
        if stats.get("sharded"):
            logger.info("Collection %s.%s is sharded", db_name, coll_name)
            shards = stats.get("shards") or {}
            for shard_name, s in shards.items():
                logger.info(
                    "  Shard %s: count=%s size=%s",
                    shard_name,
                    s.get("count"),
                    s.get("size"),
                )
        else:
            logger.info("Collection %s.%s is not sharded", db_name, coll_name)
    except Exception:
        logger.debug("Unable to get collection stats")


def ensure_sharded_location_compound(
    client,
    db_name: str,
    coll_name: str,
    key_field: str = "location",
    sentry_enabled: bool = False,
) -> None:
    """Best-effort: shard the collection on a compound key for Atlas Global Clusters.
    Uses {<key_field>: 1, _id: 'hashed'} so writes route by location while distributing within a zone.
    No fallback will be attempted.
    """

    def _capture(e: Exception):
        if HAS_SENTRY and sentry_enabled:
            try:
                sentry_sdk.capture_exception(e)  # type: ignore
            except Exception:
                pass

    # Detect mongos
    try:
        try:
            hello = client.admin.command("hello")
        except Exception:
            hello = client.admin.command("isMaster")
        is_mongos = (
            bool(getattr(client, "is_mongos", False)) or hello.get("msg") == "isdbgrid"
        )
    except Exception:
        is_mongos = False

    if not is_mongos:
        logger.info("Not connected via mongos; skipping geosharding step")
        return

    # If already sharded, nothing to do
    try:
        stats = client[db_name].command("collStats", coll_name)
        if stats.get("sharded"):
            logger.info(
                "Collection %s.%s already sharded; skipping", db_name, coll_name
            )
            return
    except Exception:
        # collStats might fail if collection doesn't exist yet; proceed
        pass

    # Ensure collection exists
    try:
        client[db_name].create_collection(coll_name)
    except Exception:
        pass

    # Enable sharding on DB (idempotent)
    try:
        client.admin.command({"enableSharding": db_name})
    except Exception as e:
        logger.debug("enableSharding skipped: %s", e)

    # Try compound shard key first
    try:
        client.admin.command(
            {
                "shardCollection": f"{db_name}.{coll_name}",
                "key": {key_field: 1, "_id": "hashed"},
            }
        )
        logger.info(
            "Sharded %s.%s on {%s: 1, _id: 'hashed'}",
            db_name,
            coll_name,
            key_field,
        )
        return
    except Exception as e:
        logger.info(
            "Compound shard step failed; trying fallback to _id hashed: %s",
            e,
        )
        _capture(e)

        # Fallback to simple _id hashed sharding
        try:
            client.admin.command(
                {
                    "shardCollection": f"{db_name}.{coll_name}",
                    "key": {"_id": "hashed"},
                }
            )
            logger.info(
                "Fallback: Sharded %s.%s on {_id: 'hashed'}", db_name, coll_name
            )
            return
        except Exception as fallback_e:
            logger.info(
                "Fallback shard step also failed (likely already sharded or unauthorized): %s",
                fallback_e,
            )
            _capture(fallback_e)
            return
