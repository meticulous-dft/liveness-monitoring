import logging
import os
import random
import string
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Dict

from bson.binary import Binary
from bson.decimal128 import Decimal128
from pymongo import ASCENDING
from pymongo.collection import Collection

from .monitoring import ensure_sharded_location_compound
from .rate_limiter import TokenBucket

try:
    import sentry_sdk  # type: ignore

    HAS_SENTRY = True
except Exception:
    HAS_SENTRY = False

# Optional realistic data generator
try:
    from faker import Faker  # type: ignore

    HAS_FAKER = True
except Exception:
    HAS_FAKER = False


@dataclass
class WorkloadConfig:
    db_name: str
    coll_name: str
    total_docs: int
    ops_per_sec: float
    workers: int
    op_mix: Dict[str, float]


# Fallback ISO country codes for geosharding (if Faker not available)
ISO_ALPHA2 = [
    "US",
    "CA",
    "GB",
    "DE",
    "FR",
    "IN",
    "JP",
    "CN",
    "BR",
    "AU",
    "SG",
    "NL",
    "SE",
    "CH",
    "IT",
    "ES",
    "MX",
    "KR",
    "ZA",
    "AE",
]

logger = logging.getLogger("atlas_liveness.workload")


class WorkloadRunner:
    def __init__(
        self,
        client,
        db_name: str,
        coll_name: str,
        total_docs: int,
        ops_per_sec: float,
        workers: int,
        op_mix: Dict[str, float],
        sentry_enabled: bool = False,
    ):
        self.client = client
        self.cfg = WorkloadConfig(
            db_name, coll_name, total_docs, ops_per_sec, workers, op_mix
        )
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()
        self._limiter = TokenBucket(
            rate_per_sec=max(ops_per_sec, 0.1), burst=max(ops_per_sec, 1.0)
        )
        self._sentry_enabled = sentry_enabled and HAS_SENTRY
        # Thread-local Faker instances (only if Faker is installed)
        self._tls = threading.local()

    def start(self):
        coll = self.client[self.cfg.db_name][self.cfg.coll_name]
        # Shard collection on {location: 1, _id: 'hashed'} when possible (mongos, permissions)
        try:
            ensure_sharded_location_compound(
                self.client,
                self.cfg.db_name,
                self.cfg.coll_name,
                "location",
                self._sentry_enabled,
            )
        except Exception:
            logger.debug("ensure_sharded_id_hashed: best-effort; continuing")
        self._prepare_collection(coll)
        logger.info("Starting %d worker threads", self.cfg.workers)
        for i in range(self.cfg.workers):
            t = threading.Thread(target=self._worker, name=f"worker-{i}", args=(coll,))
            t.daemon = True
            t.start()
            self._threads.append(t)
        # start periodic ping thread
        pt = threading.Thread(target=self._ping_loop, name="ping", daemon=True)
        pt.start()
        self._threads.append(pt)
        logger.info("Ping thread started")

    def stop(self):
        self._stop.set()
        self._limiter.stop()
        for t in self._threads:
            t.join(timeout=5.0)

    # --- internals ---

    def _prepare_collection(self, coll: Collection):
        try:
            # Index for key lookups
            coll.create_index([("k", ASCENDING)], name="k_1", background=True)
            # Preload dataset to target size (best-effort; skips if already >=)
            target = max(0, self.cfg.total_docs)
            existing = coll.estimated_document_count()
            to_insert = max(0, target - existing)
            if to_insert <= 0:
                logger.info(
                    "Dataset already sized: existing=%d target=%d", existing, target
                )
                return
            logger.info(
                "Preloading dataset: inserting %d docs (existing=%d target=%d)",
                to_insert,
                existing,
                target,
            )
            batch = []
            batch_size = 1000
            for i in range(to_insert):
                doc = self._make_doc(existing + i)
                batch.append(doc)
                if len(batch) >= batch_size:
                    coll.insert_many(batch, ordered=False)
                    batch.clear()
            if batch:
                coll.insert_many(batch, ordered=False)
            logger.info("Preload complete")
        except Exception as e:
            logger.exception("Collection prepare/preload failed")
            if self._sentry_enabled:
                try:
                    sentry_sdk.capture_exception(e)  # type: ignore
                except Exception:
                    pass

    def _get_faker(self):
        if not HAS_FAKER:
            return None
        fk = getattr(self._tls, "faker", None)
        if fk is None:
            try:
                fk = Faker()
                # Use a per-thread seed to avoid identical sequences
                fk.seed_instance(random.randrange(1, 1 << 30))
                self._tls.faker = fk
            except Exception:
                fk = None
        return fk

    def _make_doc(self, k: int) -> dict:
        fk = self._get_faker()
        loc_code = fk.country_code() if fk is not None else random.choice(ISO_ALPHA2)

        doc = {
            "k": int(k),
            "ts": time.time(),
            "n": 0,
            "location": loc_code,
        }
        if fk is not None:
            try:
                # Base profile nested object
                profile = {
                    "name": fk.name(),
                    "email": fk.free_email(),
                    "company": fk.company(),
                    "address": {
                        "street": fk.street_address(),
                        "city": fk.city(),
                        "state": fk.state_abbr(),
                        "postalCode": fk.postcode(),
                        "countryCode": loc_code,
                        "country": fk.country(),
                        "geo": {
                            "lat": float(fk.latitude()),
                            "lng": float(fk.longitude()),
                        },
                    },
                }
                # Arrays and varied shapes
                tags = fk.words(nb=random.randint(0, 6))
                phones = [fk.phone_number() for _ in range(random.randint(0, 3))]
                # Array of sub-documents
                orders = []
                for _ in range(random.randint(0, 3)):
                    items = [
                        {
                            "sku": fk.bothify(text="SKU-????-#####"),
                            "qty": random.randint(1, 5),
                            "price": Decimal128(
                                str(
                                    Decimal(random.uniform(5.0, 500.0)).quantize(
                                        Decimal("0.01"), rounding=ROUND_HALF_UP
                                    )
                                )
                            ),
                        }
                        for _ in range(random.randint(1, 3))
                    ]
                    total_dec = sum(
                        Decimal(it["qty"]) * it["price"].to_decimal() for it in items
                    )
                    orders.append(
                        {
                            "id": str(uuid.uuid4()),
                            "total": Decimal128(
                                str(
                                    (
                                        total_dec
                                        if isinstance(total_dec, Decimal)
                                        else Decimal(total_dec)
                                    ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                                )
                            ),
                            "items": items,
                            "placedAt": datetime.now(timezone.utc),
                        }
                    )
                # Optional fields create heterogeneity
                if random.random() < 0.5:
                    profile["job"] = {
                        "title": fk.job(),
                        "seniority": random.choice(["jr", "mid", "sr"]),
                    }
                if random.random() < 0.3:
                    profile["website"] = fk.url()
                if random.random() < 0.3:
                    birth_date = fk.date_of_birth(minimum_age=18, maximum_age=90)
                    profile["birthdate"] = datetime.combine(
                        birth_date, datetime.min.time()
                    ).replace(tzinfo=timezone.utc)
                # Preferences and flags
                preferences = {
                    "newsletter": fk.boolean(),
                    "categories": random.sample(
                        ["tech", "finance", "health", "travel", "food", "sports"],
                        k=random.randint(0, 4),
                    ),
                    "timezone": fk.timezone(),
                }
                metadata = {
                    "source": random.choice(["web", "mobile", "partner", "import"]),
                    "createdAt": datetime.now(timezone.utc),
                    "updatedAt": datetime.now(timezone.utc),
                }
                if random.random() < 0.2:
                    metadata["note"] = fk.sentence(nb_words=8)
                # Assemble with extra heterogeneity (binary, union types, mixed arrays)
                doc.update(
                    {
                        "profile": profile,
                        "phones": phones,
                        "tags": tags,
                        "orders": orders,
                        "preferences": preferences,
                        "metadata": metadata,
                        # Union type: sometimes a string, sometimes an object
                        "contact": (
                            fk.free_email()
                            if random.random() < 0.5
                            else {"email": fk.free_email(), "phone": fk.phone_number()}
                        ),
                        # Union numeric type: int or float rating
                        "rating": (
                            random.randint(1, 5)
                            if random.random() < 0.5
                            else round(random.uniform(1.0, 5.0), 2)
                        ),
                        # Union temporal type: datetime or epoch seconds
                        "lastSeen": (
                            datetime.now(timezone.utc)
                            if random.random() < 0.5
                            else int(time.time())
                        ),
                        # Optional binary blob
                        "avatar": (
                            Binary(os.urandom(random.randint(0, 512)))
                            if random.random() < 0.3
                            else None
                        ),
                        # Mixed-type attributes
                        "attrs": {
                            "a": random.choice(
                                [
                                    True,
                                    False,
                                    None,
                                    fk.word() if fk else "na",
                                    random.randint(0, 100),
                                ]
                            ),
                            "b": [random.randint(0, 5), (fk.word() if fk else "x")],
                        },
                    }
                )
            except Exception:
                # Fallback value if faker failed mid-flight
                doc["v"] = "".join(
                    random.choices(string.ascii_letters + string.digits, k=16)
                )
        else:
            doc["v"] = "".join(
                random.choices(string.ascii_letters + string.digits, k=16)
            )
        return doc

    def _choose_op(self) -> str:
        r = random.random()
        acc = 0.0
        for op, w in self.cfg.op_mix.items():
            acc += w
            if r <= acc:
                return op
        return next(iter(self.cfg.op_mix.keys()), "find")

    def _worker(self, coll: Collection):
        rng = random.Random(os.getpid() ^ int(time.time() * 1e6))
        while not self._stop.is_set():
            if not self._limiter.acquire(1.0):
                break
            op = self._choose_op()
            try:
                if op == "insert":
                    self._do_insert(coll, rng)
                elif op == "update":
                    self._do_update(coll, rng)
                else:
                    self._do_find(coll, rng)
            except Exception as e:
                logger.exception("Operation failed: op=%s", op)
                if self._sentry_enabled:
                    try:
                        sentry_sdk.capture_exception(e)  # type: ignore
                    except Exception:
                        pass
                # brief pause to avoid hot-looping on persistent errors
                time.sleep(0.05)

    def _ping_loop(self):
        while not self._stop.is_set():
            try:
                self.client.admin.command("ping")
            except Exception as e:
                logger.exception("Ping failed")
                if self._sentry_enabled:
                    try:
                        sentry_sdk.capture_exception(e)  # type: ignore
                    except Exception:
                        pass
            time.sleep(1.0)

    def _random_key(self, coll: Collection, rng: random.Random) -> int:
        # Estimate range from dataset size
        n = max(coll.estimated_document_count(), 1)
        return rng.randrange(0, int(n))

    def _random_location(self, rng: random.Random) -> str:
        # Pick a random location from our predefined set for shard targeting
        return rng.choice(ISO_ALPHA2)

    def _do_find(self, coll: Collection, rng: random.Random):
        k = self._random_key(coll, rng)
        location = self._random_location(rng)
        # Include location in query to target specific shard
        coll.find_one({"k": k, "location": location})

    def _do_insert(self, coll: Collection, rng: random.Random):
        k = self._random_key(coll, rng)
        coll.insert_one(self._make_doc(k))

    def _do_update(self, coll: Collection, rng: random.Random):
        k = self._random_key(coll, rng)
        location = self._random_location(rng)
        # Include location in query to target specific shard
        coll.update_one(
            {"k": k, "location": location},
            {"$inc": {"n": 1}, "$set": {"ts": time.time()}},
            upsert=True,
        )
