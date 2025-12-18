import argparse
import random
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg
from psycopg import IsolationLevel
from psycopg.errors import SerializationFailure, DeadlockDetected

USER_ID = 1

def reset_row(dsn: str):
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE user_counter SET counter=0, version=0 WHERE user_id=%s", (USER_ID,))
        conn.commit()

def get_counter(dsn: str) -> int:
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT counter FROM user_counter WHERE user_id=%s", (USER_ID,))
            return cur.fetchone()[0]

def run_threads(threads: int, worker_fn):
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futures = [ex.submit(worker_fn, tid) for tid in range(threads)]
        for f in futures:
            f.result()

def backoff_sleep(attempt: int):
    base = min(0.002 * (2 ** attempt), 0.05)
    time.sleep(base + random.random() * 0.003)

def scenario(name, expected, fn):
    print(f"\n--- Running: {name} ---")
    t0 = time.perf_counter()
    stats = fn()
    dt = time.perf_counter() - t0
    ok = "OK" if stats["value"] == expected else "BAD"
    extra = []
    if "errors" in stats: extra.append(f"errors={stats['errors']}")
    if "retries" in stats: extra.append(f"retries={stats['retries']}")
    extra_s = (" | " + ", ".join(extra)) if extra else ""
    print(f"--- Done: {name} | time={dt:.3f}s | value={stats['value']} | expected={expected} => {ok}{extra_s}")
    return (name, dt, stats["value"], expected, ok, stats)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="postgresql://postgres:postgres@127.0.0.1:5432/counterdb")
    ap.add_argument("--threads", type=int, default=10)
    ap.add_argument("--perThread", type=int, default=10_000)
    ap.add_argument("--maxRetries", type=int, default=50)  # для serializable/optimistic
    args = ap.parse_args()

    THREADS = args.threads
    PER_THREAD = args.perThread
    EXPECTED = THREADS * PER_THREAD
    DSN = args.dsn

    print(f"Config: threads={THREADS}, perThread={PER_THREAD}, expected={EXPECTED}")
    print(f"DSN: {DSN}")

    results = []


    reset_row(DSN)
    def lost_update():
        def worker(_tid):
            with psycopg.connect(DSN, autocommit=True) as conn:
                with conn.cursor() as cur:
                    for _ in range(PER_THREAD):
                        with conn.transaction():
                            cur.execute("SELECT counter FROM user_counter WHERE user_id=%s", (USER_ID,))
                            c = cur.fetchone()[0]
                            c += 1
                            cur.execute("UPDATE user_counter SET counter=%s WHERE user_id=%s", (c, USER_ID))
        run_threads(THREADS, worker)
        return {"value": get_counter(DSN)}
    results.append(scenario("1) Lost-update (SELECT then UPDATE)", EXPECTED, lost_update))


    reset_row(DSN)
    def serializable_naive():
        errors = 0
        def worker(_tid):
            nonlocal errors
            with psycopg.connect(DSN, autocommit=True) as conn:
                conn.isolation_level = IsolationLevel.SERIALIZABLE
                with conn.cursor() as cur:
                    for _ in range(PER_THREAD):
                        try:
                            with conn.transaction():
                                cur.execute("SELECT counter FROM user_counter WHERE user_id=%s", (USER_ID,))
                                c = cur.fetchone()[0] + 1
                                cur.execute("UPDATE user_counter SET counter=%s WHERE user_id=%s", (c, USER_ID))
                        except (SerializationFailure, DeadlockDetected):
                            errors += 1
                            # транзакція вже відкотилась, просто рахуємо помилку і йдемо далі
        run_threads(THREADS, worker)
        return {"value": get_counter(DSN), "errors": errors}
    results.append(scenario("2a) SERIALIZABLE naive (no retry)", EXPECTED, serializable_naive))


    reset_row(DSN)
    def serializable_retry():
        errors = 0
        retries = 0
        def worker(_tid):
            nonlocal errors, retries
            with psycopg.connect(DSN, autocommit=True) as conn:
                conn.isolation_level = IsolationLevel.SERIALIZABLE
                with conn.cursor() as cur:
                    for _ in range(PER_THREAD):
                        attempt = 0
                        while True:
                            try:
                                with conn.transaction():
                                    cur.execute("SELECT counter FROM user_counter WHERE user_id=%s", (USER_ID,))
                                    c = cur.fetchone()[0] + 1
                                    cur.execute("UPDATE user_counter SET counter=%s WHERE user_id=%s", (c, USER_ID))
                                break
                            except (SerializationFailure, DeadlockDetected):
                                errors += 1
                                retries += 1
                                attempt += 1
                                if attempt > args.maxRetries:
                                    raise
                                backoff_sleep(attempt)
        run_threads(THREADS, worker)
        return {"value": get_counter(DSN), "errors": errors, "retries": retries}
    results.append(scenario("2b) SERIALIZABLE with retry (correct)", EXPECTED, serializable_retry))


    reset_row(DSN)
    def inplace_update():
        def worker(_tid):
            with psycopg.connect(DSN, autocommit=True) as conn:
                with conn.cursor() as cur:
                    for _ in range(PER_THREAD):
                        with conn.transaction():
                            cur.execute(
                                "UPDATE user_counter SET counter = counter + 1 WHERE user_id=%s",
                                (USER_ID,)
                            )
        run_threads(THREADS, worker)
        return {"value": get_counter(DSN)}
    results.append(scenario("3) In-place UPDATE counter=counter+1", EXPECTED, inplace_update))


    reset_row(DSN)
    def row_locking():
        def worker(_tid):
            with psycopg.connect(DSN, autocommit=True) as conn:
                with conn.cursor() as cur:
                    for _ in range(PER_THREAD):
                        with conn.transaction():
                            cur.execute(
                                "SELECT counter FROM user_counter WHERE user_id=%s FOR UPDATE",
                                (USER_ID,)
                            )
                            c = cur.fetchone()[0] + 1
                            cur.execute(
                                "UPDATE user_counter SET counter=%s WHERE user_id=%s",
                                (c, USER_ID)
                            )
        run_threads(THREADS, worker)
        return {"value": get_counter(DSN)}
    results.append(scenario("4) Row-level locking SELECT FOR UPDATE", EXPECTED, row_locking))


    reset_row(DSN)
    def optimistic_version():
        retries = 0
        def worker(_tid):
            nonlocal retries
            with psycopg.connect(DSN, autocommit=True) as conn:
                with conn.cursor() as cur:
                    for _ in range(PER_THREAD):
                        attempt = 0
                        while True:
                            with conn.transaction():
                                cur.execute(
                                    "SELECT counter, version FROM user_counter WHERE user_id=%s",
                                    (USER_ID,)
                                )
                                c, v = cur.fetchone()
                                c2, v2 = c + 1, v + 1
                                cur.execute(
                                    "UPDATE user_counter "
                                    "SET counter=%s, version=%s "
                                    "WHERE user_id=%s AND version=%s",
                                    (c2, v2, USER_ID, v)
                                )
                                if cur.rowcount == 1:
                                    break
                            attempt += 1
                            retries += 1
                            if attempt > args.maxRetries:
                                raise RuntimeError("Too many optimistic retries")
                            backoff_sleep(attempt)
        run_threads(THREADS, worker)
        return {"value": get_counter(DSN), "retries": retries}
    results.append(scenario("5) Optimistic (counter+version CAS)", EXPECTED, optimistic_version))

    # Підсумкова таблиця
    print("\n=== SUMMARY ===")
    print("Scenario".ljust(42), "time(s)".rjust(10), "value".rjust(10), "expected".rjust(10), "status".rjust(8))
    for (name, dt, val, exp, ok, _stats) in results:
        print(name.ljust(42), f"{dt:10.3f}", f"{val:10d}", f"{exp:10d}", ok.rjust(8))

if __name__ == "__main__":
    main()
