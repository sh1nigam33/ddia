import time
import argparse
import multiprocessing as mp

from pymongo import MongoClient, ReturnDocument
from pymongo.errors import AutoReconnect, NotPrimaryError, ConnectionFailure, NetworkTimeout
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern

URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0&retryWrites=false"

def worker(proc_id: int, iters: int, wc_mode: str):
    client = MongoClient(URI, serverSelectionTimeoutMS=5000)
    db = client["lab4"]

    if wc_mode == "majority":
        coll = db.get_collection("likes").with_options(
            write_concern=WriteConcern(w="majority"),
        )
    else:
        coll = db.get_collection("likes").with_options(
            write_concern=WriteConcern(w=1),
        )

    # ініціалізація документа (один раз; якщо паралельно — нічого страшного)
    try:
        coll.find_one_and_update(
            {"_id": "post1"},
            {"$setOnInsert": {"likes": 0}},
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
    except Exception:
        pass

    for i in range(iters):
        # простий retry-цикл на випадок elections / reconnect
        while True:
            try:
                coll.find_one_and_update(
                    {"_id": "post1"},
                    {"$inc": {"likes": 1}},
                    upsert=True,
                    return_document=ReturnDocument.BEFORE
                )
                break
            except (AutoReconnect, NotPrimaryError, ConnectionFailure, NetworkTimeout):
                time.sleep(0.01)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", type=int, default=10)
    ap.add_argument("--iters", type=int, default=10000)
    ap.add_argument("--wc", choices=["1", "majority"], default="1")
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    client = MongoClient(URI, serverSelectionTimeoutMS=5000)
    db = client["lab4"]

    # reset counter
    if args.reset:
        db["likes"].delete_many({})
        db["likes"].insert_one({"_id": "post1", "likes": 0})

    start = time.perf_counter()

    procs = []
    for p in range(args.clients):
        pr = mp.Process(target=worker, args=(p, args.iters, args.wc))
        pr.start()
        procs.append(pr)

    for pr in procs:
        pr.join()

    elapsed = time.perf_counter() - start

    # majority read для перевірки “що реально закомічено”
    rc_coll = db.get_collection("likes").with_options(read_concern=ReadConcern("majority"))
    doc = rc_coll.find_one({"_id": "post1"})

    expected = args.clients * args.iters
    print(f"writeConcern={args.wc} | clients={args.clients} iters={args.iters}")
    print(f"time_sec={elapsed:.3f}")
    print(f"final_likes(readConcern=majority)={doc['likes'] if doc else None} | expected={expected}")

if __name__ == "__main__":
    main()
