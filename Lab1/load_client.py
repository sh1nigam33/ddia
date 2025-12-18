import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


def worker(client_id: int, base_url: str, requests_per_client: int):
    session = requests.Session()
    inc_url = base_url.rstrip("/") + "/inc"

    for i in range(requests_per_client):
        resp = session.get(inc_url)
        resp.raise_for_status()

    return client_id


def main():
    parser = argparse.ArgumentParser(description="Web-counter load client")
    parser.add_argument(
        "-c",
        "--clients",
        type=int,
        default=1,
        help="Number of parallel clients",
    )
    parser.add_argument(
        "-n",
        "--requests-per-client",
        type=int,
        default=10000,
        help="Number of /inc calls per client",
    )
    parser.add_argument(
        "-u",
        "--url",
        type=str,
        default="http://localhost:8080",
        help="Base URL of web app (without /inc)",
    )

    args = parser.parse_args()

    total_requests = args.clients * args.requests_per_client

    print(f"Starting load: {args.clients} clients x {args.requests_per_client} requests")
    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=args.clients) as executor:
        futures = [
            executor.submit(
                worker,
                client_id=i,
                base_url=args.url,
                requests_per_client=args.requests_per_client,
            )
            for i in range(args.clients)
        ]

        for f in as_completed(futures):

            f.result()

    end = time.perf_counter()
    elapsed = end - start

    # Перевіряємо кінцевий count
    count_resp = requests.get(args.url.rstrip("/") + "/count")
    count_resp.raise_for_status()
    count_value = count_resp.json().get("count")

    throughput = total_requests / elapsed if elapsed > 0 else 0.0

    print(f"Elapsed time: {elapsed:.4f} s")
    print(f"Total requests: {total_requests}")
    print(f"/count returned: {count_value}")
    print(f"Throughput: {throughput:.2f} req/s")


if __name__ == "__main__":
    main()
