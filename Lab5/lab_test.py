from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import threading
import time

KEYSPACE = 'ks_rf3'
TABLE = 'likes'
ID = 1
INCREMENTS_PER_CLIENT = 10000
CLIENTS = 10

def run_client(consistency_level, client_id):
    cluster = Cluster(['127.0.0.1'], port=9042) 
    session = cluster.connect(KEYSPACE)
    
    query = SimpleStatement(
        f"UPDATE {TABLE} SET count = count + 1 WHERE id = {ID}",
        consistency_level=consistency_level
    )
    
    for _ in range(INCREMENTS_PER_CLIENT):
        try:
            session.execute(query)
        except Exception as e:
            print(f"Error client {client_id}: {e}")
            
    cluster.shutdown()

def run_test(consistency_level_obj, test_name):
    print(f"--- Starting {test_name} ---")
    
    
    threads = []
    start_time = time.time()
    
    for i in range(CLIENTS):
        t = threading.Thread(target=run_client, args=(consistency_level_obj, i))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.2f} seconds")

    # Перевірка результату
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect(KEYSPACE)
    row = session.execute(f"SELECT count FROM {TABLE} WHERE id = {ID}").one()
    print(f"Final Count: {row.count}")
    cluster.shutdown()

# Запуск
if __name__ == "__main__":
    # Тест 1: CL ONE
    run_test(ConsistencyLevel.ONE, "Consistency Level: ONE")
    
    print("\n")
    
    # Тест 2: CL QUORUM
    run_test(ConsistencyLevel.QUORUM, "Consistency Level: QUORUM")
