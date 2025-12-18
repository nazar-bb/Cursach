import os
import time
import subprocess
import threading
import psycopg2
from psycopg2 import extras
import random
import pandas as pd
import matplotlib.pyplot as plt
from math import inf
from queue import Queue

DB_USER = "nazar"
DB_PASS_E1_E2_E3 = "pw123321"
DB_NAME = "cursach_db"
HOST = "127.0.0.1"

FAILURE_RATE_THRESHOLD = 0.05
P95_LATENCY_THRESHOLD_MS = 2000
MAX_RPS_TO_TEST = 1000

RPS_INCREMENT = 1
INITIAL_RPS = 1
TEST_DURATION_SEC = 120

DYNAMIC_ID_COUNT = 1000
ID_SETS = {}

SAMPLE_STATUS = 'delivered'
SAMPLE_DATE = '2018-01-01'

NUM_THREADS = 4

SCENARIOS = {
    "E4_FINAL": {
        "name": "4. Реплікація",
        "compose_file": "docker-compose-E4.yml",
        "db_port": 54324,
        "db_pass": DB_PASS_E1_E2_E3,
        "read_host": "127.0.0.1",
        "use_cache": False,
        "replica_ports": [54325, 54326, 54327, 54328],
    }
}
CURRENT_SCENARIO_KEY = "E4_FINAL"


def fetch_dynamic_ids(config):
    global ID_SETS
    try:
        conn = psycopg2.connect(
            host=config["read_host"],
            port=config["db_port"],
            user=DB_USER,
            password=config["db_pass"],
            dbname=DB_NAME,
            connect_timeout=5
        )
        print(f"    -> Завантаження ID з Master ({config['db_port']})...", flush=True)
        id_queries = {
            'clients': f"SELECT client_id FROM clients ORDER BY random() LIMIT {DYNAMIC_ID_COUNT};",
            'sellers': f"SELECT seller_id FROM sellers ORDER BY random() LIMIT {DYNAMIC_ID_COUNT};",
            'products': f"SELECT product_id FROM products ORDER BY random() LIMIT {DYNAMIC_ID_COUNT};"
        }
        ID_SETS = {}
        with conn.cursor() as cur:
            for key, sql in id_queries.items():
                cur.execute(sql)
                ID_SETS[key] = [row[0] for row in cur.fetchall()]
        conn.close()
        return True
    except Exception as e:
        print(f"[ПОМИЛКА ID] {e}", flush=True)
        return False

def get_random_query():
    query_type = random.choice(['Q1','Q2','Q3','Q4'])
    if query_type == 'Q1':
        client_id = random.choice(ID_SETS['clients'])
        sql = f"SELECT * FROM clients WHERE client_id = '{client_id}';"
        name = "Q1"
    elif query_type == 'Q2':
        seller_id = random.choice(ID_SETS['sellers'])
        sql = f"SELECT product_id, price FROM order_items WHERE seller_id = '{seller_id}';"
        name = "Q2"
    elif query_type == 'Q3':
        sql = f"SELECT o.client_id, oi.product_id FROM orders o JOIN order_items oi ON o.order_id = oi.order_id WHERE o.order_status = '{SAMPLE_STATUS}' LIMIT 100;"
        name = "Q3"
    else:
        sql = f"SELECT p.product_categoru, AVG(oi.price) FROM orders o JOIN order_items oi ON o.order_id = oi.order_id JOIN products p ON oi.product_id = p.product_id WHERE o.purchase_date >= '{SAMPLE_DATE}' GROUP BY p.product_categoru ORDER BY 2 DESC;"
        name = "Q4"
    return {"name": name, "sql": sql}

def docker_up(compose_file):
    print(f"\n--- ЗАПУСК КОНТЕЙНЕРІВ (E4) ---", flush=True)
    subprocess.run(["docker","compose","-f",compose_file,"up","-d"], check=True)
    time.sleep(30)

def docker_down(compose_file):
    print(f"\n--- ОЧИЩЕННЯ РЕСУРСІВ ---", flush=True)
    subprocess.run(["docker","compose","-f",compose_file,"down"])

def worker_thread_pool(config, task_queue, results_list):
    ports = config["replica_ports"]
    while True:
        try:
            query = task_queue.get(timeout=1)
        except:
            break
        port = random.choice(ports) # Балансування навантаження між репліками
        start_time = time.time()
        status = "Success"
        try:
            conn = psycopg2.connect(host=config["read_host"], port=port, user=DB_USER, password=config["db_pass"], dbname=DB_NAME, connect_timeout=5)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(query["sql"])
                cur.fetchall()
            conn.close()
        except:
            status = "Error"
        
        latency = (time.time() - start_time) * 1000
        results_list.append({"latency": latency, "status": status})
        task_queue.task_done()

def run_test_step(config, rps_target, results_list):
    total = rps_target * TEST_DURATION_SEC
    task_queue = Queue()
    for _ in range(total):
        task_queue.put(get_random_query())

    threads = []
    for _ in range(NUM_THREADS):
        t = threading.Thread(target=worker_thread_pool, args=(config, task_queue, results_list))
        t.start()
        threads.append(t)

    task_queue.join()
    for t in threads:
        t.join()

def run_scenario(scenario_key):
    config = SCENARIOS[scenario_key]
    docker_up(config["compose_file"])
    if not fetch_dynamic_ids(config):
        docker_down(config["compose_file"])
        return pd.DataFrame()

    current_rps = INITIAL_RPS
    metrics = []

    try:
        while current_rps <= MAX_RPS_TO_TEST:
            step_results = []
            print(f"    -> Крок RPS={current_rps}", flush=True)
            start_time = time.time()
            run_test_step(config, current_rps, step_results)
            duration = time.time() - start_time

            df = pd.DataFrame(step_results)
            success = df[df.status == "Success"]
            failure_rate = 1 - len(success) / len(df) if len(df) > 0 else 1
            p95 = success.latency.quantile(0.95) if not success.empty else inf

            print(f"       Час: {duration:.1f}с | P95: {p95:.1f}мс | Fail: {failure_rate:.1%}")

            metrics.append({
                "scenario": config["name"],
                "rps_target": current_rps,
                "success_rps": len(success) / duration,
                "p95_latency_ms": p95,
                "failure_rate": failure_rate
            })

            if duration > TEST_DURATION_SEC or failure_rate > FAILURE_RATE_THRESHOLD or p95 > P95_LATENCY_THRESHOLD_MS:
                break
            current_rps += RPS_INCREMENT
    finally:
        docker_down(config["compose_file"])
    return pd.DataFrame(metrics)


def visualize_comparison(df):
    print("\n--- ВІЗУАЛІЗАЦІЯ НАКОПИЧЕНИХ РЕЗУЛЬТАТІВ ---", flush=True)
    plt.figure(figsize=(12, 6))
    
    for run_num, group in df.groupby('run_number'):
        group = group[group['p95_latency_ms'] != inf]
        plt.plot(group['rps_target'], group['p95_latency_ms'], marker='o', label=f"Запуск #{run_num}")
    
    plt.title(f"Порівняння запусків (P95 Latency): {SCENARIOS[CURRENT_SCENARIO_KEY]['name']}")
    plt.xlabel('Цільовий RPS')
    plt.ylabel('Затримка P95 (мс)')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.show()

if __name__=="__main__":
    filename = "scenario_e4_metrics.csv"
    
    df_new = run_scenario(CURRENT_SCENARIO_KEY)

    if not df_new.empty:

        if os.path.exists(filename):
            df_existing = pd.read_csv(filename)
            run_id = df_existing['run_number'].max() + 1
        else:
            df_existing = pd.DataFrame()
            run_id = 1
        
        df_new['run_number'] = run_id
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        
        df_combined.to_csv(filename, index=False)
        print(f"-> Дані збережено (Запуск #{run_id})")

        visualize_comparison(df_combined)
    else:
        print("Тест не повернув результатів.")