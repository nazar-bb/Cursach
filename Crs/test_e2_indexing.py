import os
import time
import subprocess
import threading
import psycopg2
import random
import pandas as pd
import matplotlib.pyplot as plt
from math import inf
from queue import Queue

DB_USER = "nazar"
DB_PASS_E1_E2_E3 = "pw123321!"
DB_NAME = "cursach_db"

FAILURE_RATE_THRESHOLD = 0.05
P95_LATENCY_THRESHOLD_MS = 1000
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
    "E2_INDEXING": {
        "name": "2. Оптимізована БД (Індексація)",
        "compose_file": "docker-compose-E2.yml",
        "db_port": 54322,
        "db_pass": DB_PASS_E1_E2_E3,
        "read_host": "127.0.0.1",
    }
}
CURRENT_SCENARIO_KEY = "E2_INDEXING"

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

        print(f"    -> Завантаження {DYNAMIC_ID_COUNT} випадкових ID...", flush=True)

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
                print(f"        {key}: {len(ID_SETS[key])} ID", flush=True)

        conn.close()

        if not ID_SETS['clients'] or not ID_SETS['sellers']:
            raise Exception("Недостатньо ID")

        print("    -> ID успішно завантажено.", flush=True)
        return True

    except Exception as e:
        print(f"    [КРИТИЧНА ПОМИЛКА] {e}", flush=True)
        return False

def apply_optimization_indices(config):
    indices = [
        "CREATE INDEX IF NOT EXISTS idx_orders_purchase_date_id ON orders (purchase_date, order_id);",
        "CREATE INDEX IF NOT EXISTS idx_oi_product_price ON order_items (product_id, price, order_id);",
        "CREATE INDEX IF NOT EXISTS idx_products_category ON products (product_categoru);"
    ]

    try:
        conn = psycopg2.connect(
            host=config["read_host"],
            port=config["db_port"],
            user=DB_USER,
            password=config["db_pass"],
            dbname=DB_NAME,
            connect_timeout=5
        )
        conn.autocommit = True

        print("\n--- ЗАСТОСУВАННЯ ОПТИМІЗОВАНИХ ІНДЕКСІВ (E2) ---", flush=True)

        with conn.cursor() as cur:
            for sql in indices:
                cur.execute(sql)
                print(f"    -> Перевірено індекс: {sql.split('ON')[0]}...", flush=True)

        conn.close()
        return True

    except Exception as e:
        print(f"    [КРИТИЧНА ПОМИЛКА ІНДЕКСАЦІЇ] {e}", flush=True)
        return False

def get_random_query():
    q = random.choice(['Q1','Q2','Q3','Q4'])

    if q == 'Q1':
        cid = random.choice(ID_SETS['clients'])
        return {
            "name": "Q1: SELECT за PK (Clients)",
            "sql": f"SELECT * FROM clients WHERE client_id = '{cid}';"
        }

    if q == 'Q2':
        sid = random.choice(ID_SETS['sellers'])
        return {
            "name": "Q2: SELECT за індексом (Seller)",
            "sql": f"SELECT product_id, price FROM order_items WHERE seller_id = '{sid}';"
        }

    if q == 'Q3':
        return {
            "name": "Q3: JOIN Orders + Items",
            "sql": f"""
                SELECT o.client_id, oi.product_id
                FROM orders o
                JOIN order_items oi ON o.order_id = oi.order_id
                WHERE o.order_status = '{SAMPLE_STATUS}'
                LIMIT 100;
            """
        }

    return {
        "name": "Q4: Агрегація (JOIN + GROUP BY)",
        "sql": f"""
            SELECT p.product_categoru, AVG(oi.price) AS avg_price
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN products p ON oi.product_id = p.product_id
            WHERE o.purchase_date >= '{SAMPLE_DATE}'
            GROUP BY p.product_categoru
            ORDER BY avg_price DESC;
        """
    }

def docker_up(compose_file):
    print(f"\n--- ЗАПУСК КОНТЕЙНЕРІВ: {compose_file} ---", flush=True)
    subprocess.run(
        ["docker","compose","-f",compose_file,"up","-d","--build"],
        check=True
    )
    time.sleep(15)

def docker_down(compose_file):
    print(f"\n--- ОЧИЩЕННЯ РЕСУРСІВ: {compose_file} ---", flush=True)
    subprocess.run(["docker","compose","-f",compose_file,"down"])

def worker_thread_pool(config, task_queue, results_list):
    while True:
        try:
            query = task_queue.get(timeout=1)
        except:
            break

        start = time.time()
        status = "Success"
        conn = None

        try:
            conn = psycopg2.connect(
                host=config["read_host"],
                port=config["db_port"],
                user=DB_USER,
                password=config["db_pass"],
                dbname=DB_NAME,
                connect_timeout=5
            )
            conn.autocommit = True

            with conn.cursor() as cur:
                cur.execute(query["sql"])
                cur.fetchall()

        except psycopg2.Error:
            status = "DB Error"
        except Exception:
            status = "Other Error"
        finally:
            if conn:
                conn.close()

        latency = (time.time() - start) * 1000
        results_list.append({
            "latency": latency,
            "status": status,
            "query_name": query["name"]
        })
        task_queue.task_done()

def run_test_step(config, rps_target, results_list):
    total_requests = rps_target * TEST_DURATION_SEC
    task_queue = Queue()

    sample = get_random_query()
    print(f"    -> Приклад запиту: {sample['name']}", flush=True)

    for _ in range(total_requests):
        task_queue.put(get_random_query())

    threads = []
    for _ in range(NUM_THREADS):
        t = threading.Thread(
            target=worker_thread_pool,
            args=(config, task_queue, results_list)
        )
        t.start()
        threads.append(t)

    task_queue.join()
    for t in threads:
        t.join()

def run_scenario(key):
    config = SCENARIOS[key]
    docker_up(config["compose_file"])

    if not fetch_dynamic_ids(config):
        docker_down(config["compose_file"])
        return pd.DataFrame()

    if not apply_optimization_indices(config):
        docker_down(config["compose_file"])
        return pd.DataFrame()

    current_rps = INITIAL_RPS
    all_metrics = []

    try:
        while current_rps <= MAX_RPS_TO_TEST:
            print(f"\n--- КРОК RPS = {current_rps} ---", flush=True)

            step_results = []
            total_requests = current_rps * TEST_DURATION_SEC 
            
            start = time.time()
            run_test_step(config, current_rps, step_results)
            duration = time.time() - start

            print(f"    Фактичний час виконання: {duration:.2f} сек", flush=True)
            print(f"    Запитів виконано: {len(step_results)} (з {total_requests} цільових)", flush=True)
  
            df = pd.DataFrame(step_results)
            success = df[df.status == "Success"]

            failure_rate = 1 - len(success) / len(df)
            p95 = success.latency.quantile(0.95) if not success.empty else inf
            success_rps = len(success) / duration

            print(f"    Success RPS: {success_rps:.1f}", flush=True)
            print(f"    P95 latency: {p95:.2f} ms", flush=True)
            print(f"    Failure rate: {failure_rate:.2%}", flush=True)

            all_metrics.append({
                "scenario": config["name"],
                "rps_target": current_rps,
                "success_rps": success_rps,
                "p95_latency_ms": p95,
                "failure_rate": failure_rate
            })

            if duration > TEST_DURATION_SEC:
                print(f"    [STOP] Аварійна зупинка: Час виконання ({duration:.2f} с) перевищив цільовий {TEST_DURATION_SEC} с.", flush=True)
                break
            
            if failure_rate > FAILURE_RATE_THRESHOLD or p95 > P95_LATENCY_THRESHOLD_MS:
                print("    [STOP] Порогові значення перевищено", flush=True)
                break

            current_rps += RPS_INCREMENT

    finally:
        docker_down(config["compose_file"])

    return pd.DataFrame(all_metrics)


def visualize_results(df):
    print("\n--- ВІЗУАЛІЗАЦІЯ ПОРІВНЯННЯ ЗАПУСКІВ ---", flush=True)
    plt.figure(figsize=(10,6))
    
    # Групуємо дані за номером запуску, щоб намалювати кілька ліній
    for run_num, group in df.groupby('run_number'):
        group = group[group["p95_latency_ms"] != inf]
        plt.plot(group["rps_target"], group["p95_latency_ms"], marker='o', label=f"Запуск #{run_num}")
    
    plt.xlabel("Цільовий RPS")
    plt.ylabel("P95 latency (ms)")
    plt.title(f"Порівняння запусків: {SCENARIOS[CURRENT_SCENARIO_KEY]['name']}")
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.show()

if __name__ == "__main__":
    filename = f"metrics_{CURRENT_SCENARIO_KEY}.csv"
    
    print(f"\nСТАРТ ТЕСТУ: {SCENARIOS[CURRENT_SCENARIO_KEY]['name']}")
    df_new = run_scenario(CURRENT_SCENARIO_KEY)

    if not df_new.empty:
        # Логіка накопичення результатів
        if os.path.exists(filename):
            df_old = pd.read_csv(filename)
            last_run = df_old["run_number"].max()
            df_new["run_number"] = last_run + 1
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_new["run_number"] = 1
            df_combined = df_new
        
        # Збереження у CSV
        df_combined.to_csv(filename, index=False)
        print(f"-> Результати збережено у {filename}")
        
        # Візуалізація всіх запусків разом
        visualize_results(df_combined)
    else:
        print("Тест завершився без результатів.")