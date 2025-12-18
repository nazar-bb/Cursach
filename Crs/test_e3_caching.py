import os
import time
import subprocess
import threading
import psycopg2
from psycopg2 import extras
import redis
import random
import pandas as pd
import matplotlib.pyplot as plt
from math import inf
from queue import Queue

# --- КОНФІГУРАЦІЯ ---
DB_USER = "nazar"
DB_PASS_E1_E2_E3 = "pw123321!"
DB_NAME = "cursach_db"
HOST = "127.0.0.1"

FAILURE_RATE_THRESHOLD = 0.05
P95_LATENCY_THRESHOLD_MS = 1000
MAX_RPS_TO_TEST = 1000 

RPS_INCREMENT = 1
INITIAL_RPS = 1
TEST_DURATION_SEC = 120 

CACHE_HIT_RATE = 0.8
DYNAMIC_ID_COUNT = 1000
ID_SETS = {}

SAMPLE_STATUS = 'delivered'
SAMPLE_DATE = '2018-01-01'

NUM_THREADS = 4

SCENARIOS = {
    "E3_CACHING": { 
        "name": "3. Оптимізована БД (Кешування)", 
        "compose_file": "docker-compose-E3.yml", 
        "db_port": 54323, 
        "db_pass": DB_PASS_E1_E2_E3,
        "read_host": "127.0.0.1",
        "use_cache": True,
        "redis_port": 6379,
    }
}
CURRENT_SCENARIO_KEY = "E3_CACHING" 


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
        print(f"    -> Завантаження {DYNAMIC_ID_COUNT} випадкових ID", flush=True)
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
                print(f"        Завантажено {len(ID_SETS[key])} ID для '{key}'.", flush=True)
        conn.close()
        
        if not ID_SETS.get('clients') or not ID_SETS.get('sellers'):
            raise Exception("Не вдалося завантажити достатньо ID.")
            
        print("    -> ID завантажено", flush=True)
        return True
    except psycopg2.Error as e:
        print(f"    [КРИТИЧНА ПОМИЛКА] Не вдалося завантажити ID: {e}", flush=True)
        return False
    except Exception as e:
        print(f"    [КРИТИЧНА ПОМИЛКА] {e}", flush=True)
        return False


def get_random_query():
    query_type = random.choice(['Q1', 'Q2', 'Q3', 'Q4']) 
    if query_type == 'Q1':
        client_id = random.choice(ID_SETS['clients'])
        sql = f"SELECT * FROM clients WHERE client_id = '{client_id}';"
        name = "Q1: SELECT за PK (Clients)"
    elif query_type == 'Q2':
        seller_id = random.choice(ID_SETS['sellers'])
        sql = f"SELECT product_id, price FROM order_items WHERE seller_id = '{seller_id}';"
        name = "Q2: SELECT за Індексом (Seller)"
    elif query_type == 'Q3':
        sql = f"""
            SELECT o.client_id, oi.product_id
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.order_status = '{SAMPLE_STATUS}'
            LIMIT 100;
        """
        name = "Q3: JOIN (Orders + Items)"
    else: # Q4
        sql = f"""
            SELECT p.product_categoru, AVG(oi.price) AS avg_price
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN products p ON oi.product_id = p.product_id
            WHERE o.purchase_date >= '{SAMPLE_DATE}'
            GROUP BY p.product_categoru
            ORDER BY avg_price DESC;
        """
        name = "Q4: Агрегація (JOIN + GROUP BY + Date Filter)"
    return {"name": name, "sql": sql}


def docker_up(compose_file):
    print(f"\n--- ЗАПУСК КОНТЕЙНЕРІВ: {compose_file} ---", flush=True)
    try:
        subprocess.run(
            ["docker", "compose", "-f", compose_file, "up", "-d", "--build"], 
            check=True, cwd=os.getcwd(), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        print("Контейнери успішно запущені. Очікування 30 сек для ініціалізації БД...", flush=True)
        time.sleep(15)
    except subprocess.CalledProcessError as e:
        print(f"Помилка при запуску Docker: {e.stderr.decode('utf-8')}", flush=True)
        exit(1)


def docker_down(compose_file):
    print(f"\n--- ОЧИЩЕННЯ РЕСУРСІВ: {compose_file} ---", flush=True)
    subprocess.run(["docker", "compose", "-f", compose_file, "down"], cwd=os.getcwd())


def worker_thread_pool(config, r, task_queue, results_list):
    while True:
        try:
            query = task_queue.get(timeout=1)
        except:
            break
            
        start_time = time.time()
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

            is_cache_miss = True
            if config.get("use_cache") and r:
                cache_key = hash(query["sql"])
                # Емуляція Cache Hit
                if random.random() < CACHE_HIT_RATE:
                    if r.exists(cache_key):
                        time.sleep(0.001)  # Відповідь кешу (1 мс)
                        is_cache_miss = False

            if is_cache_miss:
                with conn.cursor(cursor_factory=extras.DictCursor) as cur:
                    cur.execute(query["sql"])
                    cur.fetchall() 
                if config.get("use_cache") and r:
                    r.set(cache_key, "data", ex=300) 
            
        except psycopg2.Error:
            status = "DB Error"
        except Exception:
            status = "Other Error"
        finally:
            if conn:
                conn.close()
                
        latency = (time.time() - start_time) * 1000
        results_list.append({"latency": latency, "status": status, "query_name": query["name"]})
        task_queue.task_done()


def run_test_step(config, rps_target, results_list):
    r = None
    if config.get("use_cache"):
        try:
            r = redis.Redis(host=config["read_host"], port=config.get("redis_port", 6379), decode_responses=True)
            r.ping()
        except Exception:
            print("    [КРИТИЧНА ПОМИЛКА] Redis недоступний. Неможливо запустити тест E3.", flush=True)
            return
            
    if r:
        r.flushdb()

    total_requests = rps_target * TEST_DURATION_SEC
    task_queue = Queue()
    
    sample_query = get_random_query()
    print(f"    -> Приклад запиту: '{sample_query['name']}'", flush=True)
    print(f"        SQL: {sample_query['sql'].strip().replace('\n', ' ')}", flush=True)

    for _ in range(total_requests):
        task_queue.put(get_random_query())

    threads = []
    for _ in range(NUM_THREADS):
        t = threading.Thread(target=worker_thread_pool, args=(config, r, task_queue, results_list))
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
    all_metrics = []
    run_timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    try:
        while current_rps <= MAX_RPS_TO_TEST:
            step_results = []
            target_requests = current_rps * TEST_DURATION_SEC
            print(f"    -> Крок: RPS={current_rps}, Цільовий запитів: {target_requests}", flush=True)

            start_step_time = time.time() 
            run_test_step(config, current_rps, step_results)
            actual_duration = time.time() - start_step_time 
            
            if not step_results:
                print(" [ПОМИЛКА] Відсутні результати запитів.", flush=True)
                break

            df = pd.DataFrame(step_results)
            success_count = (df['status'] == 'Success').sum()
            total_requests = len(df)
            failure_rate = (total_requests - success_count) / total_requests if total_requests > 0 else inf
            
            latency_ms = df[df['status'] == 'Success']['latency'].dropna()
            p95_latency = latency_ms.quantile(0.95) if not latency_ms.empty else inf
            success_rps = success_count / actual_duration 

            metrics = {
                "scenario": config["name"],
                "run_id": run_timestamp,
                "rps_target": current_rps,
                "success_rps": success_rps,
                "p95_latency_ms": p95_latency,
                "failure_rate": failure_rate,
                "actual_duration_sec": actual_duration
            }
            all_metrics.append(metrics)

            print(f"    --- ЗВЕДЕНІ МЕТРИКИ (КРОК RPS={current_rps}) ---", flush=True)
            print(f"        Реальний час виконання: {actual_duration:.2f} сек", flush=True)
            print(f"        P95 Latency: {p95_latency:.2f} мс | Fail: {failure_rate:.2%}", flush=True)

            if actual_duration > TEST_DURATION_SEC + 5: # Невеликий запас
                print(f"    [АВАРІЙНА ЗУПИНКА] Перевищено час виконання.", flush=True)
                break
            
            if failure_rate > FAILURE_RATE_THRESHOLD or p95_latency > P95_LATENCY_THRESHOLD_MS:
                print(f"    [ПАДІННЯ] Перевищено порогові значення.", flush=True)
                break

            current_rps += RPS_INCREMENT
            
    finally:
        docker_down(config["compose_file"])
        
    return pd.DataFrame(all_metrics)


def visualize_results(current_df, csv_filename):
    # Завантаження історичних даних для накладання
    if os.path.exists(csv_filename):
        history_df = pd.read_csv(csv_filename)
        combined_df = pd.concat([history_df, current_df], ignore_index=True)
    else:
        combined_df = current_df

    scenario_name = current_df["scenario"].iloc[0]
    prefix = scenario_name.split(' ')[0].replace('.', '')
    print("\n--- ВІЗУАЛІЗАЦІЯ ПОРІВНЯННЯ ---", flush=True)

    # Графік P95 (Накладання)
    plt.figure(figsize=(12, 6))
    for run_id, group in combined_df.groupby("run_id"):
        plt.plot(group['rps_target'], group['p95_latency_ms'], marker='o', label=f'Run: {run_id}')
    
    plt.title(f'Порівняння P95 Часу Відгуку: {scenario_name}')
    plt.xlabel('Цільовий RPS')
    plt.ylabel('P95 (мс)')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.savefig(f'p95_comparison_{prefix}.png')
    plt.show()

    # Графік RPS (Накладання)
    plt.figure(figsize=(12, 6))
    for run_id, group in combined_df.groupby("run_id"):
        plt.plot(group['rps_target'], group['success_rps'], marker='s', linestyle='--', label=f'Run: {run_id}')
        
    plt.title(f'Порівняння Фактичного RPS: {scenario_name}')
    plt.xlabel('Цільовий RPS')
    plt.ylabel('Успішний RPS')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.savefig(f'rps_comparison_{prefix}.png')
    plt.show()


if __name__ == "__main__":
    csv_file = f"metrics_{CURRENT_SCENARIO_KEY}.csv"
    
    print(f"=======================================================", flush=True)
    print(f"ПОЧАТОК ТЕСТУ: {SCENARIOS[CURRENT_SCENARIO_KEY]['name']}")
    print(f"=======================================================", flush=True)
    
    try:
        subprocess.run(
            ["docker", "compose", "-f", SCENARIOS[CURRENT_SCENARIO_KEY]['compose_file'], "down"], 
            cwd=os.getcwd(), timeout=10, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except:
        pass

    df_metrics = run_scenario(CURRENT_SCENARIO_KEY)
    
    if not df_metrics.empty:
        visualize_results(df_metrics, csv_file)
        
        # Дозапис у CSV для збереження історії
        if os.path.exists(csv_file):
            df_metrics.to_csv(csv_file, mode='a', header=False, index=False)
        else:
            df_metrics.to_csv(csv_file, index=False)
    else:
        print("\n[ПОМИЛКА] Не вдалося отримати дані для аналізу.", flush=True)

    print(f"\nТЕСТ {CURRENT_SCENARIO_KEY} ЗАВЕРШЕНО.")