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
DB_PASS_E1_E2_E3 = "pw123321!"
DB_NAME = "cursach_db"
HOST = "127.0.0.1"

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
    "E1_BASE": {
        "name": "1.(Без патернів)",
        "compose_file": "docker-compose-E1.yml",
        "db_port": 54321,
        "db_pass": DB_PASS_E1_E2_E3,
        "read_host": "127.0.0.1",
    }
}

CURRENT_SCENARIO_KEY = "E1_BASE"

#Id
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

        print("Завантаження 1000 випадкових ID", flush=True)

        id_queries = {
            'clients': f"SELECT client_id FROM clients ORDER BY random() LIMIT {DYNAMIC_ID_COUNT};",
            'sellers': f"SELECT seller_id FROM sellers ORDER BY random() LIMIT {DYNAMIC_ID_COUNT};",
            'products': f"SELECT product_id FROM products ORDER BY random() LIMIT {DYNAMIC_ID_COUNT};"
        }

        ID_SETS = {}
        with conn.cursor(cursor_factory=extras.DictCursor) as cur:
            for key, sql in id_queries.items():
                cur.execute(sql)
                ID_SETS[key] = [row[0] for row in cur.fetchall()]
                print(f"  -> {key}: {len(ID_SETS[key])} ID", flush=True)

        conn.close()

        if not ID_SETS['clients'] or not ID_SETS['sellers']:
            raise Exception("Недостатньо ID")

        print("ID успішно завантажено", flush=True)
        return True

    except Exception as e:
        print(f"[ПОМИЛКА] {e}", flush=True)
        return False

#Запити
def get_random_query():
    q = random.choice(['Q1','Q2','Q3','Q4'])

    if q == 'Q1':
        cid = random.choice(ID_SETS['clients'])
        return {
            "name": "Q1: SELECT за PK (clients)",
            "sql": f"SELECT * FROM clients WHERE client_id = '{cid}';"
        }

    if q == 'Q2':
        sid = random.choice(ID_SETS['sellers'])
        return {
            "name": "Q2: SELECT за індексом (seller)",
            "sql": f"SELECT product_id, price FROM order_items WHERE seller_id = '{sid}';"
        }

    if q == 'Q3':
        return {
            "name": "Q3: JOIN orders + items",
            "sql": f"""
                SELECT o.client_id, oi.product_id
                FROM orders o
                JOIN order_items oi ON o.order_id = oi.order_id
                WHERE o.order_status = '{SAMPLE_STATUS}'
                LIMIT 100;
            """
        }

    return {
        "name": "Q4: JOIN + GROUP BY",
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
    print(f"\nЗАПУСК КОНТЕЙНЕРІВ: {compose_file}", flush=True)
    subprocess.run(
        ["docker","compose","-f",compose_file,"up","-d","--build"],
        check=True
    )
    time.sleep(15)

def docker_down(compose_file):
    print(f"\nОЧИЩЕННЯ РЕСУРСІВ: {compose_file}", flush=True)
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

            with conn.cursor(cursor_factory=extras.DictCursor) as cur:
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

#КРОК ТЕСТУ
def run_test_step(config, rps_target, results_list):
    total = rps_target * TEST_DURATION_SEC
    task_queue = Queue()

    sample = get_random_query()
    print(f"    -> Приклад: {sample['name']}", flush=True)

    for _ in range(total):
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

#СЦЕНАРЙ
def run_scenario(key):
    config = SCENARIOS[key]
    docker_up(config["compose_file"])

    if not fetch_dynamic_ids(config):
        docker_down(config["compose_file"])
        return pd.DataFrame()

    current_rps = INITIAL_RPS
    metrics = []

    try:
        while current_rps <= MAX_RPS_TO_TEST:
            print(f"\n--- RPS = {current_rps} ---", flush=True)
            step_results = []
            
            total_requests = current_rps * TEST_DURATION_SEC

            start = time.time()
            run_test_step(config, current_rps, step_results)
            duration = time.time() - start
            
            # --- ВИВЕДЕННЯ ІНФОРМАЦІЇ ПРО КРОК ---
            print(f"    Запитів виконано: {len(step_results)} (цільових: {total_requests})", flush=True)
            print(f"    Фактичний час виконання кроку: {duration:.2f} сек", flush=True)

            df = pd.DataFrame(step_results)
            success = df[df.status == "Success"]
            failure_rate = 1 - len(success) / len(df)
            p95 = success.latency.quantile(0.95) if not success.empty else inf
            success_rps = len(success) / duration

            print(f"    Success RPS: {success_rps:.1f}", flush=True)
            print(f"    P95 latency: {p95:.2f} ms", flush=True)
            print(f"    Failure rate: {failure_rate:.2%}", flush=True)

            metrics.append({
                "scenario": config["name"],
                "rps_target": current_rps,
                "success_rps": success_rps,
                "p95_latency_ms": p95,
                "failure_rate": failure_rate
            })

            # --- ДОДАТКОВА ПЕРЕВІРКА: АВАРІЙНА ЗУПИНКА ЗА ЧАСОМ ---
            if duration > TEST_DURATION_SEC:
                print(f"    [STOP] Аварійна зупинка: Час виконання ({duration:.2f} с) перевищив цільовий {TEST_DURATION_SEC} с.", flush=True)
                break
            
            # --- ОСНОВНА ПЕРЕВІРКА: ПОРОГИ ПРОДУКТИВНОСТІ ---
            if failure_rate > FAILURE_RATE_THRESHOLD or p95 > P95_LATENCY_THRESHOLD_MS:
                print("    [STOP] Пороги перевищено", flush=True)
                break

            current_rps += RPS_INCREMENT

    finally:
        docker_down(config["compose_file"])

    return pd.DataFrame(metrics)

# ОНОВЛЕНА ФУНКЦІЯ ДЛЯ ВІЗУАЛІЗАЦІЇ КІЛЬКОХ ЗАПУСКІВ
def visualize_results_comparison(final_df):
    
    scenario_name = final_df["scenario"].iloc[0]
    scenario_key = CURRENT_SCENARIO_KEY
    print("\n--- ВІЗУАЛІЗАЦІЯ ПОРІВНЯННЯ ЗАПУСКІВ ---", flush=True)

    # 1. Графік P95 Latency по різних запусках
    plt.figure(figsize=(12, 6))
    # Групуємо за НОМЕРОМ ЗАПУСКУ (run_number)
    grouped = final_df.groupby('run_number') 
    
    for run_num, group in grouped:
        group = group[group['p95_latency_ms']!=inf]
        # Кожна група (запуск) - це окрема лінія на графіку
        plt.plot(group['rps_target'], group['p95_latency_ms'], marker='o', label=f"Запуск #{run_num}")
        
    plt.title(f'P95 Час Відгуку: {scenario_name} (Порівняння Запусків)')
    plt.xlabel('Цільовий RPS')
    plt.ylabel('P95 мс')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(title="Тестові запуски")
    plt.tight_layout()
    
    plt.savefig(f'p95_latency_all_runs_{scenario_key}.png')
    plt.show() # 

    # 2. Графік Максимальний Успішний RPS (показуємо лише останній результат)
    max_rps_data = []
    # Знаходимо максимальний успішний RPS для кожного запуску
    for run_num, group in grouped:
        last_success = group[group['p95_latency_ms']!=inf].iloc[-1] if not group[group['p95_latency_ms']!=inf].empty else {'scenario':scenario_name,'success_rps':0}
        max_rps_data.append({'run_number': run_num,'max_success_rps':last_success['success_rps']})
        
    max_rps_df = pd.DataFrame(max_rps_data).set_index('run_number').sort_values(by='max_success_rps', ascending=False)
    
    plt.figure(figsize=(10,6))
    max_rps_df['max_success_rps'].plot(kind='bar', color='#007ACC')
    plt.title(f'Максимальний Успішний RPS: {scenario_name}')
    plt.xlabel('Номер Запуску')
    plt.ylabel('RPS')
    plt.xticks(rotation=0, ha='center')
    plt.grid(axis='y', linestyle='--', alpha=0.6)
    plt.tight_layout()
    
    plt.savefig(f'max_rps_all_runs_{scenario_key}.png')
    plt.show()

# ОНОВЛЕНИЙ ОСНОВНИЙ БЛОК ДЛЯ НАКОПИЧЕННЯ РЕЗУЛЬТАТІВ
if __name__=="__main__":
    
    # 1. Визначення імені файлу для збереження
    metrics_filename = f"metrics_{CURRENT_SCENARIO_KEY}.csv"
    
    print(f"Стрес-тест БД: {SCENARIOS[CURRENT_SCENARIO_KEY]['name']}", flush=True)
    
    try:
        # Очищення Docker-контейнерів перед початком
        subprocess.run(["docker","compose","-f",SCENARIOS[CURRENT_SCENARIO_KEY]['compose_file'],"down"], cwd=os.getcwd(), timeout=5)
    except:
        pass

    print(f"\n=======================================================", flush=True)
    print(f"ПОЧАТОК ТЕСТУ: {SCENARIOS[CURRENT_SCENARIO_KEY]['name']}", flush=True)
    print(f"=======================================================", flush=True)

    df_metrics_current_run = run_scenario(CURRENT_SCENARIO_KEY)
    
    if not df_metrics_current_run.empty:
        
        # 2. Логіка накопичення результатів
        try:
            # Спроба завантажити попередні результати
            df_existing = pd.read_csv(metrics_filename)
            # Визначення нового номера запуску
            new_run_number = df_existing['run_number'].max() + 1
            print(f"\n-> Знайдено попередні результати. Новий запуск #{new_run_number}", flush=True)
        except FileNotFoundError:
            # Якщо файл не знайдено, це перший запуск
            df_existing = pd.DataFrame()
            new_run_number = 1
            print("\n-> Перший запуск тесту. Файл результатів буде створено.", flush=True)

        # 3. Додавання нового результату
        df_metrics_current_run['run_number'] = new_run_number
        df_metrics_combined = pd.concat([df_existing, df_metrics_current_run], ignore_index=True)

        # 4. Збереження об'єднаного DataFrame
        df_metrics_combined.to_csv(metrics_filename, index=False)
        print(f"-> Об'єднані результати збережено у {metrics_filename}", flush=True)
        
        # 5. Візуалізація всіх накопичених результатів
        visualize_results_comparison(df_metrics_combined)
        
    else:
        print("\nНеможливо отримати результати для демонтрації.", flush=True)

    print(f"ТЕСТ {CURRENT_SCENARIO_KEY} ЗАВЕРШЕНО.", flush=True)