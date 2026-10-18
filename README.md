# MLOps homework 2

Скрипт делает загрузку данных из csv файла, загрузку данных в kafka и загрузку данных в БД clickhouse,

В базе данных можно выполнять запросы поиска категории наибольшей транзакции по каждому штату.

Также есть возможность запустить оптимизацию таблицы и уже задавать запросы оптимизированной таблице.

[Данные с Kaggle](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394) нужно положить в папку `data`, в качестве примера там лежит test.csv из соревнования.

Команды выполнять из корня проекта.

## Файлы проекта

| Файл | Назначение |
| --- | --- |
| `docker-compose.yml` | Kafka, Zookeeper, ClickHouse и Kafka UI |
| `requirements.txt` | Зависимости Python-скриптов |
| `scripts/read_data.py` | Преобразование CSV в JSON и отправка в Kafka |
| `scripts/export_results.py` | Выполнение SQL и сохранение результата в CSV |
| `sql/create_tables.sql` | Базовая схема ClickHouse и загрузка из Kafka |
| `sql/optimize_db.sql` | Оптимизированная таблица и предварительные агрегаты |
| `sql/get_max_transactions_base.sql` | Запрос по исходной таблице |
| `sql/get_max_transactions_optimized.sql` | Запрос по предварительным агрегатам |

Данные проходят из топика `transactions` через Kafka-таблицу `hw2.transaction_queue` и materialized view в постоянную таблицу `hw2.transactions`.

## Linux — Bash

```bash
# Python-окружение
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

# Сборка и запуск
docker compose up -d --build
docker compose ps --all
# Дождаться healthy у Kafka и ClickHouse, Exited (0) у kafka-setup

docker compose exec -T clickhouse clickhouse-client --multiquery < sql/create_tables.sql

# CSV -> Kafka -> DB (ClickHouse)
python scripts/read_data.py --csv data/test.csv

# Проверка: для test.csv — 262144 строки
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM hw2.transactions"

# Базовый запрос
python scripts/export_results.py --query-file sql/get_max_transactions_base.sql --output results/results_base.csv

# Оптимизация: выполнить один раз после полной загрузки
docker compose exec -T clickhouse clickhouse-client --multiquery < sql/optimize_db.sql

# Результат из оптимизированных и заранее посчитанных таблиц
python scripts/export_results.py --query-file sql/get_max_transactions_optimized.sql --output results/results_optimized.csv

# Время запросов, секунды
docker compose exec -T clickhouse clickhouse-client --time --format Null < sql/get_max_transactions_base.sql
docker compose exec -T clickhouse clickhouse-client --time --format Null < sql/get_max_transactions_optimized.sql
```

## Windows — PowerShell

```powershell
# Python-окружение
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

# Сборка и запуск
docker compose up -d --build
docker compose ps --all
# Дождаться healthy у Kafka и ClickHouse, Exited (0) у kafka-setup

Get-Content .\sql\create_tables.sql -Raw | docker compose exec -T clickhouse clickhouse-client --multiquery

# CSV -> Kafka -> DB (ClickHouse)
python .\scripts\read_data.py --csv .\data\test.csv

# Проверка: для test.csv — 262144 строки
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM hw2.transactions"

# Базовый запрос
python .\scripts\export_results.py --query-file .\sql\get_max_transactions_base.sql --output .\results\results_base.csv

# Оптимизация: выполнить один раз после полной загрузки
Get-Content .\sql\optimize_db.sql -Raw | docker compose exec -T clickhouse clickhouse-client --multiquery

# Результат из оптимизированных и заранее посчитанных таблиц
python .\scripts\export_results.py --query-file .\sql\get_max_transactions_optimized.sql --output .\results\results_optimized.csv

# Время запросов, секунды
Get-Content .\sql\get_max_transactions_base.sql -Raw | docker compose exec -T clickhouse clickhouse-client --time --format Null
Get-Content .\sql\get_max_transactions_optimized.sql -Raw | docker compose exec -T clickhouse clickhouse-client --time --format Null
```

## Проверка результата

Выполнять команды последовательно. 

Контейнеры используют готовые Docker-образы;

После завершения producer дождаться загрузки ClickHouse: для одной отправки приложенного `test.csv` в пустую таблицу ожидается **262144 строки**.

Повторный запуск producer добавляет строки повторно.

Экспорты создают два файла:

- `results/results_base.csv` — результат базового запроса;
- `results/results_optimized.csv` — результат оптимизированного.

Результат содержит `state`, `category_of_largest_transaction`, `largest_transaction_amount`. 

Для примера — **51 строка данных**, по одной на каждое значение `state`. 

При равных максимальных суммах `argMax` может выбрать любую из соответствующих категорий.

## Как работает оптимизация

- `LowCardinality(String)` хранит повторяющиеся штаты и категории через словарь.
- Сортировка `(state, category, transaction_time)` группирует близко записи одного штата и категории.
- `AggregatingMergeTree` хранит состояния максимумов, чтобы итоговый запрос не перечитывал все транзакции.
- Materialized views автоматически обрабатывают новые вставки в `hw2.transactions`.

`optimize_db.sql` выполнить один раз после полной загрузки, не отправляя новые данные во время его выполнения. 

Повторно применять скрипт после следующих загрузок не нужно.

Исходная таблица сохраняется для сравнения. 

## Сравнение размера таблиц

Linux — Bash:

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT table, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS disk_size FROM system.parts WHERE database = 'hw2' AND active AND table IN ('transactions', 'transactions_optimized', 'transactions_state_max') GROUP BY table ORDER BY table"
```

Windows — PowerShell:

```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT table, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS disk_size FROM system.parts WHERE database = 'hw2' AND active AND table IN ('transactions', 'transactions_optimized', 'transactions_state_max') GROUP BY table ORDER BY table"
```

Команды с `--time --format Null` выше измеряют время запросов без вывода результатов. 

Kafka UI: http://localhost:8080 · Проверка ClickHouse: http://localhost:8123/ping. Kafka с хоста: `localhost:9095`, внутри Docker: `kafka:9092`.
