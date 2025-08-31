# Spark
**pipeline**:  Генератор событий → Kafka → Spark Streaming → Аналитика
Для демонстрации работы **Spark Streaming** с потоком данных, генерируемым Python-скриптом и передаваемым через **Kafka**, вы уже создали корректный и рабочий стек:

Загрузка и предварительная обработка данных
1.1. Загрузка и вывод схемы: Загрузите файл retail_store_sales.csv.  Выведите первые 5 строк загруженного DataFrame и его схему (df.printSchema()).

1.2. Очистка названий столбцов: Преобразуйте названия всех столбцов к единому регистру - snake_case.  Выведите обновленную схему DataFrame  или названия столбцов, чтобы убедиться в изменении названий.

1.3. Преобразование типов данных: Проанализируйте к каким типам данных относятся данные в столбцах и приведите столбец к соответствующему типу. Убедитесь, что некорректные или отсутствующие значения преобразуются в null в соответствующих типах данных.

2. Очистка и валидация данных
2.1. Восстановление отсутствующих item: 

Так как данные статические для каждого товара, то  составьте справочник товаров в отдельный DataFrame с Category, Item и Rrice Rer Unit.
Для транзакций, где отсутствует название товара , но имеется категория и цена , попытайтесь определить название товара, путём объединения (join) с загруженным справочником товаров. Выведите 20 строк, демонстрирующих восстановленные значения.
2.2. Восстановление Total Spent:  Найдите все транзакции, с пропусками в общей сумме и обновите ее, пересчитав её как quantity * price_per_unit для всех записей.

2.3. Заполнение отсутствующих Quantity и Rrice Rer Unit: 

Для транзакций, где отсутствуют значения о количестве проданного товара , но имеются сумма транзакции и цена за товар , вычислите количество проданного товара и заполните пропущенные значения. Результат приведите к целому числу. 
Аналогично, если  отсутствует цена за единицу товара , но общая сумма и количество имеются, вычислите цену за единицу и заполните пропущенные значения. Округлите до двух знаков после запятой. Выведите 20 строк, демонстрирующих заполненные значения.
2.4. Удалите оставшийся строки с пропусками в Category, Quantity ,Total Spent и Rrice Rer Unit

3. Разведочный анализ данных
3.1. Самые популярные категории товаров: Рассчитайте общее количество проданных единиц товара  для каждой категории. Определите Топ-5 категорий по общему количеству проданных единиц. 

3.2. Анализ среднего чека: 

Рассчитайте среднее значение Total Spent для каждого метода оплаты. Округлите до двух знаков после запятой.
Рассчитайте среднее значение Total Spent для каждой места где прошла оплата. Округлите до двух знаков после запятой.

4. Генерация признаков 

4.1. Временные признаки: Добавьте два новых столбца на основе Transaction Date:

day_of_week: День недели
transaction_month: Месяц транзакции 

---

### ✅ Стек:
1. **Docker + Kafka/ZooKeeper**
2. **Python Producer** — отправляет JSON-сообщения в топик Kafka
3. **Spark Streaming App** — читает из Kafka, парсит данные и выводит в консоль

---

### 1. **Инфраструктура (Docker + Kafka)**

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Этот `docker-compose.yml` запускает локальный кластер Kafka для тестирования.

**Запуск:**
```bash
docker compose up -d
```

---

### 2. **Python Producer**

```python
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {"value": random.randint(1, 100)}
    print("Sending:", data)
    producer.send("test-topic", value=data)
    time.sleep(1)
```

- Генерирует случайное число каждую секунду.
- Отправляет его в топик `test-topic`.

---

### 3. **Spark Streaming Consumer**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

schema = StructType().add("value", IntegerType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.value")

query = parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
```

#### Что делает этот код:
1. Подключается к Kafka как consumer.
2. Читает сообщения из топика `test-topic`.
3. Парсит JSON-строку в структурированные данные.
4. Выводит значения в консоль в режиме реального времени.

---

##  Запуск последовательности:

1. **Запустить Docker контейнеры:**
   ```bash
   docker compose up -d
   ```

2. **Запустить Python Producer:**
   ```bash
   python producer.py
   ```

3. **Запустить Spark Streaming приложение:**
   ```bash
   spark-submit spark_streaming.py
   ```

---

##  Расширение задачи

Обрабатывать  числа, сложные объекты (чеки, события кликов, метрики датчиков и т.д.). 
Вот несколько вариантов усложнения:

---

### 🔢 Пример усложненной задачи:

> Представьте, что вы получаете **чеки** из магазинов, каждый содержит:
- ID магазина
- ID кассира
- Время
- Товары (название, категория, цена, количество)

Вам нужно:
1. **Топ-5 магазинов по выручке за последние 10 секунд**
2. **Чеки дороже 500 рублей**
3. **Популярные товары по категориям**

---

###  Как реализовать:

1. Изменить Producer, чтобы он генерировал чеки (вместо простых чисел).
2. В Spark изменить схему данных (`StructType`) под структуру чека.
3. Использовать оконную функцию (`window(...)`) для анализа по временным промежуткам.
4. Группировать данные и делать агрегации (sum, count и т.д.).

---

##  Улучшения в Spark Streaming

Добавление оконной агрегации:

```python
from pyspark.sql.functions import window

# После получения flat_df
windowed_df = batch_df.withWatermark("timestamp", "10 seconds").groupBy(
    window(col("timestamp"), "10 seconds"),
    col("store_name")
).agg(spark_sum("item_total").alias("revenue"))
```
---

##  Заключение

pipeline:

**Генератор событий → Kafka → Spark Streaming → Аналитика**

Это основа для более сложных систем:  
- Мониторинг продаж в ритейле  
- Обнаружение аномалий  
- Рекомендательные системы в реальном времени  
- Логирование и мониторинг IoT устройств  

