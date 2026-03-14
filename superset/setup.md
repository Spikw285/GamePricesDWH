# Superset — подключение ClickHouse и дашборд GamePrices

## 1. Запуск

После `docker compose up -d` подождите ~60 сек (Superset медленно стартует),
затем откройте http://localhost:8088 и войдите как admin.

---

## 2. Подключение ClickHouse

После захода в Superset сделайте следующее:
`Settings → Database Connections → + Database`

Потом заполните следующие поля:

| Поле | Значение |
|------|----------|
| Database | ClickHouse Connect |
| SQLAlchemy URI | `clickhousedb://default:@clickhouse:8123/default` |
| Display Name | GamePrices ClickHouse |

Нажмите **Test Connection** → **Connect**.

> Если пароль задан: `clickhousedb://default:YOUR_PASSWORD@clickhouse:8123/default`

---

## 3. Датасеты (по одному на каждую mart-таблицу)

`Datasets → + Dataset` → выберите базу **GamePrices ClickHouse** → schema **default** (или другой который имеет нижеперечисленное)

Добавь четыре датасета:
- `dm_prices_latest`
- `dm_best_deals`
- `dm_store_stats`
- `dm_price_history`

---

## 4. Дашборд «GamePrices Overview»

Создайте дашборд (`Dashboards → + Dashboard`), добавив чарты:

### Чарт 1 — Топ-10 игр по скидке (Table)
- Dataset: `dm_best_deals`
- Metric: `MAX(best_discount)`
- Dimensions: `game_title`, `best_store`, `best_price`, `regular_price`
- Order By: `best_discount DESC`
- Row limit: 10

### Чарт 2 — Средняя скидка по магазинам (Bar Chart)
- Dataset: `dm_store_stats`
- Metric: `MAX(avg_discount_pct)`
- Dimension: `store_name`
- Sort: по убыванию метрики

### Чарт 3 — Процент игр на скидке по магазинам (Bar Chart)
- Dataset: `dm_store_stats`
- Metric: `MAX(pct_on_sale)`
- Dimension: `store_name`

### Чарт 4 — Динамика средней цены по времени (Line Chart)
- Dataset: `dm_price_history`
- Metric: `AVG(price)`
- Time column: `snapshot_date`
- Time grain: Day
- Dimension: `store_name` (опционально — для разбивки по магазинам)

### Чарт 5 — Big Number: всего игр в слежении
- Dataset: `dm_prices_latest`
- Metric: `COUNT_DISTINCT(game_id)`

### Чарт 6 — Big Number: игр на скидке прямо сейчас
- Dataset: `dm_best_deals`
- Metric: `COUNT(*)`

---

## 5. Экспорт дашборда для контроля версий

`Dashboards → ⋮ → Export`

Сохраните `dashboard_export_*.zip` в `superset/import_export/`.

При восстановлении:
`Dashboards → Import → выберите zip-файл`

---

## 6. Быстрое обновление данных вручную

Если нужно перечитать данные без ожидания Airflow:
```bash
docker exec gp_airflow airflow dags trigger itad_ingest_pipeline
```