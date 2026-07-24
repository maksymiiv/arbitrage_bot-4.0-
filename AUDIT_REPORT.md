# AUDIT_REPORT — Повний технічний аудит арбітражного парсера

> Аудитор: Claude Fable 5. Дата: 2026-07-24.
> Прочитано: 100% Python-коду в `engine/`, `cex_part/`, `dex_part/` (~6 500 рядків, 64 файли), кореневі скрипти, `.gitignore`, структура `.env` (без значень), стан git.
> Формат пріоритетів: **P0** (критично) / **P1** (важливо) / **P2** (бажано) / **P3** (косметика). Оцінка зусиль: S / M / L.

---

## 1. Резюме (Executive Summary)

Загальний стан коду — **добрий, значно вищий за середній для соло-проєкту**. Архітектура трьох підсистем (CEX → `price_store` ← DEX, зверху `spread_logic`) логічна, коментарі пояснюють *чому*, а не *що*, більшість відомих граблів (Windows file locks, rate limits, "фантомні спреди", неправильні decimals) уже мають продумані рішення. Це варто зберегти.

Найкритичніші проблеми (детально нижче):

1. **[P0] Секрети в коді та в git**: API-ключі NodeReal і Alchemy захардкоджені як дефолти в `engine/config.py`; `cex_part/.env` (з Bybit API key/secret) закомічений у git-історію. Ключі треба вважати скомпрометованими і **ротувати**.
2. **[P0] Більшість актуального коду не закомічена**: у гіті один коміт, 21 нових файлів (включно з `engine/config.py`, `liquidity_filter.py`, `v4_monitor.py` — ядро системи) — untracked. Один збій диска — і актуальна версія проєкту втрачена.
3. **[P0] Gate.io не має цінової стрічки взагалі**: `update_cex` викликається лише з Bybit і Kraken. Уся робота по Gate (метадані, network-status полінг кожні 5 хв/8 с) не породжує жодного спреду — мертвий функціонал, що споживає ресурси.
4. **[P1] Немає TTL/age-фільтра на ціни у сканері**: `PriceStore.cleanup()` свідомо не використовується, `dex_age`/`cex_age` рахуються, але не фільтрують. Замерзлий пул + живий CEX = фантомний спред (частково прикрито bound-pool-фільтром, але не повністю).
5. **[P1] Bybit ордербук без контролю послідовності**: дельти застосовуються без перевірки `u`/`seq` — пропущена дельта тихо псує книгу (і depth-фільтр).
6. **[P1] Один спільний `WS_RESTART_EVENT` на всі чейни** — гонка: перший монітор, що перезапустився, «з'їдає» подію, інші можуть не перезапуститися.

Головний вектор покращення: **зафіксувати роботу в git (Фаза 0) → закрити коректність (Фази 1–2) → потім реструктуризувати під реєстри чейнів/бірж/протоколів (Фаза 4)**, бо зараз додавання нової мережі вимагає правок у ~8 місцях, нової біржі — у ~6.

---

## 2. Карта архітектури (за фактом коду)

```
                        engine/main.py (оркестратор)
                              │ (усе через asyncio.create_task)
    ┌─────────────┬───────────┼─────────────┬──────────────┬────────────┐
    │             │           │             │              │            │
 start_cex()  start_dex()  spread_runner  pool_refresh  coingecko    cex_network_loop
 (await!)     (task)       (1s tick)      _loop (6h)    refresh+     (bybit, gate)
    │             │           │                          pending      kraken_depth_loop
    ▼             ▼           ▼
 cex_part      dex_part    engine/spread_logic
```

**Потік даних:**

- **CEX-сторона**: `exchange_loader.run_{bybit,gate,kraken}` (полінг лістингів 60 с) → `cache_manager.merge_token` → `cex_tokens.json`. Ордербуки: Bybit WS (50 рівнів, snapshot+delta → `core/orderbooks.py`), Kraken WS v2 (BBO ticker, шардовані з'єднання по 80 пар). Обидва пишуть у `price_store.update_cex`. **Gate — тільки метадані, цін немає.**
- **DEX-сторона**: `chain_monitor` (по одному WS на чейн: bsc/eth/base; підписка на Swap/Sync-топіки пулів з `pools.json`) + `v4_monitor` (одна підписка на singleton PoolManager, демультиплекс по `poolId`). Ціна: `sqrtPriceX96`/reserves → `Decimal` → `price_store.update_dex` (маршрутизація ВИКЛЮЧНО через `pool → canonical key` індекс — правильне рішення).
- **Обв'язка**: `dex_pool_manager` (DexScreener discovery, tiered retry), `pool_refresh` (валідація pools.json раз на 6 год), `liquidity_filter` (GeckoTerminal, агрегована ліквідність + авто-апгрейд пулу), `coingecko` (резолвінг `coingecko_id`, верифікація Kraken-тікерів), `orderbook_depth` (депт-фільтр спредів), `network_status` (deposit/withdraw статуси).
- **Сканер** (`spread_logic/scanner.py`, синхронний, викликається щосекунди): deepcopy снапшот → для кожної пари CEX×DEX рахує обидва напрями → фільтри: chain-blacklist → route open → min_spread → liquidity → orderbook depth → лог.

**Цикли імпортів** (усі вже "розірвані" лінивими імпортами, але це симптом): `engine.price_store → cex_part.cache_manager` (lazy), `engine.coingecko → cex_part.cache_manager` (lazy), `engine.liquidity_filter → cex_part.{cache_manager, pool_refresh}` + `dex_part.stable_native` (lazy), `cex_part.pool_refresh → dex_part.{pools_io, bad_pools, ws_subscriber}`, `dex_part.chain_monitor → cex_part.pool_refresh` (lazy). Фактично `engine`, `cex_part`, `dex_part` — не шари, а один зв'язаний граф. Це головна структурна проблема для масштабування (див. §5).

**Що працює добре (зберегти при будь-якому рефакторингу):**

- Канонічний ключ токена (не тікер!) + `_INDEX["pool"] → key` маршрутизація DEX-цін — це правильне ядро всієї системи. Клас багів "той самий тікер — різні токени" (Litentry/Lit Protocol) системно закритий: `kraken_verified` через CoinGecko, `SYMBOL_OVERRIDES`, disambig-ключі `HOLO#eth.4c4d4144`.
- `engine/atomic_io.py` і `_SafeRotatingFileHandler` — грамотні обхідні рішення Windows file-lock проблем, з re-entrancy guard у print-bridge.
- Тришаровий захист від фантомних спредів: агрегована ліквідність + 24h volume (GT) → bound-pool floor → orderbook depth у profit-band. Це найцінніша доменна логіка проєкту.
- Tiered retry (2 хв / 10 хв / 24 год) для DexScreener discovery і tiered ratio gate для авто-апгрейду пулів.
- `pools_io.operation_lock()` — правильне рішення "lost update" між discovery і refresh.
- `bad_pools` (3 хіти out-of-range → евікція + пере-discovery) — елегантний механізм самолікування.
- V4-монітор: одна підписка на PoolManager замість сотень — правильна модель.
- Auto-heal кешу метаданих (`symbol=UNKNOWN` → повторний фетч, відмова кешувати вгадані decimals).

---

## 3. Детальні знахідки по модулях

### 3.1 `engine/config.py`

- **[P0]** `engine/config.py:56-73` — секрети захардкоджені як дефолти
  - **Категорія:** security
  - **Проблема:** `BSC_WS` містить повний NodeReal API key, `ETH_WS`/`ETH_RPC`/`BASE_WS`/`BASE_RPC` — Alchemy key (`-_MW_eIhKVACqorn4l_1y`) прямо в коді. Файл зараз untracked, але щойно його закомітять — ключі витікають назавжди в історію.
  - **Рекомендація:** дефолти → публічні endpoint-и без ключів (`https://bsc-dataseed1.binance.org`, `wss://ethereum-rpc.publicnode.com`, `wss://base-rpc.publicnode.com`) або порожні рядки з fail-fast логом «BSC_WS не задано». Реальні ключі — тільки в `.env`. **Ротувати обидва ключі** (Alchemy + NodeReal), бо вони вже були в робочих копіях/архівах (`cex_part/cex_part.zip`, `dex_part/dex_part.zip` теж можуть їх містити).
  - **Ризик:** нульовий (значення ті самі, лише джерело).
  - **Зусилля:** S

- **[P2]** `engine/config.py:56-74` — `CHAINS` як літерал у коді
  - **Категорія:** scalability
  - **Проблема:** додавання чейну = правка коду. Плюс `v4_pool_manager` змішує інфраструктуру (endpoint) з протокольними константами (адреса PoolManager).
  - **Рекомендація:** розділити: `chains.toml`/`chains.json` (реєстр чейнів: id, native, стейбли, протоколи, слаги DexScreener/GT/CG) + env-оверайди endpoint-ів. Див. §5.
  - **Зусилля:** M (разом з §5)

### 3.2 `engine/price_store.py`

- **[P1]** `engine/price_store.py:171-181` — `deepcopy` у гарячих шляхах
  - **Категорія:** performance
  - **Проблема:** `snapshot()` робить `copy.deepcopy` УСЬОГО стора щосекунди для сканера, плюс `chain_monitor._has_any_dex_prices_for_chain` і `diagnostics` теж. При 1500+ токенах це десятки тисяч алокацій на тік у циклі подій.
  - **Рекомендація:** сканеру не потрібна ізольована копія — він читає і не мутує. Додати `iter_ready()` (генератор по ключах з cex і dex без копії) або shallow-view `snapshot_view()`; `deepcopy` залишити тільки для зовнішніх споживачів (`get_asset`). Для `_has_any_dex_prices_for_chain` — додати дешевий метод `has_dex_for_chain(chain) -> bool`.
  - **Ризик:** мутації під час ітерації — сканер синхронний і не await-ить всередині циклу по entry, тож у межах одного тіку стор не зміниться (asyncio однопотоковий). Безпечно за умови, що сканер лишається синхронним.
  - **Зусилля:** S

- **[P1]** `engine/price_store.py:200-229` — `cleanup()` мертвий, TTL відсутній
  - **Категорія:** bug (correctness gap)
  - **Проблема:** записи живуть вічно. Пул відв'язали (наприклад, `replace_pool`) — стара ціна в `dex` лишається і бере участь у скануванні, поки її не перепише новий апдейт (а він не прийде — пул відписаний). Коментар у сканері прямо визнає це («stale прайс міг залишитись…») і латає одним випадком (chain-blacklist), а не загальним механізмом.
  - **Рекомендація:** (а) при `replace_pool`/`drop_pool_for_replacement` — явно видаляти DEX-запис для старого пулу зі стора (додати `price_store.remove_dex_if_pool(key, chain, pool)`); (б) у сканері додати конфіговані пороги `MAX_DEX_AGE_SEC`/`MAX_CEX_AGE_SEC` (властиво, дані вже є: `dex_age`, `cex_age`) — старіші за поріг сторони пропускати. Обережно з порогом DEX: неліквідні токени легально мовчать годинами — поріг має бути великий (наприклад, 6–24 год) або залежати від ліквідності.
  - **Зусилля:** M

- **[P2]** `engine/price_store.py:107` — fallback `key = resolve(...) or symbol`
  - **Категорія:** bug-risk
  - **Проблема:** якщо резолвінг (cex, symbol) не вдався, ціна пишеться під «голим» тікером — ПАРАЛЕЛЬНО до канонічного ключа може виникнути дублікатний запис, який ніколи не з'єднається з DEX-стороною.
  - **Рекомендація:** логувати (rate-limited) і, можливо, зробити поведінку конфігурованою; мінімум — метрика кількості таких fallback-ключів у diagnostics-дампі.
  - **Зусилля:** S

### 3.3 `engine/spread_logic/`

- **[P1]** `engine/spread_logic/runner.py:14-20` — фінгерпринт містить `cex_age`/`dex_age`
  - **Категорія:** bug
  - **Проблема:** `_make_fingerprint` включає `round(o.cex_age, 2)` — вік змінюється КОЖЕН тік, отже поки існує хоч один спред, фінгерпринт відрізняється щосекунди → дедуплікація фактично не працює, лог спредів роздувається (повний блок щосекунди).
  - **Рекомендація:** прибрати ages з фінгерпринта (лишити symbol|direction|cex|chain|spread_pct округлений до 2–4 знаків), або логувати повний блок не частіше ніж раз на N секунд при незмінному складі можливостей.
  - **Ризик:** менш детальний аудит-трейл — компенсується меншим шумом.
  - **Зусилля:** S

- **[P1]** `engine/spread_logic/logger.py:27-34` — синхронний файловий I/O в event loop
  - **Категорія:** performance
  - **Проблема:** `SpreadLogger.log` відкриває файл і пише НА КОЖЕН РЯДОК прямо в циклі подій (spread_runner → scanner → log). На Windows з антивірусом open+append може коштувати мілісекунди × десятки рядків на тік.
  - **Рекомендація:** тримати відкритий file handle (пере-відкривати на зміні дати) і/або збирати блок рядків та писати одним викликом; або перевести на стандартний `logging` з окремим `TimedRotatingFileHandler(when="midnight")` — це і є «детермінований щоденний файл», який тут зроблено вручну.
  - **Зусилля:** S

- **[P2]** `engine/spread_logic/scanner.py:50` — chain-blacklist по display-символу
  - **Категорія:** bug-risk
  - **Проблема:** `is_chain_blacklisted(symbol, ...)` використовує display-тікер; при колізії тікерів (два різні токени «LIT») блекліст зачепить обидва.
  - **Рекомендація:** при переході на TTL-механізм (див. 3.2) цей safety net взагалі можна прибрати; якщо лишається — блеклістити по канонічному `key`.
  - **Зусилля:** S

- **[P3]** `scanner.py` — коментарі впереміш українською та англійською; `calculators.py` — два майже ідентичні конструктори `SpreadOpportunity` (можна звести до спільного хелпера). Стиль, не поспішати.

### 3.4 `engine/liquidity_filter.py` (609 рядків)

- **[P1]** Файл робить ЧОТИРИ різні речі — кандидат на розбиття
  - **Категорія:** rewrite (структурний)
  - **Проблема:** (1) кеш вердиктів ліквідності; (2) GT-клієнт `_fetch_aggregates`; (3) політика авто-апгрейду пулів `_maybe_upgrade_pool` (найскладніша бізнес-логіка в проєкті); (4) класифікація пулів (`_is_subscribable_pool`, `_parse_gt_dex_id`). Апгрейд-логіка тягне лізучі імпорти в `cex_part.pool_refresh` і `cache_manager._INDEX` (приватні поля!).
  - **Рекомендація:** розбити на `engine/gt_client.py` (HTTP + парсинг), `engine/liquidity_cache.py` (вердикти+TTL), `engine/pool_upgrade.py` (політика). `_get_current_pool` має користуватися ПУБЛІЧНИМ API cache_manager (додати `cache_manager.get_bound_pool(chain, token_addr)`), а не `_INDEX`/`_CACHE` напряму.
  - **Ризик:** середній — багато точок дотику; робити після Фази 1, з таргетованим тестом на `_maybe_upgrade_pool` (чиста функція над `pool_infos` — легко тестується).
  - **Зусилля:** M

- **[P2]** `liquidity_filter.py:452` — `await _persist()` пише ВЕСЬ кеш на кожен зафетчений токен
  - **Категорія:** performance
  - **Проблема:** на холодному старті 100+ токенів → 100+ повних перезаписів `liquidity_status.json`; сам `atomic_write_json` — синхронний (блокує loop на час серіалізації + запис + retry-rename).
  - **Рекомендація:** дебаунс персисту (наприклад, раз на 10 с або кожні N оновлень), а сам запис — через `asyncio.to_thread`.
  - **Зусилля:** S

- **[P3]** `liquidity_filter.py:187` — `import re as _re` посеред файлу; докстрінг каже «25 req/min», а `rate_limiter` конфігурує 15/min — розсинхрон документації.

### 3.5 `engine/coingecko.py`

- **[P2]** `refresh_platforms` тримає в пам'яті і на диску ~3 МБ дампу всіх 17k монет CG, а `kraken_pending_retry_loop` перезавантажує ОБИДВА дампи щоразу, коли хоч один pending-токен «due» (потенційно кожні 15 хв).
  - **Категорія:** efficiency
  - **Рекомендація:** для pending-ретраїв достатньо точкового ендпоінта CG `/coins/{id}/contract/{addr}` або `/search`; повний дамп лишити тільки для добового рефрешу. Це скоротить трафік CG у рази і прибере регулярні 10-сторінкові пагінації Kraken-тікерів.
  - **Зусилля:** M

- **[P3]** `_platforms_age_sec`/`_kraken_age_sec` читають і парсять JSON-файли з диска на кожну ітерацію циклу — дешево, але можна тримати timestamp у пам'яті після init/refresh.

### 3.6 `engine/rate_limiter.py`

- **[P2]** Глобальний ліміт «gecko» об'єднує CoinGecko і GeckoTerminal — правильно; але `_BUCKETS_LOCK` (рядок 43) — мертва змінна, ніде не використовується. Прибрати.
- **[P3]** `configure(...)` викликається при імпорті модуля — робочий патерн, але «given»-конфіг краще винести в `engine/config.py`, щоб усі ліміти жили в одному місці.

### 3.7 `engine/logger.py`, `atomic_io.py`, `diagnostics.py`

- Якісні модулі, зберегти як є. Дві дрібниці:
- **[P3]** `logger.py`: `LOG_LEVEL` застосовується до root — сторонні бібліотеки (websockets, web3) на DEBUG зафлудять лог; додати `logging.getLogger("websockets").setLevel(logging.INFO)` тощо.
- **[P3]** `diagnostics.py`: дамп повного стора раз на 5 хв на 1500+ токенів — це сотні КБ на запис у лог; розглянути компактний формат (лише лічильники + top-N по свіжості) або окремий файл.

### 3.8 `engine/main.py` (оркестратор)

- **[P1]** `engine/main.py:47` — `await start_cex()` блокує старт усієї системи
  - **Категорія:** reliability
  - **Проблема:** `start_cex` чекає `bybit_wss.ready.wait()` і `kraken_wss.ready.wait()` без таймаута. Якщо Bybit WS недоступний (регіональний бан, мережа) — DEX-монітори і решта підсистем ніколи не стартують.
  - **Рекомендація:** `asyncio.wait_for(..., timeout=60)` з деградацією (лог + продовжити старт; менеджер сам доконнектиться), або запускати `start_cex` теж як таск.
  - **Зусилля:** S

- **[P1]** Усі фонові таски — `asyncio.create_task(...)` без збереження референсів і без нагляду
  - **Категорія:** reliability
  - **Проблема:** (а) Python може зібрати таск GC-ом, якщо на нього немає сильного посилання (реальний документований футган); (б) якщо таск помре з винятком, ніхто не дізнається — виняток лишиться «unretrieved», система тихо працюватиме без, скажімо, spread_runner-а.
  - **Рекомендація:** завести список `_TASKS` + `done_callback`, який логує падіння і (для критичних лупів) перезапускає таск; або перейти на `asyncio.TaskGroup` (Python 3.11+). Це стосується всіх `create_task` у проєкті (`cex_part/main.py`, `liquidity_filter.is_low_liquidity`, `orderbook_depth.register_depth_watch`, `chain_monitor` тощо) — мінімум додати спільний хелпер `spawn(coro, name=...)` в engine.
  - **Зусилля:** M

### 3.9 `cex_part/cache_manager.py` (829 рядків)

- **[P1]** Файл-«бог»: персистентність + індекси + резолвінг + CG-бекфіл + Kraken-cleanup + token-list — усе в одному модулі, з module-level глобальним станом
  - **Категорія:** rewrite (структурний)
  - **Проблема:** 6 сторонніх модулів лазять у `_CACHE`/`_INDEX` напряму (`liquidity_filter`, `pool_refresh`, `dex_pool_manager`, `pool_migrate`). Це блокує і тестування, і майбутні CEX-CEX сценарії.
  - **Рекомендація:** розбити на: `token_registry.py` (клас `TokenRegistry`: entries + індекси + резолвінг + публічні методи `get_bound_pool`, `iter_entries`, `entries_for_cex`), `token_store.py` (load/flush/atomic IO), `coingecko_backfill.py` (уся CG-логіка: `_resolve_entry_with_coingecko`, `apply_coingecko_metadata*`, `cleanup_kraken_pending`). Зовнішні модулі — тільки через публічний API. Схему даних не міняти (файли ті самі), тільки код.
  - **Ризик:** великий обсяг механічних правок; робити ОДНИМ кроком з grep-верифікацією, після покриття резолверів тестами (резолвінг — чисті функції, тестуються легко).
  - **Зусилля:** L

- **[P2]** `cache_manager.py:93-111` — `flush()` виконує синхронний запис двох JSON у циклі подій
  - **Категорія:** performance
  - **Проблема:** `cex_tokens.json` — найбільший робочий файл; серіалізація + запис + Windows-retry блокують loop. Викликається часто (кожен merge, кожен цикл полінгу з `force=True`).
  - **Рекомендація:** усередині `flush` віддавати запис у `asyncio.to_thread` (дані попередньо серіалізувати або зробити знімок під локом). Це одна правка, яка прибере головний джерел блокування event loop.
  - **Зусилля:** S

- **[P2]** `merge_token` без локу навколо read-modify-write
  - **Проблема:** `add_pool` бере `_FILE_LOCK`, а `merge_token`/`update_token_list` — ні. В asyncio без await всередині мутації це не гонка, але `merge_token` await-ить `flush()` в кінці — між мутаціями інших тасків можливі interleave-и. Сьогодні безпечно (мутації атомарні до await), але крихко.
  - **Рекомендація:** при рефакторингу — один `asyncio.Lock` на всі мутації реєстру.
  - **Зусилля:** разом з P1 вище.

- **[P3]** `resolve_symbol_by_pool` (рядок 1036) — back-compat аліас; перевірити grep-ом, що ніхто не використовує, і видалити.

### 3.10 `cex_part/exchange_loader.py`

- **[P1]** Три майже ідентичні цикли `run_bybit`/`run_gate`/`run_kraken`
  - **Категорія:** scalability
  - **Проблема:** додавання 4-ї біржі = четвертий copy-paste. Різниця між ними — джерело символів, джерело метаданих, і Kraken-специфічний CG-хвіст.
  - **Рекомендація:** інтерфейс `CexAdapter` (`list_spot_symbols()`, `fetch_metadata(symbols)`, `orderbook_manager()`, `fetch_network_status()`), один generic-цикл `run_cex(adapter)` + hook `on_new_entry` (для Kraken CG-логіки). Це ядро підготовки до CEX-CEX. Деталі в §5.
  - **Зусилля:** M

- **[P2]** `run_kraken:130` — `save_kraken_unknown_symbols(missing)` дописує множину В КОЖНОМУ 60-с циклі (файл тільки росте, ніколи не чиститься і ніким не читається в коді).
  - **Рекомендація:** або писати лише дельту з логом призначення, або видалити функцію і файл `kraken_unknown.json` зовсім (виглядає як діагностичний артефакт).
  - **Зусилля:** S

### 3.11 `cex_part/cex_orderbooks/`

- **[P1]** `bybit_orderbook.py:138-164` — немає перевірки цілісності послідовності
  - **Категорія:** bug
  - **Проблема:** Bybit v5 orderbook-повідомлення несуть `u` (updateId) і `seq`; при пропуску дельти (мережевий glitch, повільний consumer) книга тихо розходиться з реальністю — best bid/ask і depth-фільтр брешуть. Зараз `data.get("u")` навіть не читається.
  - **Рекомендація:** зберігати останній `u` на ключ; якщо прийшла дельта з розривом — re-subscribe топіка (Bybit шле новий snapshot). (Потребує перевірки семантики `u` для spot у документації Bybit v5 — верифікувати перед реалізацією.)
  - **Зусилля:** M

- **[P2]** `kraken_orderbook.py:142-179` — будь-який новий символ перебудовує ВСІ шарди
  - **Категорія:** performance / reliability
  - **Проблема:** supervisor скасовує всі WS-з'єднання і створює нові при зміні набору символів. На кожен новий лістинг — повний реконект усіх ~N/80 з'єднань, втрата всіх цін на секунди (BBO прийде тільки з наступним тіком або snapshot=True — прийде, але все одно шторм).
  - **Рекомендація:** тримати відкриті шарди і досилати `subscribe` на останній незаповнений шард; повну перебудову — лише при видаленні символів (рідко).
  - **Зусилля:** M

- **[P2]** Kraken: пара будується як `f"{symbol}/USD"` з altname
  - **Категорія:** bug-risk (потребує перевірки)
  - **Проблема:** Kraken WS v2 очікує symbol-нотацію (наприклад, `BTC/USD`), а `get_kraken_spot_coins` віддає altnames (там історично `XBT`, `XDG` тощо). Якщо в списку є altname, який не збігається з WS-символом, підписка тихо не відбудеться (лог ack success=false — лише в debug).
  - **Рекомендація:** перевірити на живих даних, скільки підписок реджектиться (підняти лог ack до INFO/WARNING при `success=false`); за потреби — мапа altname→wsname з `/0/public/AssetPairs`.
  - **Зусилля:** S (діагностика) / M (мапа)

- **[P3]** `bybit_orderbook.py`: делістнуті символи ніколи не відписуються — книги висять у пам'яті і продовжують оновлюватись; узгодити з `removed_symbols` з `update_token_list` (зараз ігнорується `_`).

### 3.12 `cex_part/core/orderbooks.py`

- **[P2]** `_update_price_store_from_book` — `max(book["bids"])`/`min(book["asks"])` на КОЖНУ дельту
  - **Категорія:** performance
  - **Проблема:** O(50) на дельту × сотні символів × потік дельт — левова частка CPU CEX-сторони. Плюс далі йде повний `update_cex` (резолвінг ключа через cache_manager на кожен тік).
  - **Рекомендація:** кешувати best bid/ask у книзі та інвалідувати інкрементально; `resolve_key_for_cex_symbol` — мемоізувати (мапа (cex,symbol)→key, інвалідована при merge/rebuild індексів). Разом це скоротить hot path у рази.
  - **Зусилля:** M

### 3.13 `cex_part/core/pool_refresh.py`, `dex_pool_manager.py`, `pool_migrate.py`

- Загалом добре продумані (batch-валідація, source=gt екзепшн, resumable-міграція). Знахідки:
- **[P2]** `dex_pool_manager._process_token` → `_write_no_pools(no_pools)` пише весь файл на кожен токен усередині `asyncio.gather` багатьох тасків; та сама історія, що з liquidity-кешем.
  - **Рекомендація:** писати один раз наприкінці `_sync_pools_with_cache_locked` (там уже є фінальний `_write_no_pools`); проміжні — прибрати.
  - **Зусилля:** S
- **[P2]** `pools_save(pools)` викликається і всередині `_process_token`, і в кінці sweep — подвійна робота; лишити тільки фінальний (в межах operation_lock це безпечно).
- **[P3]** `pool_refresh.py:44-49` і `dex_pool_manager.py:80-85` — дві копії мапи чейнів DexScreener (`DEX_CHAIN_REVERSE` і `CHAIN_MAP`, одна пряма, друга обернена). Винести в реєстр чейнів (§5).
- **[P3]** `RETRY_TTL_SEC = NO_POOL_RETRY_TTL_SEC` (back-compat, рядок 46) — grep показує, що старих викликів немає; видалити.

### 3.14 `cex_part/network_status.py`

- **[P1]** Полінг Gate і Bybit працює, але Gate-статуси ніколи не використовуються з користю
  - **Категорія:** cleanup / architecture
  - **Проблема:** (див. Резюме №3) Gate не має цінового фіда → спредів по Gate не буває → hot-watch по Gate не реєструється → adaptive loop по Gate завжди в passive-режимі і оновлює прапорці, які ніхто не читає (`is_route_open` викликається тільки для пар, що мають ціни).
  - **Рекомендація:** РІШЕННЯ ВЛАСНИКА: (а) додати Gate ticker WS (`wss://api.gateio.ws/ws/v4/` — канал `spot.book_ticker` дає BBO; за структурою це ~клон Kraken-менеджера) — тоді Gate стане повноцінним третім CEX; або (б) тимчасово вимкнути `run_gate` + `cex_network_loop("gate")`, щоб не палити CPU/квоти. Рекомендую (а) — це найдешевший приріст покриття ринку в усьому проєкті.
  - **Зусилля:** M (варіант а) / S (варіант б)

### 3.15 `cex_part/config/blacklist.py`

- **[P2]** `STABLE_SYMBOLS` містить `DYDX` і `VSN` — не стейбли
  - **Категорія:** bug-risk / style
  - **Проблема:** семантика множини зламана: майбутній код, що трактуватиме `STABLE_SYMBOLS` як «стейблкоїни» (наприклад, для CEX-CEX котирувань), отримає DYDX як стейбл. Коментар `#FIND V4 POOOL...` підтверджує, що це тимчасовий милиця.
  - **Рекомендація:** перенести обидва в `MANUAL_BLACKLIST` (семантично це саме воно).
  - **Зусилля:** S
- **[P3]** Мертві закоментовані блоки (SOL/AVAX/TRX) — або активувати, або прибрати; правити список у коді незручно — кандидат на `blacklist.json` поряд з реєстром чейнів.

### 3.16 `dex_part/core/chain_monitor.py`

- **[P1]** `ws_subscriber.WS_RESTART_EVENT` — ОДИН event на всі чейни
  - **Категорія:** bug (гонка)
  - **Проблема:** `monitor_chain` для кожного чейну робить `WS_RESTART_EVENT.clear()` на вході в ітерацію. Сценарій: pools_watcher ставить event → чейн bsc виходить з `_recv_loop`, реконнектиться і чистить event ДО того, як eth/base перевірили `is_set()` → eth/base не перезапустяться і працюють зі старим списком пулів. Також будь-яка зміна pools.json на ОДНОМУ чейні рестартить ВСІ чейни (зайві реконекти → 429 у провайдерів).
  - **Рекомендація:** словник `WS_RESTART_EVENTS: dict[chain, asyncio.Event]`; pools_watcher порівнює нормалізований снапшот ПО-ЧЕЙНОВО і ставить event лише для змінених чейнів; `replace_pool` ставить event чейну, який змінив.
  - **Зусилля:** S/M

- **[P2]** `_recv_loop` не перевіряє `subscription id` і не ловить ack-помилки підписки — якщо провайдер відхилив одну з трьох підписок (ліміт на кількість адрес у фільтрі!), монітор мовчки не бачитиме частину подій. Alchemy обмежує кількість адрес в одному logs-фільтрі; при зростанні кількості пулів (сотні) `subscribe_all` з одним великим `address`-масивом упреться в ліміт.
  - **Рекомендація:** читати ack-и на 3 запити (`id: 1..3`), логувати помилки; передбачити чанкінг address-списку (наприклад, по 100 адрес на підписку). Потребує перевірки поточних лімітів Alchemy/NodeReal.
  - **Зусилля:** M

- **[P2]** `_init_metadata` виконує до 8 RPC-викликів на пул при КОЖНОМУ реконекті (кеш рятує, але новий пул → 429-ризик; semaphore=3 — добре). Після рефакторингу під `WS_RESTART_EVENTS` частота реконектів впаде і це стане не критичним.

- **[P3]** `REQUIRE_SYMBOL_EXISTS_IN_STORE` (рядок 48) — мертвий прапор (False завжди), а перевірка під ним робить `price_store.snapshot()` (deepcopy!) на кожен свап. Видалити прапор і гілку.
- **[P3]** `_should_bootstrap`/`_has_any_dex_prices_for_chain` — deepcopy заради перевірки наявності; замінити на легкий метод стора (див. 3.2).

### 3.17 `dex_part/core/v4_monitor.py`

- **[P2]** `v4_monitor.py:136-146` — перезавантаження списку пулів відбувається тільки ПІСЛЯ успішного `recv`
  - **Категорія:** bug (дрібний)
  - **Проблема:** перевірка `time.time() - last_reload > _RELOAD_INTERVAL` стоїть після `await ws.recv()`; на тихому чейні (таймаут → `continue` → знову recv) reload не виконається, поки не прилетить БУДЬ-ЯКИЙ v4-своп. Новий пул може чекати підписки довго.
  - **Рекомендація:** перенести reload-перевірку на початок ітерації циклу (до/після timeout-гілки однаково).
  - **Зусилля:** S
- **[P3]** `_load_v4_pools` читає весь `token_cache.json` щохвилини на чейн — дешево, ок; за бажання — тримати markers V4 в окремому маленькому файлі.

### 3.18 `dex_part/utils/token_metadata.py`

- **[P2]** `_save_cache()` пише ВЕСЬ `token_cache.json` під `threading.Lock` на кожен новий токен/пул; викликається з `asyncio.to_thread`-потоків — event loop не блокує, але N потоків серіалізуються на lock+запис усього файла.
  - **Рекомендація:** дебаунс (брудний прапорець + періодичний фоновий flush) або батчинг під час `_init_metadata`.
  - **Зусилля:** S/M
- **[P3]** `get_v4_pool_metadata` створює новий `Web3(HTTPProvider)` на кожен виклик з `replace_pool` — з'єднання не переюзається. Тримати по одному `Web3` на чейн (фабрика в реєстрі чейнів).

### 3.19 `dex_part/config/tokens.py`

- **[P1]** `SWAP_STABLES` для `eth` вказує на АДРЕСУ USDT У BSC
  - **Категорія:** bug (у мертвому коді)
  - **Проблема:** `SWAP_STABLES_RAW["eth"] = "0x55d398..."` — це USDT на BSC; на Ethereum USDT — `0xdAC17F958D2ee523a2206206994597C13D831ec7`. Єдиний споживач — `stable_native.get_stable`, який зараз НЕ викликається ніде (спадок від видаленого `kyberswap_executor`).
  - **Рекомендація:** видалити `SWAP_STABLES`, `SWAP_STABLES_RAW`, `get_stable` повністю (і залежність від Web3 у config-файлі піде разом з ними). Якщо колись знадобиться — повернути з правильними адресами.
  - **Зусилля:** S
- **[P2]** `STABLES["bsc"]` містить: USDC двічі (у різному регістрі, рядки 7 і 9) і непідписану адресу `0x17EAfd...` (рядок 10 — це lisUSD; підписати або прибрати). `is_stable` порівнює через lower() — дубль нешкідливий, але це смітник у критичному списку.
- **[P2]** `NATIVE_NAMES["polygon"]` — чейн, якого немає ніде більше в системі; або прибрати, або лишити з коментарем «на майбутнє».
- **[P2]** Ці списки — САМЕ ТЕ, що має жити в реєстрі чейнів (§5): зараз стейбли для фільтрів у `tokens.py`, стейбл-символи для блеклісту в `cex_part/config/blacklist.py` — два незалежні джерела правди про «що таке стейбл».

### 3.20 `dex_part/dex/` (декодери/ціни)

- Хороший, компактний шар; `uniswap_v4/price.py` як делегат до v3 — правильно.
- **[P2]** `pancake_v2/decoder.py` і `pancake_v3/decoder.py` не валідують довжину `data` (V4-декодер валідує). Малоймовірний, але можливий `ValueError/IndexError` на обрізаному лозі від провайдера → виняток у `_recv_loop` → повний реконект сесії. Додати ту саму перевірку `len(data) < N → return {}`.
  - **Зусилля:** S
- **[P3]** Найменування «pancake_v2/v3 як канонічна реалізація для всіх форків» — працює, але при додаванні DEX-DEX краще перейменувати в `dex/evm_v2/`, `dex/evm_v3/`, `dex/uniswap_v4/` — щоб не плутати новоприбулих (і себе через рік).

### 3.21 Кореневі файли

- **[P2]** `test.py`, `test_rpc.py`, `_test_v4.py` — це dev-скрипти, не тести. Перенести в `scripts/` (`scripts/gt_probe.py`, `scripts/rpc_probe.py`, `scripts/v4_poc.py`). `_test_v4.py` містить застарілі константи (ETH_USD=2140 static) — лишити як PoC-архів або видалити.
- **[P1]** **Тестів немає взагалі.** При цьому найцінніша логіка — чисті функції, які тестуються без мережі: `calculators.calc_*`, `price_engine.raw_price_from_sqrt`, `pancake_v2/price.compute_price`, декодери (фікстури з реальних логів), `cache_manager._contracts_compatible`/`_resolve_for_merge`, `_parse_gt_dex_id`, `_maybe_upgrade_pool` (з мокнутим pool_infos), `extract_base_symbol`. Один день роботи — і рефакторинги Фази 4 стають безпечними.
  - **Рекомендація:** `pytest` + `tests/` з фікстурами реальних event-логів. Мінімальний набір: decoders (v2/v3/v4), price math (відомі sqrtPriceX96 → відома ціна), spread calculators, merge/resolve cache_manager.
  - **Зусилля:** M

---

## 4. Наскрізні проблеми

### 4.1 Git-гігієна (P0, Фаза 0 — детальний план)

Стан: один коміт `d951de6 first commit`; 5538 staged-видалень (venv/pycache вже прибрані з індексу — добре); 41 модифікований; **21 untracked, серед них ядро системи** (`engine/config.py`, `engine/coingecko.py`, `engine/liquidity_filter.py`, `cex_part/core/pool_refresh.py`, `cex_part/network_status.py`, `dex_part/core/v4_monitor.py`, `dex_part/dex/uniswap_v4/`, `requirements.txt`, `.env.example` тощо). У git досі відстежуються: `cex_part/.env` (**секрети!**), `cex_part/cex_part.zip`, `dex_part/dex_part.zip`, `dex_part/logs/*.log` (21 файл), `dex_part/kyber_cache.json`, `dex_part/token_cache.json`, `cex_part/dex_best_pools_dump.json`, `cex_part/dex_pool_errors.json`.

Покроково:

1. **Ротувати ключі**: Bybit API key/secret (був у `cex_part/.env` в коміті), Alchemy, NodeReal. Спочатку ротація — потім усе інше.
2. Прибрати з коду захардкоджені ключі (див. 3.1).
3. `git rm --cached cex_part/.env cex_part/cex_part.zip dex_part/dex_part.zip dex_part/kyber_cache.json dex_part/token_cache.json cex_part/dex_best_pools_dump.json cex_part/dex_pool_errors.json` і `git rm -r --cached dex_part/logs` (робочі копії лишаться). `.gitignore` вже покриває все це — добре складений.
4. Доповнити `.gitignore`: `dex_part/token_cache.json` вже є; додати `cex_part/cex_cache/` (зараз перелічено `*.json` в ньому — еквівалентно, ок), `logs/` вже є.
5. Видалити фізично: `cex_part/cex_part.zip`, `dex_part/dex_part.zip` (архіви-дублікати коду), `dex_part/kyber_cache.json` (kyberswap-executor видалено, кеш мертвий), `dex_part/logs/` (старі логи; актуальні пишуться в кореневий `logs/`), `cex_part/.env` (джерело правди — кореневий `.env`).
6. `git add -A` + перший «справжній» коміт поточного стану. **Оскільки історія — один коміт і в ньому секрети: найчистіше зробити повний перезапуск історії** (`git checkout --orphan main-clean && git add -A && git commit && git branch -D main && git branch -m main`) — тоді секрети не залишаться в жодному коміті. Це дешево саме зараз, поки історія не цінна.
7. `.env`: прибрати мертвий ключ `POOL_UPGRADE_MIN_RATIO` (код читає тільки `POOL_UPGRADE_MIN_VOL`; ratio тепер tiered у коді) і продублювати актуальний перелік у `.env.example`.
8. Надалі: комітити щонайменше щодня робочих змін. Опційно: приватний GitHub-репозиторій як off-site бекап.

### 4.2 Конфігурація

- `engine/config.py` як єдина точка — правильно і вже дотримано (грепом підтверджено: `os.getenv` поза config — тільки в `test_rpc.py`, це ок). Але **чейни/біржі описані даними в коді** у 8+ місцях: `engine.config.CHAINS`, `dex_part/config/tokens.py` (STABLES/NATIVE), `cex_part/utils/chains_filter.py` (CHAIN_MAP/ALLOWED_CHAINS), `engine/coingecko.py` (PLATFORM_TO_CHAIN), `engine/liquidity_filter.py` (NETWORK_MAP), `cex_part/core/dex_pool_manager.py` (CHAIN_MAP), `cex_part/core/pool_refresh.py` (DEX_CHAIN_REVERSE), `cex_part/core/pool_migrate.py` (_MIGRATE_CHAINS). Див. §5.

### 4.3 Блокуючий I/O в event loop (системна тема)

Повторюється в: `cache_manager.flush`, `liquidity_filter._persist`, `dex_pool_manager._write_no_pools`, `SpreadLogger.log`, `coingecko` age-читання, `pool_refresh._save_state`. Кожен окремо — мілісекунди; разом на Windows з антивірусом — постійні мікрофризи циклу подій, які прямо збільшують вік цін. Єдине рішення: усі `atomic_write_json` з async-контексту — через `asyncio.to_thread`, плюс дебаунс найчастіших (liquidity, no_pools).

### 4.4 HTTP-сесії

Майже кожен фетч створює новий `aiohttp.ClientSession` (bybit/gate/kraken metadata, liquidity_filter на кожен токен, kraken depth на кожен запит). Це нові TCP+TLS handshake щоразу. Завести довгоживучі сесії: одна на CEX-адаптер, одна на GT/CG, одна на DexScreener (створювати ліниво при першому виклику, закривати при shutdown). Виграш: менша латентність, менше сокетів, менше 429 від WAF-ів.

### 4.5 Бібліотеки / сервіси (пропозиції із заміни)

| Що | Зараз | Пропозиція | Навіщо | Ризик міграції |
|---|---|---|---|---|
| JSON у гарячих шляхах (WS-повідомлення) | `json` (stdlib) | `orjson` | 3–6× швидший парсинг; WS-стріми — найгарячіший шлях | Мінімальний: `orjson.loads` — drop-in для читання; `dumps` повертає `bytes` — правити тільки в atomic_io/логері, або лишити stdlib для запису |
| `web3` | 6.11.1 (стара) | web3 v7.x | активна гілка, кращий async-суппорт (`AsyncWeb3` + `WebSocketProvider` — можна прибрати ручні `to_thread`) | Середній: breaking changes у v7 (camelCase→snake_case подекуди); робити окремою фазою; поточні виклики прості (contract.functions.X.call), міграція локалізована в `token_metadata.py`, `current_price.py`, `pool_refresh.replace_pool` |
| `websockets` | 15.0.1 ок | лишити | версія свіжа, API використано коректно | — |
| DexScreener (discovery) | основне джерело | лишити + GT як зараз | зв'язка DS(discovery)+GT(liquidity/upgrade) уже виправляє слабкості одне одного | — |
| CoinGecko free | self-throttle 15/min | розглянути CG Demo API key (безкоштовний, дає стабільніші ліміти і окремий bucket від GT) | менше 429, швидший pending-resolve | Нульовий (ключ уже підтримано в коді) |
| RPC-провайдери | 1 endpoint/чейн | список fallback-endpoint-ів на чейн (env: `ETH_WS=url1,url2`) з ротацією при 429/дисконекті | зараз падіння одного провайдера = сліпота на чейні | Малий: правки в реєстрі чейнів + connect-логіці |
| Стан/кеші в JSON-файлах | 8 JSON-файлів | ЛИШИТИ (свідомо) | на поточному масштабі файли прості й дебажаться очима; SQLite додасть складності без виграшу. Повернутися до питання, коли токенів стане >10k або з'явиться друге джерело запису | — |

### 4.6 Логування

Єдиний стиль уже є (engine.logger всюди) — добре. Дрібне: рівні непослідовні (важливі reject-и підписок Kraken на debug; повний snapshot на info). Пройтись по рівнях один раз при Фазі 2.

---

## 5. План рефакторингу під масштабування

Мета: «додати чейн = один запис у конфігу», «додати біржу = один адаптер-файл», «додати напрям арбітражу = нова стратегія поверх тих самих сторів».

### 5.1 Реєстр чейнів (закриває 8 місць дублювання)

`engine/chains.py` (або `chains.toml` + лоадер):

```python
@dataclass(frozen=True)
class ChainSpec:
    key: str                 # "eth"
    ws_env: str              # "ETH_WS"
    rpc_env: str             # "ETH_RPC"
    native_wrapped: str      # WETH addr
    native_zero_ok: bool     # V4 native
    stables: dict[str, str]  # addr -> label
    v4_pool_manager: str | None
    dexscreener_id: str      # "ethereum"
    geckoterminal_id: str    # "eth"
    coingecko_platform: str  # "ethereum"
    cex_aliases: tuple[str, ...]  # ("Ethereum", "ETH", ...) для chains_filter
CHAINS: dict[str, ChainSpec] = {...}
```

Після цього видаляються: `PLATFORM_TO_CHAIN`, `NETWORK_MAP` (×2), `CHAIN_MAP` (×2), `DEX_CHAIN_REVERSE`, `ALLOWED_CHAINS`/`CHAIN_MAP` у chains_filter, `STABLES`/`NATIVE_NAMES` у tokens.py, `_MIGRATE_CHAINS` (стає похідним: чейни з v4_pool_manager або всі). `is_stable`/`is_native` читають зі спеки.

### 5.2 CEX-адаптери (готує CEX-CEX)

```
cex_part/adapters/
    base.py        # class CexAdapter(Protocol): name; list_spot_symbols();
                   #   fetch_metadata_bulk(); orderbook_manager() -> BookSource;
                   #   fetch_network_status(); quote_currencies
    bybit.py       # today's bybit_metadata + bybit_orderbook
    kraken.py      # kraken_metadata + kraken_orderbook + CG-verify hook
    gate.py        # gate_metadata + НОВИЙ gate book_ticker WS
```

`exchange_loader` → один generic `run_cex(adapter)`; `network_status._REFRESHERS` → `adapter.fetch_network_status`; `engine/main.py` — цикл по списку адаптерів з конфігу. CEX-CEX сканер тоді — тривіальний прохід по `entry["cex"]` парах у тому ж price_store (дані ВЖЕ є: bid/ask по кожній біржі лежать поряд).

### 5.3 DEX-протоколи

`dex_part/dex/` вже майже реєстр (`DEX_HANDLERS`). Доукомплектувати: `ProtocolSpec(topics, decode, compute_price, monitor_kind)` де `monitor_kind ∈ {per_pool_ws, singleton_ws}` — тоді `chain_monitor`/`v4_monitor` стають двома draiver-ами, а PancakeSwap Infinity (аналог V4 на BSC) додається одним записом. DEX-DEX напрям: сканер по `entry["dex"]` між чейнами/пулами того самого ключа — знову ж, дані вже в сторі.

### 5.4 Пакетна структура (цільова)

```
core/        # price_store, models, chains registry, config, logger, atomic_io, rate_limiter
adapters/
  cex/       # bybit, kraken, gate (метадані + книги + network status)
  dex/       # evm_v2, evm_v3, uniswap_v4 (+ infinity later)
services/    # discovery (dexscreener), liquidity (gt), coingecko, pool_refresh, depth
strategies/  # dex_cex (сьогоднішній scanner), cex_cex, dex_dex
app/         # main.py (композиція)
scripts/     # gt_probe, rpc_probe, v4_poc, pool_migrate CLI
tests/
```

Це НЕ переписування — це перекладання наявних файлів по місцях з виправленням імпортів; логіка не змінюється. Робити ПІСЛЯ Фаз 1–3, одним PR, з тестами Фази 2 як страховкою.

---

## 6. Пріоритезований план виконання

### Фаза 0 — git/секрети (день; робити ПЕРШОЮ)
| # | Дія | Закриває |
|---|-----|----------|
| 0.1 | Ротація ключів Bybit/Alchemy/NodeReal | 4.1 |
| 0.2 | Прибрати ключі з `engine/config.py` (публічні дефолти) | 3.1-P0 |
| 0.3 | `git rm --cached` секретів/зіпів/логів/кешів; видалити фізично зіпи, kyber_cache, старі логи, `cex_part/.env` | 4.1 |
| 0.4 | Orphan-коміт чистої історії; закомітити ВЕСЬ актуальний код | 4.1 |
| 0.5 | `.env`: прибрати `POOL_UPGRADE_MIN_RATIO`; синхронізувати `.env.example` | 4.1 |

### Фаза 1 — критичні баги коректності/надійності (2–4 дні)
| # | Дія | Закриває |
|---|-----|----------|
| 1.1 | Per-chain `WS_RESTART_EVENTS` + по-чейновий diff у pools_watcher | 3.16-P1 |
| 1.2 | TTL/age-гейти в сканері + `remove_dex_if_pool` при заміні пулу | 3.2-P1 |
| 1.3 | Bybit: контроль `u`/`seq`, re-subscribe при розриві (після верифікації доків) | 3.11-P1 |
| 1.4 | Нагляд за тасками: хелпер `spawn()` з done-callback + рестарт критичних лупів; таймаут на `ready.wait()` у `start_cex` | 3.8-P1 |
| 1.5 | Kraken: підняти лог невдалих підписок; перевірити altname vs wsname | 3.11-P2 |
| 1.6 | v4_monitor: reload до recv-гілки | 3.17-P2 |
| 1.7 | Довжино-валідація в v2/v3 декодерах | 3.20-P2 |
| 1.8 | Рішення по Gate: додати book_ticker WS (рекомендовано) або вимкнути полінг | 3.14-P1 |

### Фаза 2 — тести + чистка (2–3 дні)
| # | Дія | Закриває |
|---|-----|----------|
| 2.1 | pytest: декодери, price math, calculators, merge/resolve, `_maybe_upgrade_pool`, `extract_base_symbol` | 3.21-P1 |
| 2.2 | Видалити: `SWAP_STABLES`/`get_stable`, `REQUIRE_SYMBOL_EXISTS_IN_STORE`, `resolve_symbol_by_pool`, `RETRY_TTL_SEC`, `_BUCKETS_LOCK`, `kraken_unknown.json`-логіку (або задокументувати), закоментовані блоки blacklist | 3.19, 3.16-P3, 3.9-P3, 3.13-P3, 3.6, 3.10-P2 |
| 2.3 | Почистити `STABLES["bsc"]` (дублікат USDC, підписати lisUSD); DYDX/VSN → MANUAL_BLACKLIST | 3.19-P2, 3.15-P2 |
| 2.4 | test.py/test_rpc.py/_test_v4.py → `scripts/`; злити requirements у корені (прибрати `dex_part/requirements.txt`) | 3.21-P2 |
| 2.5 | Прохід по рівнях логування | 4.6 |

### Фаза 3 — продуктивність (3–5 днів)
| # | Дія | Закриває |
|---|-----|----------|
| 3.1 | `price_store`: iter-API без deepcopy; `has_dex_for_chain` | 3.2-P1(perf) |
| 3.2 | Усі `atomic_write_json` з async-контексту → `to_thread`; дебаунс liquidity/no_pools персисту | 4.3, 3.4-P2, 3.13-P2 |
| 3.3 | Кешований best bid/ask + мемоізація резолвінгу (cex,symbol)→key | 3.12-P2 |
| 3.4 | Фінгерпринт без ages; SpreadLogger з відкритим handle | 3.3-P1 |
| 3.5 | Довгоживучі aiohttp-сесії | 4.4 |
| 3.6 | `orjson` для WS-парсингу | 4.5 |
| 3.7 | Kraken: інкрементальні підписки замість повної перебудови шардів | 3.11-P2 |
| 3.8 | CG: точковий resolve для pending замість повних дампів | 3.5-P2 |

### Фаза 4 — масштабування / структура (1–2 тижні, після стабілізації)
| # | Дія | Закриває |
|---|-----|----------|
| 4.1 | Реєстр чейнів `ChainSpec` + виправлення 8 місць дублювання | 5.1, 4.2 |
| 4.2 | Розбиття `cache_manager` → registry/store/backfill з публічним API; заборона доступу до `_CACHE`/`_INDEX` ззовні | 3.9-P1, 3.4-P1 |
| 4.3 | CEX-адаптери + generic run-loop (готує CEX-CEX) | 5.2, 3.10-P1 |
| 4.4 | Розбиття `liquidity_filter` (gt_client / cache / upgrade) | 3.4-P1 |
| 4.5 | Пакетна реструктуризація (§5.4) + strategies/ (dex_cex сьогоднішній; каркаси cex_cex, dex_dex) | 5.3, 5.4 |
| 4.6 | Міграція web3 v7 (+ AsyncWeb3) — опційно, окремим кроком | 4.5 |
| 4.7 | Fallback RPC-ендпоінти на чейн | 4.5 |

---

## 7. Позначки «потребує перевірки»

1. Семантика Bybit v5 spot `u`/`seq` для orderbook-стріму — перевірити в офіційній документації перед 1.3.
2. Kraken WS v2: чи всі altnames зі списку Assets валідні як `SYMBOL/USD` — перевірити логами ack (крок 1.5).
3. Ліміт кількості адрес у `eth_subscribe logs`-фільтрі в Alchemy/NodeReal — перевірити перед чанкінгом (3.16-P2).
4. Твердження про поведінку зовнішніх API (DexScreener 300/min, GT free-tier, CG demo key) взяті з коментарів у коді та загальних знань — перед зміною лімітів звіритися з актуальними доками.

---

*Кінець звіту. Жоден рядок коду не змінено; єдиний створений файл — цей `AUDIT_REPORT.md`.*
