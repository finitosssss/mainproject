# Project Documentation - Funding & Strategy Monitor

Comprehensive guide for the bots and their MongoDB configurations.

---

## 🚀 Overview of Modules

### 1. `funding_monitor.py`
**Функционал**: Мониторинг ставок фандинга на биржах. Оповещает, когда ставка падает ниже критического порога. Имеет встроенную логику "временного окна" для проверки только перед расчетом фандинга.

### 2. `hedge_strategy.py`
**Функционал**: Мониторинг открытых хедж-позиций (Long/Short). Оповещает о риске ликвидации на любой из бирж и о невыгодности хеджа (когда суммарная стоимость фандинга становится отрицательной).

### 3. `volume_tracker.py`
**Функционал**: Отслеживание аномальных объемов и специфических паттернов (зеленые свечи определенной волатильности, массовые мелкие транзакции) в реальном времени.

### 4. `unique_strategy.py`
**Функционал**: Реализация продвинутых стратегий: "1-часовая манипуляция" (падение после серии красных свечей) и "Spot/Futures Pump" (обнаружение пампов на основе объема и формы свечей).

### 5. `trading_tools.py`
**Функционал**: Стратегия "Flash Crush". Использует комбинацию волатильности, объема и технических индикаторов (RSI, MACD) для обнаружения резких движений рынка.

### 6. `main.py`
**Функционал**: Точка входа. Запускает все вышеперечисленные боты параллельно в одном процессе.

---

## 📊 MongoDB Structures & Parameters

### 🟢 Funding Monitor Configuration
`Database: funding_monitor | Collection: funding_monitor_collection`

```json
{
  "global_tracking": true,     // Общий выключатель бота (true - включен)
  "tokens": {
    "TOKEN_NAME": {            // Название токена (например, "BTC", "ERA")
      "active": true,          // Включение/выключение мониторинга для этого токена
      "threshold": -0.1,       // Порог фандинга в %. Если ставка ниже - алерт.
      "time-funding_left": 10,  // Окно проверки (в минутах) ДО расчета фандинга
      "exchanges": ["Binance", "Bybit"] // Список бирж для проверки
    }
  }
}
```

### 🔵 Hedge Strategy Configuration
`Database: hedge_strategy | Collection: hedge_strategy_collection`

```json
{
  "global_tracking": true,
  "tokens": {
    "TOKEN_NAME": {
      "active": true,
      "exchanges": {
        "Binance": {
          "deals": {
            "1": {             // ID сделки (для связки Long и Short позиций)
              "active": true,
              "position": "short",      // Тип позиции: long или short
              "liquidation_price": 0.06, // Цена ликвидации на бирже
              "tokens": 100000          // Количество токенов в позиции
            }
          }
        }
      }
    }
  }
}
```

### 📈 Volume Tracker Configuration
`Database: volume_tracker | Collection: exchange_configs`
*Один документ может описывать одну или несколько бирж сразу.*

```json
{
  "global_tracking": true,
  "exchange": ["bybit", "okx"], // Теперь поддерживает как строку "bybit", так и список ["bybit", "okx"]
  "tokens": [
    {
      "symbol": "BTCUSDT",      // Стандартный тикер
      "bybit_symbol": "BTCUSDT", // Опционально: специфичный тикер для конкретной биржи
      "enabled": true,
      "minute_volume": 1000,    // Порог объема в $ за 1 минуту
      "volatility": 0.01,       // Порог волатильности (0.01 = 1%)
      "volty_handels": 0.003,   // Порог для детекции низкой волатильности (на 3-х свечах)
      "tokens_transactions": "100-500", // Поиск сделок в диапазоне кол-ва токенов
      "green_candles": {
        "count": 5,
        "volatility_thresholds": [0.001, 0.001, 0.001, 0.001, 0.001]
      }
    }
  ]
}
```
> [!NOTE]
> **Bybit Alpha (DEX Pro):** Для токенов из раздела Alpha, которые уже залистились на основной Spot (например, BSB, BASED), рекомендуется использовать `symbol` формата `BSBUSDT` и тип данных `exchange`. Это обеспечивает более стабильное получение данных.

---

## ⚙️ Environment Variables (.env)

Для корректной работы всех модулей в корне проекта должен быть файл `.env` со следующими параметрами:

```bash
# MongoDB Connection
MONGO_URI=mongodb://...

# Telegram Bot Tokens
VOLUME_TRACKER_TOKEN=...
TRADING_TOOLS_TOKEN=...
UNIQUE_STRATEGY_TOKEN=...
FUNDING_BOT_TOKEN=...
HEDGE_BOT_TOKEN=...

# Chat IDs
VOLUME_TRACKER_CHAT_IDS=-100...
TRADING_TOOLS_CHAT_IDS=-100...
UNIQUE_STRATEGY_CHAT_IDS=-100...
TELEGRAM_CHAT_ID=-100... # Общий для фандинга и хеджа

# Database & Collection Names
MONGO_DB_NAME_VOLUME_TRACKER=volume_tracker
MONGO_COLLECTION_NAME_VOLUME_TRACKER=exchange_configs
# ... и другие для каждого модуля
```

## 🛠 Deployment & Maintenance

Обновление и перезапуск контейнера на Azure выполняется через скрипт `redeploy.sh`:
```bash
bash redeploy.sh [container_name]
```
Скрипт автоматически собирает новый Docker-образ, пушит его в Azure Container Registry и обновляет Group Instance.

### 🔴 Unique Strategy Configuration
`Database: unique_strategy | Collection: unique_strategy_collection`

```json
{
  "global_tracking": true,
  "1hour_manipulation": [      // Список конфигов для стратегии 1H
    {
      "symbol": "BTCUSDT",
      "enabled": true,
      "min_volatility": 0.008, // Мин. волатильность последней свечи
      "red_candles": 5         // Кол-во красных свечей подряд перед алертом
    }
  ],
  "spot_futures_manipulation": [
    {
      "symbol": "all",         // "all" или конкретный символ
      "vol_multiplier": 2.5,   // Во сколько раз объем должен превысить средний
      "green_candles": 5       // Кол-во зеленых свечей подряд (памп)
    }
  ]
}
```

### ⚡ Trading Tools (Flash Crush)
`Database: trading_tools | Collection: trading_tools_collection`

```json
{
  "global_tracking": true,
  "flash_crush_strategy": [
    {
      "symbol": "BTCUSDT",
      "enabled": true,
      "timeframe": 1,          // Таймфрейм в минутах
      "min_volatility": 0.002, // Порог волатильности
      "volume": 30000,         // Минимальный объем (в токенах)
      "rsi": "on",             // "on"/"off" - использование RSI
      "rsi_low": 30,           // Нижняя граница RSI
      "rsi_high": 70,          // Верхняя граница RSI
      "macd": "on",            // "on"/"off" - использование MACD
      "macd_signal_type": "crossover" // Тип сигнала: crossover, divergence, zero_cross
    }
  ]
}
```