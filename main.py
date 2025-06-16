from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
import os, asyncio, json, threading, time, requests, base64, atexit, logging
from functools import lru_cache
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs, OrderType, BalanceAllowanceParams, AssetType, BookParams, MarketOrderArgs
)
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.exceptions import PolyApiException

load_dotenv()

# ========== LOGGING SETUP ==========
# Create logs directory if it doesn't exist
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'polymarket-bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========== ENVIRONMENT VARIABLES ==========
# Load secrets from environment variables for security
key = os.getenv("POLYMARKET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# ========== CONFIGURATION ==========
host = "https://clob.polymarket.com"
chain_id = 137
POLYMARKET_PROXY_ADDRESS: str = '0x8318eb2fc77f5830c6f99280d26ecf0dc239e497'

client = ClobClient(host, key=key, chain_id=chain_id, signature_type=2, funder=POLYMARKET_PROXY_ADDRESS)
client.set_api_creds(client.create_or_derive_api_creds())

SUBGRAPH_GRAPHQL_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"

chat_thresholds: dict[str, float] = {}
known_chats: set[str] = set()      # gets populated on first contact

# Rate limiting globals
_rate_lock = asyncio.Lock()
_last_markets_call = 0.0
_MIN_INTERVAL = 10.0 / 50.0   # = 0.2 seconds between calls

# Global stop event for graceful shutdown
stop_event = threading.Event()

# Cache dictionaries
_market_slug_cache: dict[str, str] = {}
_token_outcome_cache: dict[str, str] = {}

# Memory management
MAX_CACHE_SIZE = 10000

# ========== HELPER FUNCTIONS ==========

async def register_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Runs for every update; remembers the chat ID."""
    chat_id = str(update.effective_chat.id)
    known_chats.add(chat_id)

async def send_telegram_message_to(dest_chat_id: str, message: str):
    """Send one message to exactly one Telegram chat."""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("No bot token; skipping message")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": dest_chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        await asyncio.to_thread(requests.post, url, json=payload, timeout=10)
    except Exception as e:
        logger.error("send_to_chat %s failed: %s", dest_chat_id, e)

async def set_threshold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    known_chats.add(chat_id)
    if not context.args:
        await context.bot.send_message(chat_id=chat_id,
            text="Usage: /setthreshold <amount>\nExample: /setthreshold 1000")
        return

    try:
        v = float(context.args[0])
    except ValueError:
        await context.bot.send_message(chat_id=chat_id,
            text="❌ Threshold must be a number.  Try again.")
        return

    chat_thresholds[chat_id] = v
    await context.bot.send_message(chat_id=chat_id,
        text=f"✅ Alert threshold set to **${v:,.2f}**", parse_mode="Markdown")

async def show_threshold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    if chat_id not in chat_thresholds:
        await update.message.reply_text(
            "⚠️ You don't have a threshold set yet. Use /setthreshold <amount> to set one."
        )
    else:
        val = chat_thresholds[chat_id]
        await update.message.reply_text(f"Current threshold: ${val:.2f}")

async def rate_limited_get_markets(next_cursor: str = None) -> dict:
    """Calls client.get_markets but ensures we never exceed 50 calls per 10s."""
    global _last_markets_call
    async with _rate_lock:
        now = time.monotonic()
        elapsed = now - _last_markets_call
        if elapsed < _MIN_INTERVAL:
            await asyncio.sleep(_MIN_INTERVAL - elapsed)
        
        resp = await asyncio.to_thread(
            client.get_markets,
            next_cursor=str(next_cursor) if next_cursor else None
        )
        _last_markets_call = time.monotonic()
        return resp

async def get_market_slug(asset_id: str, *, max_pages: int = 200) -> str:
    """REST-only slug lookup with caching."""
    if asset_id in _market_slug_cache:
        return _market_slug_cache[asset_id]

    page_size, offset, pages = 100, 0, 0
    while pages < max_pages:
        cursor = base64.b64encode(str(offset).encode()).decode()
        try:
            resp = await rate_limited_get_markets(next_cursor=cursor)
            markets = resp.get("data", [])
        except Exception as e:
            logger.error("Slug lookup error page %d: %s", pages + 1, e)
            break

        if not markets:
            break

        for m in markets:
            for tok in m.get("tokens", []):
                if str(tok.get("token_id")) == asset_id:
                    slug = m.get("market_slug", "N/A")
                    _market_slug_cache[asset_id] = slug
                    logger.debug("Found slug '%s' for asset %s", slug, asset_id[:16])
                    return slug

        offset += page_size
        pages += 1
        await asyncio.sleep(0.1)

    logger.warning("Slug not found for asset %s after %d pages", asset_id[:16], pages)
    _market_slug_cache[asset_id] = "N/A"
    return "N/A"

async def _send_large_trade_alert(chat_id: str, tx_hash: str, info: dict):
    """Background task that sends Telegram alerts for large trades."""
    try:
        slug = await get_market_slug(info["asset"])
        market_url = (f"https://polymarket.com/market/{slug}" if slug != "N/A" else "N/A")

        outcome = _token_outcome_cache.get(info["asset"], "UNKNOWN")
        if outcome == "UNKNOWN":
            logger.warning("Unknown token %s - skipping alert", info["asset"][:16] + "...")
            return

        message = (
            "🚨 <b>Trade Alert!</b> 🚨\n"
            f"{info['side']}: {outcome}\n"
            f"Price: {info['price']:.3f}\n"
            f"Total Shares: {info['size']:.2f}\n"
            f"Notional Value: ${info['notional']:,.2f}\n"
            f'<a href="https://polygonscan.com/tx/{tx_hash}">🔗 Tx Hash</a>\n'
            f'<a href="https://polymarketanalytics.com/traders/{info["maker"]}">👤 Maker</a>\n'
            f'<a href="{market_url}">🎰 Market</a>\n'
        )

        logger.info("ALERT: %s %s %s shares, $%.2f", 
                   tx_hash[:8], info['side'], outcome, info['notional'])
        await send_telegram_message_to(chat_id, message)
        
    except Exception as e:
        logger.error("Error sending alert for tx %s: %s", tx_hash[:8], e)

def clean_old_cache_entries():
    """Remove old cache entries if cache gets too large."""
    if len(_market_slug_cache) > MAX_CACHE_SIZE:
        items = list(_market_slug_cache.items())
        _market_slug_cache.clear()
        _market_slug_cache.update(items[-MAX_CACHE_SIZE//2:])
        logger.info("Cache cleaned. New size: %d", len(_market_slug_cache))

async def trade_monitor():
    """Monitors Polymarket CLOB trades and sends alerts for large trades."""
    logger.info("Trade monitor active.")

    last_processed_trade_timestamp = 0
    MAX_TRADES_PER_QUERY = 250
    processed_tx_hashes: set[str] = set()
    missing_threshold_notified: set[str] = set()

    GRAPHQL_QUERY = """
    query GetRecent($lastTimestamp: BigInt!, $first: Int!) {
      orderFilledEvents(
        where:{ timestamp_gt:$lastTimestamp }
        orderBy: timestamp, orderDirection: asc, first:$first
      ){
        timestamp 
        transactionHash
        orderHash
        maker
        taker
        makerAssetId 
        takerAssetId
        makerAmountFilled
        takerAmountFilled 
      }
    }
    """

    # Initialize resume point
    try:
        init_q = """{ orderFilledEvents(orderBy:timestamp,orderDirection:desc, first:1){ timestamp } }"""
        data = (await asyncio.to_thread(
            requests.post, SUBGRAPH_GRAPHQL_URL,
            json={'query': init_q}, timeout=10)).json()
        latest = data.get('data', {}).get('orderFilledEvents')
        if latest:
            last_processed_trade_timestamp = int(latest[0]['timestamp'])
            logger.info("Starting from timestamp: %d", last_processed_trade_timestamp)
    except Exception as e:
        logger.warning("Init error, starting from 0: %s", e)

    # Main polling loop
    while not stop_event.is_set():
        try:
            vars_ = {
                "lastTimestamp": str(last_processed_trade_timestamp),
                "first": MAX_TRADES_PER_QUERY
            }
            resp = await asyncio.to_thread(
                requests.post,
                SUBGRAPH_GRAPHQL_URL,
                json={'query': GRAPHQL_QUERY, 'variables': vars_},
                timeout=15
            )
            data = resp.json()
            
            # ── handle subgraph / network errors ─────────────────────────
            if "errors" in data:
                # log the full payload once
                logger.error("GraphQL errors: %s", data["errors"])

                # if it was a re-org, step the cursor back ~30 s
                first_msg = data["errors"][0].get("message", "").lower()
                if "reorganize" in first_msg or "reorganized" in first_msg:
                    rewind = 30             # seconds
                    last_processed_trade_timestamp = max(
                        0, last_processed_trade_timestamp - rewind
                    )
                    logger.warning(
                        "Chain re-org detected. Rewound cursor by %d s to %d",
                        rewind, last_processed_trade_timestamp,
                    )

                # brief back-off, then try the query again
                await asyncio.sleep(5)
                continue


            trades = data.get('data', {}).get('orderFilledEvents', [])
            if not isinstance(trades, list):
                await asyncio.sleep(5)
                continue

            batch_max_ts = last_processed_trade_timestamp
            
            for tr in trades:
                ts = int(tr['timestamp'])
                if ts <= last_processed_trade_timestamp:
                    continue
                batch_max_ts = max(batch_max_ts, ts)

                tx_hash = tr["transactionHash"]
                if tx_hash in processed_tx_hashes:
                    continue

                maker = tr["maker"]
                taker = tr["taker"]
                maker_asset = tr["makerAssetId"]
                taker_asset = tr["takerAssetId"]
                maker_amt = float(tr["makerAmountFilled"]) / 1e6
                taker_amt = float(tr["takerAmountFilled"]) / 1e6

                # Only track maker actions
                if maker_asset == "0":
                    # Maker gives USDC → Maker is BUYING outcome tokens
                    side = "BUY"
                    notional = maker_amt
                    size = taker_amt
                    outcome_asset = taker_asset
                elif taker_asset == "0":
                    # Maker gives outcome tokens → Maker is SELLING outcome tokens  
                    side = "SELL"
                    notional = taker_amt
                    size = maker_amt
                    outcome_asset = maker_asset
                else:
                    # No USDC involved → skip
                    continue

                for chat_id in list(known_chats):
                
                    if chat_id not in chat_thresholds:
                        if chat_id not in missing_threshold_notified:
                            await send_telegram_message_to(chat_id, "👉 Please set your threshold with /setthreshold to receive alerts.")
                            missing_threshold_notified.add(chat_id)
                        continue

                    thresh = chat_thresholds[chat_id]
                    if notional < thresh:
                        continue

                    # Send alert
                    processed_tx_hashes.add(tx_hash)
                    logger.info(
                        "Alert → %s | side=%s | notional=%.2f",
                        chat_id, side, notional
                    )

                    asyncio.create_task(_send_large_trade_alert(chat_id, tx_hash, {
                        "maker": maker,
                        "taker": taker,
                        "makerAssetId": maker_asset,
                        "takerAssetId": taker_asset,
                        "makerAmount": maker_amt,
                        "takerAmount": taker_amt,
                        "side": side,
                        "price": (notional / size) if size else 0,
                        "size": size,
                        "notional": notional,
                        "asset": outcome_asset,
                        "first_ts": ts
                }))

            last_processed_trade_timestamp = batch_max_ts

        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            logger.error("Network/JSON error: %s. Retrying in 5s", e)
            await asyncio.sleep(5)
        except Exception as e:
            logger.error("Unexpected error: %s. Retrying in 5s", e)
            await asyncio.sleep(5)

        await asyncio.sleep(1)

async def warm_market_slug_cache(max_pages: int = 500) -> None:
    """Scan markets and cache token mappings for live markets only."""
    page_size, offset, pages_seen, retries = 100, 0, 0, 0
    total_tokens_processed = 0
    
    logger.info("Starting market cache warming (max %d pages)", max_pages)
    
    while pages_seen < max_pages:
        cursor = base64.b64encode(str(offset).encode()).decode()
        try:
            resp = await rate_limited_get_markets(next_cursor=cursor)
            retries = 0
        except Exception as e:
            if retries < 3:
                retries += 1
                logger.warning("Cache warming page %d error: %s (retry %d/3)", 
                              pages_seen + 1, e, retries)
                await asyncio.sleep(1)
                continue
            logger.error("Cache warming failed at page %d: %s", pages_seen + 1, e)
            break
            
        markets = resp.get("data", [])
        if not markets:
            logger.info("No more markets found, stopping at page %d", pages_seen + 1)
            break
            
        page_tokens = 0
        for mkt in markets:
            if mkt.get("active") and not mkt.get("closed"):
                slug = mkt.get("market_slug", "")
                tokens = mkt.get("tokens", [])
                
                for tok in tokens:
                    token_id = str(tok.get("token_id", ""))
                    outcome_label = tok.get("outcome", "").upper()
                    
                    if not token_id or not outcome_label:
                        continue
                        
                    _market_slug_cache[token_id] = slug
                    _token_outcome_cache[token_id] = outcome_label
                    
                    page_tokens += 1
                    total_tokens_processed += 1
        
        if (pages_seen + 1) % 10 == 0 or page_tokens > 100:
            logger.info("Cache warming: page %d, processed %d tokens this page", 
                       pages_seen + 1, page_tokens)
        
        await asyncio.sleep(0.25)
        offset += page_size
        pages_seen += 1
    
    logger.info("Cache warming complete: %s slugs, %s outcomes from %d pages", 
                f"{len(_market_slug_cache):,}", 
                f"{len(_token_outcome_cache):,}", 
                pages_seen)
    
    if len(_market_slug_cache) > MAX_CACHE_SIZE:
        logger.warning("Large cache size: %d entries - consider cleanup", 
                      len(_market_slug_cache))

async def periodic_cache_refresh():
    """Refresh cache periodically and clean up if needed."""
    while not stop_event.is_set():
        await asyncio.sleep(3600)  # Every hour
        logger.info("Starting periodic cache refresh")
        await warm_market_slug_cache()
        clean_old_cache_entries()
        save_slug_cache()

# Cache persistence
CACHE_FILE = Path("slug_cache.json")

def load_slug_cache() -> None:
    """Load cached slugs from disk."""
    try:
        if CACHE_FILE.exists():
            data = json.loads(CACHE_FILE.read_text())
            _market_slug_cache.update(data)
            logger.info("Loaded %d slugs from disk cache", len(_market_slug_cache))
    except Exception as e:
        logger.error("Cache load failed: %s", e)

def save_slug_cache() -> None:
    """Save cached slugs to disk."""
    try:
        CACHE_FILE.write_text(json.dumps(_market_slug_cache))
        logger.info("Saved %d slugs to disk cache", len(_market_slug_cache))
    except Exception as e:
        logger.error("Cache save failed: %s", e)

atexit.register(save_slug_cache)

def run_goldsky_trade_monitor() -> None:
    """Main entry point for the trade monitor bot, with per-chat threshold commands."""
    logger.info("Starting Polymarket Trade Monitor Bot")

    # Validate required environment variables
    if not key:
        logger.error("POLYMARKET_KEY environment variable not set")
        return
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN environment variable not set")
        return

    # Build Telegram application and register command handlers
    application = Application.builder()\
        .token(TELEGRAM_BOT_TOKEN)\
        .build()
    application.add_handler(
        MessageHandler(filters.ALL, register_chat),   # function from previous reply
        group=-1                                      # runs before the others
    )
    application.add_handler(CommandHandler("setthreshold", set_threshold))
    application.add_handler(CommandHandler("showthreshold", show_threshold))

    load_slug_cache()

    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # 1) Warm cache before starting monitor
            loop.run_until_complete(warm_market_slug_cache())

            # 2) Start background tasks
            loop.create_task(periodic_cache_refresh())
            loop.create_task(trade_monitor())

            # 3) Initialize and start the Telegram bot
            loop.run_until_complete(application.initialize())
            loop.run_until_complete(application.start())

            loop.run_until_complete(application.updater.start_polling())


            logger.info("Bot started successfully. Cache: %d entries", len(_market_slug_cache))

            # 4) Run forever (both monitor and Telegram live on this loop)
            loop.run_forever()

        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
            break

        except Exception as e:
            retry_count += 1
            logger.error("Bot crashed: %s. Retry %d/%d", e, retry_count, max_retries)
            if retry_count < max_retries:
                time.sleep(30)
            else:
                logger.critical("Max retries reached. Bot stopping.")
        finally:
            stop_event.set()
            save_slug_cache()

            # cleanly shut down Telegram app
            try:
                loop.run_until_complete(application.stop())
                loop.run_until_complete(application.shutdown())
            except Exception as e:
                logger.error("Error shutting down Telegram application: %s", e)

            # cancel any remaining tasks
            for task in asyncio.all_tasks(loop):
                task.cancel()
            try:
                loop.run_until_complete(
                    asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True)
                )
            except Exception:
                pass

            loop.close()
            logger.info("Bot shutdown complete")

if __name__ == "__main__":
    run_goldsky_trade_monitor()