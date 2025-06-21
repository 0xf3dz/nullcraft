#!/usr/bin/env python3
"""
Polymarket Trade Monitor Bot with Indexing Lag Protection
Monitors OrderFilled events and sends Telegram alerts for large TRADE events.
Includes protection against subgraph indexing delays that can cause missed transactions.
"""

from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
import os, asyncio, json, threading, time, base64, atexit, logging, psutil
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs, OrderType, BalanceAllowanceParams, AssetType, BookParams, MarketOrderArgs
)
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.exceptions import PolyApiException
import aiohttp

load_dotenv()

# ========== LOGGING SETUP ==========
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
key = os.getenv("POLYMARKET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# ========== CONFIGURATION ==========
host = "https://clob.polymarket.com"
chain_id = 137
POLYMARKET_PROXY_ADDRESS: str = '0x8318eb2fc77f5830c6f99280d26ecf0dc239e497'
POLYMARKET_DATA_API_URL = "https://data-api.polymarket.com/activity"

client = ClobClient(host, key=key, chain_id=chain_id, signature_type=2, funder=POLYMARKET_PROXY_ADDRESS)
client.set_api_creds(client.create_or_derive_api_creds())

SUBGRAPH_GRAPHQL_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"

# ========== LAG PROTECTION CONFIGURATION ==========
INDEXING_LAG_BUFFER = 10      # Look back 10 seconds to catch delayed indexing
TIMESTAMP_SAFETY_MARGIN = 30  # Keep 30 second safety margin on startup
MAX_PROCESSED_HASHES = 10000  # Memory limit for processed transactions
HASH_CLEANUP_AGE = 7200       # Remove hashes older than 2 hours (7200 seconds)

# ========== HTTP SESSION AND SEMAPHORE ==========
_http_session: aiohttp.ClientSession = None
_http_semaphore = asyncio.Semaphore(8)

async def get_http_session():
    """Get or create the shared HTTP session with connection pooling."""
    global _http_session
    if _http_session is None:
        connector = aiohttp.TCPConnector(
            limit=15,
            limit_per_host=8,
            ttl_dns_cache=300,
            use_dns_cache=True,
            enable_cleanup_closed=True
        )
        _http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=15)
        )
    return _http_session

async def close_http_session():
    """Properly close the HTTP session."""
    global _http_session
    if _http_session:
        await _http_session.close()
        _http_session = None

# ========== CHAT TRACKING ==========
chat_thresholds: dict[str, float] = {}
known_chats: set[str] = set()
DATA_DIR = Path(__file__).resolve().parent
CHAT_FILE = DATA_DIR / "known_chats.txt"

def load_known_chats():
    """Load known chats from file with error handling."""
    try:
        if CHAT_FILE.exists():
            with open(CHAT_FILE, "r") as f:
                chats = [line.strip() for line in f if line.strip()]
                known_chats.update(chats)
            logger.info("Loaded %d known chats from %s", len(chats), CHAT_FILE)
        else:
            logger.info("Chat file doesn't exist yet: %s", CHAT_FILE)
    except Exception as e:
        logger.error("Error loading known chats: %s", e)

load_known_chats()

def save_chat_id(chat_id: str):
    """Save a new chat ID to file with error handling."""
    try:
        # Ensure directory exists
        DATA_DIR.mkdir(exist_ok=True)
        
        # Append chat ID to file
        with open(CHAT_FILE, "a") as f:
            f.write(chat_id + "\n")
            f.flush()  # Force write to disk
        
        logger.info("✅ Saved new chat ID %s to %s", chat_id, CHAT_FILE)
        
        # Verify it was written
        if CHAT_FILE.exists():
            size = CHAT_FILE.stat().st_size
            logger.debug("Chat file size after write: %d bytes", size)
        
    except Exception as e:
        logger.error("❌ Error saving chat ID %s: %s", chat_id, e)

def save_all_known_chats():
    """Save all known chats to file (overwrites existing file)."""
    try:
        DATA_DIR.mkdir(exist_ok=True)
        
        with open(CHAT_FILE, "w") as f:
            for chat_id in sorted(known_chats):
                f.write(chat_id + "\n")
            f.flush()
        
        logger.info("✅ Saved %d chat IDs to %s", len(known_chats), CHAT_FILE)
        
    except Exception as e:
        logger.error("❌ Error saving all chat IDs: %s", e)

# ========== RATE LIMITING ==========
_api_call_times = []
_MAX_API_CALLS_PER_MINUTE = 20

async def check_api_rate_limit():
    """Ensure we don't exceed API rate limits."""
    global _api_call_times
    now = time.time()
    
    # Remove calls older than 1 minute
    _api_call_times = [t for t in _api_call_times if now - t < 60]
    
    # If we're at the limit, wait
    if len(_api_call_times) >= _MAX_API_CALLS_PER_MINUTE:
        wait_time = 60 - (now - _api_call_times[0]) + 1
        logger.warning("Rate limit reached, waiting %.1f seconds", wait_time)
        await asyncio.sleep(wait_time)
        _api_call_times = []
    
    # Record this call
    _api_call_times.append(now)

# Global stop event for graceful shutdown
stop_event = threading.Event()

# ========== PROCESSED TRANSACTIONS TRACKING ==========
processed_tx_hashes: dict[str, float] = {}  # tx_hash -> timestamp when processed
lag_protection_metrics = {
    'total_recoveries': 0,
    'last_cleanup_time': time.time(),
    'max_set_size': 0,
    'cleanup_count': 0
}

def manage_processed_hashes_memory():
    """
    Manage memory usage of processed_tx_hashes set.
    Remove old entries and track memory statistics.
    """
    global processed_tx_hashes, lag_protection_metrics
    
    current_time = time.time()
    initial_size = len(processed_tx_hashes)
    
    # Remove hashes older than HASH_CLEANUP_AGE
    cutoff_time = current_time - HASH_CLEANUP_AGE
    old_hashes = [tx_hash for tx_hash, timestamp in processed_tx_hashes.items() 
                  if timestamp < cutoff_time]
    
    for tx_hash in old_hashes:
        del processed_tx_hashes[tx_hash]
    
    # If still too large, remove oldest 50%
    if len(processed_tx_hashes) > MAX_PROCESSED_HASHES:
        sorted_hashes = sorted(processed_tx_hashes.items(), key=lambda x: x[1])
        keep_count = MAX_PROCESSED_HASHES // 2
        
        # Keep newest half
        to_remove = [tx_hash for tx_hash, _ in sorted_hashes[:-keep_count]]
        for tx_hash in to_remove:
            del processed_tx_hashes[tx_hash]
        
        logger.warning("Memory cleanup: removed %d additional hashes (size limit reached)", 
                      len(to_remove))
    
    # Update metrics
    final_size = len(processed_tx_hashes)
    lag_protection_metrics['max_set_size'] = max(lag_protection_metrics['max_set_size'], initial_size)
    lag_protection_metrics['cleanup_count'] += 1
    lag_protection_metrics['last_cleanup_time'] = current_time
    
    if old_hashes or initial_size != final_size:
        # Get memory usage
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        logger.info("Memory cleanup: %d → %d hashes (removed %d old, %d total), Memory: %.1f MB", 
                   initial_size, final_size, len(old_hashes), 
                   initial_size - final_size, memory_mb)

# ========== TRADE INFO LOOKUP ==========
_trade_info_cache: dict[str, dict] = {}
_failed_lookups: dict[str, float] = {}

async def get_trade_info_from_api(tx_hash: str, maker_address: str) -> dict:
    """
    Get trade information from Polymarket Data API using maker address.
    FIXED: Now includes required 'user' parameter.
    """
    
    # Check cache first
    if tx_hash in _trade_info_cache:
        return _trade_info_cache[tx_hash]
    
    # Check if we recently failed to look up this tx
    if tx_hash in _failed_lookups:
        if time.time() - _failed_lookups[tx_hash] < 300:  # 5 minute cooldown
            return None
    
    try:
        await check_api_rate_limit()
        
        session = await get_http_session()
        # FIXED: Include required 'user' parameter
        params = {
            "limit": "100",
            "sortDirection": "DESC", 
            "user": maker_address,  # ← This was missing!
            "type": "TRADE"
        }
        
        async with _http_semaphore:
            async with session.get(POLYMARKET_DATA_API_URL, params=params) as response:
                if response.status != 200:
                    logger.warning("Data API returned %d for tx %s, maker %s", 
                                  response.status, tx_hash[:8], maker_address[:8])
                    _failed_lookups[tx_hash] = time.time()
                    return None
                
                data = await response.json()
                logger.debug("API returned %d trades for maker %s", len(data), maker_address[:8])
                
                # Look for matching transaction hash
                for trade in data:
                    if trade.get("transactionHash") == tx_hash and trade.get("type") == "TRADE":
                        trade_info = {
                            "slug": trade.get("slug", "N/A"),
                            "title": trade.get("title", "N/A"),
                            "outcome": trade.get("outcome", "UNKNOWN"),
                            "side": trade.get("side", "UNKNOWN"),
                            "price": trade.get("price", 0),
                            "size": trade.get("size", 0),
                            "usdcSize": trade.get("usdcSize", 0),
                            "type": trade.get("type"),
                            "icon": trade.get("icon", ""),
                            "name": trade.get("name", "Anonymous"),
                            "proxyWallet": trade.get("proxyWallet", ""),
                            "_cached_at": time.time()
                        }
                        
                        _trade_info_cache[tx_hash] = trade_info
                        logger.debug("✅ Found TRADE info for tx %s: %s", tx_hash[:8], trade_info["slug"])
                        return trade_info
                
                # No matching TRADE found
                logger.debug("❌ No TRADE type found for tx %s in %d results", tx_hash[:8], len(data))
                _failed_lookups[tx_hash] = time.time()
                return None
                
    except Exception as e:
        logger.error("Error looking up trade info for tx %s: %s", tx_hash[:8], e)
        _failed_lookups[tx_hash] = time.time()
        return None

# ========== TELEGRAM FUNCTIONS ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    
    logger.info("🆕 /start command received from chat %s", chat_id)
    
    # Add to memory
    if chat_id not in known_chats:
        known_chats.add(chat_id)
        
        # Save to file immediately
        save_chat_id(chat_id)
        
        logger.info("🆕 New chat registered: %s (total: %d)", chat_id, len(known_chats))
    else:
        logger.info("🔄 Existing chat started: %s", chat_id)

    await context.bot.send_message(
        chat_id=chat_id, 
        text="👋 Bot activated! You'll now receive alerts for TRADE events only.\n"
             "🛡️ Enhanced with indexing lag protection to prevent missed transactions.\n"
             f"📝 Your chat ID {chat_id} has been saved to file.\n"
             f"💾 Total known chats: {len(known_chats)}"
    )

async def register_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Runs for every update; remembers the chat ID."""
    chat_id = str(update.effective_chat.id)
    
    if chat_id not in known_chats:
        known_chats.add(chat_id)
        save_chat_id(chat_id)
        logger.info("🔄 Auto-registered new chat: %s", chat_id)

async def send_telegram_message_to(dest_chat_id: str, message: str):
    """Send message to Telegram chat with error handling."""
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
    
    async with _http_semaphore:
        try:
            session = await get_http_session()
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.warning("Telegram API returned %d: %s", resp.status, error_text[:200])
                else:
                    logger.debug("Telegram message sent successfully to %s", dest_chat_id)
        except Exception as e:
            logger.error("send_to_chat %s failed: %s", dest_chat_id, e)

async def set_threshold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    
    # Register chat if not already known
    if chat_id not in known_chats:
        known_chats.add(chat_id)
        save_chat_id(chat_id)
        logger.info("🔄 Registered chat via /setthreshold: %s", chat_id)
    
    if not context.args:
        await context.bot.send_message(chat_id=chat_id,
            text="Usage: /setthreshold <amount>\nExample: /setthreshold 1000")
        return

    try:
        v = float(context.args[0])
    except ValueError:
        await context.bot.send_message(chat_id=chat_id,
            text="❌ Threshold must be a number. Try again.")
        return

    chat_thresholds[chat_id] = v
    await context.bot.send_message(chat_id=chat_id,
        text=f"✅ Alert threshold set to **${v:,.2f}**\n"
             f"🛡️ Lag protection will ensure no qualifying trades are missed.", 
        parse_mode="Markdown")

async def show_threshold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    
    # Register chat if not already known
    if chat_id not in known_chats:
        known_chats.add(chat_id)
        save_chat_id(chat_id)
        logger.info("🔄 Registered chat via /showthreshold: %s", chat_id)
    
    if chat_id not in chat_thresholds:
        await update.message.reply_text(
            "⚠️ You don't have a threshold set yet. Use /setthreshold <amount> to set one."
        )
    else:
        val = chat_thresholds[chat_id]
        await update.message.reply_text(f"Current threshold: ${val:.2f}")

def debug_chat_file_location():
    """Debug function to check chat file location and permissions."""
    import os
    from pathlib import Path
    
    print(f"Current working directory: {os.getcwd()}")
    print(f"Script file location: {Path(__file__).resolve()}")
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"CHAT_FILE path: {CHAT_FILE}")
    print(f"CHAT_FILE exists: {CHAT_FILE.exists()}")
    
    # Check if directory is writable
    try:
        test_file = DATA_DIR / "test_write.tmp"
        test_file.write_text("test")
        test_file.unlink()
        print(f"Directory is writable: Yes")
    except Exception as e:
        print(f"Directory is writable: No - {e}")
    
    # Show current known chats
    print(f"Known chats in memory: {known_chats}")
    
    # If file exists, show contents
    if CHAT_FILE.exists():
        try:
            content = CHAT_FILE.read_text()
            print(f"File contents: {repr(content)}")
        except Exception as e:
            print(f"Error reading file: {e}")

async def debug_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug command with enhanced chat file information."""
    chat_id = str(update.effective_chat.id)
    
    # Register chat if not already known
    if chat_id not in known_chats:
        known_chats.add(chat_id)
        save_chat_id(chat_id)
        logger.info("🔄 Registered chat via /debug: %s", chat_id)
    
    # Basic status
    is_known = chat_id in known_chats
    threshold = chat_thresholds.get(chat_id, "Not set")
    
    # Cache and memory stats
    cache_size = len(_trade_info_cache)
    failed_lookups = len(_failed_lookups)
    processed_hashes_count = len(processed_tx_hashes)
    
    # Memory usage
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    
    # Lag protection metrics
    lag_metrics = lag_protection_metrics
    
    # Chat file info
    chat_file_exists = CHAT_FILE.exists()
    chat_file_size = CHAT_FILE.stat().st_size if chat_file_exists else 0
    
    # Read current file contents
    file_contents = ""
    if chat_file_exists:
        try:
            file_contents = CHAT_FILE.read_text().strip()
        except Exception as e:
            file_contents = f"Error reading: {e}"
    
    message = (
        f"🔍 <b>Debug Status</b>\n"
        f"Chat ID: <code>{chat_id}</code>\n"
        f"Known chat: {is_known}\n"
        f"Threshold: {threshold if threshold == 'Not set' else f'${threshold:.2f}'}\n\n"
        
        f"📊 <b>Cache Status:</b>\n"
        f"Trade info cache: {cache_size}\n"
        f"Failed lookups: {failed_lookups}\n"
        f"Total known chats: {len(known_chats)}\n\n"
        
        f"💾 <b>Chat File:</b>\n"
        f"File exists: {chat_file_exists}\n"
        f"File size: {chat_file_size} bytes\n"
        f"File contents: <code>{file_contents}</code>\n\n"
        
        f"🛡️ <b>Lag Protection:</b>\n"
        f"Processed transactions: {processed_hashes_count:,}\n"
        f"Total recoveries: {lag_metrics['total_recoveries']:,}\n"
        f"Max set size: {lag_metrics['max_set_size']:,}\n"
        f"Memory cleanups: {lag_metrics['cleanup_count']}\n\n"
        
        f"💾 <b>Memory:</b>\n"
        f"Process memory: {memory_mb:.1f} MB\n"
        f"Lag buffer: {INDEXING_LAG_BUFFER}s\n"
        f"Hash cleanup age: {HASH_CLEANUP_AGE//3600}h"
    )
    
    await context.bot.send_message(chat_id=chat_id, text=message, parse_mode="HTML")

async def _send_large_trade_alert(chat_id: str, tx_hash: str, trade_info: dict, calculated_notional: float):
    """Send Telegram alert with maker profile link."""
    try:
        market_url = (f"https://polymarket.com/market/{trade_info['slug']}" 
                     if trade_info['slug'] != "N/A" else "N/A")
        
        # Create maker profile link using proxyWallet if available
        maker_wallet = trade_info.get('proxyWallet', '')
        maker_name = trade_info.get('name', 'Anonymous')
        
        if maker_wallet:
            maker_link = f'<a href="https://polymarketanalytics.com/traders/{maker_wallet}">{maker_name}</a>'
        else:
            maker_link = maker_name

        message = (
            "🚨 <b>Trade Alert!</b> 🚨\n"
            f"<b>{trade_info['title'][:60]}{'...' if len(trade_info['title']) > 60 else ''}</b>\n"
            f"{trade_info['side']}: {trade_info['outcome']}\n"
            f"Price: ${trade_info['price']:.3f}\n"
            f"Size: {trade_info['size']:,.0f} shares\n"
            f"Value: ${trade_info['usdcSize']:,.2f}\n"
            f"Trader: {maker_link}\n"
            f'<a href="https://polygonscan.com/tx/{tx_hash}">🔗 Transaction</a>\n'
            f'<a href="{market_url}">🎰 Market</a>\n'
        )

        logger.info("ALERT: %s %s %s, $%.2f by %s", 
                   tx_hash[:8], trade_info['side'], trade_info['outcome'], 
                   trade_info['usdcSize'], maker_name)
        await send_telegram_message_to(chat_id, message)
        
    except Exception as e:
        logger.error("Error sending alert for tx %s: %s", tx_hash[:8], e)

# ========== GRAPHQL FUNCTIONS ==========
async def make_graphql_request(query: str, variables: dict = None) -> dict:
    """Make GraphQL request with proper async HTTP and error handling."""
    async with _http_semaphore:
        try:
            session = await get_http_session()
            payload = {"query": query}
            if variables:
                payload["variables"] = variables
                
            async with session.post(SUBGRAPH_GRAPHQL_URL, json=payload) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error("GraphQL request failed with status %d: %s", resp.status, error_text[:200])
                    return {"errors": [{"message": f"HTTP {resp.status}"}]}
                
                return await resp.json()
                
        except asyncio.TimeoutError:
            logger.error("GraphQL request timed out")
            return {"errors": [{"message": "Request timeout"}]}
        except Exception as e:
            logger.error("GraphQL request failed: %s", e)
            return {"errors": [{"message": str(e)}]}

# ========== MAIN TRADE MONITOR ==========

async def trade_monitor_with_lag_protection():
    """
    FIXED: Back to per-maker API calls with lag protection and loop prevention.
    """
    logger.info("Trade monitor with lag protection active (per-maker API)")
    logger.info("Lag buffer: %ds, Safety margin: %ds, Max hashes: %d", 
               INDEXING_LAG_BUFFER, TIMESTAMP_SAFETY_MARGIN, MAX_PROCESSED_HASHES)

    last_processed_trade_timestamp = 0
    MAX_TRADES_PER_QUERY = 250
    missing_threshold_notified: set[str] = set()
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    # Debug counters
    total_events_processed = 0
    usdc_events_found = 0
    api_calls_made = 0
    confirmed_trades = 0
    alerts_sent = 0

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

    # Initialize resume point with safety margin
    try:
        init_q = """{ orderFilledEvents(orderBy:timestamp,orderDirection:desc, first:1){ timestamp } }"""
        data = await make_graphql_request(init_q)
        latest = data.get('data', {}).get('orderFilledEvents')
        if latest:
            raw_timestamp = int(latest[0]['timestamp'])
            last_processed_trade_timestamp = raw_timestamp - TIMESTAMP_SAFETY_MARGIN
            logger.info("Starting from timestamp: %d (with %ds safety margin)", 
                       last_processed_trade_timestamp, TIMESTAMP_SAFETY_MARGIN)
        else:
            last_processed_trade_timestamp = int(time.time()) - 60
    except Exception as e:
        logger.warning("Init error, starting from 1 minute ago: %s", e)
        last_processed_trade_timestamp = int(time.time()) - 60

    # Main polling loop
    while not stop_event.is_set():
        try:
            current_time = int(time.time())
            
            # Calculate query timestamp with lag protection
            query_timestamp = last_processed_trade_timestamp - INDEXING_LAG_BUFFER
            min_timestamp = current_time - 300  # Don't look back more than 5 minutes
            query_timestamp = max(query_timestamp, min_timestamp)
            
            vars_ = {
                "lastTimestamp": str(query_timestamp),
                "first": MAX_TRADES_PER_QUERY
            }
            
            data = await make_graphql_request(GRAPHQL_QUERY, vars_)
            
            # Handle subgraph errors with chain reorg detection
            if "errors" in data:
                consecutive_errors += 1
                logger.error("GraphQL errors (consecutive: %d): %s", consecutive_errors, data["errors"])

                if consecutive_errors >= max_consecutive_errors:
                    backoff_time = min(300, 30 * (2 ** (consecutive_errors - max_consecutive_errors)))
                    logger.warning("Too many consecutive errors, backing off for %d seconds", backoff_time)
                    await asyncio.sleep(backoff_time)
                else:
                    # Handle chain reorgs
                    first_msg = data["errors"][0].get("message", "").lower()
                    if "reorganize" in first_msg or "reorganized" in first_msg:
                        rewind = 30
                        last_processed_trade_timestamp = max(
                            0, last_processed_trade_timestamp - rewind
                        )
                        logger.warning(
                            "Chain re-org detected. Rewound cursor by %d s to %d",
                            rewind, last_processed_trade_timestamp,
                        )

                    await asyncio.sleep(min(60, 5 * consecutive_errors))
                continue

            consecutive_errors = 0
            all_events = data.get('data', {}).get('orderFilledEvents', [])
            
            if not isinstance(all_events, list):
                await asyncio.sleep(5)
                continue

            # Filter out duplicates and old events (lag protection)
            new_events = []
            duplicate_skips = 0
            old_event_skips = 0
            
            for event in all_events:
                timestamp = int(event['timestamp'])
                tx_hash = event['transactionHash']
                
                # Skip if already processed
                if tx_hash in processed_tx_hashes:
                    duplicate_skips += 1
                    continue
                
                # Only process events newer than last processed timestamp
                if timestamp <= last_processed_trade_timestamp:
                    old_event_skips += 1
                    continue
                
                new_events.append(event)

            # Update lag protection metrics
            if duplicate_skips > 0:
                lag_protection_metrics['total_recoveries'] += duplicate_skips
                logger.debug("🛡️ Lag protection: %d duplicates skipped", duplicate_skips)

            logger.info("📊 Processing batch: %d events (%d new, %d duplicates, %d old)", 
                       len(all_events), len(new_events), duplicate_skips, old_event_skips)

            # FIXED: Advance timestamp even if no qualifying events to prevent loops
            batch_max_ts = last_processed_trade_timestamp
            if all_events:
                batch_max_ts = max(int(event['timestamp']) for event in all_events)

            if not new_events:
                # Update timestamp to prevent loops
                if batch_max_ts > last_processed_trade_timestamp:
                    last_processed_trade_timestamp = batch_max_ts
                    logger.debug("Advanced timestamp to prevent loop: %d", last_processed_trade_timestamp)
                await asyncio.sleep(2 if len(all_events) > 0 else 5)
                continue

            # Process individual qualifying transactions (per-maker API calls)
            # REPLACE the transaction processing section in trade_monitor_with_lag_protection():

            # Process individual qualifying transactions (per-maker API calls)
            qualifying_txs_by_hash = {}  # Group by transaction hash to avoid duplicates
            
            for event in new_events:
                ts = int(event['timestamp'])
                tx_hash = event["transactionHash"]
                batch_max_ts = max(batch_max_ts, ts)
                total_events_processed += 1
                
                maker = event["maker"]
                maker_asset = event["makerAssetId"]
                taker_asset = event["takerAssetId"]
                maker_amt = float(event["makerAmountFilled"]) / 1e6
                taker_amt = float(event["takerAmountFilled"]) / 1e6

                # Only process USDC trades
                if maker_asset != "0" and taker_asset != "0":
                    processed_tx_hashes[tx_hash] = time.time()
                    continue

                usdc_events_found += 1
                
                # Calculate notional value
                notional = maker_amt if maker_asset == "0" else taker_amt
                side = "BUY" if maker_asset == "0" else "SELL"
                
                # Check which chats need this alert
                relevant_chats = []
                for chat_id in list(known_chats):
                    if chat_id not in chat_thresholds:
                        if chat_id not in missing_threshold_notified:
                            await send_telegram_message_to(chat_id, 
                                "👉 Please set your threshold with /setthreshold to receive alerts.")
                            missing_threshold_notified.add(chat_id)
                        continue

                    thresh = chat_thresholds[chat_id]
                    if notional >= thresh:
                        relevant_chats.append(chat_id)

                if relevant_chats:
                    # Group by transaction hash to avoid duplicate alerts
                    if tx_hash not in qualifying_txs_by_hash:
                        qualifying_txs_by_hash[tx_hash] = {
                            'events': [],
                            'relevant_chats': set(),
                            'max_notional': 0,
                            'first_maker': maker,
                            'timestamp': ts
                        }
                    
                    # Accumulate data for this transaction
                    qualifying_txs_by_hash[tx_hash]['events'].append({
                        'maker': maker,
                        'notional': notional,
                        'side': side
                    })
                    qualifying_txs_by_hash[tx_hash]['relevant_chats'].update(relevant_chats)
                    qualifying_txs_by_hash[tx_hash]['max_notional'] = max(
                        qualifying_txs_by_hash[tx_hash]['max_notional'], 
                        notional
                    )

                # Mark as processed with timestamp
                processed_tx_hashes[tx_hash] = time.time()

            # Process each unique transaction only once
            for tx_hash, tx_data in qualifying_txs_by_hash.items():
                api_calls_made += 1
                events_count = len(tx_data['events'])
                max_notional = tx_data['max_notional']
                first_maker = tx_data['first_maker']
                
                logger.info("💰 Qualifying TX: %s $%.2f (%d events) - API call #%d", 
                           tx_hash[:8], max_notional, events_count, api_calls_made)
                
                # Make ONE API call per transaction using the first maker
                trade_info = await get_trade_info_from_api(tx_hash, first_maker)
                
                if trade_info is None:
                    logger.info("❌ TX %s is NOT a TRADE event", tx_hash[:8])
                    continue
                
                confirmed_trades += 1
                logger.info("✅ TRADE Confirmed #%d: %s - %s %s (%d subgraph events)", 
                           confirmed_trades, tx_hash[:8], trade_info.get('side'), 
                           trade_info.get('outcome'), events_count)

                # Send ONE alert per transaction to each relevant chat
                for chat_id in tx_data['relevant_chats']:
                    alerts_sent += 1
                    logger.info("🚨 ALERT #%d → %s | %s %s | $%.2f (API) vs $%.2f (max subgraph)", 
                               alerts_sent, chat_id, trade_info['side'], 
                               trade_info['outcome'], trade_info['usdcSize'], max_notional)
                    
                    # Send alert with API data (more reliable than subgraph)
                    asyncio.create_task(_send_large_trade_alert(
                        chat_id, tx_hash, trade_info, trade_info['usdcSize']
                    ))

                # Mark as processed with timestamp
                processed_tx_hashes[tx_hash] = time.time()

            # Update timestamp for next iteration
            last_processed_trade_timestamp = batch_max_ts

            # Log summary every 20 API calls or when alerts sent
            if api_calls_made % 20 == 0 or alerts_sent > 0:
                logger.info("📈 STATS: %d events, %d USDC, %d API calls, %d confirmed, %d alerts, %d lag recoveries", 
                           total_events_processed, usdc_events_found, api_calls_made, 
                           confirmed_trades, alerts_sent, lag_protection_metrics['total_recoveries'])

        except Exception as e:
            consecutive_errors += 1
            logger.error("Unexpected error in trade monitor (consecutive: %d): %s", consecutive_errors, e)
            backoff_time = min(60, 5 * consecutive_errors)
            await asyncio.sleep(backoff_time)

        # Adaptive polling
        await asyncio.sleep(2)

# ========== CACHE AND MEMORY CLEANUP ==========
async def periodic_cache_cleanup():
    """Clean up old cache entries and manage memory usage."""
    while not stop_event.is_set():
        await asyncio.sleep(1800)  # Every 30 minutes
        
        current_time = time.time()
        
        # Clean up old trade info cache (keep for 1 hour)
        old_trade_keys = [k for k, v in _trade_info_cache.items() 
                         if current_time - v.get('_cached_at', current_time) > 3600]
        for key in old_trade_keys:
            del _trade_info_cache[key]
        
        # Clean up old failed lookups (keep for 1 hour)
        old_failed_keys = [k for k, v in _failed_lookups.items() 
                          if current_time - v > 3600]
        for key in old_failed_keys:
            del _failed_lookups[key]
        
        # Manage processed hashes memory
        manage_processed_hashes_memory()
        
        if old_trade_keys or old_failed_keys:
            logger.info("Cache cleanup: removed %d trade entries, %d failed entries", 
                       len(old_trade_keys), len(old_failed_keys))

# ========== TELEGRAM BOT STARTUP ==========
async def start_telegram_with_retry():
    """Start Telegram polling with exponential backoff retry logic."""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            logger.info("Starting Telegram polling (attempt %d/%d)", attempt + 1, max_retries)
            await application.updater.start_polling()
            logger.info("Telegram polling started successfully")
            return
        except Exception as e:
            wait_time = min(300, 30 * (2 ** attempt))
            logger.error("Telegram polling failed (attempt %d/%d): %s", attempt + 1, max_retries, e)
            if attempt < max_retries - 1:
                logger.info("Retrying in %d seconds...", wait_time)
                await asyncio.sleep(wait_time)
            else:
                logger.critical("Failed to start Telegram polling after %d attempts", max_retries)
                raise

# ========== MAIN ENTRY POINT ==========
# REPLACE the main entry point section with this:

def run_production_trade_monitor() -> None:
    """Main entry point for the production trade monitor bot."""
    logger.info("Starting Production Polymarket Trade Monitor Bot with Lag Protection")
    logger.info("Lag buffer: %ds, Safety margin: %ds, Max processed hashes: %d", 
               INDEXING_LAG_BUFFER, TIMESTAMP_SAFETY_MARGIN, MAX_PROCESSED_HASHES)

    # Validate required environment variables
    if not key:
        logger.error("POLYMARKET_KEY environment variable not set")
        return
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN environment variable not set")
        return

    # Check for required dependencies
    try:
        import psutil
    except ImportError:
        logger.error("psutil package required for memory monitoring. Install with: pip install psutil")
        return

    # Build Telegram application and register command handlers
    global application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(MessageHandler(filters.ALL, register_chat), group=-1)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("setthreshold", set_threshold))
    application.add_handler(CommandHandler("showthreshold", show_threshold))
    application.add_handler(CommandHandler("debug", debug_status))

    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Start background tasks
            loop.create_task(periodic_cache_cleanup())
            loop.create_task(trade_monitor_with_lag_protection())

            # Initialize and start the Telegram bot
            loop.run_until_complete(application.initialize())
            loop.run_until_complete(application.start())
            loop.run_until_complete(start_telegram_with_retry())

            logger.info("Production bot started successfully with lag protection")
            logger.info("Memory management: %d max hashes, %dh cleanup age", 
                       MAX_PROCESSED_HASHES, HASH_CLEANUP_AGE//3600)

            # FIXED: Check stop_event instead of running forever
            async def monitor_stop_event():
                """Monitor stop_event and shutdown gracefully when set."""
                while not stop_event.is_set():
                    await asyncio.sleep(1)
                logger.info("Stop event detected, initiating shutdown...")
                
                # Stop the Telegram application
                try:
                    await application.stop()
                    logger.info("Telegram application stopped")
                except Exception as e:
                    logger.error("Error stopping Telegram application: %s", e)

            # Run the stop monitor
            loop.run_until_complete(monitor_stop_event())
            break  # Exit the retry loop

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received")
            stop_event.set()
            break

        except Exception as e:
            retry_count += 1
            logger.error("Bot crashed: %s. Retry %d/%d", e, retry_count, max_retries)
            if retry_count < max_retries:
                time.sleep(30)
            else:
                logger.critical("Max retries reached. Bot stopping.")
        finally:
            logger.info("Entering cleanup phase...")
            stop_event.set()

            # Clean shutdown
            try:
                if loop and not loop.is_closed():
                    loop.run_until_complete(close_http_session())
            except Exception as e:
                logger.error("Error closing HTTP session: %s", e)

            try:
                if loop and not loop.is_closed():
                    loop.run_until_complete(application.shutdown())
            except Exception as e:
                logger.error("Error shutting down Telegram application: %s", e)

            # Cancel remaining tasks
            if loop and not loop.is_closed():
                pending_tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
                if pending_tasks:
                    logger.info("Cancelling %d pending tasks...", len(pending_tasks))
                    for task in pending_tasks:
                        task.cancel()
                    try:
                        loop.run_until_complete(
                            asyncio.gather(*pending_tasks, return_exceptions=True)
                        )
                    except Exception as e:
                        logger.error("Error cancelling tasks: %s", e)

            try:
                if loop and not loop.is_closed():
                    loop.close()
            except Exception as e:
                logger.error("Error closing event loop: %s", e)
            
            # Final lag protection summary
            logger.info("Final lag protection stats: %d total recoveries, max set size: %d", 
                       lag_protection_metrics['total_recoveries'], 
                       lag_protection_metrics['max_set_size'])
            logger.info("Bot shutdown complete")

if __name__ == "__main__":

    debug_chat_file_location()

    # FIXED: Better signal handling
    import signal
    import sys
    
    def signal_handler(signum, frame):
        signal_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
        logger.info("Received %s (signal %d), shutting down gracefully...", signal_name, signum)
        stop_event.set()
        
        # Give the bot 10 seconds to shutdown gracefully
        def force_exit():
            time.sleep(10)
            logger.warning("Graceful shutdown timeout, forcing exit...")
            sys.exit(1)
        
        # Start force exit timer in background thread
        import threading
        threading.Thread(target=force_exit, daemon=True).start()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        run_production_trade_monitor()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt in main thread")
        stop_event.set()
    except Exception as e:
        logger.error("Unhandled exception in main: %s", e)
        stop_event.set()
    finally:
        logger.info("Main thread exiting")
