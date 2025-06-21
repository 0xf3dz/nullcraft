# Polymarket Trade Monitor Bot

Real-time Telegram alerts for large Polymarket trades. Track whale activity and insider trading patterns.

## Features

- Real-time trade monitoring with lag protection
- Customizable alert thresholds
- Detailed trade info with trader profiles
- Memory efficient with error handling

## Live Bot

[@nullcraft_bot](https://t.me/nullcraft_bot)

## Use Cases

- Track whale traders and institutional activity
- Spot insider trading before news breaks
- Monitor market sentiment shifts
- Generate alpha from large trade signals

## Setup

1. Clone repo
2. `pip install -r requirements.txt` 
3. Set environment variables:
```
POLYMARKET_KEY=your_key
TELEGRAM_BOT_TOKEN=your_token
POLYMARKET_PROXY_ADDRESS=your_proxy_address
```

4. `python main.py`

## Commands

- `/start` - Activate bot
- `/setthreshold <amount>` - Set alert threshold (e.g. `/setthreshold 5000`)
- `/showthreshold` - View current threshold
- `/debug` - Bot status

## Why It Matters

Large trades often happen before major news. Monitor $10K+ trades to spot:
- Insider information
- Market manipulation
- Institutional patterns
- Early signals on developing stories

---

**For inquiries, DM me on X @0xf3dz or on Discord @f3dz**
