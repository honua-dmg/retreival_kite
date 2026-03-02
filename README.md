# Stock Market Data Collection System

A robust, production-ready system for collecting real-time stock market data from Indian exchanges (NSE and BSE) using the Zerodha KiteConnect API.

## Architecture

```
┌─────────────────┐     WebSocket      ┌─────────────────┐     Redis Streams     ┌─────────────────┐
│  Zerodha Kite   │ ─────────────────► │    Producer     │ ────────────────────► │     Redis       │
│    Ticker       │    Real-time       │  (producer.py)  │    XADD per stock     │    Streams      │
└─────────────────┘                    └─────────────────┘                       └────────┬────────┘
                                                                                          │
                                                                                          │ XREAD
                                                                                          ▼
┌─────────────────┐                    ┌─────────────────┐     ┌────────────────────────────────────┐
│   CSV Files     │◄───────────────────│    Consumers    │◄────│  5 Consumer Threads                │
│  (Data Store)   │      Save.py       │ (Consumers.py)  │     │  - Load balanced by stream size    │
└─────────────────┘                    └─────────────────┘     │  - Self-healing with monitoring    │
                                                               └────────────────────────────────────┘
```

## Project Structure

```
src/
├── Main.py          # Entry point - orchestrates the pipeline
├── producer.py      # WebSocket producer with heartbeat monitoring
├── Consumers.py     # Multi-threaded consumers with load balancing
├── Auth.py          # Automated Kite authentication with TOTP
├── Save.py          # CSV file persistence
├── report.py        # Email notifications and reporting
├── upload.py        # Cloud storage (DigitalOcean Spaces)
├── config.py        # Centralized configuration
└── utils.py         # Shared utilities (token mapping, timezone, etc.)
```

## Requirements

- Docker and Docker Compose
- KiteConnect API credentials
- Zerodha trading account
- Python 3.11 or higher
- Resend API key for email notifications (or configure your own SMTP)

## Features

- **Real-time data collection** from NSE and BSE via WebSocket
- **Multi-threaded consumers** with automatic load balancing
- **Self-healing** - automatic reconnection and thread recovery
- **Heartbeat monitoring** with email alerts on failures
- **Market hours aware** - waits for open, stops at close
- **Holiday detection** - automatically skips market holidays
- **Cloud backup** - uploads to DigitalOcean Spaces (S3-compatible)
- **Redis-based message queue** for reliable data pipeline
- **Docker containerization** for easy deployment
## Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd retreival_kite
   ```

2. **Create and configure environment variables:**
   ```bash
   cp .env.example .env
   ```
   
   Edit the `.env` file with your credentials:
   ```env
   # Kite Connect API
   APIKEY=your_kite_api_key
   APISECRET=your_kite_api_secret
   
   # Zerodha Login
   USERID=your_zerodha_user_id
   PASSWORD=your_zerodha_password
   TOTPKEY=your_external_totp_key  # See note below
   
   # Stocks to track (comma-separated)
   STOCKS=RELIANCE,INFY,TCS,HDFCBANK
   
   # Email notifications (Resend API)
   RESEND_API_KEY=your_resend_api_key
   TO_EMAIL=your_email@example.com
   
   # Data storage
   FILEPATH=/path/to/local/data
   
   # Cloud storage (DigitalOcean Spaces)
   DIGITALOCEAN_KEY_ID=your_do_key_id
   DIGITALOCEAN_KEY_SECRET=your_do_key_secret
   DIGITALOCEAN_REGION=nyc3
   DIGITALOCEAN_ENDPOINT=https://nyc3.digitaloceanspaces.com
   DIGITALOCEAN_BUCKET_NAME=kite
   ```

   > **TOTP Note:** By default, Zerodha uses their proprietary TOTP. You need to 
   > switch to an external authenticator (like Google Authenticator) and copy the 
   > TOTP secret key to use here.

3. **Build and run with Docker:**
   ```bash
   docker-compose --env-file .env up -d --build
   ```



## Usage

The system runs automatically within Docker:

1. **Waits for market open** (9:15 AM IST)
2. **Collects data** via WebSocket until market close (3:30 PM IST)
3. **Sends daily report** via email
4. **Uploads to cloud** storage
5. **Cleans up** old local files (>7 days)

### Data Storage Structure

```
data/
├── NSE/
│   ├── RELIANCE/
│   │   ├── 2026-03-01.csv
│   │   └── 2026-03-02.csv
│   └── INFY/
│       └── ...
└── BSE/
    └── ...
```

Each CSV contains:
- Timestamp
- Price data (last price, OHLC)
- Volume information
- 5-level order book depth (bid/ask)
- Open interest (for F&O)

### Monitoring

```bash
# View live logs
docker-compose logs -f app

# Check Redis streams
docker exec -it <redis_container> redis-cli
> XLEN RELIANCE
```

# Troubleshooting
1. If Redis port is already in use:
```bash
docker stop $(docker ps -q --filter ancestor=redis)
docker-compose --env-file user-env/.env up -d --build
```
2. If authentication fails:
Check your KiteConnect credentials
Verify TOTP key
Ensure API key and secret are correct

## Security Notes
Never commit your .env file to version control
Keep your config.json file secure as it contains sensitive information
The data/ directory contains persistent data that should be backed up
Use strong, unique passwords and API keys
Regularly rotate your API keys and passwords

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details