# Stock Market Data Collection System

A system for collecting and analyzing real-time stock market data from Indian exchanges (NSE and BSE).

## Requirements

- Docker and Docker Compose
- KiteConnect API credentials
- Zerodha trading account

## Features

- Real-time market data collection from NSE and BSE
- Automated data processing and analysis
- Email reporting system
- Configurable stock tracking
- Persistent data storage
- Redis-based caching

## Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd stonks
2. Create configuration files:
   ```bash
   cp .env.example .env
   cp config.json.example config.json
   ```
3. Edit the configuration files
    APIKEY=your_api_key
    APISECRET=your_api_secret
    USERID=your_user_id
    PASSWORD=your_password
    TOTPKEY=your_totp_key
4. create the data directory
   ```bash
   mkdir data
   ```
5.run the container
   ```bash
   docker-compose -f docker/docker-compose.yml up -d --build
   ```

## USAGE:
    1.Connect to KiteConnect using your credentials
    2.Start collecting market data for your configured stocks
    3.Process and analyze the data
    4.Generate periodic reports
    5.Store data persistently in the data/ directory

## Configuration
The system uses two configuration files:

user-env/.env: Contains sensitive credentials:
API keys
Passwords
Authentication codes

## Security Notes
Never commit your .env file to version control
Keep your config.json file secure as it contains sensitive information
The data/ directory contains persistent data that should be backed up
Use strong, unique passwords and API keys
Regularly rotate your API keys and passwords