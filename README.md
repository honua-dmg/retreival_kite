# Stock Market Data Collection System

A system for collecting and analyzing real-time stock market data from Indian exchanges (NSE and BSE).

## Requirements

- Docker and Docker Compose
- KiteConnect API credentials
- Zerodha trading account
- Python 3.11 or higher
- Google SMTP server for sending emails- well if you have your app password you're good to go.

## Features

- Real-time market data collection from NSE and BSE
- Automated data processing and analysis
- Email reporting system
- Configurable stock tracking
- Persistent data storage
- Redis-based caching
- Docker containerization
- WebSocket-based real-time updates
## Setup
1. Clone the repository:
```bash
git clone <repository-url>
cd stonks
```
2. Create and configure environment variables:
```bash
cp user-env/.env.example user-env/.env
```
Edit the .env file with your API keys, passwords, and other sensitive information.
   APIKEY=your_api_key - kiteconnect
   APISECRET=your_api_secret - kiteconnect
   USERID=your_user_id - zerodha
   PASSWORD=your_password - zerodha
   TOTPKEY=your_totp_key - kite - this takes some finicking; by default it's linked to kite's proprietary TOTP, we need to alter it to use external TOTP apps like Google authenticator. in this process we copy the TOTP key and paste it here.
   STOCKS=your,comma,separated,stocks
   EMAIL_ADDRESS=your_email@example.com
   EMAIL_PASSWORD=your_email_password - google app password - not regular password - https://support.google.com/mail/answer/185833?hl=en
   TO_EMAIL=recipient@example.com
   FILEPATH= where you want your data to be stored locally. 

3. Build and run the Docker containers:
```bash
docker-compose --env-file user-env/.env up -d --build
```



## USAGE:
The system will:

1. Connect to KiteConnect using your credentials
2. Start collecting market data for your configured stocks
3. Process and analyze the data
4. Generate periodic reports
5. Store data persistently in the data/ directory
Data Storage
Data is stored in CSV files at:
   data/NSE/<stock>/<date>.csv
   data/BSE/<stock>/<date>.csv
   Each file contains:

   Real-time price data
   Volume information
   Buy/sell depth data
   OHLC (Open, High, Low, Close) values

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