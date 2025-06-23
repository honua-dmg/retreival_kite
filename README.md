# Real-Time Stock Market Data Engineering Platform

This project is a robust, containerized data engineering platform designed to collect, process, and store real-time stock market data from Indian exchanges (NSE and BSE) using the KiteConnect API. It features a resilient, distributed architecture built for continuous, unattended operation.

## Key Features

- **Real-Time Data Ingestion:** Utilizes KiteConnect's WebSocket API to stream live market data for a configurable list of stocks.
- **Producer-Consumer Architecture:** Decouples data fetching from data processing using Redis as a high-performance message broker.
    - **Producer:** Connects to the WebSocket, ingests tick data, and pushes it to dedicated Redis streams for each stock.
    - **Consumers:** A pool of parallel worker threads that consume data from Redis streams and write it to structured CSV files.
- **High Availability & Resilience:**
    - **Heartbeat Monitoring:** The producer's health is constantly monitored. If it becomes unresponsive, the system automatically terminates and restarts it to ensure continuous data flow.
    - **Automatic Reconnection:** Handles WebSocket connection drops and attempts to reconnect automatically.
    - **Consumer Thread Monitoring:** A dedicated monitor ensures that all consumer threads are active and restarts any that have failed.
- **Dynamic Workload Balancing:** A scheduler periodically rebalances the data processing workload across consumer threads to ensure efficient resource utilization.
- **Data Persistence:** Stores time-series data in a structured, date-partitioned directory format (`data/{EXCHANGE}/{STOCK}/{YYYY-MM-DD}.csv`).
- **Simulation Mode:** Allows for backtesting and data reprocessing by simulating a market session from a specified historical date.
- **Automated Reporting:** Generates and sends a summary email report at the end of each market session with key statistics.
- **Automated Data Upload:** Automatically uploads daily data to a configured storage backend (e.g., cloud storage).
- **Containerized Deployment:** Fully containerized with Docker and Docker Compose for easy setup, deployment, and scalability.

## System Architecture

The platform follows a classic producer-consumer pattern:

```
+----------------+      +-----------------+      +----------------------+
|                |      |                 |      |                      |
|  KiteConnect   |----->|    Producer     |----->|   Redis Streams      |
| WebSocket API  |      |  (producer.py)  |      | (One per stock)      |
|                |      |                 |      |                      |
+----------------+      +-----------------+      +----------+-----------+
                                                              |
                                                              |
+-------------------------------------------------------------+
|
v
+----------------------+      +----------------------+
|                      |      |                      |
|   Consumer Pool      |----->|   CSV File Storage   |
|   (Consumers.py)     |      | (data/...)           |
|                      |      |                      |
+----------------------+      +----------------------+
```

1.  The **Producer** establishes a connection to the KiteConnect WebSocket and subscribes to ticks for the specified stocks.
2.  As ticks arrive, the producer publishes them as messages to dedicated **Redis Streams**, with one stream for each stock symbol.
3.  A pool of **Consumers** runs in parallel. Each consumer is assigned a subset of the stock streams.
4.  Consumers read messages from their assigned streams, format the data, and append it to the corresponding **CSV file** for that day.

## Requirements

- Docker and Docker Compose
- KiteConnect API credentials from a Zerodha account
- Python 3.11+
- Google Account with an "App Password" for sending email reports.

## Setup Instructions

1.  **Clone the Repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Configure Environment Variables:**
    Create a `.env` file in the root directory by copying the example file.
    ```bash
    cp .env.example .env
    ```
    Now, edit `.env` and fill in your credentials and configuration settings.

3.  **Build and Run with Docker Compose:**
    ```bash
    docker-compose up -d --build
    ```
    This command will build the Docker image for the application, start the application container, and a Redis container.

## Configuration (`.env` file)

| Variable          | Description                                                                                                                                                           | Example                               |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------- |
| `APIKEY`          | Your KiteConnect API key.                                                                                                                                             | `your_api_key`                        |
| `APISECRET`       | Your KiteConnect API secret.                                                                                                                                          | `your_api_secret`                     |
| `USERID`          | Your Zerodha user ID.                                                                                                                                                 | `AB1234`                              |
| `PASSWORD`        | Your Zerodha password.                                                                                                                                                | `your_password`                       |
| `TOTPKEY`         | The secret key from your TOTP app (e.g., Google Authenticator) used for 2FA.                                                                                            | `your_totp_key`                       |
| `STOCKS`          | A comma-separated list of stock symbols (from the `tradingsymbol` column in `NSE.csv`/`BSE.csv`) to track.                                                              | `RELIANCE,TCS,INFY`                   |
| `EMAIL_ADDRESS`   | The Gmail address from which to send email reports.                                                                                                                   | `your_email@gmail.com`                |
| `EMAIL_PASSWORD`  | The Google App Password for the sending email account. [How to get an App Password](https://support.google.com/mail/answer/185833?hl=en).                              | `your_app_password`                   |
| `TO_EMAIL`        | The recipient's email address for reports.                                                                                                                            | `recipient@example.com`               |
| `FILEPATH`        | The local path inside the container where CSV data is stored. **Should generally not be changed.**                                                                    | `/app/data`                           |
| `SIMULATION_MODE` | Set to `true` to run in simulation mode, `false` for live data collection.                                                                                              | `false`                               |
| `SIMULATION_DATE` | If `SIMULATION_MODE` is `true`, specify the date for which to run the simulation.                                                                                       | `2023-01-20`                          |

## Usage

### Live Mode

By default (`SIMULATION_MODE="false"`), the system operates in live mode.
- The application will start, authenticate with KiteConnect, and wait until 9:15 AM IST.
- At 9:15 AM, it will start the producer and consumers to collect live market data.
- The process runs until 3:30 PM IST.
- After the market closes, it will generate and send an email report, upload the data, and shut down the producer.

### Simulation Mode

To reprocess data for a specific day:
1.  Set `SIMULATION_MODE="true"` in your `.env` file.
2.  Set `SIMULATION_DATE` to the desired date (e.g., `2023-10-27`).
3.  Restart the container: `docker-compose up -d --build`.
The system will use a simulator to feed historical data into the consumers for the specified date.

### Monitoring the System

You can view the application logs in real-time to monitor its activity:
```bash
docker-compose logs -f app
```

## Data Storage

-   All collected data is stored in the `data/` directory on the host machine, which is mapped to `/app/data` inside the container.
-   The data is organized by exchange, stock symbol, and date:
    ```
    data/
    ├── NSE/
    │   └── RELIANCE/
    │       └── 2023-10-27.csv
    └── BSE/
        └── TCS/
            └── 2023-10-27.csv
    ```

## Troubleshooting

-   **Redis Port in Use:** If you have another Redis instance running locally, it may conflict. Stop it before running `docker-compose`.
-   **Authentication Fails:** Double-check all your KiteConnect credentials (`APIKEY`, `APISECRET`, `USERID`, `PASSWORD`, `TOTPKEY`) in the `.env` file. Ensure the TOTP key is correct.

## Security Notes

-   **Never commit your `.env` file to version control.** The `.gitignore` file is already configured to ignore it.
-   Use strong, unique passwords and API keys.
-   Regularly rotate your API keys and passwords for enhanced security.

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.