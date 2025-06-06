import dotenv
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
import pyotp
from kiteconnect import KiteConnect
import datetime as dt
from playwright.async_api import async_playwright   
import asyncio 

ENVLOC = '/app/.env'
def save_auth_code(new_auth_code):
    """
    Saves the authentication code to the .env file.
    
    Args:
        new_auth_code (str): The authentication code to save.
    """
    # Load existing environment variables from the .env file
    dotenv.load_dotenv(ENVLOC)
    env_vars = dotenv.dotenv_values(ENVLOC)

    # Update with new values
    env_vars["AUTH_CODE"] = new_auth_code
    #tiem = dt.datetime.now(dt.UTC) + dt.timedelta(hours=5.5)

    IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
    now_ist = dt.datetime.now(IST)
    iso_time = now_ist.isoformat()
    env_vars["AUTH_CODE_TIMESTAMP"] = iso_time

    # Write back to the .env file
    with open(ENVLOC, "w") as f:
        for key, value in env_vars.items():
            f.write(f"{key}={value}\n")

def timezone_isoformat(tz: dt.timezone) -> str:
    """
    Return the timezone offset as an ISO 8601 formatted string like '+05:30'.
    """
    offset = tz.utcoffset(None)
    if offset is None:
        return ''
    total_seconds = offset.total_seconds()
    sign = '+' if total_seconds >= 0 else '-'
    total_seconds = abs(int(total_seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes = remainder // 60
    return f"{sign}{hours:02d}:{minutes:02d}"

async def async_getAuth():
    """
    Authenticates the user with the Kite API using API key, secret, and TOTP-based 2FA.
    This function performs the following steps:
    1. Loads environment variables for API credentials and user details.
    2. Initiates a login session using Selenium to automate the browser.
    3. Inputs the username, password, and TOTP for 2FA.
    4. Extracts the request token from the redirected URL.
    5. Generates an access token using the KiteConnect API.
    Returns:
        str: The access token for authenticated API requests.
    Raises:
        Exception: If any step in the authentication process fails.
    Note:
        - Ensure that the `.env` file contains the required environment variables:
          `APIKEY`, `APISECRET`, `USERID`, `PASSWORD`, and `TOTPKEY`.
        - Chromedriver must be installed and available in the system PATH.
        - Uncomment the `--headless` option in Chrome options for headless execution.

    """
    dotenv.load_dotenv(ENVLOC)
    print('getting auth code',flush=True)
    api_key = os.getenv('APIKEY')
    api_secret = os.getenv("APISECRET")
    user_id = os.getenv('USERID')
    password = os.getenv('PASSWORD')
    totp_key = os.getenv('TOTPKEY')

    # we will first check our env file to see if we have an existing auth_code
    # that is less than one day old, if it is, we'll return that.
    auth_code = os.getenv("AUTH_CODE")
    timestamp_str = os.getenv("AUTH_CODE_TIMESTAMP")

    if auth_code and timestamp_str:
        try:
            IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
            timestamp = dt.datetime.fromisoformat(timestamp_str)

            now = dt.datetime.now(IST)  # timezone-aware in IST
            six_am_today = dt.datetime.combine(now.date(), dt.time(6, 0, tzinfo=IST))

            if timestamp >= six_am_today:
                print("✅ Auth code is still valid (obtained after 6 AM).",flush=True)
                return auth_code
            else:
                print("❌ Auth code was obtained before 6 AM. Fetching new code...",flush=True)
        except Exception as e:
            print(f"⚠️ Error parsing timestamp, regenerating auth code...{e}",flush=True)


    # URL to initiate login
    login_url = f'https://kite.zerodha.com/connect/login?v=3&api_key={api_key}'

    # Setup Chrome
    async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto(login_url)
            print("🔗 Navigated to login page")
            await page.fill('#userid', user_id)
            await page.fill('#password', password)
            await page.click('button[type="submit"]')

            await page.wait_for_timeout(2000)
            print("🔗 Submitted login form")
            totp = pyotp.TOTP(totp_key).now()
            await page.fill('#userid', totp)
            #await page.click('button[type="submit"]')  # Only needed if 2FA step requires submit
            print("🔗 Filled TOTP field")
            await page.wait_for_timeout(3000)

            url = page.url
            await browser.close()
            print(f"🔗 Redirected URL: {url}",flush=True)
            if "request_token=" not in url:
                raise Exception("❌ Failed to retrieve request_token from redirected URL",flush=True)

            request_token = next(i.split('=')[1] for i in url.split('?')[1].split('&') if i.startswith('request_token='))

            kite = KiteConnect(api_key=api_key)
            data = kite.generate_session(request_token, api_secret=api_secret)
            access_token = data["access_token"]
            print(f"✅ Auth code: {access_token}",flush=True)
            save_auth_code(access_token)
            return access_token

def getAuth():
    return asyncio.run(async_getAuth())
if __name__ == '__main__':
    asyncio.run(async_getAuth())