import dotenv
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time
import pyotp
from kiteconnect import KiteConnect
def getAuth():
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
    dotenv.load_dotenv()

    api_key = os.getenv('APIKEY')
    api_secret = os.getenv("APISECRET")
    user_id = os.getenv('USERID')
    password = os.getenv('PASSWORD')
    totp_key = os.getenv('TOTPKEY')

    # URL to initiate login
    login_url = f'https://kite.zerodha.com/connect/login?v=3&api_key={api_key}'

    # Setup Chrome
    chrome_options = Options()
    #chrome_options.add_argument('--headless')  # Comment this out to see browser
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    # If chromedriver is in PATH
    driver = webdriver.Chrome(options=chrome_options)

    
    driver.get(login_url)
    time.sleep(2)

    # Step 1: Username and password
    driver.find_element(By.ID, "userid").send_keys(user_id)
    driver.find_element(By.ID, "password").send_keys(password)
    driver.find_element(By.XPATH, "//button[@type='submit']").click()

    time.sleep(2)

    # Step 2: TOTP-based 2FA
    totp = pyotp.TOTP(totp_key).now()
    driver.find_element(By.ID, "userid").send_keys(totp)
    #driver.find_element(By.XPATH, "//button[@type='submit']").click()
    time.sleep(.5)

    for i in driver.current_url.split('?')[1].split('&'):
        if i.split('=')[0] == 'request_token':
            request_token = i.split('=')[1]
    driver.close()

    kite = KiteConnect(api_key=api_key) # might be an issue, look into it. 
    data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = data["access_token"]
    return access_token