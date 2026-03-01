"""
Authentication module for the Stock Market Data Collection System.

This module handles automated authentication with the Zerodha Kite API,
including TOTP-based two-factor authentication using headless browser automation.

Functions:
    - getAuth: Get a valid access token (cached or fresh)
    - async_getAuth: Async implementation of authentication flow

Token Caching:
    Access tokens are cached in the .env file and reused if obtained after 6 AM IST
    on the current day. This minimizes login attempts and TOTP usage.
"""

import os
import asyncio
import datetime as dt

import pyotp
from dotenv import load_dotenv, dotenv_values
from playwright.async_api import async_playwright
from kiteconnect import KiteConnect

import config
from utils import IST


def _save_auth_code(access_token: str):
    """
    Save the access token to the .env file for caching.
    
    Args:
        access_token: Valid Kite access token to cache.
    """
    load_dotenv(config.ENVLOC, override=True)
    env_vars = dotenv_values(config.ENVLOC)
    
    # Update with new values
    env_vars["AUTH_CODE"] = access_token
    env_vars["AUTH_CODE_TIMESTAMP"] = dt.datetime.now(IST).isoformat()
    
    # Write back to .env file
    with open(config.ENVLOC, "w") as f:
        for key, value in env_vars.items():
            f.write(f"{key}={value}\n")


def _is_token_valid() -> tuple[bool, str | None]:
    """
    Check if the cached access token is still valid.
    
    A token is valid if:
    - It exists in the .env file
    - It was obtained after 6 AM IST today
    
    Returns:
        tuple: (is_valid: bool, token: str | None)
    """
    load_dotenv(config.ENVLOC, override=True)
    
    auth_code = os.getenv("AUTH_CODE")
    timestamp_str = os.getenv("AUTH_CODE_TIMESTAMP")
    
    if not auth_code or not timestamp_str:
        return False, None
    
    try:
        timestamp = dt.datetime.fromisoformat(timestamp_str)
        now = dt.datetime.now(IST)
        six_am_today = dt.datetime.combine(now.date(), dt.time(6, 0, tzinfo=IST))
        
        if timestamp >= six_am_today:
            return True, auth_code
        else:
            return False, None
            
    except Exception as e:
        print(f"⚠️ Error parsing auth timestamp: {e}", flush=True)
        return False, None


async def async_getAuth() -> str:
    """
    Authenticate with Zerodha Kite API using browser automation.
    
    This function:
    1. Checks for a cached valid token
    2. If not valid, uses Playwright to automate the login flow
    3. Handles TOTP-based 2FA
    4. Extracts the request token and generates an access token
    5. Caches the token for future use
    
    Returns:
        str: Valid access token for Kite API calls.
    
    Raises:
        Exception: If login fails or token extraction fails.
    
    Note:
        Requires Playwright browsers to be installed:
        `playwright install chromium`
    """
    # Check for cached token first
    is_valid, cached_token = _is_token_valid()
    if is_valid and cached_token:
        print("✅ Using cached auth token (obtained after 6 AM).", flush=True)
        return cached_token
    
    print("🔐 Fetching new auth token...", flush=True)
    
    # Load credentials
    load_dotenv(config.ENVLOC, override=True)
    api_key = os.getenv('APIKEY')
    api_secret = os.getenv("APISECRET")
    user_id = os.getenv('USERID')
    password = os.getenv('PASSWORD')
    totp_key = os.getenv('TOTPKEY')
    
    login_url = f'https://kite.zerodha.com/connect/login?v=3&api_key={api_key}'
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Navigate to login page
        await page.goto(login_url)
        print("🔗 Navigated to login page", flush=True)
        
        # Enter credentials
        await page.fill('#userid', user_id)
        await page.fill('#password', password)
        await page.click('button[type="submit"]')
        await page.wait_for_timeout(2000)
        print("📝 Submitted login form", flush=True)
        
        # Enter TOTP
        totp = pyotp.TOTP(totp_key).now()
        await page.fill('#userid', totp)
        await page.wait_for_timeout(3000)
        print("🔢 Entered TOTP", flush=True)
        
        # Get redirected URL with request token
        url = page.url
        await browser.close()
        
        print(f"🔗 Redirected URL: {url}", flush=True)
        
        if "request_token=" not in url:
            raise Exception("❌ Failed to retrieve request_token from redirected URL")
        
        # Extract request token
        request_token = None
        for param in url.split('?')[1].split('&'):
            if param.startswith('request_token='):
                request_token = param.split('=')[1]
                break
        
        if not request_token:
            raise Exception("❌ Could not parse request_token")
        
        # Generate access token
        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]
        
        print(f"✅ Successfully obtained access token", flush=True)
        
        # Cache the token
        _save_auth_code(access_token)
        
        return access_token


def getAuth() -> str:
    """
    Get a valid Kite access token (synchronous wrapper).
    
    This is a convenience wrapper around async_getAuth() for use
    in synchronous code.
    
    Returns:
        str: Valid access token for Kite API calls.
    """
    return asyncio.run(async_getAuth())


if __name__ == '__main__':
    # Test authentication
    token = getAuth()
    print(f"Access token: {token[:20]}...")
