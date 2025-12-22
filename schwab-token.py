# This code is from https://www.reddit.com/r/Schwab/comments/1c2ioe1/the_unofficial_guide_to_charles_schwabs_trader/
# To generate refresh token and access token
#
import os
import base64
import requests
import webbrowser
import threading
import time
import ssl
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, unquote
from loguru import logger
import socket
# pyperclip removed - using direct URL monitoring instead

# Try to import selenium for browser automation
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logger.warning("Selenium not available. Install with: pip install selenium")
try:
    from settings import SCHWAB_APP_KEY, SCHWAB_APP_SECRET
except ImportError:
    logger.warning("SCHWAB_APP_KEY and SCHWAB_APP_SECRET not found in settings.py")
    logger.info("Please add your Schwab API credentials to settings.py:")
    logger.info("SCHWAB_APP_KEY = 'your_app_key'")
    logger.info("SCHWAB_APP_SECRET = 'your_app_secret'")
    SCHWAB_APP_KEY = None
    SCHWAB_APP_SECRET = None


class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        
        # Store the authorization code
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        
        if 'code' in query_params:
            CallbackHandler.auth_code = unquote(query_params['code'][0])
            self.wfile.write(b'<html><body><h1>Authentication successful!</h1><p>You can close this window.</p></body></html>')
        else:
            self.wfile.write(b'<html><body><h1>Authentication failed!</h1><p>No authorization code received.</p></body></html>')
        
        # Signal that we got the callback
        CallbackHandler.callback_received = True

    def log_message(self, format, *args):
        # Suppress server logs
        pass

CallbackHandler.auth_code = None
CallbackHandler.callback_received = False
CallbackHandler.tokens_received = False
CallbackHandler.token_result = None


def construct_init_auth_url() -> tuple[str, str, str]:
    app_key = SCHWAB_APP_KEY
    app_secret = SCHWAB_APP_SECRET
    
    if not app_key or not app_secret:
        raise ValueError("SCHWAB_APP_KEY and SCHWAB_APP_SECRET must be set in settings.py")

    auth_url = f"https://api.schwabapi.com/v1/oauth/authorize?client_id={app_key}&redirect_uri=https://127.0.0.1"

    logger.info("Opening authentication URL in browser...")
    logger.info(auth_url)

    return app_key, app_secret, auth_url


def construct_headers_and_payload(auth_code, app_key, app_secret):
    credentials = f"{app_key}:{app_secret}"
    base64_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {base64_credentials}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    payload = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": "https://127.0.0.1",
    }

    return headers, payload


def retrieve_tokens(headers, payload) -> dict:
    init_token_response = requests.post(
        url="https://api.schwabapi.com/v1/oauth/token",
        headers=headers,
        data=payload,
    )

    init_tokens_dict = init_token_response.json()

    return init_tokens_dict


def process_auth_code_immediately(auth_code, app_key, app_secret):
    """Process authorization code immediately to prevent expiration"""
    try:
        logger.info(f"Processing auth code immediately: {auth_code[:20]}...")
        
        headers, payload = construct_headers_and_payload(auth_code, app_key, app_secret)
        tokens_dict = retrieve_tokens(headers, payload)
        
        if 'error' in tokens_dict:
            logger.error(f"Token exchange failed: {tokens_dict}")
            CallbackHandler.token_result = f"Error: {tokens_dict.get('error_description', 'Unknown error')}"
        else:
            access_token = tokens_dict.get('access_token', '')
            refresh_token = tokens_dict.get('refresh_token', '')
            
            if access_token and refresh_token:
                logger.success("✅ LOGIN SUCCESSFUL! Tokens received successfully!")
                logger.success(f"Access token: {access_token[:20]}...{access_token[-10:]}")
                logger.success(f"Refresh token: {refresh_token[:20]}...{refresh_token[-10:]}")
                
                # Update SCHWAB_REFRESH_TOKEN in settings.py
                try:
                    # Read current settings.py content
                    with open('settings.py', 'r') as f:
                        content = f.read()
                    
                    # Find and replace the SCHWAB_REFRESH_TOKEN line
                    import re
                    pattern = r'SCHWAB_REFRESH_TOKEN\s*=\s*"[^"]*"'
                    replacement = f'SCHWAB_REFRESH_TOKEN = "{refresh_token}"'
                    
                    if re.search(pattern, content):
                        # Replace existing token
                        updated_content = re.sub(pattern, replacement, content)
                        logger.info("Updated existing SCHWAB_REFRESH_TOKEN in settings.py")
                    else:
                        # Add new token if not found
                        updated_content = content + f'\nSCHWAB_REFRESH_TOKEN = "{refresh_token}"\n'
                        logger.info("Added SCHWAB_REFRESH_TOKEN to settings.py")
                    
                    # Write updated content back to settings.py
                    with open('settings.py', 'w') as f:
                        f.write(updated_content)
                    
                    logger.success("🎉 SCHWAB_REFRESH_TOKEN updated in settings.py - Authentication complete!")
                    
                    # Also save backup to schwab_tokens.txt for reference
                    with open('schwab_tokens.txt', 'w') as f:
                        f.write(f"Access Token: {access_token}\n")
                        f.write(f"Refresh Token: {refresh_token}\n")
                        f.write(f"Token Type: {tokens_dict.get('token_type', '')}\n")
                        f.write(f"Expires In: {tokens_dict.get('expires_in', '')} seconds\n")
                    
                    logger.info("Backup tokens also saved to schwab_tokens.txt")
                    CallbackHandler.token_result = "Success"
                    
                except Exception as e:
                    logger.error(f"Failed to update settings.py: {e}")
                    # Fallback to file only
                    with open('schwab_tokens.txt', 'w') as f:
                        f.write(f"Access Token: {access_token}\n")
                        f.write(f"Refresh Token: {refresh_token}\n")
                        f.write(f"Token Type: {tokens_dict.get('token_type', '')}\n")
                        f.write(f"Expires In: {tokens_dict.get('expires_in', '')} seconds\n")
                    logger.warning("Updated schwab_tokens.txt instead - please manually copy refresh token to settings.py")
                    CallbackHandler.token_result = "Success"
                    
            else:
                logger.error("Tokens received but missing access_token or refresh_token")
                CallbackHandler.token_result = "Error: Incomplete tokens"
            
        CallbackHandler.tokens_received = True
        
    except Exception as e:
        logger.error(f"Error processing auth code: {e}")
        CallbackHandler.token_result = f"Error: {e}"
        CallbackHandler.tokens_received = True


def automated_browser_auth(auth_url, app_key, app_secret):
    """Use browser automation to login to Schwab and capture redirect URL automatically"""
    if not SELENIUM_AVAILABLE:
        return False
        
    try:
        logger.info("Starting automated browser authentication with Schwab login...")
        
        # No credentials needed - going directly to OAuth
        logger.info("🚀 Skipping credential loading - using direct OAuth flow")
        
        # Set up Chrome options with separate profile to avoid conflicts
        options = Options()
        
        # Create temporary profile directory to avoid conflicts with running Chrome
        import os
        import tempfile
        temp_profile = tempfile.mkdtemp(prefix="schwab_chrome_")
        
        # Use temporary profile directory to avoid "Chrome instance exited" error
        options.add_argument(f"--user-data-dir={temp_profile}")
        options.add_argument("--profile-directory=Default")
        logger.info(f"Using temporary Chrome profile: {temp_profile}")
        
        # Essential options to avoid crashes
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-software-rasterizer")
        
        # Window and display settings
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--start-maximized")
        
        # Disable problematic features that can cause crashes (but keep JS enabled)
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-images")  # Reduce loading for speed
        
        # Minimal automation detection
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Security and stability
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--ignore-ssl-errors")
        
        # Modern user agent
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
        
        # Logging for debugging
        options.add_argument("--enable-logging")
        options.add_argument("--log-level=0")
        options.add_argument("--v=1")
        
        # Kill any existing Chrome processes to avoid conflicts
        try:
            import subprocess
            subprocess.run("taskkill /f /im chrome.exe /t", shell=True, capture_output=True)
            time.sleep(2)
            logger.debug("Cleared existing Chrome processes")
        except:
            pass
        
        # Start Chrome with better error handling
        try:
            logger.info("Starting Chrome browser...")
            driver = webdriver.Chrome(options=options)
            
            # Hide automation indicators
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.success("✅ Chrome browser started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start Chrome: {e}")
            
            # Try with minimal options as fallback
            logger.info("Trying with minimal Chrome options...")
            options = Options()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage") 
            options.add_argument("--window-size=1920,1080")
            
            try:
                driver = webdriver.Chrome(options=options)
                logger.success("✅ Chrome started with minimal options")
            except Exception as e2:
                logger.error(f"Chrome startup failed completely: {e2}")
                return False
        
        # Navigate directly to OAuth authorization URL (skip login page)
        logger.info("🚀 Navigating directly to OAuth authorization URL...")
        logger.info(f"URL: {auth_url}")
        
        try:
            driver.get(auth_url)
            time.sleep(3)  # Wait for page to load
            logger.success("✅ OAuth authorization page loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load OAuth URL: {e}")
            driver.quit()
            return False
        
        # Wait for OAuth authorization page to load
        logger.info("⏳ Waiting for OAuth authorization page to load...")
        
        # Wait for page to fully load
        try:
            wait = WebDriverWait(driver, 15)
            WebDriverWait(driver, 10).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            time.sleep(2)  # Additional wait for dynamic content
            logger.success("✅ OAuth page loaded successfully")
        except Exception as e:
            logger.warning(f"Page load timeout: {e}, continuing anyway...")
        
        logger.info("🔐 OAUTH AUTHENTICATION - AUTO URL MONITORING:")
        logger.info("The browser opened directly to Schwab's OAuth authorization page.")
        logger.info("Please complete the following steps:")
        logger.info("1. ✅ Login with your Schwab username/password when prompted")
        logger.info("2. ✅ Complete any 2FA (SMS/app) if required") 
        logger.info("3. ✅ Click 'Allow' or 'Authorize' to grant API permissions")
        logger.info("4. ✅ No manual copying needed - script monitors URL automatically!")
        logger.info("")
        logger.info("💡 After authorization, you'll see an SSL error page - that's expected!")
        logger.info("🚀 Script is monitoring browser URL for the OAuth redirect...")
        logger.info("⏱️  Please complete the steps above within 5 minutes")
        
        # Enhanced URL monitoring - continuously check browser URL
        redirect_timeout = 300  # 5 minutes
        start_time = time.time()
        last_url = ""
        url_check_count = 0
        
        logger.info("🔍 Starting continuous URL monitoring...")
        
        while time.time() - start_time < redirect_timeout:
            try:
                # Get current URL from browser
                current_url = driver.current_url
                url_check_count += 1
                
                # Log URL changes for debugging
                if current_url != last_url:
                    logger.info(f"🌐 Browser navigated to: {current_url}")
                    last_url = current_url
                elif url_check_count % 30 == 0:  # Status update every 15 seconds
                    elapsed = int(time.time() - start_time)
                    logger.debug(f"🔄 Still monitoring... ({elapsed}s elapsed, current: {current_url[:50]}...)")
                
                # Check if we've hit the OAuth redirect (even if SSL error)
                if "127.0.0.1" in current_url:
                    logger.success(f"🎯 Detected 127.0.0.1 redirect!")
                    logger.info(f"📋 Full URL: {current_url}")
                    
                    # Extract auth code from URL
                    try:
                        if "code=" in current_url:
                            # Parse the URL to extract the authorization code
                            parsed_url = urlparse(current_url)
                            query_params = parse_qs(parsed_url.query)
                            
                            if 'code' in query_params:
                                auth_code = unquote(query_params['code'][0])
                                logger.success(f"✅ Authorization code found: {auth_code[:25]}...")
                                
                                # Store auth code
                                CallbackHandler.auth_code = auth_code
                                
                                # Process immediately
                                logger.info("⚡ Processing authorization code immediately...")
                                process_auth_code_immediately(auth_code, app_key, app_secret)
                                
                                # Wait for processing to complete
                                processing_timeout = 30
                                processing_start = time.time()
                                while not CallbackHandler.tokens_received and (time.time() - processing_start) < processing_timeout:
                                    time.sleep(0.5)
                                
                                # Close browser
                                logger.info("🎉 Authentication complete! Closing browser...")
                                driver.quit()
                                
                                if CallbackHandler.token_result == "Success":
                                    return True
                                else:
                                    logger.error(f"Token processing failed: {CallbackHandler.token_result}")
                                    return False
                            else:
                                logger.warning("127.0.0.1 URL found but no 'code' parameter detected")
                        else:
                            logger.warning(f"127.0.0.1 detected but no 'code' parameter in URL: {current_url}")
                            
                    except Exception as e:
                        logger.error(f"Error processing redirect URL: {e}")
                        logger.info(f"Raw URL: {current_url}")
                
                # Check for explicit error conditions
                elif any(error in current_url.lower() for error in ['error=', 'denied', 'cancelled']):
                    logger.error(f"❌ OAuth error detected: {current_url}")
                    driver.quit()
                    return False
                
                # Sleep briefly before next check
                time.sleep(0.5)  # Check every 500ms for responsive detection
                
            except Exception as e:
                # Handle browser navigation errors (common with SSL errors)
                error_msg = str(e).lower()
                if any(term in error_msg for term in ['navigation', 'net::', 'ssl', 'connection']):
                    # These are expected when hitting 127.0.0.1 SSL errors
                    logger.debug(f"Expected navigation error (SSL redirect): {e}")
                    
                    # Try to get URL via JavaScript as fallback
                    try:
                        js_url = driver.execute_script("return window.location.href;")
                        if js_url and "127.0.0.1" in js_url:
                            logger.success(f"🎯 URL detected via JavaScript: {js_url}")
                            current_url = js_url
                            continue  # Go back to main processing loop
                    except:
                        pass
                
                logger.debug(f"URL monitoring exception: {e}")
                time.sleep(1)
                continue
        
        # Timeout
        logger.warning("⏰ URL monitoring timeout (5 minutes)")
        try:
            driver.quit()
        except:
            pass
        return False
        
    except Exception as e:
        logger.error(f"Browser automation error: {e}")
        try:
            driver.quit()
        except:
            pass
        return False
    
    finally:
        # Clean up temporary profile directory
        try:
            import shutil
            if 'temp_profile' in locals():
                shutil.rmtree(temp_profile, ignore_errors=True)
                logger.debug(f"Cleaned up temporary profile: {temp_profile}")
        except:
            pass


# Clipboard monitoring removed - now using direct URL monitoring instead


def create_self_signed_cert():
    """Create a temporary self-signed certificate for HTTPS"""
    try:
        import tempfile
        import ipaddress
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        import datetime
        
        # Generate private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        
        # Create certificate
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, u"127.0.0.1"),
        ])
        
        cert = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            private_key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            datetime.datetime.utcnow()
        ).not_valid_after(
            datetime.datetime.utcnow() + datetime.timedelta(days=1)
        ).add_extension(
            x509.SubjectAlternativeName([
                x509.IPAddress(ipaddress.IPv4Address('127.0.0.1')),
            ]),
            critical=False,
        ).sign(private_key, hashes.SHA256())
        
        # Create temporary files
        cert_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.crt')
        key_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.key')
        
        # Write certificate and key
        cert_file.write(cert.public_bytes(serialization.Encoding.PEM))
        cert_file.close()
        
        key_file.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ))
        key_file.close()
        
        return cert_file.name, key_file.name
        
    except ImportError:
        logger.warning("cryptography package not available for HTTPS. Install with: pip install cryptography")
        return None, None
    except Exception as e:
        logger.warning(f"Could not create self-signed certificate: {e}")
        return None, None


def start_local_server():
    """Start simple HTTP server (clipboard monitoring is primary method)"""
    try:
        server = HTTPServer(('127.0.0.1', 8080), CallbackHandler)
        logger.info("Starting backup HTTP server on 127.0.0.1:8080...")
        
        def run_server():
            try:
                server.serve_forever()
            except Exception as e:
                pass
        
        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()
        
        return [server]  # Return as list for compatibility
    except Exception as e:
        logger.debug(f"Could not start backup server: {e}")
        return []


def main():
    try:
        # Reset callback state
        CallbackHandler.auth_code = None
        CallbackHandler.callback_received = False
        CallbackHandler.tokens_received = False
        CallbackHandler.token_result = None
        
        # Get app credentials
        app_key, app_secret, cs_auth_url = construct_init_auth_url()
        
        # Try automated redirect capture first
        if SELENIUM_AVAILABLE:
            logger.info("🚀 Starting fully automated authentication...")
            logger.info("🔍 The script will monitor the browser URL automatically - no manual copying needed!")
            automated_success = automated_browser_auth(cs_auth_url, app_key, app_secret)
            
            if automated_success:
                if CallbackHandler.token_result == "Success":
                    return "✅ Done! Tokens obtained with automatic URL monitoring."
                else:
                    return CallbackHandler.token_result
            else:
                logger.warning("Automated URL monitoring failed...")
        
        # If automated browser monitoring failed
        logger.error("❌ Automated URL monitoring failed after timeout")
        logger.info("Please try running the script again or check your browser settings")
        return "Authentication timeout - automated URL monitoring failed"
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        return "Cancelled"
    except Exception as e:
        logger.error(f"Error during authentication: {e}")
        return f"Error: {e}"
    
    finally:
        # Clean up servers
        try:
            if 'servers' in locals() and servers:
                for server in servers:
                    server.shutdown()
                logger.info("Local servers stopped")
        except:
            pass


if __name__ == "__main__":
    main()
