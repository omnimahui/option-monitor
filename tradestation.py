# This code is from https://www.reddit.com/r/Schwab/comments/1c2ioe1/the_unofficial_guide_to_charles_schwabs_trader/
# To generate refresh token and access token
#
import os
import base64
import requests
import webbrowser
from loguru import logger
from settings import TRADESTATION_KEY, TRADESTATION_SECRET


def construct_init_auth_url() -> tuple[str, str, str]:

    app_key = TRADESTATION_KEY
    app_secret = TRADESTATION_SECRET

    auth_url = f"https://signin.tradestation.com/authorize?response_type=code&audience=https://api.tradestation.com&client_id={app_key}&redirect_uri=http://localhost:3001&scope=openid%20MarketData%20profile%20ReadAccount%20Trade%20OptionSpreads%20offline_access%20Matrix"

    logger.info("Click to authenticate:")
    logger.info(auth_url)

    return app_key, app_secret, auth_url


def construct_headers_and_payload(returned_url, app_key, app_secret):
    response_code = (
        f"{returned_url[returned_url.index('code=') + 5:]}"
    )

    credentials = f"{app_key}:{app_secret}"
    base64_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"Basic {base64_credentials}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    payload = {
        "grant_type": "authorization_code",
        "code": response_code,
        "redirect_uri": "http://localhost:3001",
    }

    return headers, payload


def retrieve_tokens(headers, payload) -> dict:
    init_token_response = requests.post(
        url="https://signin.tradestation.com/oauth/token",
        headers=headers,
        data=payload,
    )

    init_tokens_dict = init_token_response.json()

    return init_tokens_dict


def main():
    app_key, app_secret, cs_auth_url = construct_init_auth_url()
    webbrowser.open(cs_auth_url)

    logger.info("Paste Returned URL:")
    returned_url = input()

    init_token_headers, init_token_payload = construct_headers_and_payload(
        returned_url, app_key, app_secret
    )

    init_tokens_dict = retrieve_tokens(
        headers=init_token_headers, payload=init_token_payload
    )

    logger.debug(init_tokens_dict)

    return "Done!"


if __name__ == "__main__":
    main()
