#!/opt/anaconda3/bin/python

import requests
import json
from munch import DefaultMunch
import re
from datetime import datetime, timedelta, date
import numpy as np

# numpy.random._bit_generator = numpy.random.bit_generator
import pandas as pd
import pprint
import time
import scipy.stats
import math
import finnhub
from IPython.display import display, HTML

display(HTML("<style>.container { width:100% !important; }</style>"))
from pretty_html_table import build_table
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from smtplib import SMTP
import smtplib
import sys
import os
import base64
import io
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for email generation
import matplotlib.pyplot as plt
from settings import (
    SCHWAB_APP_KEY,
    SCHWAB_APP_SECRET,
    SCHWAB_REFRESH_TOKEN,
    FINNHUB_KEY,
    SMTP_SERVER,
    SMTP_PORT,
    SMTP_USER,
    SMTP_PASS,
    TRADESTATION_FRESHTOKEN,
    TRADESTATION_KEY,
    TRADESTATION_SECRET,
    TRADESTATION_ACCOUNTID,
    FIDELITY_CREDENTIALS,
)
import yfinance as yf

UNIFIED_OPTION_PATTERN = r"([a-zA-Z][a-zA-Z0-9]*)(\d*)_(\d+)([C|P])(\d+\.?\d*)"


def getERdate2(symbol):
    # Special case: Adjusted option symbols -> Actual ticker for API queries
    if symbol == "TSLL1":
        symbol = "TSLL"

    finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)
    try:
        er_dates = finnhub_client.earnings_calendar(
            _from=date.today() - timedelta(days=1),
            to=date.today() + timedelta(days=100),
            symbol=symbol,
            international=False,
        )
        time.sleep(1)
    except:
        er_dates = {}
    if not er_dates or not er_dates["earningsCalendar"]:
        return "2099-12-31"
    else:
        return er_dates["earningsCalendar"][-1]["date"]


class Option:
    # symbol requires unified format
    def __init__(self, symbol):
        m = re.compile(UNIFIED_OPTION_PATTERN).search(symbol)
        self.underlying = m.group(1)
        self.optional_digit = m.group(2)
        self.exp = datetime.strptime(m.group(3), "%y%m%d")
        self.callput = "CALL" if m.group(4) == "C" else "PUT"
        self.strike = float(m.group(5))
        self.price = 0
        self.underlyingPrice = 0
        self.daysToExpiration = 0
        self.intrinsic = 0
        self.extrinsic = 0
        self.itm = 0
        self.actionNeed = 0
        self.daysToER = 0
        self.delta = 0
        self.gamma = 0
        self.theta = 0
        self.vega = 0
        self.openInterest = 0
        self.volatility = 0
        self.underlying_volatility = 0
        self.Xstd = 0

    def is_expired(self):
        # Compare with today's date
        return self.exp + timedelta(days=1) < datetime.now()

    def download_underlying_OHLC(self):
        # Special case: Adjusted option symbols -> Actual ticker for API queries
        underlying = self.underlying
        if underlying == "TSLL1":
            underlying = "TSLL"

        return yf.download(
            tickers=underlying,
            start=datetime.today() - timedelta(days=365),
            interval="1d",
        )


class Position:
    def __init__(self, symbol, equity_type, quantity, account=""):
        self.symbol = symbol
        self.equity_type = equity_type
        self.quantity = quantity
        self.account = account
        self.property = None


class Portfolio:
    def __init__(self):
        self.portf_list = []
        return

    def add(self, new_pos: Position):
        for pos in self.portf_list:
            if (pos.symbol == new_pos.symbol
                    and pos.equity_type == new_pos.equity_type
                    and pos.account == new_pos.account):
                pos.quantity += new_pos.quantity
                return

        self.portf_list.append(new_pos)
        return

    # def get(self, symbol="", equity_type=""):


class Exchange:
    def __init__(self):
        self.positions = dict()
        self.pos_list = []
        return

    def auth(self):
        return


class IB(Exchange):

    def __init__(self):
        super().__init__()
        self.money_pattern = r"^USD*"
        self.option_pattern = r"^([A-Z][A-Z0-9]*).*\[\1\s+(\d{6})([CP])(\d{8})\s+(\d+)\]$"
        return

    def get_positions(self):
        curr_dir = os.path.dirname(__file__)
        for f in [
            f"{curr_dir}/IB.csv",
        ]:
            df = pd.read_csv(f, header=1)
            # Try to extract last 3 digits of account number from the CSV header row
            acct_label = "IB"
            try:
                header_df = pd.read_csv(f, header=None, nrows=1)
                for cell in header_df.values.flatten():
                    digits = re.sub(r'\D', '', str(cell))
                    if len(digits) >= 3:
                        acct_label = digits[-3:]
                        break
            except Exception:
                pass

            for s in df["Financial Instrument Description"].dropna().values:
                if s in ["Cash Balances", "CNH", "Total (in USD)"]:
                    continue
                # if it is money market (cash)
                m = re.compile(self.money_pattern).search(s)
                if m:
                    pos = Position(
                        "IB",
                        "CASH",
                        int(
                            float(
                                df.loc[
                                    df["Financial Instrument Description"] == s,
                                    "Position",
                                ].values[0]
                            )
                        ),
                        acct_label,
                    )
                    self.pos_list.append(pos)
                    continue

                m = re.compile(self.option_pattern).search(s)
                if m:
                    o = (
                        m.group(1)
                        + "_"
                        + m.group(2)
                        + m.group(3)
                        + str(round(float(m.group(4)) / 1000, 2))
                    )
                    pos = Position(
                        o,
                        "OPTION",
                        df.loc[
                            df["Financial Instrument Description"] == s, "Position"
                        ].values[0],
                        acct_label,
                    )
                    self.pos_list.append(pos)
                    continue
                else:
                    if s and (
                        df.loc[df["Financial Instrument Description"] == s, "Exchange"]
                        .values[0]
                        .lower()
                        == "PINK".lower()
                        or df.loc[
                            df["Financial Instrument Description"] == s, "Exchange"
                        ]
                        .values[0]
                        .lower()
                        == "NYSE".lower()
                        or df.loc[
                            df["Financial Instrument Description"] == s, "Exchange"
                        ]
                        .values[0]
                        .lower()
                        == "NASDAQ".lower()
                    ):
                        pos = Position(
                            s,
                            "STOCK",
                            df.loc[
                                df["Financial Instrument Description"] == s, "Position"
                            ].values[0],
                            acct_label,
                        )
                        self.pos_list.append(pos)

        return self.pos_list


class Fidelity(Exchange):
    def __init__(self):
        super().__init__()
        self.money_pattern = r"^FDRXX.*|^SPAXX.*"
        # handles optional leading '-' (short), optional space between ticker and date
        self.option_pattern = r"[^a-zA-Z0-9]*([a-zA-Z0-9]+)\s*(\d{6})([C|P]\d+\.?\d*)"

    def get_positions(self):
        active = [(u, p) for u, p in FIDELITY_CREDENTIALS if u and p]
        if active:
            return self._get_positions_browser(active)
        return self._get_positions_csv()

    def _get_positions_browser(self, credentials: list):
        """Fetch positions using saved browser sessions (headless, no prompts).
        Run fidelity-login.py first to create the session files.
        """
        from fidelity.fidelity import FidelityAutomation
        curr_dir = os.path.dirname(os.path.abspath(__file__))

        for idx, (username, _password) in enumerate(credentials):
            session_file = os.path.join(curr_dir, f"Fidelity_acct{idx}.json")
            if not os.path.exists(session_file):
                print(f"[Fidelity] Session file not found for account {idx} ({username}). "
                      f"Run fidelity-login.py first.")
                continue

            fid = FidelityAutomation(
                headless=True,
                save_state=True,
                title=f"acct{idx}",
                profile_path=curr_dir,
            )
            try:
                print(f"[Fidelity] Loading positions for {username} ...")
                account_dict = fid.getAccountInfo()
                if not account_dict:
                    print(f"[Fidelity] Session expired for {username}. Run fidelity-login.py to refresh.")
                    continue
                self._parse_browser_account_dict(account_dict)
                print(f"[Fidelity] {username}: {len(account_dict)} account(s) loaded")
            except Exception as e:
                print(f"[Fidelity] Error for {username}: {e}. Run fidelity-login.py to refresh session.")
            finally:
                fid.close_browser()

        return self.pos_list

    def _parse_browser_account_dict(self, account_dict: dict):
        """Convert FidelityAutomation.account_dict to Position objects."""
        opt_re = re.compile(self.option_pattern)
        mm_re  = re.compile(self.money_pattern)

        for account_num, acct_data in account_dict.items():
            acct_label = re.sub(r'\D', '', str(account_num))[-3:] or "FID"
            cash_total = 0.0

            for stock in acct_data.get("stocks", []):
                ticker   = str(stock["ticker"])
                quantity = float(stock["quantity"])
                value    = float(stock["value"])

                # fidelity_api strips '-' from Quantity; detect short via ticker prefix
                is_short = ticker.startswith("-")

                # Money market / cash equivalent
                if mm_re.search(ticker):
                    cash_total += value
                    continue

                # Option
                m = opt_re.search(ticker)
                if m:
                    underlying = m.group(1)
                    exp        = m.group(2)
                    cp_strike  = m.group(3)  # e.g. "C14.0"
                    o = f"{underlying}_{exp}{cp_strike}"
                    qty = -quantity if is_short else quantity
                    self.pos_list.append(Position(o, "OPTION", qty, acct_label))
                    continue

                # Plain stock — skip ticker prefixed with '-' that didn't match option
                clean = ticker.lstrip("-").strip()
                if clean:
                    qty = -quantity if is_short else quantity
                    self.pos_list.append(Position(clean, "STOCK", qty, acct_label))

            if cash_total > 0:
                self.pos_list.append(
                    Position("Fidelity", "CASH", int(cash_total), acct_label)
                )

    def _get_positions_csv(self):
        curr_dir = os.path.dirname(__file__)
        opt_re = re.compile(self.option_pattern)
        mm_re  = re.compile(self.money_pattern)
        for f in [
            f"{curr_dir}/fidelity18-ira.csv",
            f"{curr_dir}/fidelity18-roth.csv",
            f"{curr_dir}/fidelity20.csv",
        ]:
            print(f"[DEBUG Fidelity] Reading file: {f}")
            df = pd.read_csv(f, encoding='utf-8-sig', index_col=False)

            acct_label = re.sub(r'\D', '', os.path.basename(f))[-3:] or "FID"
            if "Account Number" in df.columns:
                acct_nums = df["Account Number"].dropna().astype(str)
                if len(acct_nums) > 0:
                    acct_label = re.sub(r'\D', '', acct_nums.iloc[0])[-3:] or acct_label

            for s in df["Symbol"].dropna().values:
                if not s or s == "Pending Activity":
                    continue

                print(f"[DEBUG Fidelity] Processing symbol: '{s}'")

                if mm_re.search(s):
                    print(f"[DEBUG Fidelity]   -> Matched as CASH")
                    pos = Position(
                        s,
                        "CASH",
                        int(float(df.loc[df["Symbol"] == s, "Current Value"].values[0].strip("$"))),
                        acct_label,
                    )
                    self.pos_list.append(pos)
                    continue

                m = opt_re.search(s)
                if m:
                    o = m.group(1) + "_" + m.group(2) + m.group(3)
                    qty = df.loc[df['Symbol'] == s, 'Quantity'].values[0]
                    print(f"[DEBUG Fidelity]   -> Matched as OPTION: '{o}' (Qty: {qty})")
                    if not re.compile(UNIFIED_OPTION_PATTERN).search(o):
                        print(f"[DEBUG Fidelity]   WARNING: '{o}' does NOT match UNIFIED_OPTION_PATTERN!")
                    self.pos_list.append(Position(o, "OPTION", qty, acct_label))
                    continue

                acct_name = df.loc[df["Symbol"] == s, "Account Name"].values[0].lower()
                if s and acct_name in ("roth ira", "traditional ira"):
                    print(f"[DEBUG Fidelity]   -> Matched as STOCK")
                    self.pos_list.append(
                        Position(s, "STOCK", df.loc[df["Symbol"] == s, "Quantity"].values[0], acct_label)
                    )
                else:
                    print(f"[DEBUG Fidelity]   -> SKIPPED")

        return self.pos_list


class TradeStation(Exchange):
    def __init__(self):
        super().__init__()
        self.access_token = ""
        self.option_pattern = r"([a-zA-Z][a-zA-Z0-9]*)(\d*)\s+(\d+)([C|P])(\d+\.?\d*)"
        self.base_url = "https://api.tradestation.com"
        self.auth()

    def auth(self) -> str:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": TRADESTATION_FRESHTOKEN,
            "client_id": TRADESTATION_KEY,
            "client_secret": TRADESTATION_SECRET,
        }
        response = requests.post(
            url="https://signin.tradestation.com/oauth/token",
            headers=headers,
            data=payload,
        )
        if response.status_code == 200:
            self.access_token = response.json()["access_token"]
            return self.access_token
        else:
            return ""

    def send_request(self, url):
        response = requests.get(
            url, headers={"Authorization": "Bearer " + self.access_token}
        )
        if response.status_code == 200:
            return DefaultMunch.fromDict(response.json())
        else:
            raise Exception(response.status_code)

    def parse_positions(self, positions):
        l = []
        for pos in positions:
            if pos.AssetType == "OPTION":
                try:
                    m = re.compile(self.option_pattern).search(pos.instrument.symbol)
                    underlying = m.group(1)
                    optional_digit = m.group(2)
                    exp = m.group(3)
                    callput = m.group(4)
                    strike = round(float(m.group(5)) / 1000, 2)
                    symbol = f"{underlying}{optional_digit}_{exp}{callput}{strike}"  # normalize option symbol
                    pos = Position(
                        symbol, "OPTION", pos.longQuantity - pos.shortQuantity
                    )
                    l.append(pos)
                except Exception as ex:
                    print(f"{pos.instrument.symbol} {ex}")
            elif pos.AssetType == "STOCK":
                pos = Position(pos.Symbol, "STOCK", float(pos.Quantity))
                l.append(pos)
        return l

    def get_positions(self) -> dict:
        self.pos_list = []
        url = (
            f"{self.base_url}/v3/brokerage/accounts/{TRADESTATION_ACCOUNTID}/positions"
        )
        response = self.send_request(url)
        self.pos = self.parse_positions(response.Positions)

        url = f"{self.base_url}/v3/brokerage/accounts/{TRADESTATION_ACCOUNTID}/balances"
        response = self.send_request(url)
        self.pos.append(
            Position(
                f"TradeStation",
                "CASH",
                response.Balances[0].CashBalance,
            )
        )
        self.pos_list.extend(self.pos)

        return self.pos_list


class Schwab(Exchange):
    def __init__(self):
        super().__init__()
        self.access_token = ""
        self.account_number = []        # hash values used for API calls
        self.account_number_plain = []  # last-3 digits of real account numbers
        self.base_url = "https://api.schwabapi.com"
        self.option_pattern = r"([a-zA-Z][a-zA-Z0-9]*)(\d*)\s+(\d+)([C|P])(\d+\.?\d*)"
        self.auth()

    def auth(self) -> str:
        headers = {
            "Authorization": f'Basic {base64.b64encode(f"{SCHWAB_APP_KEY}:{SCHWAB_APP_SECRET}".encode()).decode()}',
            "Content-Type": "application/x-www-form-urlencoded",
        }
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": SCHWAB_REFRESH_TOKEN,
        }
        response = requests.post(
            url="https://api.schwabapi.com/v1/oauth/token",
            headers=headers,
            data=payload,
        )
        if response.status_code == 200:
            self.access_token = response.json()["access_token"]
            return self.access_token
        else:
            return ""

    def get_account_number_hash_value(self) -> str:
        url = f"{self.base_url}/trader/v1/accounts/accountNumbers"
        response = self.send_request(url)
        for i in range(len(response)):
            self.account_number.append(response[i].hashValue)
            plain = str(response[i].accountNumber)
            self.account_number_plain.append(re.sub(r'\D', '', plain)[-3:] or str(i))
        return self.account_number

    def parse_positions(self, positions):
        l = []
        if positions is None:
            return l
        for pos in positions:
            if pos.instrument.assetType == "OPTION":
                try:
                    m = re.compile(self.option_pattern).search(pos.instrument.symbol)
                    underlying = m.group(1)
                    optional_digit = m.group(2)
                    exp = m.group(3)
                    callput = m.group(4)
                    strike = round(float(m.group(5)) / 1000, 2)
                    symbol = f"{underlying}{optional_digit}_{exp}{callput}{strike}"  # normalize option symbol
                    pos = Position(
                        symbol, "OPTION", pos.longQuantity - pos.shortQuantity
                    )
                    l.append(pos)
                except Exception as ex:
                    print(f"{pos.instrument.symbol} {ex}")
            elif (
                pos.instrument.assetType == "EQUITY"
                or pos.instrument.assetType == "COLLECTIVE_INVESTMENT"
            ):
                pos = Position(
                    pos.instrument.symbol, "STOCK", pos.longQuantity - pos.shortQuantity
                )
                l.append(pos)
        return l

    def send_request(self, url):
        response = requests.get(
            url, headers={"Authorization": "Bearer " + self.access_token}
        )
        if response.status_code == 200:
            return DefaultMunch.fromDict(response.json())
        else:
            raise Exception(response.status_code)

        return response

    def schwab_option_symbol(self, symbol):
        m = re.compile(UNIFIED_OPTION_PATTERN).search(symbol)
        underlying = m.group(1)

        # Special case: Adjusted option symbols -> Actual ticker for API queries
        if underlying == "TSLL1":
            underlying = "TSLL"

        exp = m.group(2)
        callput = m.group(3)
        strike = int(float(m.group(4)) * 1000)
        # schwab requires symbol = 'JD    240524C00032000'
        schwab_option_symbol = f"{underlying:<6}{exp}{callput}{strike :>08}"
        return schwab_option_symbol

    def get_positions(self) -> dict:
        self.get_account_number_hash_value()
        self.pos_list = []
        for i, acct in enumerate(self.account_number):
            acct_label = self.account_number_plain[i] if i < len(self.account_number_plain) else str(i)
            url = f"{self.base_url}/trader/v1/accounts/{acct}?fields=positions"
            response = self.send_request(url)
            self.pos = self.parse_positions(response.securitiesAccount.positions)
            for p in self.pos:
                p.account = acct_label
            self.pos.append(
                Position(
                    f"SchWab{i}",
                    "CASH",
                    response.securitiesAccount.initialBalances.cashBalance,
                    acct_label,
                )
            )
            self.pos_list.extend(self.pos)
        return self.pos_list

    def get_quote_obj(self, symbol, equity_type):
        if equity_type == "OPTION":
            symbol = self.schwab_option_symbol(symbol)
        url = f"{self.base_url}/marketdata/v1/{symbol}/quotes?fields=quote"
        response = self.send_request(url)
        if response:
            quote_obj = response[symbol].quote
        else:
            quote_obj = None
        return quote_obj

    def get_chain_obj(self, option: Option):
        # https://api.schwabapi.com/marketdata/v1/chains?symbol=JD&contractType=CALL&strike=32&fromDate=2024-05-24&toDate=2024-05-24
        underlying = option.underlying

        # Special case: Adjusted option symbols -> Actual ticker for API queries
        if underlying == "TSLL1":
            underlying = "TSLL"

        exp = option.exp.strftime("%Y-%m-%d")
        callput = option.callput
        strike = f"{option.strike:g}"
        url = f"{self.base_url}/marketdata/v1/chains?symbol={underlying}&contractType={callput}&strike={strike}&fromDate={exp}&toDate={exp}"
        response = self.send_request(url)
        if response:
            chain_obj = response
        else:
            chain_obj = None
        return chain_obj

    def get_full_chain_obj(self, underlying, callput, from_date=None, to_date=None, days_out=45):
        """
        Get full option chain for rollover analysis
        Args:
            underlying: stock symbol
            callput: "CALL" or "PUT"
            from_date: start date (defaults to today)
            to_date: end date (defaults to days_out)
            days_out: number of days to fetch (default 45, can extend to 90+ for fallback)
        """
        # Special case: TSLL1 -> TSLL for API queries
        if underlying == "TSLL1":
            underlying = "TSLL"

        if from_date is None:
            from_date = datetime.today().strftime("%Y-%m-%d")
        if to_date is None:
            to_date = (datetime.today() + timedelta(days=days_out)).strftime("%Y-%m-%d")

        # Don't filter by strike to get all available options
        url = f"{self.base_url}/marketdata/v1/chains?symbol={underlying}&contractType={callput}&fromDate={from_date}&toDate={to_date}"
        print(f"  Fetching chain: {url}")
        response = self.send_request(url)

        if response:
            # Debug: show what date ranges we actually got back
            if callput == "CALL":
                exp_map = response.callExpDateMap if hasattr(response, 'callExpDateMap') else {}
            else:
                exp_map = response.putExpDateMap if hasattr(response, 'putExpDateMap') else {}

            if exp_map:
                dates = list(exp_map.keys())
                print(f"  Received {len(dates)} expiration dates from API")

        return response if response else None

    def load_option_properties(self, pos):
        if pos.equity_type != "OPTION":
            return pos
        option = Option(pos.symbol)
        if option.is_expired():
            print(f"[DEBUG]   Option is expired: {pos.symbol}")
            return None

        try:
            chain_obj = self.get_chain_obj(option)
            if not chain_obj:
                print(f"[DEBUG]   Failed to get chain data for: {pos.symbol}")
                return None
        except Exception as e:
            print(f"[DEBUG]   Exception getting chain for {pos.symbol}: {e}")
            return None

        option.underlyingPrice = chain_obj.underlyingPrice
        if option.callput == "CALL":
            option_data = (
                chain_obj.callExpDateMap.values()
                .__iter__()
                .__next__()
                .values()
                .__iter__()
                .__next__()[0]
            )
        else:
            option_data = (
                chain_obj.putExpDateMap.values()
                .__iter__()
                .__next__()
                .values()
                .__iter__()
                .__next__()[0]
            )
        option.price = round((option_data.ask + option_data.bid) / 2, 2)
        option.daysToExpiration = option_data.daysToExpiration
        option.intrinsic = (
            max(option.underlyingPrice - option.strike, 0)
            if option.callput == "CALL"
            else max(option.strike - option.underlyingPrice, 0)
        )
        option.extrinsic = option.price - option.intrinsic
        option.itm = 1 if option_data.inTheMoney == True else 0
        option.actionNeed = (
            1
            if (option.itm == 1 and option.daysToExpiration <= 5)
            or (option.extrinsic <= option.strike / 100)
            else 0
        )
        option.daysToER = int(
            (
                datetime.strptime(getERdate2(option.underlying), "%Y-%m-%d").date()
                - date.today()
            ).days
        )
        option.delta = option_data.delta
        option.gamma = option_data.gamma
        option.theta = option_data.theta
        option.vega = option_data.vega
        option.openInterest = option_data.openInterest
        option.volatility = option_data.volatility
        option.underlying_volatility = round(
            option.download_underlying_OHLC()["Close"].std().values[0],
            2,
        )
        option.Xstd = abs(
            round(
                (option.strike - option.underlyingPrice) / option.underlying_volatility,
                2,
            )
        )
        pos.property = option

        return pos

    def find_best_rollover(self, current_option: Option, position_quantity: int, action_needed: int):
        """
        Find best rollover option when Action is 1
        Args:
            current_option: the current option position
            position_quantity: number of contracts (negative for short positions)
            action_needed: the actionNeed flag (1 = needs action)
        Returns:
            dict with rollover recommendation or None
        """
        # Only consider rollover for short positions (quantity < 0)
        if position_quantity >= 0:
            return None

        # Only evaluate rollover if Action is 1
        if action_needed != 1:
            print(f"  No action needed (Action={action_needed})")
            return None

        print(f"  Action needed (Action=1), evaluating rollover options...")
        print(f"  Current position: DTE={current_option.daysToExpiration}, Strike=${current_option.strike}, Extrinsic=${current_option.extrinsic:.2f}")

        # Try with standard criteria first (45 days), then fallback to relaxed criteria (90 days)
        for attempt, (days_out, allow_larger_debit, min_distance) in enumerate([
            (45, False, 2.0),  # First attempt: strict criteria
            (90, True, 1.0),   # Second attempt: relaxed criteria - allow larger debits, closer strikes, longer DTE
        ], start=1):

            if attempt > 1:
                print(f"  No candidates found with strict criteria. Trying fallback with relaxed criteria...")
                print(f"    - Extending DTE range to {days_out} days")
                print(f"    - Allowing larger debits (up to 30% of extrinsic)")
                print(f"    - Reducing minimum distance to {min_distance}%")

            try:
                # Get full chain for the same option type
                chain_obj = self.get_full_chain_obj(
                    current_option.underlying,
                    current_option.callput,
                    from_date=datetime.today().strftime("%Y-%m-%d"),
                    to_date=None,
                    days_out=days_out
                )

                if not chain_obj:
                    print(f"  No chain data available for {current_option.underlying}")
                    continue

                print(f"  Fetched chain data for {current_option.underlying}")

                # Parse chain data
                exp_date_map = chain_obj.callExpDateMap if current_option.callput == "CALL" else chain_obj.putExpDateMap
                underlying_price = chain_obj.underlyingPrice

                rollover_candidates = []
                total_options_checked = 0
                filtered_by_expiration = 0
                filtered_by_pricing = 0
                filtered_by_spread = 0
                filtered_by_debit = 0

                for exp_date_str, strikes_dict in exp_date_map.items():
                    for strike_str, options_list in strikes_dict.items():
                        for opt_data in options_list:
                            total_options_checked += 1

                            # Only consider options with later expiration (at least 1 day more)
                            if opt_data.daysToExpiration <= current_option.daysToExpiration:
                                filtered_by_expiration += 1
                                continue

                            # Prefer rollovers that add meaningful time (at least 7 days)
                            # But still allow shorter rollovers if needed
                            days_gained = opt_data.daysToExpiration - current_option.daysToExpiration

                            # Calculate metrics
                            bid = opt_data.bid
                            ask = opt_data.ask
                            mid_price = (bid + ask) / 2

                            # Skip if no valid pricing
                            if bid <= 0 or ask <= 0:
                                filtered_by_pricing += 1
                                continue

                            # Calculate bid/ask spread percentage
                            bid_ask_spread_pct = (ask - bid) / mid_price * 100

                            # Skip if spread is too wide (> 20%)
                            if bid_ask_spread_pct > 20:
                                filtered_by_spread += 1
                                continue

                            # Calculate intrinsic and extrinsic value
                            if current_option.callput == "CALL":
                                intrinsic = max(underlying_price - opt_data.strikePrice, 0)
                            else:
                                intrinsic = max(opt_data.strikePrice - underlying_price, 0)

                            extrinsic = mid_price - intrinsic

                            # Skip ITM options (we don't want to risk assignment)
                            if intrinsic > 0:
                                filtered_by_debit += 1  # Use debit counter for ITM filtering
                                continue

                            # Calculate distance from current price (safety margin)
                            # Per user requirement:
                            # - CALL with strike > price = safe from assignment, but show as NEGATIVE distance
                            # - PUT with strike < price = safe from assignment, but show as NEGATIVE distance
                            # More negative = safer (further OTM)
                            if current_option.callput == "CALL":
                                # CALL: strike > price (OTM) should be negative
                                distance_pct = (underlying_price - opt_data.strikePrice) / underlying_price * 100
                                # Negative = OTM (good), Positive = ITM (bad)
                            else:  # PUT
                                # PUT: strike < price (OTM) should be negative
                                distance_pct = (opt_data.strikePrice - underlying_price) / underlying_price * 100
                                # Negative = OTM (good), Positive = ITM (bad)

                            # Skip options too close to current price
                            # Since negative = safe (OTM), we want distance_pct < -min_distance (at least min_distance% OTM)
                            # This means: distance_pct > -min_distance is too close or ITM
                            if distance_pct > -min_distance:
                                filtered_by_debit += 1
                                continue

                            # Calculate net credit (what we receive from rollover)
                            # For short positions: we buy back current (pay current.price) and sell new (receive mid_price)
                            net_credit = mid_price - current_option.price

                            # Prefer credit rollovers, but allow debits based on attempt
                            # First attempt: allow debit up to 20% of extrinsic
                            # Fallback attempt: allow debit up to 30% of extrinsic
                            max_debit_pct = 0.3 if allow_larger_debit else 0.2
                            if net_credit < 0 and abs(net_credit) > (extrinsic * max_debit_pct):
                                filtered_by_debit += 1
                                continue

                            # Calculate extrinsic per day - key metric for comparing options across different DTEs
                            extrinsic_per_day = extrinsic / opt_data.daysToExpiration

                            # APR calculation based on extrinsic per day
                            # This gives annualized return based on the daily income rate
                            apr = (extrinsic_per_day * 365 / opt_data.strikePrice) * 100

                            # Calculate quality score considering multiple factors
                            # Factors: Extrinsic per Day (30%), Safety Distance (25%), Bid/Ask Spread (20%), Theta (15%), IV stability (5%), Liquidity (5%)

                            # Extrinsic value score - use extrinsic per day to normalize across different DTEs
                            # Higher extrinsic per day is better (more efficient income generation)
                            # Normalize: assume $0.10/day per $100 strike is excellent
                            extrinsic_per_day_pct = extrinsic_per_day / (opt_data.strikePrice / 100)
                            extrinsic_score = min(extrinsic_per_day_pct / 0.10, 1.0) * 30

                            # Safety distance score - more negative is safer (normalize to 0-25)
                            # distance_pct is negative for OTM options
                            # Prefer -5% to -15% distance (5-15% OTM), penalize if too far (<-20% or >-2%)
                            abs_distance = abs(distance_pct)
                            if abs_distance >= 5 and abs_distance <= 15:
                                distance_score = 25
                            elif abs_distance > 15:
                                distance_score = max(0, 25 - (abs_distance - 15))  # Penalty for being too far OTM
                            else:  # 2-5%
                                distance_score = (abs_distance - 2) / 3 * 25  # Linear scale from 2-5% OTM

                            # Bid/Ask spread score - smaller spread is better (normalize to 0-20)
                            # Perfect score for spread < 5%, penalty for wider spreads
                            if bid_ask_spread_pct <= 5:
                                spread_score = 20
                            elif bid_ask_spread_pct <= 10:
                                spread_score = 20 - (bid_ask_spread_pct - 5)  # Linear penalty 5-10%
                            else:  # 10-20%
                                spread_score = max(0, 15 - (bid_ask_spread_pct - 10))  # Steeper penalty

                            # Theta score - higher theta decay is better (normalize to 0-15)
                            theta_score = min(abs(opt_data.theta) / 1.0, 1.0) * 15

                            # IV stability score - similar IV is better (normalize to 0-5)
                            iv_diff = abs(opt_data.volatility - current_option.volatility) / max(current_option.volatility, 0.01)
                            iv_score = (1 - min(iv_diff, 1.0)) * 5

                            # Liquidity score - higher OI is better (normalize to 0-5)
                            liquidity_score = min(opt_data.openInterest / 100, 1.0) * 5

                            quality_score = extrinsic_score + distance_score + spread_score + theta_score + iv_score + liquidity_score

                            # Parse expiration date - handle both formats
                            exp_date_str = opt_data.expirationDate
                            if 'T' in exp_date_str:
                                # Has timestamp, extract just the date part
                                exp_date_str = exp_date_str.split('T')[0]
                            expiration_dt = datetime.strptime(exp_date_str, "%Y-%m-%d")

                            rollover_candidates.append({
                                'strike': opt_data.strikePrice,
                                'expiration': expiration_dt,
                                'dte': opt_data.daysToExpiration,
                                'bid': bid,
                                'ask': ask,
                                'mid_price': mid_price,
                                'bid_ask_spread_pct': bid_ask_spread_pct,
                                'net_credit': net_credit,
                                'extrinsic': extrinsic,
                                'extrinsic_per_day': extrinsic_per_day,
                                'theta': opt_data.theta,
                                'delta': opt_data.delta,
                                'iv': opt_data.volatility,
                                'open_interest': opt_data.openInterest,
                                'apr': apr,
                                'quality_score': quality_score,
                                'days_gained': days_gained,
                                'distance_pct': distance_pct,
                                'intrinsic': intrinsic
                            })

                # Print filtering statistics
                print(f"  Options checked: {total_options_checked}")
                print(f"  Filtered by expiration: {filtered_by_expiration}")
                print(f"  Filtered by pricing: {filtered_by_pricing}")
                print(f"  Filtered by spread: {filtered_by_spread}")
                print(f"  Filtered by debit/ITM/distance: {filtered_by_debit}")
                print(f"  Viable candidates: {len(rollover_candidates)}")

                # Show DTE distribution of candidates
                if rollover_candidates:
                    dte_counts = {}
                    for c in rollover_candidates:
                        dte = c['dte']
                        if dte not in dte_counts:
                            dte_counts[dte] = 0
                        dte_counts[dte] += 1
                    print(f"  DTE distribution: {sorted(dte_counts.items())}")

                # Sort by quality score and return top candidate
                if rollover_candidates:
                    rollover_candidates.sort(key=lambda x: x['quality_score'], reverse=True)

                    # Show top 5 candidates for comparison
                    print(f"  Top 5 rollover candidates:")
                    for i, cand in enumerate(rollover_candidates[:5]):
                        print(f"    {i+1}. DTE={cand['dte']}, Strike=${cand['strike']:.2f}, Extrinsic=${cand['extrinsic']:.2f} (${cand['extrinsic_per_day']:.3f}/day), "
                              f"Distance={cand['distance_pct']:.1f}%, Spread={cand['bid_ask_spread_pct']:.1f}%, Quality={cand['quality_score']:.1f}")

                    best = rollover_candidates[0]

                    return {
                        'symbol': f"{current_option.underlying}_{best['expiration'].strftime('%y%m%d')}{current_option.callput[0]}{best['strike']:g}",
                        'strike': best['strike'],
                        'expiration': best['expiration'],
                        'dte': best['dte'],
                        'net_credit': best['net_credit'],
                        'extrinsic': best['extrinsic'],
                        'extrinsic_per_day': best['extrinsic_per_day'],
                        'apr': best['apr'],
                        'bid_ask_spread': best['bid_ask_spread_pct'],
                        'theta': best['theta'],
                        'iv': best['iv'],
                        'open_interest': best['open_interest'],
                        'quality_score': best['quality_score'],
                        'days_gained': best['days_gained'],
                        'distance_pct': best['distance_pct']
                    }

                # No candidates found in this attempt, continue to next attempt (or return None if last attempt)

            except Exception as e:
                print(f"  Error in attempt {attempt}: {e}")
                continue

        # If we reach here, no candidates found in any attempt
        print(f"  No viable rollover candidates found after all attempts")
        return None


def build_option_table(portf: Portfolio, schwab: Schwab) -> str:
    # Create options table
    rows = []
    print(f"\n=== Building Option Table ===")
    print(f"Total positions in portfolio: {len(portf.portf_list)}")

    for pos in portf.portf_list:
        if pos.equity_type != "OPTION":
            continue
        option = pos.property
        print(f"\nProcessing: {option.underlying} {option.callput} {option.strike} Qty={pos.quantity} Action={option.actionNeed}")
        apr = round(
            (option.extrinsic * 100 / (option.strike * 100))
            * (365 / (option.daysToExpiration + 1))
            * 100
            * (-1 if pos.quantity > 0 else 1)
        )
        xstd_value = option.Xstd * (-1 if option.itm == 1 else 1)

        # Check for rollover opportunity when Action is 1
        rollover_recommendation = None
        if option.actionNeed == 1 and pos.quantity < 0:  # Only for short positions with action needed
            print(f"Checking rollover for {option.underlying} {option.callput} {option.strike} (Action=1)")
            rollover_recommendation = schwab.find_best_rollover(option, pos.quantity, option.actionNeed)
            if rollover_recommendation:
                print(f"  -> Found rollover: {rollover_recommendation['symbol']} Credit=${rollover_recommendation['net_credit']:.2f} APR={rollover_recommendation['apr']:.1f}%")
            else:
                print(f"  -> No suitable rollover found")

        # Build base row with Roll_To placeholder (will be filled based on rollover recommendation)
        row = {
            "Symbol": option.underlying,
            "Acct": pos.account,
            "ITM": option.itm,
            "Roll_To": "",  # Placeholder, filled below
            "Action": option.actionNeed,
            "Price": option.price,
            "DaysToExp": option.daysToExpiration,
            "DaysToER": option.daysToER,
            "Quantity": pos.quantity,
            "Extrinsic": option.extrinsic,
            "APR(%)": apr,
            "APR*Xstd": apr * xstd_value,
            "CallPut": option.callput,
            "Strike": option.strike,
            "Underlying": option.underlyingPrice,
            "Xstd": xstd_value,
            "Exp": option.exp,
            "Delta": option.delta,
            "Gamma": option.gamma,
            "Theta": option.theta,
            "Vega": option.vega,
            "OpenInterest": option.openInterest,
            "Volatility": option.volatility,
            "Roll_Strike": "",  # Rollover columns placeholders
            "Roll_DTE": "",
            "Roll_Credit": "",
            "Roll_Extrinsic": "",
            "Roll_Ext/Day": "",
            "Roll_Distance(%)": "",
            "Roll_APR(%)": "",
            "Roll_BidAskSpread(%)": "",
            "Roll_Theta": "",
            "Roll_IV": "",
            "Roll_OI": "",
            "Roll_Quality": "",
        }

        # Fill rollover columns if recommendation exists
        if rollover_recommendation:
            print(f"  Adding rollover recommendation to row: {rollover_recommendation['symbol']}")
            # Remove ticker prefix from Roll_To (e.g., BIDU_251219C131 -> 251219C131)
            roll_to_symbol = rollover_recommendation['symbol']
            if '_' in roll_to_symbol:
                roll_to_symbol = roll_to_symbol.split('_', 1)[1]  # Remove everything before first underscore
            row["Roll_To"] = roll_to_symbol
            row["Roll_Strike"] = rollover_recommendation['strike']
            row["Roll_DTE"] = rollover_recommendation['dte']
            row["Roll_Credit"] = round(rollover_recommendation['net_credit'], 2)
            row["Roll_Extrinsic"] = round(rollover_recommendation['extrinsic'], 2)
            row["Roll_Ext/Day"] = round(rollover_recommendation['extrinsic_per_day'], 3)
            row["Roll_Distance(%)"] = round(rollover_recommendation['distance_pct'], 1)
            row["Roll_APR(%)"] = round(rollover_recommendation['apr'], 1)
            row["Roll_BidAskSpread(%)"] = round(rollover_recommendation['bid_ask_spread'], 1)
            row["Roll_Theta"] = round(rollover_recommendation['theta'], 2)
            row["Roll_IV"] = round(rollover_recommendation['iv'], 2)
            row["Roll_OI"] = rollover_recommendation['open_interest']
            row["Roll_Quality"] = round(rollover_recommendation['quality_score'], 1)
        else:
            print(f"  No rollover recommendation")

        rows.append(row)
        print(f"  Row added. Roll_To={row['Roll_To']}")

    df = pd.DataFrame.from_records(rows)
    df["Unit"] = 100
    df.sort_values(by=["DaysToExp", "Symbol"], inplace=True)

    print(f"\n=== DataFrame Summary ===")
    print(f"Total rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    if 'Roll_To' in df.columns:
        rollover_count = df[df['Roll_To'] != ''].shape[0]
        print(f"Rollover recommendations found: {rollover_count}")
        rollover_yes = df[df['Roll_To'] != '']
        if len(rollover_yes) > 0:
            print(f"\nRollover details:")
            for idx, row in rollover_yes.iterrows():
                print(f"  {row['Symbol']} {row['CallPut']} {row['Strike']}: Roll to {row['Roll_To']} for ${row['Roll_Credit']} credit ({row['Roll_APR(%)']}% APR)")

    # Apply compact styling - narrow columns, no padding
    styled_df = df.style.highlight_between(
        left=1, right=1, subset=["ITM"], props="background:#FFFF00"
    ).highlight_between(
        left=0, right=5, subset=["DaysToExp"], props="background:#a1eafb"
    ).highlight_between(
        left=0, right=5, subset=["DaysToER"], props="background:#a1eafb"
    ).format(
        precision=2
    ).set_table_styles([
        dict(selector="th", props=[
            ("max-width", "fit-content"),
            ("padding", "2px 4px"),
            ("white-space", "nowrap"),
            ("font-size", "11px")
        ]),
        dict(selector="td", props=[
            ("max-width", "fit-content"),
            ("padding", "2px 4px"),
            ("white-space", "nowrap"),
            ("font-size", "11px")
        ]),
        dict(selector="table", props=[
            ("border-collapse", "collapse"),
            ("width", "auto")
        ])
    ]).set_properties(**{
        'text-align': 'left',
        'white-space': 'nowrap'
    })

    return df


def build_sold_put_table(portf: Portfolio) -> pd.DataFrame:
    """Build a table showing all sold (short) PUT options with totals."""
    rows = []
    total_quantity = 0
    total_secured_cash = 0

    for pos in portf.portf_list:
        if pos.equity_type != "OPTION":
            continue
        option = pos.property
        # Only include short PUT positions (quantity < 0 and callput == PUT)
        if option.callput != "PUT" or pos.quantity >= 0:
            continue

        # Calculate secured cash for this position (strike * 100 * |quantity|)
        secured_cash = option.strike * 100 * abs(pos.quantity)
        total_quantity += abs(pos.quantity)
        total_secured_cash += secured_cash

        apr = round(
            (option.extrinsic * 100 / (option.strike * 100))
            * (365 / (option.daysToExpiration + 1))
            * 100
        )
        xstd_value = option.Xstd * (-1 if option.itm == 1 else 1)

        row = {
            "Symbol": option.underlying,
            "Acct": pos.account,
            "Strike": option.strike,
            "Exp": option.exp.strftime("%Y-%m-%d"),
            "DaysToExp": option.daysToExpiration,
            "Quantity": pos.quantity,
            "SecuredCash": secured_cash,
            "Price": option.price,
            "Underlying": option.underlyingPrice,
            "ITM": option.itm,
            "Extrinsic": option.extrinsic,
            "APR(%)": apr,
            "Xstd": xstd_value,
            "Delta": option.delta,
            "Theta": option.theta,
            "DaysToER": option.daysToER,
            "Action": option.actionNeed,
        }
        rows.append(row)

    df = pd.DataFrame.from_records(rows)
    if len(df) > 0:
        df.sort_values(by=["DaysToExp", "Symbol"], inplace=True)
        # Add summary row
        summary_row = {
            "Symbol": "TOTAL",
            "Strike": "",
            "Exp": "",
            "DaysToExp": "",
            "Quantity": -total_quantity,
            "SecuredCash": total_secured_cash,
            "Price": "",
            "Underlying": "",
            "ITM": "",
            "Extrinsic": "",
            "APR(%)": "",
            "Xstd": "",
            "Delta": "",
            "Theta": "",
            "DaysToER": "",
            "Action": "",
        }
        df = pd.concat([df, pd.DataFrame([summary_row])], ignore_index=True)

    return df


def build_stock_table(portf):
    rows = []
    for pos in portf.portf_list:
        if pos.equity_type != "STOCK":
            continue
        row = {
            "Symbol": pos.symbol,
            "Acct": pos.account,
            "Quantity": pos.quantity,
            "DaysToER": int(
                (
                    datetime.strptime(getERdate2(pos.symbol), "%Y-%m-%d").date()
                    - date.today()
                ).days
            ),
        }
        rows.append(row)
    df = pd.DataFrame.from_records(rows)
    df["Delta"] = 1
    df["Unit"] = 1
    df["CallPut"] = "STOCK"

    return df


def build_cash_table(portf):
    rows = []
    for pos in portf.portf_list:
        if pos.equity_type != "CASH":
            continue
        row = {"Symbol": pos.symbol, "Acct": pos.account, "Quantity": pos.quantity}
        rows.append(row)
    df = pd.DataFrame.from_records(rows)
    new_row = {"Symbol": "Total", "Quantity": df["Quantity"].astype(float).sum()}
    df.loc[len(df)] = new_row
    return df


def build_tsll_put_histogram(portf: Portfolio) -> bytes:
    """Build a histogram of TSLL short PUT and CALL positions (x: strike, y: quantity).
    Returns PNG image bytes for email attachment."""
    put_data = []  # List of (strike, quantity, dte, secured_cash)
    call_data = []  # List of (strike, quantity, dte)
    tsll_price = None
    tsll_stock_position = 0  # Total TSLL stock shares

    # First pass: collect TSLL stock positions
    for pos in portf.portf_list:
        if pos.equity_type == "STOCK" and pos.symbol in ["TSLL", "TSLL1"]:
            tsll_stock_position += pos.quantity

    # Second pass: collect option positions
    for pos in portf.portf_list:
        if pos.equity_type != "OPTION":
            continue
        option = pos.property
        # Only include TSLL or TSLL1 positions
        if option.underlying not in ["TSLL", "TSLL1"]:
            continue
        # Only include short positions (quantity < 0)
        if pos.quantity >= 0:
            continue

        # Capture the underlying price (should be same for all TSLL options)
        if tsll_price is None:
            tsll_price = option.underlyingPrice

        qty = abs(pos.quantity)
        dte = option.daysToExpiration

        if option.callput == "PUT":
            secured_cash = option.strike * 100 * qty
            put_data.append((option.strike, qty, dte, secured_cash))
        elif option.callput == "CALL":
            call_data.append((option.strike, qty, dte))

    if not put_data and not call_data:
        return None  # No TSLL short positions

    # Calculate totals
    total_put_qty = sum(d[1] for d in put_data) if put_data else 0
    total_call_qty = sum(d[1] for d in call_data) if call_data else 0
    total_secured_cash = sum(d[3] for d in put_data) if put_data else 0

    # Group data by strike for stacked bars
    # PUT: {strike: [(qty, dte, secured_cash), ...]} sorted by DTE ascending
    put_by_strike = {}
    for d in put_data:
        strike, qty, dte, secured = d
        if strike not in put_by_strike:
            put_by_strike[strike] = []
        put_by_strike[strike].append((qty, dte, secured))
    # Sort each strike's data by DTE ascending (near expiry first/bottom)
    for strike in put_by_strike:
        put_by_strike[strike].sort(key=lambda x: x[1])

    # CALL: {strike: [(qty, dte), ...]} sorted by DTE ascending
    call_by_strike = {}
    for d in call_data:
        strike, qty, dte = d
        if strike not in call_by_strike:
            call_by_strike[strike] = []
        call_by_strike[strike].append((qty, dte))
    for strike in call_by_strike:
        call_by_strike[strike].sort(key=lambda x: x[1])

    # Get unique DTEs to create discrete color mapping
    all_unique_dtes = sorted(set(d[2] for d in put_data) | set(d[2] for d in call_data))
    num_dtes = len(all_unique_dtes)
    dte_to_rank = {dte: i for i, dte in enumerate(all_unique_dtes)}

    # Color function: darker for near expiry (low DTE), lighter for far expiry (high DTE)
    def get_put_color(dte):
        if num_dtes <= 1:
            return (0.1, 0.3, 0.7)  # Single DTE: medium dark blue
        norm = dte_to_rank[dte] / (num_dtes - 1)
        # Dark blue (0.0, 0.1, 0.5) to light blue (0.6, 0.8, 1.0)
        r = 0.0 + norm * 0.6
        g = 0.1 + norm * 0.7
        b = 0.5 + norm * 0.5
        return (r, g, b)

    def get_call_color(dte):
        if num_dtes <= 1:
            return (0.9, 0.4, 0.0)  # Single DTE: medium dark orange
        norm = dte_to_rank[dte] / (num_dtes - 1)
        # Dark orange (0.7, 0.2, 0.0) to light orange (1.0, 0.8, 0.4)
        r = 0.7 + norm * 0.3
        g = 0.2 + norm * 0.6
        b = 0.0 + norm * 0.4
        return (r, g, b)

    # Create the histogram
    fig, ax = plt.subplots(figsize=(12, 6))

    # Combine all strikes for x-axis positioning
    all_strikes = sorted(set(put_by_strike.keys()) | set(call_by_strike.keys()))
    bar_width = 0.35

    # Track if we've added legend entries
    put_legend_added = False
    call_legend_added = False

    # Create stacked bar charts for PUT
    for strike in put_by_strike:
        bottom = 0
        for qty, dte, secured in put_by_strike[strike]:
            color = get_put_color(dte)
            label = 'Short PUT' if not put_legend_added else None
            ax.bar(strike - bar_width/2, qty, width=bar_width, bottom=bottom,
                   color=color, edgecolor='navy', label=label)
            put_legend_added = True
            # Add label in the middle of this segment
            ax.annotate(f'{int(qty)}({dte}d)',
                        xy=(strike - bar_width/2, bottom + qty/2),
                        ha='center', va='center', fontsize=8, fontweight='bold', color='white')
            bottom += qty

    # Create stacked bar charts for CALL
    for strike in call_by_strike:
        bottom = 0
        for qty, dte in call_by_strike[strike]:
            color = get_call_color(dte)
            label = 'Short CALL' if not call_legend_added else None
            ax.bar(strike + bar_width/2, qty, width=bar_width, bottom=bottom,
                   color=color, edgecolor='darkred', label=label)
            call_legend_added = True
            # Add label in the middle of this segment
            ax.annotate(f'{int(qty)}({dte}d)',
                        xy=(strike + bar_width/2, bottom + qty/2),
                        ha='center', va='center', fontsize=8, fontweight='bold', color='white')
            bottom += qty

    # Add vertical line for current TSLL price
    if tsll_price:
        ax.axvline(x=tsll_price, color='red', linestyle='--', linewidth=2, label=f'TSLL Price: ${tsll_price:.2f}')

    ax.set_xlabel('Strike Price ($)', fontsize=12)
    ax.set_ylabel('Quantity (Contracts)', fontsize=12)
    current_datetime = datetime.now().strftime('%Y-%m-%d %A %H:%M:%S')
    ax.set_title(f'TSLL Short Options by Strike\n{current_datetime}', fontsize=14, fontweight='bold')
    ax.set_xticks(all_strikes)
    ax.set_xticklabels([f'${s:g}' for s in all_strikes], rotation=45, ha='right')
    ax.grid(axis='y', alpha=0.3)

    # Add summary text box in top right
    tsll_market_value = (tsll_price or 0) * tsll_stock_position
    summary_text = f'Total Short PUT: {total_put_qty}\nTotal Short CALL: {total_call_qty}\nPUT Secured Cash: ${total_secured_cash:,.0f}\nTSLL Position: {tsll_stock_position:,.0f} (${tsll_market_value:,.0f})'
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.8)
    ax.text(0.98, 0.97, summary_text, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', horizontalalignment='right', bbox=props)

    # Add legend below the summary box
    ax.legend(loc='upper right', fontsize=10, bbox_to_anchor=(0.99, 0.75))

    # Ensure y-axis shows integers
    ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))

    plt.tight_layout()

    # Save to bytes for email attachment
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight')
    buffer.seek(0)
    img_bytes = buffer.getvalue()
    plt.close(fig)

    return img_bytes  # Return raw bytes for email attachment


def send_email(option_df, esp, stock_df, cash_df, sold_put_df, tsll_histogram):
    recipients = ["omnimahui@gmail.com"]
    emaillist = [elem.strip().split(",") for elem in recipients]
    msg = MIMEMultipart('related')  # Use 'related' for inline images
    msg["Subject"] = "Option Portfolio"
    msg["From"] = "omnimahui@gmail.com"

    # Build sold PUT table HTML
    sold_put_html = ""
    if len(sold_put_df) > 0:
        sold_put_html = "<h3>Sold PUT Positions</h3>" + build_table(sold_put_df, "blue_light")

    # Build TSLL histogram HTML with CID reference
    tsll_hist_html = ""
    if tsll_histogram:
        tsll_hist_html = '<h3>TSLL Short Options Distribution</h3><img src="cid:tsll_histogram" alt="TSLL Short Options Histogram"/>'

    html = """\
    <html>
      <head></head>
      <body>
        {0}
        {1}
        {2}
        {3}
        {4}
        {5}
      </body>
    </html>
    """.format(
        build_table(option_df, "blue_light"),
        sold_put_html,
        tsll_hist_html,
        build_table(esp, "blue_light"),
        build_table(stock_df, "blue_light"),
        build_table(cash_df, "blue_light"),
    )

    # Create alternative part for HTML
    msg_alternative = MIMEMultipart('alternative')
    msg.attach(msg_alternative)

    part1 = MIMEText(html, "html")
    msg_alternative.attach(part1)

    # Attach the histogram image with Content-ID
    if tsll_histogram:
        img = MIMEImage(tsll_histogram)
        img.add_header('Content-ID', '<tsll_histogram>')
        img.add_header('Content-Disposition', 'inline', filename='tsll_put_histogram.png')
        msg.attach(img)

    smtp = smtplib.SMTP(SMTP_SERVER, port=SMTP_PORT)
    smtp.ehlo()  # send the extended hello to our server
    smtp.starttls()  # tell server we want to communicate with TLS encryption
    smtp.login(SMTP_USER, SMTP_PASS)  # login to our email server

    # send our email message 'msg' to our boss
    smtp.sendmail(msg["From"], emaillist, msg.as_string())

    smtp.quit()  # finally, don't forget to close the connection
    return


def esp(group_df):
    # print (group_df['symbol'])
    # print (group_df['delta']*100*group_df['quantity'])
    d = {}
    d["Delta"] = round(
        (group_df["Delta"] * group_df["Unit"] * group_df["Quantity"]).sum(), 4
    )
    d["Gamma"] = (group_df["Gamma"] * group_df["Unit"] * group_df["Quantity"]).sum()
    d["Vega"] = (group_df["Vega"] * group_df["Unit"] * group_df["Quantity"]).sum()
    d["Theta"] = round(
        (group_df["Theta"] * group_df["Unit"] * group_df["Quantity"]).sum(), 4
    )
    d["Covercall_capability"] = group_df.loc[group_df["CallPut"] == "CALL"][
        "Quantity"
    ].sum() + math.floor(
        group_df.loc[group_df["CallPut"] == "STOCK"]["Quantity"].sum() / 100
    )
    return pd.Series(
        d, index=["Delta", "Gamma", "Vega", "Theta", "Covercall_capability"]
    )


ib = IB()
positions_ib = ib.get_positions()

#tradestation = TradeStation()
#positions_tradestation = tradestation.get_positions()

schwab = Schwab()
positions_schwab = schwab.get_positions()
positions_fidelity = Fidelity().get_positions()
portf = Portfolio()
for positions in [
    positions_schwab,
    positions_fidelity,
#    positions_tradestation,
    positions_ib,
]:
    for pos in positions:
        print(f"[DEBUG] Processing position: {pos.symbol} ({pos.equity_type})")
        pos = schwab.load_option_properties(pos)
        if pos:
            print(f"[DEBUG]   -> Added to portfolio")
            portf.add(pos)
        else:
            print(f"[DEBUG]   -> FILTERED OUT (likely expired or failed to load)")


option_df = build_option_table(portf, schwab)
stock_df = build_stock_table(portf)
cash_df = build_cash_table(portf)
sold_put_df = build_sold_put_table(portf)
tsll_histogram = build_tsll_put_histogram(portf)
total_df = pd.concat([option_df, stock_df], join="outer").fillna(0)
esp = total_df.groupby("Symbol").apply(esp).astype(int)
esp.reset_index(inplace=True)

send_email(option_df, esp, stock_df, cash_df, sold_put_df, tsll_histogram)
