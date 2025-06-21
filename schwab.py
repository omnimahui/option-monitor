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
from smtplib import SMTP
import smtplib
import sys
import os
import base64
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
)
import yfinance as yf

UNIFIED_OPTION_PATTERN = r"([a-zA-Z]+)(\d*)_(\d+)([C|P])(\d+\.?\d*)"


def getERdate2(symbol):
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
        return self.exp < datetime.now()

    def download_underlying_OHLC(self):
        return yf.download(
            tickers=self.underlying,
            start=datetime.today() - timedelta(days=365),
            interval="1d",
        )


class Position:
    def __init__(self, symbol, equity_type, quantity):
        self.symbol = symbol
        self.equity_type = equity_type
        self.quantity = quantity
        self.property = None


class Portfolio:
    def __init__(self):
        self.portf_list = []
        return

    def add(self, new_pos: Position):
        for pos in self.portf_list:
            if pos.symbol == new_pos.symbol and pos.equity_type == new_pos.equity_type:
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
        self.option_pattern = r"^([A-Z]+).*\[\1\s+(\d{6})([CP])(\d{8})\s+(\d+)\]$"
        return

    def get_positions(self):
        curr_dir = os.path.dirname(__file__)
        for f in [
            f"{curr_dir}/IB.csv",
        ]:
            df = pd.read_csv(f, header=1)
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
                        )
                        self.pos_list.append(pos)

        return self.pos_list


class Fidelity(Exchange):
    def __init__(self):
        super().__init__()
        self.money_pattern = r"^FDRXX.*|^SPAXX.*"
        self.option_pattern = r"[^a-zA-Z]*([a-zA-Z]*)(\d{6})([C|P]\d+\.?\d*)"
        return

    def get_positions(self):
        curr_dir = os.path.dirname(__file__)
        for f in [
            f"{curr_dir}/fidelity18-ira.csv",
            f"{curr_dir}/fidelity18-roth.csv",
            f"{curr_dir}/fidelity20.csv",
        ]:
            df = pd.read_csv(f)
            for s in df["Symbol"].dropna().values:
                if not s or s == "Pending Activity":
                    continue

                # if it is money market (cash)
                m = re.compile(self.money_pattern).search(s)
                if m:
                    pos = Position(
                        s,
                        "CASH",
                        int(
                            float(
                                df.loc[df["Symbol"] == s, "Current Value"]
                                .values[0]
                                .strip("$")
                            )
                        ),
                    )
                    self.pos_list.append(pos)
                    continue

                m = re.compile(self.option_pattern).search(s)
                if m:
                    o = m.group(1) + "_" + m.group(2) + m.group(3)
                    pos = Position(
                        o, "OPTION", df.loc[df["Symbol"] == s, "Quantity"].values[0]
                    )
                    self.pos_list.append(pos)
                    continue
                else:
                    if s and (
                        df.loc[df["Symbol"] == s, "Account Name"].values[0].lower()
                        == "ROTH IRA".lower()
                        or df.loc[df["Symbol"] == s, "Account Name"].values[0].lower()
                        == "TRADITIONAL IRA".lower()
                    ):
                        pos = Position(
                            s, "STOCK", df.loc[df["Symbol"] == s, "Quantity"].values[0]
                        )
                        self.pos_list.append(pos)

        return self.pos_list

    def get_positions(self):
        curr_dir = os.path.dirname(__file__)
        for f in [
            f"{curr_dir}/fidelity18-ira.csv",
            f"{curr_dir}/fidelity18-roth.csv",
            f"{curr_dir}/fidelity20.csv",
        ]:
            df = pd.read_csv(f)
            for s in df["Symbol"].dropna().values:
                if not s or s == "Pending Activity":
                    continue

                # if it is money market (cash)
                m = re.compile(self.money_pattern).search(s)
                if m:
                    pos = Position(
                        s,
                        "CASH",
                        int(
                            float(
                                df.loc[df["Symbol"] == s, "Current Value"]
                                .values[0]
                                .strip("$")
                            )
                        ),
                    )
                    self.pos_list.append(pos)
                    continue

                m = re.compile(self.option_pattern).search(s)
                if m:
                    o = m.group(1) + "_" + m.group(2) + m.group(3)
                    pos = Position(
                        o, "OPTION", df.loc[df["Symbol"] == s, "Quantity"].values[0]
                    )
                    self.pos_list.append(pos)
                    continue
                else:
                    if s and (
                        df.loc[df["Symbol"] == s, "Account Name"].values[0].lower()
                        == "ROTH IRA".lower()
                        or df.loc[df["Symbol"] == s, "Account Name"].values[0].lower()
                        == "TRADITIONAL IRA".lower()
                    ):
                        pos = Position(
                            s, "STOCK", df.loc[df["Symbol"] == s, "Quantity"].values[0]
                        )
                        self.pos_list.append(pos)

        return self.pos_list


class TradeStation(Exchange):
    def __init__(self):
        super().__init__()
        self.access_token = ""
        self.option_pattern = r"([a-zA-Z]+)(\d*)\s+(\d+)([C|P])(\d+\.?\d*)"
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
        self.account_number = []
        self.base_url = "https://api.schwabapi.com"
        self.option_pattern = r"([a-zA-Z]+)(\d*)\s+(\d+)([C|P])(\d+\.?\d*)"
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
        return self.account_number

    def parse_positions(self, positions):
        l = []
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
        exp = m.group(2)
        callput = m.group(3)
        strike = int(float(m.group(4)) * 1000)
        # schwab requires symbol = 'JD    240524C00032000'
        schwab_option_symbol = f"{underlying:<6}{exp}{callput}{strike :>08}"
        return schwab_option_symbol

    def get_positions(self) -> dict:
        self.get_account_number_hash_value()
        self.pos_list = []
        i = 0
        for acct in self.account_number:
            url = f"{self.base_url}/trader/v1/accounts/{acct}?fields=positions"
            response = self.send_request(url)
            self.pos = self.parse_positions(response.securitiesAccount.positions)
            self.pos.append(
                Position(
                    f"SchWab{i}",
                    "CASH",
                    response.securitiesAccount.initialBalances.cashBalance,
                )
            )
            self.pos_list.extend(self.pos)
            i = i + 1
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

    def load_option_properties(self, pos):
        if pos.equity_type != "OPTION":
            return pos
        option = Option(pos.symbol)
        if option.is_expired():
            return None
        chain_obj = self.get_chain_obj(option)
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


def build_option_table(portf: Portfolio, schwab: Schwab) -> str:
    # Create options table
    rows = []
    for pos in portf.portf_list:
        if pos.equity_type != "OPTION":
            continue
        option = pos.property
        row = {
            "Symbol": option.underlying,
            "Action": option.actionNeed,
            "ITM": option.itm,
            "Price": option.price,
            "DaysToExp": option.daysToExpiration,
            "DaysToER": option.daysToER,
            "Quantity": pos.quantity,
            "Extrinsic": option.extrinsic,
            "CallPut": option.callput,
            "Strike": option.strike,
            "Underlying": option.underlyingPrice,
            "Xstd": option.Xstd,
            "Exp": option.exp,
            "Delta": option.delta,
            "Gamma": option.gamma,
            "Theta": option.theta,
            "Vega": option.vega,
            "OpenInterest": option.openInterest,
            "Volatility": option.volatility,
        }
        rows.append(row)
    df = pd.DataFrame.from_records(rows)
    df["Unit"] = 100
    df.sort_values(by=["DaysToExp", "Symbol"], inplace=True)
    df.style.highlight_between(
        left=1, right=1, subset=["ITM"], props="background:#FFFF00"
    ).highlight_between(
        left=0, right=5, subset=["DaysToExp"], props="background:#a1eafb"
    ).highlight_between(
        left=0, right=5, subset=["DaysToER"], props="background:#a1eafb"
    ).format(
        precision=2
    )
    df.style.set_table_styles([dict(selector="th", props=[("max-width", "20px")])])

    return df


def build_stock_table(portf):
    rows = []
    for pos in portf.portf_list:
        if pos.equity_type != "STOCK":
            continue
        row = {
            "Symbol": pos.symbol,
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
        row = {"Symbol": pos.symbol, "Quantity": pos.quantity}
        rows.append(row)
    df = pd.DataFrame.from_records(rows)
    new_row = {"Symbol": "Total", "Quantity": df["Quantity"].astype(float).sum()}
    df.loc[len(df)] = new_row
    return df


def send_email(option_df, esp, stock_df, cash_df):
    recipients = ["omnimahui@gmail.com"]
    emaillist = [elem.strip().split(",") for elem in recipients]
    msg = MIMEMultipart()
    msg["Subject"] = "Option Portfolio"
    msg["From"] = "omnimahui@gmail.com"

    html = """\
    <html>
      <head></head>
      <body>
        {0}
        {1}
        {2}
        {3}
      </body>
    </html>
    """.format(
        build_table(option_df, "blue_light"),
        build_table(esp, "blue_light"),
        build_table(stock_df, "blue_light"),
        build_table(cash_df, "blue_light"),
    )
    part1 = MIMEText(html, "html")
    msg.attach(part1)

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

tradestation = TradeStation()
positions_tradestation = tradestation.get_positions()

schwab = Schwab()
positions_schwab = schwab.get_positions()
positions_fidelity = Fidelity().get_positions()
portf = Portfolio()
for positions in [
    positions_schwab,
    positions_fidelity,
    positions_tradestation,
    positions_ib,
]:
    for pos in positions:
        pos = schwab.load_option_properties(pos)
        if pos:
            portf.add(pos)


option_df = build_option_table(portf, schwab)
stock_df = build_stock_table(portf)
cash_df = build_cash_table(portf)
total_df = pd.concat([option_df, stock_df], join="outer").fillna(0)
esp = total_df.groupby("Symbol").apply(esp).astype(int)
esp.reset_index(inplace=True)

send_email(option_df, esp, stock_df, cash_df)
