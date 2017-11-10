#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
from collections import namedtuple, defaultdict
from time import sleep
from math import fabs

import datetime as dt

from six import itervalues
import pandas as pd
import numpy as np

from zipline.gens.brokers.broker import Broker
from zipline.finance.order import (Order as ZPOrder,
                                   ORDER_STATUS as ZP_ORDER_STATUS)
from zipline.finance.execution import (MarketOrder,
                                       LimitOrder,
                                       StopOrder,
                                       StopLimitOrder)
import zipline.protocol as zp
from zipline.api import symbol as symbol_lookup
from zipline.errors import SymbolNotFound

from Robinhood import Robinhood
from pyEX import api
from threading import Thread
from time import sleep

from logbook import Logger

if sys.version_info > (3,):
    long = int

log = Logger('Robinhood Broker')

Position = namedtuple('Position', ['position', 'market_price',
                                   'market_value', 'average_cost',
                                   'unrealized_pnl', 'realized_pnl',
                                   'account_name'])


def log_message(message, mapping):
    try:
        del(mapping['self'])
    except (KeyError, ):
        pass
    items = list(mapping.items())
    items.sort()
    log.debug(('### %s' % (message, )))
    for k, v in items:
        log.debug(('    %s:%s' % (k, v)))

def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return wrapper

class RHConnection():
    def __init__(self):
        self._next_ticker_id = 0
        self._next_order_id = None
        self.managed_accounts = None
        self.symbol_to_ticker_id = {}
        self.ticker_id_to_symbol = {}
        self.last_tick = defaultdict(dict)
        self.bars = {}
        # accounts structure: accounts[account_id][currency][value]
        self.accounts = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: np.NaN)))
        self.accounts_download_complete = False
        self.positions = {}
        self.portfolio = {}
        self.orders = {}
        self.time_skew = None

        self.trader = None

        self.connect()
        self.process_tickers()

    def connect(self):
       # Connect to Robinhood here
       self.trader = Robinhood()
       self.trader.login_prompt()

       log.info("Local-Broker Time Skew: {}".format(self.time_skew))

    @property
    def next_ticker_id(self):
        ticker_id = self._next_ticker_id
        self._next_ticker_id += 1
        return ticker_id

    @property
    def next_order_id(self):
        order_id = self._next_order_id
        self._next_order_id += 1
        return order_id

    def subscribe_to_market_data(self, symbol):
        if symbol in self.symbol_to_ticker_id:
            # Already subscribed to market data
            return

        ticker_id = self.next_ticker_id

        self.symbol_to_ticker_id[symbol] = ticker_id
        self.ticker_id_to_symbol[ticker_id] = symbol

    @threaded
    def process_tickers(self):
        while (1):
            sleep(1)
            for ticker, symbol in self.ticker_id_to_symbol.items():
                self._process_tick(ticker)

    def _process_tick(self, ticker_id):
        try:
            symbol = self.ticker_id_to_symbol[ticker_id]
        except KeyError:
            log.error("Tick {} for id={} is not registered".format(tick_type,
                                                                   ticker_id))
            return

        # RT Volume Bar. Format:
        # Last trade price; Last trade size;Last trade time;Total volume;\
        # VWAP;Single trade flag
        # e.g.: 701.28;1;1348075471534;67854;701.46918464;true
        
        last_trade_price = api.get_lastSalePrice(symbol)
        last_trade_size = api.get_lastSaleSize(symbol)
        last_trade_time = api.get_lastSaleTime(symbol)
        total_volume = api.get_volume(symbol)
        vwap = api.chart(symbol, '1m')[-1]['vwap']
        single_trade_flag = False

        # Ignore this update if last_trade_price is empty:
        # tickString: tickerId=0 tickType=48/RTVolume ;0;1469805548873;\
        # 240304;216.648653;true
        # if len(last_trade_price) == 0:
        #     return

        last_trade_dt = pd.to_datetime(float(last_trade_time), unit='ms', utc=True)

        self._add_bar(symbol, last_trade_price,
                      last_trade_size, last_trade_dt,
                      total_volume, vwap,
                      single_trade_flag)

    def _add_bar(self, symbol, last_trade_price, last_trade_size,
                 last_trade_time, total_volume, vwap, single_trade_flag):
        bar = pd.DataFrame(index=pd.DatetimeIndex([last_trade_time]),
                           data={'last_trade_price': last_trade_price,
                                 'last_trade_size': last_trade_size,
                                 'total_volume': total_volume,
                                 'vwap': vwap,
                                 'single_trade_flag': single_trade_flag})
        if symbol not in self.bars:
            self.bars[symbol] = bar
        else:
            self.bars[symbol] = self.bars[symbol].append(bar)


class ROBINHOODBroker(Broker):
    def __init__(self):
        self.orders = {}

        self._rh = RHConnection()
        self.currency = 'USD'

        self._subscribed_assets = []

        print("Robinhood Broker intializing...")

        super(self.__class__, self).__init__()

    # Required - called in algorithm_live.py
    def subscribed_assets(self):
        return self._subscribed_assets

    # Called in get_spot_value()
    def subscribe_to_market_data(self, asset):
        if asset not in self.subscribed_assets():
            # remove str() cast to have a fun debugging journey
            self._rh.subscribe_to_market_data(str(asset.symbol))
            self._subscribed_assets.append(asset)

            while asset.symbol not in self._rh.bars:
                sleep(0.1)

    # Not called anywhere ???
    @property
    def positions(self):
        z_positions = zp.Positions()
        for symbol in self._rh.positions:
            robinhood_position = self._rh.positions[symbol]
            try:
                z_position = zp.Position(symbol_lookup(symbol))
            except SymbolNotFound:
                # The symbol might not have been ingested to the db therefore
                # it needs to be skipped.
                continue
            z_position.amount = int(robinhood_position.position)
            z_position.cost_basis = float(robinhood_position.average_cost)
            z_position.last_sale_price = None
            z_position.last_sale_date = None 
            z_positions[symbol_lookup(symbol)] = z_position

        return z_positions

    # Required - called in algorithm_live.py
    @property
    def portfolio(self):
        z_portfolio = zp.Portfolio()
        z_portfolio.capital_used = None
        z_portfolio.starting_cash = None
        z_portfolio.portfolio_value = _rh.trader.portfolios()['equity']
        z_portfolio.pnl = None
        z_portfolio.returns = None # pnl / total_at_start
        z_portfolio.cash = _rh.trader.get_account()['margin_balances']['unallocated_margin_cash']
        z_portfolio.start_date = _rh.trader.portfolios()['start_date']
        z_portfolio.positions = self.positions()
        z_portfolio.positions_value = None
        z_portfolio.positions_exposure = None

        return z_portfolio

    # Required - called in algorithm_live.py
    @property
    def account(self):
        robinhood_account = self._rh.accounts[self.account_id][self.currency]

        z_account = zp.Account()

        z_account.settled_cash = None
        z_account.accrued_interest = None
        z_account.buying_power = _rh.trader.get_account()['margin_balances']['unallocated_margin_cash']
        z_account.equity_with_loan = _rh.trader.portfolios()['equity']
        z_account.total_positions_value = None
        z_account.total_positions_exposure = None
        z_account.regt_equity = None
        z_account.regt_margin = None
        z_account.initial_margin_requirement = None
        z_account.maintenance_margin_requirement = None
        z_account.available_funds = None
        z_account.excess_liquidity = None
        z_account.cushion = None
        z_account.day_trades_remaining = None
        z_account.leverage = None
        z_account.net_leverage = None 
        z_account.net_liquidation = None

        return z_account

    # Required - called in algorithm_live.py
    @property
    def time_skew(self):
        return dt.timedelta(seconds=0)
        # return self._rh.time_skew

    # Required - called in algorithm_live.py
    def order(self, asset, amount, limit_price, stop_price, style):
        is_buy = (amount > 0)
        zp_order = ZPOrder(
            dt=pd.to_datetime('now', utc=True),
            asset=asset,
            amount=amount,
            stop=style.get_stop_price(is_buy),
            limit=style.get_limit_price(is_buy))

        # TODO(RIGO)

        abs_amount = int(fabs(amount))
        transaction = "buy" if amount > 0 else "sell"

        if isinstance(style, MarketOrder):
            _rh.trader.place_market_order(stock_instrument, abs_amount, transaction)
        elif isinstance(style, LimitOrder):
            _rh.trader.place_limit_order(stock_instrument, abs_amount, limit_price, transaction)
        elif isinstance(style, StopOrder):
            _rh.trader.place_stop_loss_orders(stock_instrument, abs_amount, stop_price, transaction)
        elif isinstance(style, StopLimitOrder):
            _rh.trader.place_stop_limit_order(stock_instrument, abs_amount, limit_price, stop_price, transaction)

        robinhood_order_id = self._rh.next_order_id
        zp_order.broker_order_id = robinhood_order_id
        self.orders[zp_order.id] = zp_order

        return zp_order.id

    # Required - called in algorithm_live.py
    def get_open_orders(self, asset):
        if asset is None:
            assets = set([order.asset for order in itervalues(self.orders)
                          if order.open])
            return {
                asset: [order.to_api_obj() for order in itervalues(self.orders)
                        if order.asset == asset]
                for asset in assets
            }
        return [order.to_api_obj() for order in itervalues(self.orders)
                if order.asset == asset and order.open]

    # Required - called in algorithm_live.py
    def get_order(self, zp_order_id):
        return self.orders[zp_order_id].to_api_obj()

    # Required - called in algorithm_live.py
    def cancel_order(self, zp_order_id):
        robinhood_order_id = self.orders[zp_order_id].broker_order_id
        # ZPOrder cancellation will be done indirectly through _order_update

        # TODO(RIGO)

        # self._rh.cancelOrder(robinhood_order_id)

    # Required - called in data_portal_live.py
    def get_spot_value(self, assets, field, dt, data_frequency):
        symbol = str(assets.symbol)

        self.subscribe_to_market_data(assets)

        bars = self._rh.bars[symbol]

        last_event_time = bars.index[-1]

        minute_start = (last_event_time - pd.Timedelta('1 min')) \
            .time()
        minute_end = last_event_time.time()

        if bars.empty:
            return pd.NaT if field == 'last_traded' else np.NaN
        else:
            if field == 'price':
                return bars.last_trade_price.iloc[-1]
            elif field == 'last_traded':
                return last_event_time or pd.NaT

            minute_df = bars.between_time(minute_start, minute_end,
                                          include_start=True, include_end=True)
            if minute_df.empty:
                return np.NaN
            else:
                if field == 'open':
                    return minute_df.last_trade_price.iloc[0]
                elif field == 'close':
                    return minute_df.last_trade_price.iloc[-1]
                elif field == 'high':
                    return minute_df.last_trade_price.max()
                elif field == 'low':
                    return minute_df.last_trade_price.min()
                elif field == 'volume':
                    return minute_df.last_trade_size.sum()

    # Required - called in data_portal_live.py
    def get_last_traded_dt(self, asset):
        self.subscribe_to_market_data(asset)

        return self._rh.bars[asset.symbol].index[-1]

    # Required - called in data_portal_live.py and algorithm_live.py
    def get_realtime_bars(self, assets, frequency):
        if frequency == '1m':
            resample_freq = '1 Min'
        elif frequency == '1d':
            resample_freq = '24 H'
        else:
            raise ValueError("Invalid frequency specified: %s" % frequency)

        df = pd.DataFrame()
        for asset in assets():
            symbol = str(asset.symbol)
            self.subscribe_to_market_data(asset)

            trade_prices = self._rh.bars[symbol]['last_trade_price']
            trade_sizes = self._rh.bars[symbol]['last_trade_size']
            ohlcv = trade_prices.resample(resample_freq).ohlc()
            ohlcv['volume'] = trade_sizes.resample(resample_freq).sum()

            # Add asset as level 0 column; ohlcv will be used as level 1 cols
            ohlcv.columns = pd.MultiIndex.from_product([[asset, ],
                                                        ohlcv.columns])

            df = pd.concat([df, ohlcv], axis=1)

        return df
