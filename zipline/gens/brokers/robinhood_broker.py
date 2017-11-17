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

from datetime import datetime, timedelta

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
from pyEX import api as iex
from threading import Thread
from time import sleep
import calendar

from logbook import Logger

if sys.version_info > (3,):
    long = int

log = Logger('Robinhood Broker')


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


def utc_to_local(utc_dt):
    timestamp = calendar.timegm(utc_dt.timetuple())
    local_dt = datetime.fromtimestamp(timestamp)
    assert utc_dt.resolution >= timedelta(microseconds=1)
    return local_dt.replace(microsecond=utc_dt.microsecond)


Position = namedtuple('Position', ['amount', 'cost_basis', 'last_price', 'created'])


class Portfolio:
    def __init__(self):
        self.capital_used = 0
        self.cash = 0
        self.pnl = 0
        self.positions = {}
        self.portfolio_value = 0
        self.positions_value = 0
        self.returns = 0
        self.starting_cash = 0
        self.start_date = None

    def __str__(self):
        return str(self.__dict__)


class Account:
    def __init__(self):
        self.accrued_interest = 0
        self.available_funds = 0
        self.buying_power = 0
        self.cushion = 0
        self.day_trades_remaining = 0
        self.equity_with_loan = 0
        self.excess_liquidity = 0
        self.initial_margin_requirement = 0
        self.leverage = 0
        self.maintenance_margin_requirement = 0
        self.net_leverage = 0
        self.net_liquidation = 0
        self.settled_cash = 0
        self.total_positions_value = 0

    def __str__(self):
        return str(self.__dict__)


class RHConnection():
    def __init__(self):
        self._next_ticker_id = 0

        self.symbol_to_ticker_id = {}
        self.ticker_id_to_symbol = {}
        self.bars = {}

        self.time_skew = None

        self.trader = None
        self.account = None
        self.portfolio = None

        self._starting_cash = None
        self._start_date = datetime.now()

        self.connect()
        self.process_tickers()

    def connect(self):
       self.trader = Robinhood()
       self.trader.login_prompt()

       log.info("Local-Broker Time Skew: {}".format(self.time_skew))

    @property
    def next_ticker_id(self):
        ticker_id = self._next_ticker_id
        self._next_ticker_id += 1
        return ticker_id

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

        last_trade_price = iex.get_lastSalePrice(symbol)
        last_trade_size = iex.get_lastSaleSize(symbol)
        last_trade_time = iex.get_lastSaleTime(symbol)
        total_volume = iex.get_volume(symbol)
        vwap = iex.chart(symbol, '1m')[-1]['vwap']
        single_trade_flag = False

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

    def update_info(self):
        pos_infos = self.trader.positions()
        port_info = self.trader.portfolios()
        acct_info = self.trader.get_account()

        unsettled_funds = float(acct_info["unsettled_funds"])
        market_value = float(port_info["market_value"])
        equity = float(port_info["equity"])
        yesterday_equity = float(port_info["equity_previous_close"])
        uncleared_deposits = float(acct_info["uncleared_deposits"])
        cash_held_for_orders = float(acct_info["cash_held_for_orders"])
        cash = float(acct_info["cash"])
        total_cash = cash + unsettled_funds
        portfolio_value = equity
        buying_power = equity-market_value-cash_held_for_orders

        if not self._starting_cash:
            self._starting_cash = portfolio_value

        if not self._start_date:
            self._start_date = datetime.now()

        returns = 0
        if self._starting_cash and self._starting_cash > 0:
            returns = (portfolio_value - self._starting_cash) / self._starting_cash
        long_position_value = 0
        short_position_value = 0
        unrealized_pl = 0
        positions = {}

        # log.info("pos: %s" % pos_infos["results"])
        if pos_infos and pos_infos["results"]:
            for result in pos_infos["results"]:
                amount = int(float(result["quantity"]))
                if amount == 0:
                    continue

                instrument = self.trader.get_url_content_json(result["instrument"])
                symbol = instrument["symbol"]
                last_price = iex.get_lastSalePrice(symbol) 
                
                if not last_price:
                    # Lets try again
                    last_price = iex.get_lastSalePrice(symbol)

                created = utc_to_local(datetime.strptime(result["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ"))
                cost_basis = float(result["average_buy_price"])
                
                positions[symbol_lookup(symbol)] = Position(amount=amount,
                                                      cost_basis=cost_basis,
                                                      last_price=last_price,
                                                      created=created)

                # position_value = position_value+(cost_basis*amount)
                if amount > 0:
                    unrealized_pl = unrealized_pl + ((last_price * amount) - (cost_basis * amount))
                    long_position_value = long_position_value + (cost_basis * amount)
                else:
                    unrealized_pl = unrealized_pl + ((cost_basis * np.abs([amount])[0]) - (last_price * np.abs([amount])[0]))
                    short_position_value = long_position_value + (cost_basis * np.abs([amount])[0])

        pnl = equity-uncleared_deposits-yesterday_equity  # unrealized_pl + unsettled_funds
        leverage = 0
        net_leverage = 0
        if portfolio_value > 0:
            leverage = (long_position_value + short_position_value) / portfolio_value
            net_leverage = market_value / portfolio_value

        portfolio = Portfolio()
        portfolio.capital_used = np.abs([(short_position_value - long_position_value)])[0]
        portfolio.cash = total_cash
        portfolio.pnl = pnl
        portfolio.positions = positions
        portfolio.portfolio_value = portfolio_value
        portfolio.positions_value = market_value
        portfolio.returns = returns
        portfolio.starting_cash = self._starting_cash
        portfolio.start_date = self._start_date

        self.portfolio = portfolio

        account = Account()
        # account.accrued_interest=acct_info
        account.available_funds = portfolio_value
        account.buying_power = buying_power
        account.cushion = total_cash / portfolio_value
        account.day_trades_remaining = float("inf")
        account.equity_with_loan = portfolio_value
        account.excess_liquidity = port_info["excess_margin"]
        account.initial_margin_requirement = float(acct_info["margin_balances"]["margin_limit"]) if "margin_balances" in acct_info and "margin_limit" in acct_info["margin_balances"] else 0
        account.leverage = leverage
        account.maintenance_margin_requirement = portfolio_value-float(port_info["excess_margin"])
        account.net_leverage = net_leverage
        account.net_liquidation = portfolio_value
        account.settled_cash = cash
        account.total_positions_value = market_value

        self.account = account


class ROBINHOODBroker(Broker):
    def __init__(self):
        self.orders = {}

        self._rh = RHConnection()
        self.currency = 'USD'

        self._subscribed_assets = []

        super(self.__class__, self).__init__()

    def subscribed_assets(self):
        return self._subscribed_assets

    def subscribe_to_market_data(self, asset):
        if asset not in self.subscribed_assets():
            self._rh.subscribe_to_market_data(str(asset.symbol))
            self._subscribed_assets.append(asset)

            while asset.symbol not in self._rh.bars:
                sleep(0.1)

    @property
    def positions(self):
        z_positions = zp.Positions()
        self._rh.update_info()
        for symbol in self._rh.positions:
            robinhood_position = self._rh.portfolio.positions[symbol]
            try:
                z_position = zp.Position(symbol)
            except SymbolNotFound:
                # The symbol might not have been ingested to the db therefore
                # it needs to be skipped.
                continue

            z_position.amount = robinhood_position.amount
            z_position.cost_basis = robinhood_position.cost_basis
            z_position.last_sale_price = robinhood_position.last_price
            z_position.last_sale_date = None 
            z_positions[symbol] = z_position

        return z_positions

    @property
    def portfolio(self):
        z_portfolio = zp.Portfolio()
        self._rh.update_info()
        z_portfolio.capital_used = self._rh.portfolio.capital_used
        z_portfolio.starting_cash = self._rh.portfolio.starting_cash 
        z_portfolio.portfolio_value = self._rh.portfolio.portfolio_value
        z_portfolio.pnl = self._rh.portfolio.pnl
        z_portfolio.returns = self._rh.portfolio.returns
        z_portfolio.cash = self._rh.portfolio.cash
        z_portfolio.start_date = self._rh.portfolio.start_date
        z_portfolio.positions = self._rh.portfolio.positions
        z_portfolio.positions_value = self._rh.portfolio.positions_value
        z_portfolio.positions_exposure = None

        return z_portfolio

    @property
    def account(self):
        z_account = zp.Account()
        self._rh.update_info()
        z_account.settled_cash = self._rh.account.settled_cash
        z_account.accrued_interest = None
        z_account.buying_power = self._rh.account.buying_power
        z_account.equity_with_loan = self._rh.account.equity_with_loan
        z_account.total_positions_value = self._rh.account.total_positions_value
        z_account.total_positions_exposure = None
        z_account.regt_equity = None
        z_account.regt_margin = None
        z_account.initial_margin_requirement = self._rh.account.initial_margin_requirement
        z_account.maintenance_margin_requirement = self._rh.account.maintenance_margin_requirement
        z_account.available_funds = self._rh.account.available_funds
        z_account.excess_liquidity = self._rh.account.excess_liquidity
        z_account.cushion = self._rh.account.cushion
        z_account.day_trades_remaining = self._rh.account.day_trades_remaining
        z_account.leverage = self._rh.account.leverage
        z_account.net_leverage = self._rh.account.net_leverage 
        z_account.net_liquidation = self._rh.account.net_liquidation

        return z_account

    @property
    def time_skew(self):
        return timedelta(seconds=0)
        # return self._rh.time_skew

    def order(self, asset, amount, limit_price, stop_price, style):
        is_buy = (amount > 0)
        zp_order = ZPOrder(
            dt=pd.to_datetime('now', utc=True),
            asset=asset,
            amount=amount,
            stop=style.get_stop_price(is_buy),
            limit=style.get_limit_price(is_buy))

        abs_amount = int(fabs(amount))
        transaction = "buy" if amount > 0 else "sell"

        stock_instrument = self._rh.trader.instruments(str(asset.symbol))[0]

        if isinstance(style, MarketOrder):
            robinhood_order_id = self._rh.trader.place_market_order(stock_instrument, abs_amount, transaction)
        elif isinstance(style, LimitOrder):
            robinhood_order_id = self._rh.trader.place_limit_order(stock_instrument, abs_amount, style.limit_price, transaction)
        elif isinstance(style, StopOrder):
            robinhood_order_id = self._rh.trader.place_stop_loss_orders(stock_instrument, abs_amount, style.stop_price, transaction)
        elif isinstance(style, StopLimitOrder):
            robinhood_order_id = self._rh.trader.place_stop_limit_order(stock_instrument, abs_amount, style.limit_price, style.stop_price, transaction)

        zp_order.broker_order_id = robinhood_order_id
        self.orders[zp_order.id] = zp_order

        return zp_order.id

    def update_open_orders(self):
        orders = {}
        open_orders = self._rh.trader.list_open_orders()

        for order_id, order in open_orders.items():
            zp_order = ZPOrder(
                dt=utc_to_local(datetime.strptime(order['dt'], "%Y-%m-%dT%H:%M:%S.%fZ")),
                asset=symbol_lookup(order['asset']),
                amount=int(float(order['amount'])),
                stop=float(order['stop_price']) if (order['stop_price'] != None) else None,
                limit=float(order['limit_price']) if (order['limit_price'] != None) else None)

            zp_order.broker_order_id = order_id
            orders[zp_order.id] = zp_order

        self.orders = orders

    def get_open_orders(self, asset):
        self.update_open_orders()

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

    def get_order(self, zp_order_id):
        self.update_open_orders()
        return self.orders[zp_order_id].to_api_obj()

    def cancel_order(self, zp_order_id):
        robinhood_order_id = self.orders[zp_order_id].broker_order_id
        self._rh.trader.cancel_order(robinhood_order_id)

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
                elif field == 'total_volume':
                    return minute_df.last_trade_size.sum()

    def get_last_traded_dt(self, asset):
        self.subscribe_to_market_data(asset)
        return self._rh.bars[asset.symbol].index[-1]

    def get_realtime_bars(self, assets, frequency):
        if frequency == '1m':
            resample_freq = '1 Min'
        elif frequency == '1d':
            resample_freq = '24 H'
        else:
            raise ValueError("Invalid frequency specified: %s" % frequency)

        df = pd.DataFrame()
        for asset in assets:
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
