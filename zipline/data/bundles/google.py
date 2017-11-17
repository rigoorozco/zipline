import os

import numpy  as np
import pandas as pd
from pandas_datareader.data import DataReader
from pandas_datareader._utils import RemoteDataError
import requests

from zipline.utils.cli import maybe_show_progress

from . import core as bundles

def _cachpath(symbol, type_):
    return '-'.join((symbol.replace(os.path.sep, '_'), type_))

@bundles.register('google')
def google_bundle(environ,
           asset_db_writer,
           minute_bar_writer,  # unused
           daily_bar_writer,
           adjustment_writer,
           calendar,
           start_session,
           end_session,
           cache,
           show_progress,
           output_dir):

    symbols = tuple(["SPY", "XIV", "NAK", "MO", "AMPE", "MU"])

    metadata = pd.DataFrame(np.empty(len(symbols), dtype=[
        ('start_date', 'datetime64[ns]'),
        ('end_date', 'datetime64[ns]'),
        ('auto_close_date', 'datetime64[ns]'),
        ('symbol', 'object'),
    ]))

    def _pricing_iter():
        sid = 0
        with maybe_show_progress(
                symbols,
                show_progress,
                label='Downloading Google pricing data: ') as it, \
                requests.Session() as session:
            for symbol in it:
                path = _cachpath(symbol, 'ohlcv')
                try:
                    df = cache[path]
                except KeyError:
                    df = cache[path] = DataReader(
                        symbol,
                        'google',
                        '2017-07-31',
                        '2017-11-15',
                        session=session,
                    ).sort_index()

                # the start date is the date of the first trade and
                # the end date is the date of the last trade
                start_date = df.index[0]
                end_date = df.index[-1]
                # The auto_close date is the day after the last trade.
                ac_date = end_date + pd.Timedelta(days=1)
                metadata.iloc[sid] = start_date, end_date, ac_date, symbol

                df.rename(
                    columns={
                        'Open': 'open',
                        'High': 'high',
                        'Low': 'low',
                        'Close': 'close',
                        'Volume': 'volume',
                    },
                    inplace=True,
                )
                yield sid, df
                sid += 1

    daily_bar_writer.write(_pricing_iter(), show_progress=show_progress)

    symbol_map = pd.Series(metadata.symbol.index, metadata.symbol)

    metadata['exchange'] = "GOOGLE"
    asset_db_writer.write(equities=metadata)

    adjustments = []
    with maybe_show_progress(
            symbols,
            show_progress,
            label='Downloading Yahoo adjustment data: ') as it, \
            requests.Session() as session:
        for symbol in it:
            path = _cachpath(symbol, 'adjustment')
            sid = symbol_map[symbol]
            try:
                df = cache[path]
            except KeyError:
                try:
                    df = cache[path] = DataReader(
                        symbol,
                        'yahoo-actions',
                        metadata.ix[sid].start_date,
                        metadata.ix[sid].end_date,
                        session=session,
                    ).sort_index()
                except RemoteDataError:
                    print("No data returned from Yahoo for %s" % symbol)
                    df = pd.DataFrame(columns=['value', 'action'])

            df['sid'] = sid
            adjustments.append(df)

    adj_df = pd.concat(adjustments)
    adj_df.index.name = 'date'
    adj_df.reset_index(inplace=True)

    splits = adj_df[adj_df.action == 'SPLIT']
    splits = splits.rename(
        columns={'value': 'ratio', 'date': 'effective_date'},
    )
    splits.drop('action', axis=1, inplace=True)

    dividends = adj_df[adj_df.action == 'DIVIDEND']
    dividends = dividends.rename(
        columns={'value': 'amount', 'date': 'ex_date'},
    )
    dividends.drop('action', axis=1, inplace=True)
    # we do not have this data in the yahoo dataset
    dividends['record_date'] = pd.NaT
    dividends['declared_date'] = pd.NaT
    dividends['pay_date'] = pd.NaT

    adjustment_writer.write(splits=splits, dividends=dividends)
