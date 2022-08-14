import re
from datetime import datetime

import pandas as pd
import requests

_FORECAST_EXPRESSION = '_classic'
_CHAMBERS = dict(
    names=['senate', 'governor'],
    patterns=dict(
        senate='Which party will win the ([A-Z]{2}) Senate race',
        governor='Which party will win ([A-Z]{2}) governor\'s race?',
    ),
    filenames=dict(
        senate='senate_state_toplines_2022.csv',
        governor='governor_state_toplines_2022.csv',
    ),
)


def _get_pi_contracts(market: dict, contract: dict) -> dict:
    contract_data = dict((f'm{market_field}', market[market_field]) for market_field in ('shortName', 'url'))
    contract_data.update(dict((f'c{contract_field}', contract[contract_field]) for contract_field in (
        'name', 'bestBuyYesCost', 'bestBuyNoCost', 'bestSellYesCost', 'bestSellNoCost')))
    return contract_data


def _get_pi_markets(markets: dict) -> pd.DataFrame:
    market_data = []
    for market in markets['markets']:
        market_data.extend(_get_pi_contracts(market, contract) for contract in market['contracts'])
    return pd.DataFrame(market_data).drop_duplicates()


def get_pi_data() -> pd.DataFrame:
    markets = requests.get('https://www.predictit.org/api/marketdata/all/').json()
    return _get_pi_markets(markets)


def _filter_pi_data(pi_data: pd.DataFrame, chamber: str) -> pd.DataFrame:
    pattern = _CHAMBERS['patterns'][chamber]
    pi_data = pi_data.rename(columns=dict((i, i.replace('cbest', '')) for i in pi_data.columns))
    pi_data['state'] = pi_data.mshortName.apply(lambda x: re.search(pattern, x)).apply(
        lambda x: x.group(1) if x else None)
    return pi_data


def _get_fte_data(chamber: str) -> pd.DataFrame:
    filename = _CHAMBERS['filenames'][chamber]
    base_url = 'https://projects.fivethirtyeight.com/2022-general-election-forecast-data/'
    fte = pd.read_csv(base_url + filename, usecols=['district', 'expression', 'winner_Dparty', 'winner_Rparty'])
    fte = fte[fte.expression == _FORECAST_EXPRESSION].drop_duplicates(keep='first', subset='district')
    fte['state'] = fte.district.apply(lambda x: x.split('-', 1)[0])
    fte = fte.drop(columns=['expression', 'district'])
    return fte


def merge_data(pi_data: pd.DataFrame, chamber: str) -> pd.DataFrame:
    chamber = chamber.lower()
    pi = _filter_pi_data(pi_data, chamber)
    pi = pi[pi.state.notna()].copy()
    fte = _get_fte_data(chamber)

    _separate_by_party = lambda party: pi[pi.cname == party].drop(columns='cname')
    pi = _separate_by_party('Democratic').merge(_separate_by_party('Republican'), on=[
        'mshortName', 'murl', 'state'], suffixes=('D', 'R'))

    merged = (
        pi.merge(fte, on='state')
            .drop(columns=['state', 'SellYesCostD', 'SellNoCostR', 'SellYesCostR', 'SellNoCostD'])
            .rename(columns=dict(winner_Dparty='fteD', winner_Rparty='fteR'))
    )
    return merged


def add_profit_columns_to_merged(merged: pd.DataFrame) -> pd.DataFrame:
    buy_columns = ('BuyYesCostD', 'BuyNoCostR', 'BuyYesCostR', 'BuyNoCostD')

    merged['profitBuyYesCostD'] = merged.fteD - merged.BuyYesCostD
    merged['profitBuyNoCostR'] = merged.fteD - merged.BuyNoCostR
    merged['profitBuyYesCostR'] = merged.fteR - merged.BuyYesCostR
    merged['profitBuyNoCostD'] = merged.fteR - merged.BuyNoCostD

    for col in buy_columns:
        # merged[f'roi{col}'] = (merged[f'profit{col}'] / merged[col]).fillna(0).apply(lambda x: int(round(x * 100)))
        merged[f'profit{col}'] = merged[f'profit{col}'].round(2)

    for col in ('fteD', 'fteR'):
        merged[col] = merged[col].round(2)

    merged = merged.reset_index(drop=True)
    transposed = merged[[f'profit{i}' for i in buy_columns]].transpose()
    addnl = [dict(
        buyActionRec=transposed[i].idxmax().replace('profit', '').replace('Cost', ''),
        buyActionProfit=transposed[i].max(),
    ) for i in transposed]
    merged = merged.join(pd.DataFrame(addnl))
    merged = merged[merged.buyActionProfit > 0].sort_values('buyActionProfit', ascending=False)
    return merged


def compare_fte_and_pi() -> None:
    pi_data = get_pi_data()
    merged = pd.concat(merge_data(pi_data, chamber) for chamber in _CHAMBERS['names'])
    merged = add_profit_columns_to_merged(merged)
    summary = merged.groupby('buyActionRec', as_index=False).murl.count().sort_values(
        by='murl', ascending=False).rename(columns=dict(murl='count'))

    html = open('templates/page.html').read().format(
        data='\n'.join(open('templates/item.html').read().format(**record) for record in merged.to_dict('records')),
        summary=summary.to_html(index=False),
        last_updated=datetime.now().strftime('%d %B %Y %H:%M'),
    )
    with open('index.html', 'w') as f:
        f.write(html)


if __name__ == '__main__':
    compare_fte_and_pi()
