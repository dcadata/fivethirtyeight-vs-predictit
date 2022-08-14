import re
from datetime import datetime

import pandas as pd
import requests

_FORECAST_EXPRESSION = '_classic'
_MIN_PROFIT_PER_SHARE = 0.05
_CHAMBERS = dict(
    names=['senate', 'governor'],
    patterns=dict(
        senate='Which party will win the ([A-Z]{2}) (Sen)ate race',
        governor='Which party will win ([A-Z]{2}) (gov)ernor\'s race?',
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
    pi_data = pi_data.rename(columns=dict((i, i.replace('cbest', 'best')) for i in pi_data.columns))
    shortname_with_pattern = pi_data.mshortName.apply(lambda x: re.search(pattern, x))
    pi_data['state'] = shortname_with_pattern.apply(lambda x: x.group(1) if x else None)
    pi_data['seat'] = shortname_with_pattern.apply(lambda x: '-'.join(x.groups()).upper() if x else None)
    return pi_data


def _get_fte_data(chamber: str) -> pd.DataFrame:
    filename = _CHAMBERS['filenames'][chamber]
    base_url = 'https://projects.fivethirtyeight.com/2022-general-election-forecast-data/'
    fte = pd.read_csv(base_url + filename, usecols=['district', 'expression', 'winner_Dparty', 'winner_Rparty'])
    fte = fte[fte.expression == _FORECAST_EXPRESSION].drop_duplicates(keep='first', subset='district')
    fte['state'] = fte.district.apply(lambda x: x.split('-', 1)[0])
    fte = fte.drop(columns=['expression', 'district'])
    return fte


def merge_fte_and_pi(pi_data: pd.DataFrame, chamber: str) -> pd.DataFrame:
    chamber = chamber.lower()
    pi = _filter_pi_data(pi_data, chamber)
    pi = pi[pi.state.notna()].copy()
    fte = _get_fte_data(chamber)

    _separate_by_party = lambda party: pi[pi.cname == party].drop(columns='cname')
    pi = _separate_by_party('Democratic').merge(_separate_by_party('Republican'), on=[
        'mshortName', 'murl', 'state', 'seat'], suffixes=('D', 'R'))

    merged = (
        pi.merge(fte, on='state')
            .drop(columns=['state'])
            .rename(columns=dict(winner_Dparty='fteD', winner_Rparty='fteR'))
    )
    return merged


def add_profit_columns(merged: pd.DataFrame) -> None:
    merged['profit_bestBuyYesCostD'] = merged.fteD - merged.bestBuyYesCostD
    merged['profit_bestBuyNoCostR'] = merged.fteD - merged.bestBuyNoCostR
    merged['profit_bestBuyYesCostR'] = merged.fteR - merged.bestBuyYesCostR
    merged['profit_bestBuyNoCostD'] = merged.fteR - merged.bestBuyNoCostD

    merged['profit_bestSellYesCostD'] = merged.bestSellYesCostD - merged.fteD
    merged['profit_bestSellNoCostR'] = merged.bestSellNoCostR - merged.fteD
    merged['profit_bestSellYesCostR'] = merged.bestSellYesCostR - merged.fteR
    merged['profit_bestSellNoCostD'] = merged.bestSellNoCostD - merged.fteR


def add_action_columns(merged: pd.DataFrame, side: str) -> pd.DataFrame:
    cost_columns = list(map(lambda x: x.format(side.title()), (
        'best{}YesCostD', 'best{}NoCostR', 'best{}YesCostR', 'best{}NoCostD')))
    merged = merged.reset_index(drop=True)
    transposed = merged[[f'profit_{i}' for i in cost_columns]].transpose()
    addnl = [{
        'actionRec': re.search('profit_best(Buy|Sell)(Yes|No)Cost([DR])', transposed[i].idxmax()),
        'actionProfit': transposed[i].max(),
    } for i in transposed]

    merged = merged.join(pd.DataFrame(addnl))
    merged.actionRec = merged.actionRec.apply(lambda x: x.groups()).apply(
        lambda x: '{} {} on the {}'.format(x[0], x[1], dict(D='Democrat', R='Republican')[x[2]]))
    merged['actionSide'] = side
    return merged


def create_fte_and_pi_comparison() -> pd.DataFrame:
    pi_data = get_pi_data()
    merged = pd.concat(merge_fte_and_pi(pi_data, chamber) for chamber in _CHAMBERS['names'])
    add_profit_columns(merged)
    merged = pd.concat(add_action_columns(merged, side) for side in ('buy', 'sell'))
    merged = merged[merged.actionProfit >= _MIN_PROFIT_PER_SHARE].sort_values('actionSide').sort_values(
        'actionProfit', ascending=False)

    merged.fteD = merged.fteD.round(2)
    merged.fteR = merged.fteR.round(2)
    merged.actionProfit = merged.actionProfit.round(2)

    return merged


def create_html_output(merged: pd.DataFrame) -> None:
    summary = merged.groupby('actionRec', as_index=False).agg(dict(
        murl='count', seat=', '.join)).sort_values(by='murl', ascending=False)
    forecast_exp_title = _FORECAST_EXPRESSION[1:].title()

    market_item_template = open('templates/market_item.html').read()
    market_item_costs_template = open('templates/market_item_costs.html').read()
    summary_item_template = open('templates/summary_item.html').read()

    items = [
        market_item_template.format(
            forecast_expression=forecast_exp_title,
            costs=market_item_costs_template.format(actionSideTitle=i['actionSide'].title()).format(**i),
            **i,
        ) for i in merged.to_dict('records')
    ]
    items.insert(5, open('templates/notes_item.html').read().format(
        forecast_expression=forecast_exp_title, min_profit_per_share=_MIN_PROFIT_PER_SHARE))
    links_idx = 9
    html = open('templates/page.html').read().format(
        data0='\n'.join(items[:links_idx]),
        data1='\n'.join(items[links_idx:]),
        summary='\n'.join(summary_item_template.format(**i) for i in summary.to_dict('records')),
        update_interval='hourly',
        last_updated=datetime.now().strftime('%d %B %Y %H:%M'),
    )
    with open('index.html', 'w') as f:
        f.write(html)


def main():
    create_html_output(create_fte_and_pi_comparison())


if __name__ == '__main__':
    main()
