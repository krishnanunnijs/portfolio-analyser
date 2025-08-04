from flask import Flask, render_template
import json
import os

app = Flask(__name__)

def format_market_cap(value):
    if value is None:
        return "N/A"
    try:
        value = float(value)
        if value >= 1e12:
            return f"${value/1e12:.2f}T"
        elif value >= 1e9:
            return f"${value/1e9:.2f}B"
        elif value >= 1e6:
            return f"${value/1e6:.2f}M"
        else:
            return f"${value:,.0f}"
    except Exception:
        return str(value)

@app.route('/')
def index():
    # Read and parse the JSON file
    with open('data.json', 'r', encoding='utf-8') as f:
        stocks_raw = json.load(f)
    with open('quotes.json', 'r', encoding='utf-8') as f:
        quotes = json.load(f)["quotes"]
    with open('stock_data.json', 'r', encoding='utf-8') as f:
        stock_json = json.load(f)
        stock_data = stock_json["data"]
        extra_data = stock_json.get("extraData", [])

    # Build lookup dicts for fast access
    quotes_by_ticker = {q["ticker"]: q for q in quotes}
    stock_data_by_ticker = {s["ticker"]: s for s in stock_data}
    extra_data_by_ticker = {e["ticker"]: e for e in extra_data}

    # Parse to stock objects with required attributes
    stocks = []
    for s in stocks_raw:
        ticker = s.get('ticker')
        quote = quotes_by_ticker.get(ticker, {})
        stock_info = stock_data_by_ticker.get(ticker, {})
        extra_info = extra_data_by_ticker.get(ticker, {})
        market_cap = s.get('marketCap')
        formatted_market_cap = format_market_cap(market_cap)
        # Get oneMonthGain and upside from extraData
        one_month_gain = None
        upside = None
        research = extra_info.get('research', {})
        if research:
            one_month_gain = research.get('oneMonthGain')
            upside = research.get('upside')
        stock = {
            'ticker': ticker,
            'companyName': s.get('companyName'),
            'priceTarget': s.get('priceTarget'),
            'buy': s.get('buy'),
            'sell': s.get('sell'),
            'hold': s.get('hold'),
            'isin': s.get('isin'),
            'sector': s.get('sector'),
            'marketCap': formatted_market_cap,
            'price': quote.get('price'),
            'analystConsensus': stock_info.get('analystConsensus'),
            'oneMonthGainPct': f"{one_month_gain*100:.2f}%" if one_month_gain is not None else "N/A",
            'upsidePct': f"{upside*100:.2f}%" if upside is not None else "N/A"
        }
        stocks.append(stock)
    return render_template('index.html', stocks=stocks)

if __name__ == '__main__':
    app.run(debug=True)
