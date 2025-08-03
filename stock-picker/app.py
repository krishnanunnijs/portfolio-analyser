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
        stock_data = json.load(f)["data"]

    # Build lookup dicts for fast access
    quotes_by_ticker = {q["ticker"]: q for q in quotes}
    stock_data_by_ticker = {s["ticker"]: s for s in stock_data}

    # Parse to stock objects with required attributes
    stocks = []
    for s in stocks_raw:
        ticker = s.get('ticker')
        quote = quotes_by_ticker.get(ticker, {})
        stock_info = stock_data_by_ticker.get(ticker, {})
        market_cap = s.get('marketCap')
        formatted_market_cap = format_market_cap(market_cap)
        price = quote.get('price')
        price_target = s.get('priceTarget')
        # Calculate price target as % difference from price
        price_target_pct = None
        if price is not None and price_target is not None and price != 0:
            price_target_pct = ((price_target - price) / price) * 100
        stock = {
            'ticker': ticker,
            'companyName': s.get('companyName'),
            'priceTarget': price_target,
            'priceTargetPct': f"{price_target_pct:.2f}%" if price_target_pct is not None else "N/A",
            'buy': s.get('buy'),
            'sell': s.get('sell'),
            'hold': s.get('hold'),
            'isin': s.get('isin'),
            'sector': s.get('sector'),
            'rating': s.get('rating'),
            'marketCap': formatted_market_cap,
            'price': price,
            'analystConsensus': stock_info.get('analystConsensus')
        }
        stocks.append(stock)
    return render_template('index.html', stocks=stocks)

if __name__ == '__main__':
    app.run(debug=True)
