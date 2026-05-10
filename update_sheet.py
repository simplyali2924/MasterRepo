import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
import json

# ============================================================
# 1. GOOGLE SHEETS AUTHENTICATION
# ============================================================
# Use environment variable for credentials
creds_json = os.environ.get('GCP_CREDENTIALS')
if creds_json:
    creds_dict = json.loads(creds_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
else:
    # Or use file directly
    SERVICE_ACCOUNT_FILE = r'C:\SIMPLY_Official\2024\TechHome24\drfcore\equityHome\equityLibs\gSheet\Basics\gsheetProject\credentials.json'
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)

client = gspread.authorize(creds)

spreadsheet_id = "1VzwkUvJ4XKckUq25cgLdamB62c4IIymol9_1YRhghFs"
worksheet = client.open_by_key(spreadsheet_id).worksheet("Top 250 Stocks")

# ============================================================
# 2. FETCH TOP STOCKS BY VOLUME USING YFINANCE + NIFTY 50 LIST
# ============================================================

# Nifty 50 symbols (common liquid stocks)
NIFTY_50_SYMBOLS = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "HINDUNILVR.NS", "SBIN.NS", "BHARTIARTL.NS", "ITC.NS", "KOTAKBANK.NS",
    "BAJFINANCE.NS", "AXISBANK.NS", "WIPRO.NS", "TATAMOTORS.NS", "MARUTI.NS",
    "SUNPHARMA.NS", "ADANIPORTS.NS", "TITAN.NS", "M&M.NS", "ULTRACEMCO.NS",
    "NTPC.NS", "POWERGRID.NS", "NESTLEIND.NS", "HCLTECH.NS", "BAJAJFINSV.NS",
    "ONGC.NS", "TECHM.NS", "ASIANPAINT.NS", "LT.NS", "JSWSTEEL.NS",
    "HEROMOTOCO.NS", "GRASIM.NS", "DRREDDY.NS", "BPCL.NS", "DIVISLAB.NS",
    "HDFC.NS", "UPL.NS", "BRITANNIA.NS", "INDUSINDBK.NS", "EICHERMOT.NS",
    "COALINDIA.NS", "SHREECEM.NS", "SBILIFE.NS", "BAJAJ-AUTO.NS", "HINDALCO.NS",
    "TATASTEEL.NS", "CIPLA.NS", "ADANIGREEN.NS", "ADANIENT.NS", "VEDL.NS"
]

def fetch_top_stocks():
    """Fetch current stock data using yfinance"""
    stock_data = []
    
    for symbol in NIFTY_50_SYMBOLS:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # Get volume and price
            volume = info.get('regularMarketVolume', 0)
            price = info.get('regularMarketPrice', info.get('currentPrice', 0))
            symbol_clean = symbol.replace('.NS', '')
            
            if volume > 0 and price > 0:
                stock_data.append({
                    'symbol': symbol_clean,
                    'volume': volume,
                    'price': price
                })
        except Exception as e:
            continue
    
    # Sort by volume and get top 250 (or available stocks)
    df = pd.DataFrame(stock_data)
    if len(df) > 0:
        df_top = df.sort_values(by='volume', ascending=False).head(250)
        return df_top[['symbol', 'volume', 'price']].values.tolist()
    
    return None

def fetch_bhavcopy_for_date(date_obj):
    """Alternative: Use nsepython library (install: pip install nsepython)"""
    try:
        from nsepython import nse_bhav_copy
        
        # Get bhavcopy for the date
        bhav_data = nse_bhav_copy(date_obj)
        
        if bhav_data is not None and len(bhav_data) > 0:
            # Filter EQ series
            bhav_data = bhav_data[bhav_data['SERIES'] == 'EQ']
            
            # Filter out ETFs
            filter_keywords = 'BEES|ETF|GOLD|LIQUID|CASE|SILVER|LIQ|NIFTY|BANKEX'
            bhav_data = bhav_data[~bhav_data['SYMBOL'].astype(str).str.contains(filter_keywords, case=False, na=False)]
            
            # Get top 250 by volume
            df_top = bhav_data.nlargest(250, 'TOTTRDQTY')
            
            return df_top[['SYMBOL', 'TOTTRDQTY', 'CLOSE']].values.tolist()
    except ImportError:
        print("nsepython not installed. Install with: pip install nsepython")
        return None
    except Exception as e:
        print(f"nsepython error: {e}")
        return None
    
    return None

# ============================================================
# 3. EXECUTION LOGIC
# ============================================================

print("Fetching stock data...")

# Try current market data first (reliable)
data_to_insert = fetch_top_stocks()

if data_to_insert:
    fetched_date_str = datetime.now().strftime('%d-%b-%Y')
    print(f"✅ Got current market data for {len(data_to_insert)} stocks")
else:
    # Fallback: Try historical bhavcopy using nsepython
    print("Trying historical bhavcopy...")
    date = datetime.now()
    for i in range(10):
        test_date = date - timedelta(days=i)
        if test_date.weekday() >= 5:
            continue
        
        print(f"Trying {test_date.strftime('%Y-%m-%d')}...")
        data_to_insert = fetch_bhavcopy_for_date(test_date)
        if data_to_insert:
            fetched_date_str = test_date.strftime('%d-%b-%Y')
            print(f"✅ Got data for {fetched_date_str}")
            break
        else:
            print("No data")

# ============================================================
# 4. UPDATE GOOGLE SHEET
# ============================================================
if data_to_insert:
    # Clear existing data
    worksheet.batch_clear(['A2:C251'])
    
    # Update with new data
    worksheet.update('A2', data_to_insert, value_input_option='USER_ENTERED')
    
    # Update status
    ist_now = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime('%d-%b %H:%M')
    status_msg = f"Data Date: {fetched_date_str} | Last Update: {ist_now} (IST)"
    worksheet.update('K2', [[status_msg]], value_input_option='USER_ENTERED')
    
    print(f"\n✅ SUCCESS: Sheet Updated!")
    print(f"   Rows inserted: {len(data_to_insert)}")
    print(f"   Status: {status_msg}")
else:
    print("\n❌ ERROR: Could not fetch data")
    error_msg = f"ERROR: Failed to fetch data. Last attempt: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    worksheet.update('K2', [[error_msg]], value_input_option='USER_ENTERED')

print("\nScript completed.")
