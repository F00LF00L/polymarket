import requests
import pandas as pd
import time
import firebase_admin
from firebase_admin import credentials, firestore

print("🔌 מתחבר למסד הנתונים בענן...")

# --- חיבור לפיירבייס ---
# ודא ששם הקובץ כאן זהה לשם הקובץ שהורדת ושמת בתיקייה
cred = credentials.Certificate("firebase-key.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

BASE_URL = "https://clob.polymarket.com"
MARKETS_URL = f"{BASE_URL}/sampling-markets"

print("📡 סורק שווקים, מחשב השקעות ואוסף נתוני קהל...")

try:
    response = requests.get(MARKETS_URL)
    if response.status_code == 200:
        raw_data = response.json()
        markets_data = raw_data.get('data', []) if isinstance(raw_data, dict) else (raw_data if isinstance(raw_data, list) else [])
        
        active_markets = [m for m in markets_data if isinstance(m, dict) and m.get('active') == True]
        all_markets_results = []
        
        for market in active_markets[:10]:
            question = market.get('question', 'שוק ללא שם')
            tags = market.get('tags', [])
            tokens = market.get('tokens', [])
            
            biggest_whale_in_market = 0
            predicted_answer = "N/A"
            
            yes_voters = 0
            no_voters = 0
            yes_money = 0
            no_money = 0
            
            if tokens and isinstance(tokens, list):
                for token in tokens:
                    token_id = token.get('token_id')
                    outcome = token.get('outcome')
                    
                    book_url = f"{BASE_URL}/book?token_id={token_id}"
                    book_response = requests.get(book_url)
                    
                    if book_response.status_code == 200:
                        book_data = book_response.json()
                        bids = pd.DataFrame(book_data.get('bids', []))
                        
                        if not bids.empty:
                            bids['price'] = pd.to_numeric(bids['price'])
                            bids['size'] = pd.to_numeric(bids['size'])
                            bids['total_dollars'] = bids['price'] * bids['size']
                            
                            max_investment = bids['total_dollars'].max()
                            if max_investment > biggest_whale_in_market:
                                biggest_whale_in_market = max_investment
                                predicted_answer = outcome
                            
                            total_outcome_money = bids['total_dollars'].sum()
                            num_voters = len(bids)
                            
                            if outcome == 'Yes':
                                yes_voters = num_voters
                                yes_money = total_outcome_money
                            elif outcome == 'No':
                                no_voters = num_voters
                                no_money = total_outcome_money
                    
                    time.sleep(0.5)
            
            total_market_money = yes_money + no_money
            
            if biggest_whale_in_market > 0:
                category = tags[0] if tags else "כללי"
                all_markets_results.append({
                    'category': category,
                    'question': question[:35] + "...", 
                    'whale_answer': predicted_answer,
                    'whale_investment': round(biggest_whale_in_market, 2),
                    'total_yes_money': round(yes_money, 2),
                    'total_no_money': round(no_money, 2),
                    'total_market_money': round(total_market_money, 2),
                    'yes_voters': yes_voters,
                    'no_voters': no_voters
                })
        
        if all_markets_results:
            # ממיינים את הרשימה מהגדול לקטן לפני השמירה
            all_markets_results_sorted = sorted(all_markets_results, key=lambda x: x['whale_investment'], reverse=True)
            
            print("\n✅ הסריקה הסתיימה! שולח נתונים לענן...")
            
            # --- שמירה לפיירבייס ---
            doc_ref = db.collection('scans').document('latest')
            doc_ref.set({
                'timestamp': firestore.SERVER_TIMESTAMP,
                'markets': all_markets_results_sorted
            })
            
            print("🚀 הנתונים נשמרו בהצלחה במסד הנתונים של Firebase!")
        else:
            print("📭 לא מצאנו נתונים מתאימים בדגימה הזו.")

    else:
        print(f"❌ שגיאה בגישה ל-API: {response.status_code}")
        
except Exception as e:
    print(f"⚠️ שגיאה כללית: {e}")