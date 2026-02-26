import requests
import pandas as pd
import time
import firebase_admin
from firebase_admin import credentials, firestore
import os
import json

def connect_firebase():
    """פונקציה לחיבור לפיירבייס שתומכת גם במחשב וגם ב-GitHub"""
    # בדיקה אם קיים Secret של GitHub
    firebase_key = os.environ.get('FIREBASE_KEY')
    
    if firebase_key:
        # התחברות באמצעות ה-Secret ב-GitHub
        cred_dict = json.loads(firebase_key)
        cred = credentials.Certificate(cred_dict)
    else:
        # התחברות מקומית באמצעות הקובץ (למטרות בדיקה במחשב)
        cred = credentials.Certificate("firebase-key.json")
    
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    return firestore.client()

def get_polymarket_data():
    BASE_URL = "https://clob.polymarket.com"
    MARKETS_URL = f"{BASE_URL}/sampling-markets"
    
    print("📡 סורק שווקים ומחפש לווייתנים...")
    
    try:
        response = requests.get(MARKETS_URL)
        if response.status_code != 200:
            print(f"❌ שגיאה בגישה ל-API: {response.status_code}")
            return []

        raw_data = response.json()
        markets_data = raw_data.get('data', []) if isinstance(raw_data, dict) else raw_data
        
        active_markets = [m for m in markets_data if isinstance(m, dict) and m.get('active') == True]
        all_markets_results = []
        
        # סורק את 15 השווקים הפעילים הראשונים (אפשר להגדיל)
        for market in active_markets[:15]:
            question = market.get('question', 'שוק ללא שם')
            tags = market.get('tags', [])
            tokens = market.get('tokens', [])
            
            biggest_whale_in_market = 0
            predicted_answer = "N/A"
            yes_voters, no_voters = 0, 0
            yes_money, no_money = 0, 0
            
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
                    
                    time.sleep(0.3) # השהייה קלה למניעת חסימה
            
            total_market_money = yes_money + no_money
            
            if biggest_whale_in_market > 0:
                category = tags[0] if tags else "General"
                all_markets_results.append({
                    'category': category,
                    'question': question, # השאלה המלאה עוברת כאן
                    'whale_answer': predicted_answer,
                    'whale_investment': round(biggest_whale_in_market, 2),
                    'total_yes_money': round(yes_money, 2),
                    'total_no_money': round(no_money, 2),
                    'total_market_money': round(total_market_money, 2),
                    'yes_voters': yes_voters,
                    'no_voters': no_voters
                })
        
        # מיון לפי גובה השקעת הלווייתן
        return sorted(all_markets_results, key=lambda x: x['whale_investment'], reverse=True)

    except Exception as e:
        print(f"⚠️ שגיאה באיסוף הנתונים: {e}")
        return []

def upload_to_firebase(data):
    if not data:
        print("📭 אין נתונים חדשים להעלות.")
        return

    try:
        db = connect_firebase()
        doc_ref = db.collection('scans').document('latest')
        doc_ref.set({
            'timestamp': firestore.SERVER_TIMESTAMP,
            'markets': data
        })
        print(f"🚀 {len(data)} שווקים עודכנו בהצלחה ב-Firebase!")
    except Exception as e:
        print(f"❌ שגיאה בשמירה לענן: {e}")

if __name__ == "__main__":
    results = get_polymarket_data()
    upload_to_firebase(results)