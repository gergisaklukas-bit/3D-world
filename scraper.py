import os
import pandas as pd
import requests
import zipfile
import io
from google import genai
import json
from supabase import create_client, Client

# --- Kľúče sa teraz ťahajú bezpečne z GitHub Trezoru ---
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

ai_client = genai.Client(api_key=GEMINI_API_KEY)
db_client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ... (Tu pokračuje funkcia def fetch_analyze_and_save(): presne ako predtým) ...import pandas as pd

def fetch_analyze_and_save():
    # --- 0. DYNAMICKÉ ZÍSKANIE POVOLENÉHO MODELU (Riešenie 404 chyby) ---
    print("0. Dopytujem Google o zoznam tvojich povolených modelov...")
    models_url = f"https://generativelanguage.googleapis.com/v1beta/models?key={GEMINI_API_KEY}"
    
    try:
        models_response = requests.get(models_url)
        models_data = models_response.json()
        
        valid_models = []
        # Prehľadáme zoznam a vyberieme len tie, ktoré vedia generovať text
        for m in models_data.get('models', []):
            if 'generateContent' in m.get('supportedGenerationMethods', []):
                valid_models.append(m['name']) # Vráti napr. 'models/gemini-1.5-flash-001'
        
        if not valid_models:
            print("❌ Chyba: Tvoj API kľúč nemá povolený ani jeden textový model. Skontroluj Google AI Studio.")
            return
            
        # Skúsime nájsť model typu 'flash' (je najlacnejší a najrýchlejší), ak nie je, vezmeme prvý dostupný
        chosen_model = next((m for m in valid_models if 'flash' in m), valid_models[0])
        print(f"   [+] Úspech! Našiel som a použijem presný model: {chosen_model}")
        
    except Exception as e:
        print(f"❌ Chyba pri načítaní modelov z Googlu: {e}")
        return

    # --- 1. SŤAHOVANIE DÁT Z GDELT ---
    print("\n1. Sťahujem a čistím reálne dáta z GDELT...")
    url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    r = requests.get(url)
    latest_zip_url = r.text.split('\n')[0].split(' ')[2]
    
    r_zip = requests.get(latest_zip_url)
    z = zipfile.ZipFile(io.BytesIO(r_zip.content))
    
    df = pd.read_csv(z.open(z.namelist()[0]), sep='\t', header=None, low_memory=False, on_bad_lines='skip')
    
    news_data = df.iloc[:, [31, 56, 57, -1]].copy() 
    news_data.columns = ['Mentions', 'Lat', 'Long', 'URL']
    
    news_data['URL'] = news_data['URL'].astype(str)
    news_data = news_data[news_data['URL'].str.startswith('http')]
    
    news_data['Lat'] = pd.to_numeric(news_data['Lat'], errors='coerce')
    news_data['Long'] = pd.to_numeric(news_data['Long'], errors='coerce')
    clean_data = news_data.dropna(subset=['Lat', 'Long']).copy()
    
    if clean_data.empty:
        print("V tomto balíku neboli žiadne správy s GPS. Skúste znova neskôr.")
        return
        
    top_news = clean_data.sort_values(by='Mentions', ascending=False).head(3)
    
    # --- 2. AI ANALÝZA CEZ REST API ---
    print(f"\n2. Spúšťam AI analýzu (cez REST API) pre {len(top_news)} správy...")
    analyzed_data = []
    
    # Vytvoríme URL presne pre model, ktorý sme si stiahli v Kroku 0
    gemini_url = f"https://generativelanguage.googleapis.com/v1beta/{chosen_model}:generateContent?key={GEMINI_API_KEY}"
    
    for index, row in top_news.iterrows():
        news_url = row['URL']
        impact = min(float(row['Mentions']) * 1.5, 10.0) 
        
        print(f" -> Analyzujem: {news_url[:60]}...")
        
        prompt = f"""
        Analyze this news URL: {news_url}
        Respond ONLY with a valid JSON. No markdown formatting.
        Keys must be exactly:
        "title" (Short 3-5 word headline),
        "summary" (1 short sentence explaining what happened),
        "category" (Economy, Tech, Politics, Conflict, Climate, Crypto, Other),
        "sentiment" (Positive, Negative, Neutral)
        """
        
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"response_mime_type": "application/json"}
        }
        
        try:
            response = requests.post(gemini_url, headers={'Content-Type': 'application/json'}, json=payload)
            resp_json = response.json()
            
            if response.status_code == 200:
                ai_text = resp_json['candidates'][0]['content']['parts'][0]['text']
                ai_result = json.loads(ai_text)
                
                analyzed_data.append({
                    "title": ai_result.get("title", "Unknown"),
                    "summary": ai_result.get("summary", ""),
                    "category": ai_result.get("category", "Other"),
                    "impact_score": round(impact, 1),
                    "lat": float(row['Lat']),
                    "lng": float(row['Long']),
                    "source_url": news_url,
                    "sentiment": ai_result.get("sentiment", "Neutral")
                })
                print("    [+] Úspech: AI spracovalo dáta.")
            else:
                print(f"    [!] Odmietnuté Googlom: {resp_json.get('error', {}).get('message', 'Neznáma chyba')}")
                
        except Exception as e:
            print(f"    [!] Technická chyba spojenia: {e}")

    # --- 3. ULOŽENIE DO SUPABASE ---
    if len(analyzed_data) > 0:
        print(f"\n3. Pripájam sa k Supabase a odosielam {len(analyzed_data)} čistých správ...")
        try:
            db_client.table('news_signals').insert(analyzed_data).execute()
            print("✅ HOTOVO! Reálne dáta sú v databáze!")
        except Exception as e:
            print(f"❌ Chyba databázy: {e}")
    else:
        print("Tabuľka na odoslanie je prázdna.")

fetch_analyze_and_save()
