import csv
import sys
import os
import time
import requests

# --- INSTÄLLNINGAR --- 
# Kör med: python3 bulk_download.py
CSV_FILE = "eurlex.csv"
CSV_DELIMITER = ',' 
TIMEOUT_SECONDS = 60 
SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
OVERWRITE_EXISTING = False  # Sätt till False för att kunna återuppta nedladdning

# --- SETUP (Mock/Imports) ---
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
try:
    from ferenda.sources.legal.eu import EURLexActs
    from ferenda import util
except ImportError:
    class MockRepo:
        class Config:
            languages = ['swe']
        config = Config()
        class Store:
            def path(self, celex, folder, ext):
                return os.path.join("data", "eu", folder, f"{celex}{ext}")
        store = Store()
    EURLexActs = lambda: MockRepo()
    class MockUtil:
        def ensure_dir(self, path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
    util = MockUtil()

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=3)
session.mount("http://", adapter)
session.mount("https://", adapter)
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (compatible; Ferenda-BulkLoader/4.2)',
})

# --- SPARQL-FUNKTIONER ---

def get_consolidation_history(basic_celex):
    """
    Hämtar en lista på ALLA konsoliderade versioner som är BASERADE PÅ denna akt.
    Sorterat nyast först.
    """
    query = f"""
        PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        
        SELECT ?cons_celex
        WHERE {{
            ?basic_act cdm:resource_legal_id_celex "{basic_celex}"^^xsd:string .
            ?cons_act cdm:act_consolidated_based_on_resource_legal ?basic_act .
            ?cons_act cdm:resource_legal_id_celex ?cons_celex .
        }}
        ORDER BY DESC(?cons_celex)
    """
    try:
        headers = {'Accept': 'application/sparql-results+json'}
        r = session.get(SPARQL_ENDPOINT, params={'query': query}, headers=headers, timeout=20)
        
        if r.status_code == 200:
            bindings = r.json().get('results', {}).get('bindings', [])
            return [b['cons_celex']['value'] for b in bindings]
            
    except Exception as e:
        print(f"   [SPARQL ERROR] Historik-sökning: {e}")
    
    return []

def get_manifestation_url(celex_id):
    """Hämtar URL om det finns en svensk HTML/XHTML-fil."""
    query = f"""
        PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX lang: <http://publications.europa.eu/resource/authority/language/>

        SELECT ?manifestation_uri ?type
        WHERE {{
            ?work cdm:resource_legal_id_celex "{celex_id}"^^xsd:string .
            ?expression cdm:expression_belongs_to_work ?work .
            ?expression cdm:expression_uses_language lang:SWE .
            ?manifestation_uri cdm:manifestation_manifests_expression ?expression .
            OPTIONAL {{ ?manifestation_uri cdm:manifestation_type ?type . }}
        }}
    """
    try:
        headers = {'Accept': 'application/sparql-results+json'}
        r = session.get(SPARQL_ENDPOINT, params={'query': query}, headers=headers, timeout=20)
        
        if r.status_code == 200:
            bindings = r.json().get('results', {}).get('bindings', [])
            PRIORITY = ['xhtml', 'xhtml_simplified', 'html']
            
            for p in PRIORITY:
                for b in bindings:
                    if b.get('type', {}).get('value') == p:
                        return b['manifestation_uri']['value'], p
    except Exception: pass
    return None, None

def download_file(url, target_path_base):
    try:
        headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'}
        r = session.get(url, headers=headers, allow_redirects=True, timeout=TIMEOUT_SECONDS)
        
        if r.status_code == 200:
            ct = r.headers.get('Content-Type', '').lower()
            ext = ".html" if "html" in ct and "xhtml" not in ct else ".xhtml"
            final_path = target_path_base.replace(".xhtml", ext)
            
            with open(final_path, "wb") as f:
                f.write(r.content)
            return True, final_path
    except Exception: pass
    return False, None

# --- MAIN ---

def run():
    repo = EURLexActs()
    if not os.path.exists(CSV_FILE):
        print(f"FEL: {CSV_FILE} saknas.")
        return

    stats = {'total': 0, 'success_cons': 0, 'success_basic': 0, 'failed': 0, 'skipped': 0}
    
    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        celex_col = next((c for c in reader.fieldnames if "celex" in c.lower()), None)
        
        if not celex_col:
            print("FEL: Ingen CELEX-kolumn hittades i CSV.")
            return

        for row in reader:
            original_celex = row.get(celex_col, '').strip()
            if not original_celex: continue
            stats['total'] += 1
            print(f"\n[{stats['total']}] Bearbetar {original_celex}...")

            base_path = repo.store.path(original_celex, 'downloaded', '.xhtml')
            alt_path = base_path.replace('.xhtml', '.html')
            
            # --- START NY HOPPA-ÖVER LOGIK ---
            # Vi kollar om filen finns OCH att den har innehåll (> 0 bytes)
            file_exists_and_valid = False
            
            # Kolla .xhtml
            if os.path.exists(base_path) and os.path.getsize(base_path) > 0:
                file_exists_and_valid = True
            # Kolla .html (fallback-namn)
            elif os.path.exists(alt_path) and os.path.getsize(alt_path) > 0:
                file_exists_and_valid = True

            if not OVERWRITE_EXISTING and file_exists_and_valid:
                print("   -> Finns redan (och verkar hel). Hoppar över.")
                stats['skipped'] += 1
                continue
            # --- SLUT NY LOGIK ---

            util.ensure_dir(base_path)

            # 1. Hämta historia (med striktare sökning)
            history = get_consolidation_history(original_celex)
            download_success = False
            
            # Skapa ett "säkert" ID-mönster för att dubbelkolla
            safe_id_pattern = original_celex[1:] # Tar bort första tecknet ('3')
            
            if history:
                print(f"   -> Hittade {len(history)} konsoliderade versioner.")
                
                for cons_id in history:
                    # SÄKERHETSKONTROLL
                    if safe_id_pattern not in cons_id:
                        print(f"   -> [VARNING] Ignorerar {cons_id} (Verkar tillhöra annan akt).")
                        continue

                    url, ftype = get_manifestation_url(cons_id)
                    if url:
                        print(f"   -> URL hittad för {cons_id} ({ftype}). Laddar ner...")
                        ok, path = download_file(url, base_path)
                        if ok:
                            print(f"   -> KLAR! (Sparad som {os.path.basename(path)})")
                            stats['success_cons'] += 1
                            download_success = True
                            break 
                    else:
                        print(f"   -> [INFO] Ingen svensk fil för {cons_id}. Backar...")
            else:
                print("   -> Ingen konsolideringshistorik hittad.")

            # 2. FALLBACK
            if not download_success:
                print("   -> Fallback: Försöker hämta grundakten.")
                url, ftype = get_manifestation_url(original_celex)
                
                if url:
                    print(f"   -> Laddar ner grundakt ({ftype})...")
                    ok, path = download_file(url, base_path)
                    if ok:
                        print(f"   -> KLAR! (GRUNDAKT)")
                        stats['success_basic'] += 1
                    else:
                        print("   -> FEL: Misslyckades med grundakten också.")
                        stats['failed'] += 1
                else:
                    print("   -> FEL: Varken konsoliderad eller grundakt hittades.")
                    stats['failed'] += 1
            
            time.sleep(0.5)

    print("\n" + "="*30 + "\nSLUTRAPPORT\n" + "="*30)
    print(f"Totalt: {stats['total']}")
    print(f"Konsoliderade: {stats['success_cons']}")
    print(f"Grundakter:    {stats['success_basic']}")
    print(f"Misslyckade:   {stats['failed']}")
    print(f"Hoppade över:  {stats['skipped']}")

if __name__ == '__main__':
    run()