import requests
import sys

# --- KONFIGURATION --- Kör med python3 diagnose.py
# Testa med "32025R0535" (Grundakt) eller "32003L0087" (Konsoliderad) 31972R2351
TEST_CELEX = "31972R2351" 

SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
OUTPUT_FILENAME = "downloaded_act.html"

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (compatible; Ferenda-Downloader/1.0)',
    # Vi är tydliga med att vi vill ha resultatet som JSON från SPARQL
    'Accept': 'application/sparql-results+json'
})

def log(msg):
    print(f"[LOG] {msg}")

# --- STEG 1: KONSOLIDERINGS-CHECK ---
# (Denna del fungerade perfekt i debug-loggen, vi behåller den exakt så)

def check_for_consolidation(basic_celex):
    log(f"1. Kollar konsolideringsstatus för {basic_celex}...")
    
    query = f"""
        PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        
        SELECT ?cons_celex
        WHERE {{
            ?basic_act cdm:resource_legal_id_celex "{basic_celex}"^^xsd:string .
            ?cons_act cdm:act_consolidated_consolidates_resource_legal ?basic_act .
            ?cons_act cdm:resource_legal_id_celex ?cons_celex .
        }}
        ORDER BY DESC(?cons_celex)
        LIMIT 1
    """
    try:
        r = session.get(SPARQL_ENDPOINT, params={'query': query}, timeout=30)
        if r.status_code == 200:
            bindings = r.json().get('results', {}).get('bindings', [])
            if bindings:
                found_id = bindings[0]['cons_celex']['value']
                log(f"   -> TRÄFF: Hittade konsoliderad version: {found_id}")
                return found_id
    except Exception as e:
        log(f"   -> Fel vid konsolideringskoll: {e}")
    
    log("   -> Ingen konsolidering hittad, använder grundakten.")
    return None

# --- STEG 2: HITTA OCH VÄLJ RÄTT MANIFESTATION ---

def get_best_manifestation_url(celex_id):
    """
    Hämtar alla manifestationer och väljer strikt ut xhtml eller html.
    Returnerar URL eller None.
    """
    log(f"2. Letar efter fil-länk (Manifestation) för {celex_id}...")

    # Vi använder exakt samma query som i ditt debug-script som fungerade.
    # Vi filtrerar INTE i SPARQL (OPTIONAL) för att vara säkra på att få träff,
    # sen filtrerar vi hårt i Python.
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
        r = session.get(SPARQL_ENDPOINT, params={'query': query}, timeout=30)
        
        if r.status_code == 200:
            bindings = r.json().get('results', {}).get('bindings', [])
            
            if not bindings:
                log("   -> Inga manifestationer alls hittades i databasen.")
                return None

            # --- PRIORITERINGSLOGIK ---
            # Vi letar efter xhtml eller html. Vi ignorerar pdf, fmx4, etc.
            
            selected_uri = None
            selected_type = None

            # Prioriteringsordning
            PRIORITY = ['xhtml', 'xhtml_simplified', 'html']

            for p in PRIORITY:
                for b in bindings:
                    m_type = b.get('type', {}).get('value', 'okänd')
                    m_uri = b['manifestation_uri']['value']
                    
                    if m_type == p:
                        selected_uri = m_uri
                        selected_type = m_type
                        break # Hittade högsta prio!
                if selected_uri:
                    break

            if selected_uri:
                log(f"   -> VALD FIL: {selected_type} på {selected_uri}")
                return selected_uri
            else:
                log("   -> HITTADE INGEN HTML/XHTML. (Endast PDF/FMX4 hittades, laddar ej ner).")
                # Loopa för att visa vad vi hoppade över
                for b in bindings:
                    log(f"      Hoppade över: {b.get('type', {}).get('value', 'okänd')}")
                return None

    except Exception as e:
        log(f"   -> Fel vid SPARQL-sökning: {e}")
    
    return None

# --- STEG 3: LADDA NER ---

def download_file(url):
    log(f"3. Laddar ner filen från {url}...")
    
    try:
        # Här måste vi ändra Accept-headern. SPARQL vill ha JSON, men filen är HTML.
        file_headers = {
            'User-Agent': session.headers['User-Agent'],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        
        r = requests.get(url, headers=file_headers, allow_redirects=True, timeout=60)
        
        if r.status_code == 200:
            return True, r.text
        else:
            log(f"   -> Servern gav felkod: {r.status_code}")
            return False, None
            
    except Exception as e:
        log(f"   -> Krasch vid nedladdning: {e}")
        return False, None

# --- MAIN ---

def run():
    print(f"--- STARTAR NEDLADDNING FÖR {TEST_CELEX} ---")
    
    # 1. Hitta rätt ID (Grund eller Konsoliderad)
    cons_id = check_for_consolidation(TEST_CELEX)
    target_id = cons_id if cons_id else TEST_CELEX
    
    print(f"[TARGET] {target_id}")

    # 2. Hämta URL via SPARQL (W-E-M)
    url = get_best_manifestation_url(target_id)
    
    if not url:
        print("AVBRYTER: Ingen passande filtyp (html/xhtml) hittades.")
        return

    # 3. Ladda ner
    success, content = download_file(url)
    
    print("-" * 30)
    if success:
        print("NEDLADDNING LYCKADES!")
        # Spara till fil
        with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Sparad till: {OUTPUT_FILENAME} ({len(content)} tecken)")
        
        # Enkel verifiering
        if "<html" in content or "<!DOCTYPE html" in content:
            print("[OK] Filen ser ut att vara HTML/XHTML.")
        else:
            print("[VARNING] Filen verkar inte vara HTML. Kontrollera innehållet.")
            
    else:
        print("NEDLADDNING MISSLYCKADES.")

if __name__ == '__main__':
    run()