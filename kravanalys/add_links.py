import os
import re
import sys
from lxml import etree
from datetime import date

# Kör med: python3 add_links.py [filnamn eller katalog]
# Konstanter
PARSED_DIR = "data/eurlexacts/parsed"
LOG_FILE = "link_processing.log"

# --- GLOBALA VARIABLER FÖR RAPPORTERING ---
INVALID_CELEX_HITS = []
CURRENT_PROCESSING_FILE = ""

# --- 1. REGEX FÖR CELEX-LÄNKNING ---

# UPPDATERAD: "i":et på slutet är nu valfritt ((?:\s+i)?).
# Detta gör att vi matchar både "artikel X i förordning" och "artikel X förordning".
R_ARTIKEL = r'(?:artikel\s+(?P<art_num>\d[\w\.\-\(\)]*(?:(?:\s*[\,\-]\s*|\s+(?:och|till|med)\s+|\s+)(?:[a-z]\b|\d[\w\.\-\(\)]*))*)(?:\s+i)?\s+)?'

R_INST = r'(?P<inst>Europaparlamentets\s+och\s+rådets|Europeiska\s+[\w\s]+\s+(?:myndighetens|centralbankens)|rådets|kommissionens)?\s*'
R_TYP = r'(?P<typ>förordning|direktiv)'

# Fångar "era_prefix" (EU/EG/EEG)
R_PREFIX = r'(?:\s*\(?(?P<era_prefix>EU|EG|EEG|Euratom)\)?\s*)?(?:\s*nr\.?)?\s*'

# Fångar "suffix" (för fall som 36/63/EEG)
R_NUMMER = r'(?P<n1>\d{2,4})\/(?P<n2>\d{1,5})(?:/(?P<suffix>[A-Z]{2,3}))?'

FULL_PATTERN = re.compile(
    f"""
    {R_ARTIKEL}
    {R_INST}
    {R_TYP}
    {R_PREFIX}
    {R_NUMMER}
    """,
    re.IGNORECASE | re.UNICODE | re.VERBOSE
)

# --- 2. REGEX FÖR BORTTAGNING AV GAMLA NOTER ---
# Scenario 1: Parenteserna är inuti länken, t.ex. <a>(1)</a>
STRICT_FOOTNOTE_PATTERN = re.compile(r'^\(\d+\)$', re.UNICODE)
# Scenario 2: Endast siffror i länken, t.ex. <a>1</a> (kräver kontroll av omgivande text)
DIGIT_ONLY_PATTERN = re.compile(r'^\d+$', re.UNICODE)


def is_valid_year(val):
    if val is None: return False
    try:
        y = int(val)
        current_year = date.today().year
        return 1951 <= y <= (current_year + 1)
    except (ValueError, TypeError):
        return False

def expand_year(val):
    """Gör om '90' till 1990 och '15' till 2015."""
    try:
        val_str = str(val)
        if len(val_str) == 4:
            return int(val_str)
        if len(val_str) == 2:
            y = int(val_str)
            return int("19" + val_str) if y > 50 else int("20" + val_str)
        return int(val_str) # Fallback
    except:
        return 0

def fits_in_era(year, era):
    """
    Kollar om ett årtal matchar en Era (EEG, EG, EU).
    Hjälper oss välja rätt siffra som år när det är tvetydigt.
    """
    if not era: 
        return True # Inget prefix = ingen åsikt
    
    era = era.upper()
    
    if "EEG" in era:
        return 1957 <= year <= 1993
    if "EG" in era:
        return 1993 <= year <= 2009
    if "EU" in era:
        return year >= 2009
    
    return True

def make_celex_uri(match):
    gd = match.groupdict()
    typ = gd['typ'].lower()
    
    if "förordning" in typ: letter = "R"
    elif "direktiv" in typ: letter = "L"
    else: return None

    n1, n2 = gd['n1'], gd['n2']
    
    # Hämta era från prefix (t.ex. "(EEG)") ELLER suffix (t.ex. "/EEG")
    era = gd.get('era_prefix') or gd.get('suffix')
    
    year = ""
    num = ""

    # --- LOGIK: Bestäm vad som är år och vad som är löpnummer ---

    y1_candidate = expand_year(n1)
    y2_candidate = expand_year(n2)
    
    is_y1_valid = is_valid_year(y1_candidate)
    is_y2_valid = is_valid_year(y2_candidate)

    # SCENARIO 1: En klar vinnare (ett giltigt år, ett ogiltigt)
    if is_y1_valid and not is_y2_valid:
        year, num = y1_candidate, n2
    elif is_y2_valid and not is_y1_valid:
        year, num = y2_candidate, n1
    
    # SCENARIO 2: Tvetydigt (t.ex. 28/90). Båda KAN vara år.
    # Här använder vi Era-logiken ("Nudgen").
    elif is_y1_valid and is_y2_valid:
        
        fits_1 = fits_in_era(y1_candidate, era)
        fits_2 = fits_in_era(y2_candidate, era)
        
        if fits_1 and not fits_2:
            year, num = y1_candidate, n2
        elif fits_2 and not fits_1:
            year, num = y2_candidate, n1
        else:
            # Om båda passar eller ingen passar, gå på standardformatet "År/Nummer"
            year, num = y1_candidate, n2

    # SCENARIO 3: Inget ser ut som ett år
    else:
        year = y1_candidate
        num = n2

    try:
        celex = f"3{year}{letter}{int(num):04d}"
        
        # Validering
        prefix_check = int(celex[:5])
        max_allowed = int(f"3{date.today().year + 1}")
        
        if not (31951 <= prefix_check <= max_allowed):
            INVALID_CELEX_HITS.append((CURRENT_PROCESSING_FILE, celex))
            
    except (ValueError, IndexError):
        INVALID_CELEX_HITS.append((CURRENT_PROCESSING_FILE, f"PARSE_ERROR: {n1}/{n2}"))
        return None

    # Hantering för artikelhänvisning
    fragment = ""
    if gd.get('art_num'):
        # Vi använder en regex som fångar siffror FÖLJT AV valfria bokstäver
        # Exempel: "3" -> "3", "3a" -> "3a", "3bis" -> "3bis"
        art_match = re.search(r'(\d+[a-z]*)', gd['art_num'], re.IGNORECASE)
        if art_match:
            fragment = f"#A{art_match.group(1)}"
            
    return f"http://localhost:8000/res/eurlexacts/{celex}{fragment}"

def process_text_segment(text, parent, insert_index=0):
    if not text: return 0, None
    matches = list(FULL_PATTERN.finditer(text))
    if not matches: return 0, None

    created_elems = []
    prefix_text = text[:matches[0].start()]
    
    for i, match in enumerate(matches):
        full_str = match.group(0)
        url = make_celex_uri(match)
        
        a = etree.Element("a")
        a.text = full_str
        if url:
            a.set("href", url)
            a.set("class", "celex-ref")
        
        start_next = matches[i+1].start() if i+1 < len(matches) else len(text)
        a.tail = text[match.end():start_next]
        created_elems.append(a)

    for elem in reversed(created_elems):
        parent.insert(insert_index, elem)

    return len(created_elems), prefix_text

def linkify_tree(elem):
    total_links = 0
    if elem.text:
        count, prefix = process_text_segment(elem.text, elem, 0)
        if count > 0:
            elem.text = prefix
            total_links += count

    children = list(elem)
    for i in range(len(children) - 1, -1, -1):
        child = children[i]
        tag_local = etree.QName(child).localname.lower()
        if tag_local != 'a':
            total_links += linkify_tree(child)

        if child.tail:
            count, prefix = process_text_segment(child.tail, elem, i + 1)
            if count > 0:
                child.tail = prefix
                total_links += count
    return total_links

def clean_old_links(root):
    """
    Tar bort gamla fotnotslänkar. Hanterar två fall:
    1. Länken innehåller parenteserna: <a>(1)</a>
    2. Länken är bara siffror, men omges av parenteser i texten: (<a...>1</a>)
    """
    removed_count = 0
    # Skapa en lista för att kunna iterera säkert medan vi modifierar trädet
    all_links = list(root.xpath(".//*[local-name()='a']"))
    
    for a in all_links:
        text_content = "".join(a.itertext()).strip()
        should_remove = False
        
        # Fall 1: Parenteser inuti, t.ex. (1)
        if STRICT_FOOTNOTE_PATTERN.match(text_content):
            should_remove = True
            
        # Fall 2: Bara siffror, t.ex. 1. Kontrollera omgivningen.
        elif DIGIT_ONLY_PATTERN.match(text_content):
            # Hitta noden som håller texten precis före
            prev = a.getprevious()
            
            # Hämta texten före (antingen tail på föregående syskon, eller text på föräldern)
            if prev is not None:
                prev_text = prev.tail or ""
                def update_prev(txt): prev.tail = txt
            else:
                parent = a.getparent()
                if parent is None: continue
                prev_text = parent.text or ""
                def update_prev(txt): parent.text = txt
            
            tail_text = a.tail or ""
            
            # Kontrollera om vi har formen (... och ...)
            if prev_text.rstrip().endswith('(') and tail_text.lstrip().startswith(')'):
                # Ta bort sista '(' från föregående text
                last_paren_index = prev_text.rfind('(')
                if last_paren_index != -1:
                    new_prev = prev_text[:last_paren_index] + prev_text[last_paren_index+1:]
                    update_prev(new_prev)
                
                # Ta bort första ')' från tail
                first_paren_index = tail_text.find(')')
                if first_paren_index != -1:
                    a.tail = tail_text[:first_paren_index] + tail_text[first_paren_index+1:]
                
                should_remove = True

        if should_remove:
            parent = a.getparent()
            if parent is None: continue 
            
            tail_text = a.tail or ""
            prev = a.getprevious()
            
            # Slå ihop texten (tail) med föregående nod
            if prev is not None:
                prev.tail = (prev.tail or "") + tail_text
            else:
                parent.text = (parent.text or "") + tail_text
                
            parent.remove(a)
            removed_count += 1
            
    return removed_count

def process_file(filepath):
    try:
        parser = etree.XMLParser(encoding="utf-8", remove_comments=True, recover=True)
        tree = etree.parse(filepath, parser)
        root = tree.getroot()
        
        # Kör rensning först för att undvika konflikter
        removed = clean_old_links(root)
        created = linkify_tree(root)
        
        if removed > 0 or created > 0:
            with open(filepath, 'wb') as f:
                tree.write(f, encoding='utf-8', method='xml', 
                           xml_declaration=False,
                           pretty_print=False)
            return created, removed
        return 0, 0
    except Exception as e:
        print(f"Fel vid {filepath}: {e}")
        return 0, 0

def find_file_recursive(base_dir, search_name):
    candidates = {search_name, search_name + ".xhtml", search_name + ".xml"}
    for root, dirs, files in os.walk(base_dir):
        for filename in files:
            if filename in candidates:
                return os.path.join(root, filename)
    return None

if __name__ == '__main__':
    
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        target_file = None
        if os.path.exists(arg):
            target_file = arg
        else:
            print(f"Letar efter '{arg}' i {PARSED_DIR}...")
            target_file = find_file_recursive(PARSED_DIR, arg)
        
        if target_file:
            print(f"Bearbetar: {target_file}")
            CURRENT_PROCESSING_FILE = os.path.basename(target_file)
            created, removed = process_file(target_file)
            print("-" * 40)
            print(f"Resultat: +{created} skapade, -{removed} borttagna")
        else:
            print(f"Kunde inte hitta filen '{arg}'.")

    else:
        if not os.path.exists(PARSED_DIR):
            print(f"Målmappen {PARSED_DIR} saknas.")
        else:
            print(f"Startar massbearbetning av mapp: {PARSED_DIR}")
            total_created = 0
            total_removed = 0
            files_modified = 0
            
            with open(LOG_FILE, 'w', encoding='utf-8') as log:
                log.write("FIL;SKAPADE;BORTTAGNA\n")
                
                for root, dirs, files in os.walk(PARSED_DIR):
                    for filename in files:
                        if filename.endswith(".xhtml") or filename.endswith(".xml"):
                            filepath = os.path.join(root, filename)
                            CURRENT_PROCESSING_FILE = filename
                            created, removed = process_file(filepath)
                            
                            if created > 0 or removed > 0:
                                print(f"  -> {filename}: +{created}, -{removed}")
                                log.write(f"{filepath};{created};{removed}\n")
                                total_created += created
                                total_removed += removed
                                files_modified += 1

            print("-" * 40)
            print(f"Klar. Ändrade filer: {files_modified}")

    if INVALID_CELEX_HITS:
        print("\n" + "="*50)
        print(f"VARNING: Hittade {len(INVALID_CELEX_HITS)} misstänkta CELEX-nummer")
        print(f"Intervall: 31951 - 3{date.today().year + 1}")
        print("="*50)
        print(f"{'FILNAMN':<35} | {'CELEX'}")
        print("-" * 50)
        for fname, c_num in INVALID_CELEX_HITS:
            disp_name = (fname[:32] + '..') if len(fname) > 34 else fname
            print(f"{disp_name:<35} | {c_num}")