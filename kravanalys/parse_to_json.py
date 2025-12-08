import os
import re
import json
import argparse
import unicodedata
import csv
from lxml import etree

# Konfiguration
SOURCE_DIR = "data/eurlexacts/parsed"
DEST_DIR = "data/eurlexacts/json"
LOG_FILE = "validation_report.csv"

class Validator:
    def __init__(self):
        self.logs = []

    def get_text_length(self, data):
        """Rekursiv funktion för att räkna antal tecken i JSON-strukturen."""
        count = 0
        if isinstance(data, dict):
            for key, value in data.items():
                if key == "celex": continue # Ignorera ID
                count += self.get_text_length(value)
        elif isinstance(data, list):
            for item in data:
                count += self.get_text_length(item)
        elif isinstance(data, str):
            count += len(data)
        return count

    def check_order(self, items):
        """Kollar att ID:n kommer i någorlunda stigande ordning."""
        if not items: return True
        last_num = -1
        for item in items:
            item_id = item.get("id", "")
            match = re.match(r'^(\d+)', str(item_id))
            if match:
                current = int(match.group(1))
                if current < last_num:
                    return False 
                last_num = current
        return True

    def validate(self, extractor, data):
        flags = []
        
        # 1. Hämta textlängder (Normaliserat för att undvika falska larm pga whitespace)
        root = extractor.tree.getroot()
        original_len = 0
        
        if root is not None:
            full_text = "".join(root.itertext())
            # Normalisera på samma sätt som clean_text
            normalized_text = unicodedata.normalize("NFKC", full_text)
            normalized_text = " ".join(normalized_text.split())
            original_len = len(normalized_text)

        json_len = self.get_text_length(data)
        diff = original_len - json_len
        
        doc = data['document']
        fmt = doc['metadata']['original_format']
        is_consolidated = 'consolidated' in fmt

        # 2. Kolla data loss 
        # Tillåter lite mer svinn för konsoliderade pga ändrings-tabeller i början
        threshold = 3000 if is_consolidated else 1000
        if diff > threshold:
            flags.append(f"HIGH_DATA_LOSS_({diff}_chars)")

        # 3. Kolla tomma fält
        if not doc['metadata']['title']:
            flags.append("MISSING_TITLE")
        
        # Preamble check
        if not is_consolidated:
            if not doc['preamble']['intro_text'] and not doc['preamble']['recitals']:
                flags.append("EMPTY_PREAMBLE")
        
        # Body check
        if not doc['body']:
            flags.append("EMPTY_BODY")
        
        # Final provisions check
        if not is_consolidated:
            if not doc['final_provisions'].get('text') and not doc['final_provisions'].get('signatures'):
                flags.append("MISSING_FINAL_PROVISIONS")

        # 4. Kolla ordning
        if not self.check_order(doc['preamble']['recitals']):
            flags.append("RECITALS_ORDER_ERR")
        if not self.check_order(doc['body']):
            flags.append("ARTICLES_ORDER_ERR")

        status = "FAIL" if flags else "OK"
        
        return {
            "celex": extractor.celex_id,
            "parser": fmt,
            "status": status,
            "original_len": original_len,
            "json_len": json_len,
            "diff": diff,
            "flags": "; ".join(flags)
        }

class LegalActParser:
    def __init__(self, filepath):
        self.filepath = filepath
        self.filename = os.path.basename(filepath)
        self.celex_id = os.path.splitext(self.filename)[0]
        
        parser = etree.HTMLParser(recover=True, remove_comments=True)
        self.tree = etree.parse(filepath, parser)
        
        self.data = {
            "document": {
                "metadata": {
                    "celex": self.celex_id,
                    "title": "",
                    "date_published": "",
                    "language": "SV",
                    "original_format": ""
                },
                "preamble": {
                    "intro_text": "",
                    "recitals": []
                },
                "body": [],
                "annexes": [],
                "final_provisions": {}
            }
        }

    def clean_text(self, element):
        if element is None:
            return ""
        
        text_parts = []
        for text in element.itertext():
            if text:
                text_parts.append(text)
        
        text = " ".join(text_parts)
        text = unicodedata.normalize("NFKC", text)
        text = text.replace('\u2013', '-')
        text = text.replace('\u00a0', ' ')
        return " ".join(text.split())

    def extract_references(self, element):
        refs = []
        if not hasattr(element, 'xpath'): return refs
        links = element.xpath(".//a[contains(@class, 'celex-ref')]")
        for link in links:
            href = link.get('href', '')
            if 'eurlexacts' in href:
                try:
                    parts = href.split('/')[-1].split('#')
                    celex = parts[0]
                    article = parts[1].replace('A', '') if len(parts) > 1 else None
                    refs.append({"celex": celex, "article": article})
                except: continue
        return refs

    # ---------------------------------------------------------
    # 1. MODERN ELI PARSER (Nested DIVs with IDs)
    # ---------------------------------------------------------
    def parse_modern_eli(self):
        is_consolidated = len(self.tree.xpath("//*[contains(@class, 'disclaimer')]")) > 0
        self.data['document']['metadata']['original_format'] = 'modern_eli_consolidated' if is_consolidated else 'modern_eli'
        
        titles = self.tree.xpath("//*[contains(@class, 'oj-doc-ti') or contains(@class, 'title-doc-first')]")
        if titles:
            self.data['document']['metadata']['title'] = " ".join([self.clean_text(t) for t in titles])

        pbl_divs = self.tree.xpath("//div[starts-with(@id, 'pbl_')]")
        if pbl_divs:
            pbl_node = pbl_divs[0]
            intro_paras = pbl_node.xpath(".//p[contains(@class, 'oj-normal') and not(ancestor::*[starts-with(@id, 'rct_')])]")
            intro_lines = [self.clean_text(p) for p in intro_paras]
            self.data['document']['preamble']['intro_text'] = " ".join(intro_lines)

        recitals = self.tree.xpath("//*[starts-with(@id, 'rct_')]")
        for rct in recitals:
            number_node = rct.xpath(".//td[1]")
            number = self.clean_text(number_node[0]).strip("() .") if number_node else ""
            text_node = rct.xpath(".//td[2]")
            text = self.clean_text(text_node[0]) if text_node else self.clean_text(rct)
            self.data['document']['preamble']['recitals'].append({
                "id": number, "text": text, "references": self.extract_references(rct)
            })

        articles = self.tree.xpath("//div[starts-with(@id, 'art_') and contains(@class, 'eli-subdivision')]")
        for art in articles:
            art_id_node = art.xpath(".//*[contains(@class, 'oj-ti-art') or contains(@class, 'title-article-norm')]")
            art_id = ""
            if art_id_node:
                art_id = self.clean_text(art_id_node[0]).replace('Artikel ', '').strip().rstrip('.')
            
            art_title_node = art.xpath(".//*[contains(@class, 'oj-sti-art') or contains(@class, 'stitle-article-norm')]")
            art_title = self.clean_text(art_title_node[0]) if art_title_node else ""

            content_nodes = art.xpath(".//p[contains(@class, 'oj-normal') or contains(@class, 'norm')] | .//div[contains(@class, 'norm') and not(contains(@class, 'title'))]")
            content_paras = [self.clean_text(p) for p in content_nodes if self.clean_text(p)]

            self.data['document']['body'].append({
                "type": "article", "id": art_id, "title": art_title,
                "content": content_paras, "references": self.extract_references(art)
            })

        annexes = self.tree.xpath("//div[starts-with(@id, 'anx_')]")
        for anx in annexes:
            anx_id = anx.get('id').replace('anx_', '')
            title_node = anx.xpath(".//*[contains(@class, 'oj-doc-ti') or contains(@class, 'title-annex-1')]")
            title = self.clean_text(title_node[0]) if title_node else ""
            content_paras = []
            for elem in anx.xpath(".//p[contains(@class, 'oj-normal') or contains(@class, 'norm')] | .//tr"):
                text = self.clean_text(elem)
                if text: content_paras.append(text)
            self.data['document']['annexes'].append({
                "id": anx_id, "title": title, "content": content_paras, "references": self.extract_references(anx)
            })

        fnp = self.tree.xpath("//div[starts-with(@id, 'fnp_')]")
        if fnp:
            fnp_node = fnp[0]
            final_text = [self.clean_text(p) for p in fnp_node.xpath(".//p[contains(@class, 'oj-normal') or contains(@class, 'norm')]")]
            self.data['document']['final_provisions']['text'] = " ".join(final_text)
            signatory_divs = fnp_node.xpath(".//*[contains(@class, 'oj-signatory') or contains(@class, 'signatory')]")
            collected_sigs = [self.clean_text(s) for s in signatory_divs if self.clean_text(s)]
            if collected_sigs: self.data['document']['final_provisions']['signatures'] = collected_sigs

    # ---------------------------------------------------------
    # 2. MODERN FLAT PARSER (Consolidated text, no ELI divs)
    # ---------------------------------------------------------
    def parse_modern_flat(self):
        self.data['document']['metadata']['original_format'] = 'modern_flat_consolidated'
        
        titles = self.tree.xpath("//*[contains(@class, 'title-doc-first')]")
        if titles:
            self.data['document']['metadata']['title'] = " ".join([self.clean_text(t) for t in titles])

        preamble_div = self.tree.xpath("//div[@class='preamble']")
        if preamble_div:
            node = preamble_div[0]
            intro_parts = []
            in_recitals = False
            recital_count = 1
            
            split_pattern = re.compile(r'med beaktande av följande', re.IGNORECASE)
            body_trigger = re.compile(r'(HÄRIGENOM FÖRESKRIVS|HÄRMED FÖRESKRIVS|HÄRIGENOM FÖRESKRIVS FÖLJANDE)', re.IGNORECASE)

            if node.text and split_pattern.search(node.text):
                intro_parts.append(node.text.strip())
                in_recitals = True
            
            for p in node.xpath(".//p"):
                p_text = self.clean_text(p)
                if not p_text: continue

                if body_trigger.search(p_text):
                    break 

                if split_pattern.search(p_text):
                    intro_parts.append(p_text)
                    in_recitals = True
                    continue 

                if in_recitals:
                    self.data['document']['preamble']['recitals'].append({
                        "id": str(recital_count),
                        "text": p_text,
                        "references": self.extract_references(p)
                    })
                    recital_count += 1
                else:
                    intro_parts.append(p_text)
                
                if p.tail and split_pattern.search(p.tail):
                    intro_parts.append(p.tail.strip())
                    in_recitals = True

            self.data['document']['preamble']['intro_text'] = " ".join(intro_parts)

        start_nodes = self.tree.xpath("//*[contains(@class, 'title-article-norm') or contains(@class, 'title-annex-1')]")
        
        if not start_nodes: return

        current_node = start_nodes[0]
        
        while current_node is not None:
            classes = current_node.get('class', '')
            text = self.clean_text(current_node)
            
            if 'title-article-norm' in classes:
                art_id = text.replace('Artikel ', '').strip().rstrip('.')
                art_title = ""
                content = []
                refs = []
                
                next_elem = current_node.getnext()
                if next_elem is not None and 'stitle-article-norm' in next_elem.get('class', ''):
                    art_title = self.clean_text(next_elem)
                    current_node = next_elem 
                
                scanner = current_node.getnext()
                while scanner is not None:
                    scan_class = scanner.get('class', '')
                    if 'title-article-norm' in scan_class or 'title-annex-1' in scan_class:
                        break
                    
                    if 'modref' in scan_class or 'arrow' in scan_class:
                        scanner = scanner.getnext()
                        continue

                    chunk = self.clean_text(scanner)
                    if chunk: content.append(chunk)
                    refs.extend(self.extract_references(scanner))
                    
                    scanner = scanner.getnext()
                
                self.data['document']['body'].append({
                    "type": "article", "id": art_id, "title": art_title,
                    "content": content, "references": refs
                })
                current_node = scanner
                continue

            elif 'title-annex-1' in classes:
                anx_id = text.replace('BILAGA ', '').strip()
                anx_title = ""
                content = []
                refs = []
                
                scanner = current_node.getnext()
                while scanner is not None:
                    scan_class = scanner.get('class', '')
                    if 'title-article-norm' in scan_class or 'title-annex-1' in scan_class:
                        break
                    
                    if 'title-annex-2' in scan_class:
                         if not anx_title: anx_title = self.clean_text(scanner)
                         else: content.append(self.clean_text(scanner))
                    else:
                        chunk = self.clean_text(scanner)
                        if chunk: content.append(chunk)
                        refs.extend(self.extract_references(scanner))
                        
                    scanner = scanner.getnext()
                
                self.data['document']['annexes'].append({
                    "id": anx_id, "title": anx_title, "content": content, "references": refs
                })
                current_node = scanner
                continue
            
            else:
                current_node = current_node.getnext()

    # ---------------------------------------------------------
    # 3. TRANSITIONAL PARSER
    # ---------------------------------------------------------
    def parse_transitional(self):
        self.data['document']['metadata']['original_format'] = 'transitional'
        
        first_article = self.tree.xpath("//*[contains(concat(' ', normalize-space(@class), ' '), ' ti-art ')]")
        first_article_node = first_article[0] if first_article else None
        
        titles = self.tree.xpath("//*[contains(@class, 'doc-ti')]")
        if titles:
            doc_titles = [t for t in titles if "BILAGA" not in self.clean_text(t).upper()]
            self.data['document']['metadata']['title'] = " ".join([self.clean_text(t) for t in doc_titles])

        intro_paras = []
        if first_article_node is not None:
            preceding = first_article_node.xpath("./preceding-sibling::*")
            for elem in preceding:
                if 'doc-ti' in elem.get('class', ''): continue
                if elem.tag == 'table':
                    row = elem.find('.//tr')
                    cells = row.findall('.//td') if row is not None else []
                    if len(cells) >= 2:
                        raw_num = self.clean_text(cells[0])
                        text = self.clean_text(cells[1])
                        if '.' in raw_num or len(raw_num) > 5: continue
                        number = raw_num.strip("() .")
                        self.data['document']['preamble']['recitals'].append({
                            "id": number, "text": text, "references": self.extract_references(cells[1])
                        })
                    else: intro_paras.append(self.clean_text(elem))
                elif elem.tag == 'p' and 'normal' in elem.get('class', ''):
                    text = self.clean_text(elem)
                    if text: intro_paras.append(text)
            self.data['document']['preamble']['intro_text'] = " ".join(intro_paras)

        art_headers = self.tree.xpath("//*[contains(concat(' ', normalize-space(@class), ' '), ' ti-art ')]")
        for header in art_headers:
            header_text = self.clean_text(header)
            art_id = header_text.replace('Artikel ', '').strip().rstrip('.')
            art_title = None
            content_text = []
            refs = []
            current = header.getnext()
            while current is not None:
                current_class = current.get('class', '')
                is_next_article = 'ti-art' in current_class and 'sti-art' not in current_class
                is_final = current.tag == 'div' and 'final' in current_class
                is_annex = 'doc-ti' in current_class and "BILAGA" in self.clean_text(current).upper()
                
                if is_next_article or is_final or is_annex: break
                
                text = self.clean_text(current)
                if 'sti-art' in current_class:
                    art_title = text
                elif text:
                    content_text.append(text)
                    refs.extend(self.extract_references(current))
                current = current.getnext()

            self.data['document']['body'].append({
                "type": "article", "id": art_id, "title": art_title,
                "content": content_text, "references": refs
            })

        final_div = self.tree.xpath("//div[contains(@class, 'final')]")
        if final_div:
            fnp_node = final_div[0]
            final_text_elems = fnp_node.xpath("./*[not(contains(@class, 'signatory'))]")
            final_text = [self.clean_text(e) for e in final_text_elems]
            self.data['document']['final_provisions']['text'] = " ".join(final_text)
            sig_divs = fnp_node.xpath(".//*[contains(@class, 'signatory')]")
            collected_sigs = [self.clean_text(s) for s in sig_divs if self.clean_text(s)]
            if collected_sigs: self.data['document']['final_provisions']['signatures'] = collected_sigs

        annex_headers = []
        for title in self.tree.xpath("//*[contains(@class, 'doc-ti')]"):
            if "BILAGA" in self.clean_text(title).upper(): annex_headers.append(title)
        
        for header in annex_headers:
            header_text = self.clean_text(header)
            parts = header_text.split(maxsplit=1)
            anx_id = parts[1] if len(parts) > 1 else header_text
            anx_title = ""
            content_paras = []
            refs = []
            current = header.getnext()
            while current is not None:
                current_class = current.get('class', '')
                if 'doc-ti' in current_class and "BILAGA" in self.clean_text(current).upper(): break
                if 'doc-ti' in current_class: anx_title = self.clean_text(current)
                else:
                    text = self.clean_text(current)
                    if text: content_paras.append(text)
                    refs.extend(self.extract_references(current))
                current = current.getnext()

            self.data['document']['annexes'].append({
                "id": anx_id, "title": anx_title, "content": content_paras, "references": refs
            })

    # ---------------------------------------------------------
    # 4. LEGACY PARSER (STATE MACHINE)
    # ---------------------------------------------------------
    def parse_legacy(self):
        self.data['document']['metadata']['original_format'] = 'legacy'
        
        desc_meta = self.tree.xpath("//*[local-name()='meta' and @name='DC.description']")
        if desc_meta: self.data['document']['metadata']['title'] = desc_meta[0].get('content')
        else:
            title_meta = self.tree.xpath("//*[local-name()='meta' and @name='DC.title']")
            if title_meta: self.data['document']['metadata']['title'] = title_meta[0].get('content')
            
        date_meta = self.tree.xpath("//*[local-name()='meta' and @name='DC.date.published']")
        if date_meta: self.data['document']['metadata']['date_published'] = date_meta[0].get('content')

        container = self.tree.xpath("//*[@id='TexteOnly']//txt_te | //txt_te")
        if not container: container = self.tree.xpath("//*[@id='TexteOnly']")
        if not container: return 

        root_elem = container[0]
        
        REGEX_BODY_TRIGGER = re.compile(r'(HÄRIGENOM FÖRESKRIVS|HÄRMED FÖRESKRIVS|HÄRIGENOM FÖRESKRIVS FÖLJANDE)', re.IGNORECASE)
        REGEX_ART_START = re.compile(r'^Artikel\s+(\d+[a-z]*)', re.IGNORECASE)
        REGEX_ANNEX_START = re.compile(r'^BILAGA(\s+([IVX0-9A-Z]+))?', re.IGNORECASE)
        REGEX_INTRO_START = re.compile(r'\s+HAR\s+(ANTAGIT|UTFÄRDAT|BESLUTAT|FASTSTÄLLT|MEDDELAT|FÖRESKRIVIT)', re.IGNORECASE)
        REGEX_RECITAL_START = re.compile(r'med beaktande av följande', re.IGNORECASE)
        REGEX_FINAL_START = re.compile(r'^\s*(Utfärdad|Ufärdat|Utffärdad|På\s+rådets|På\s+kommissionens)', re.IGNORECASE)

        state = "PREAMBLE_WAIT"
        current_article = None
        current_annex = None
        recital_counter = 1
        
        all_elements = root_elem.xpath(".//p | .//table | .//div")
        
        for elem in all_elements:
            has_block_children = len(elem.xpath(".//p | .//div | .//table")) > 0
            text_to_process = ""
            
            if not has_block_children: text_to_process = self.clean_text(elem)
            else:
                if elem.text and len(elem.text.strip()) > 1:
                    norm_text = unicodedata.normalize("NFKC", elem.text)
                    norm_text = norm_text.replace('\u2013', '-').replace('\u00a0', ' ')
                    text_to_process = " ".join(norm_text.split())
            
            if not text_to_process: continue
            text = text_to_process
            
            if REGEX_BODY_TRIGGER.search(text) and not state.startswith("BODY") and not state == "FINAL":
                state = "BODY_WAIT"
                continue
            
            art_match = REGEX_ART_START.match(text)
            if art_match and not state.startswith("BODY") and not state == "FINAL" and not state == "ANNEX":
                state = "BODY"
                current_article = {
                    "type": "article", "id": art_match.group(1), "title": None,
                    "content": [], "references": []
                }
                remainder = text[len(art_match.group(0)):].strip()
                if remainder:
                      current_article['content'].append(remainder)
                      current_article['references'].extend(self.extract_references(elem))
                continue

            annex_match = REGEX_ANNEX_START.match(text)
            if annex_match and len(text) < 50:
                if current_article:
                    self.data['document']['body'].append(current_article)
                    current_article = None
                state = "ANNEX"
                if current_annex: self.data['document']['annexes'].append(current_annex)
                raw_id = annex_match.group(2)
                anx_id = raw_id if raw_id else str(len(self.data['document']['annexes']) + 1)
                current_annex = {"id": anx_id, "title": "", "content": [], "references": []}
                remainder = text[len(annex_match.group(0)):].strip()
                if remainder: current_annex["title"] = remainder
                continue

            if (state == "BODY" or state == "BODY_WAIT") and REGEX_FINAL_START.match(text):
                if current_article:
                    self.data['document']['body'].append(current_article)
                    current_article = None
                state = "FINAL"
                self.data['document']['final_provisions']['text'] = text
                continue

            if state == "PREAMBLE_WAIT":
                if REGEX_INTRO_START.search(text):
                    state = "PREAMBLE_INTRO"
                    self.data['document']['preamble']['intro_text'] = text
                elif "med beaktande av" in text.lower():
                    state = "PREAMBLE_INTRO"
                    self.data['document']['preamble']['intro_text'] = text

            elif state == "PREAMBLE_INTRO":
                if REGEX_RECITAL_START.search(text):
                    state = "PREAMBLE_RECITALS"
                    current = self.data['document']['preamble']['intro_text']
                    self.data['document']['preamble']['intro_text'] = (current + " " + text).strip()
                    continue
                current = self.data['document']['preamble']['intro_text']
                self.data['document']['preamble']['intro_text'] = (current + " " + text).strip()

            elif state == "PREAMBLE_RECITALS":
                self.data['document']['preamble']['recitals'].append({
                    "id": str(recital_counter), "text": text, "references": self.extract_references(elem)
                })
                recital_counter += 1

            elif state == "BODY" or state == "BODY_WAIT":
                if art_match:
                    state = "BODY"
                    if current_article: self.data['document']['body'].append(current_article)
                    current_article = {
                        "type": "article", "id": art_match.group(1), "title": None,
                        "content": [], "references": []
                    }
                    remainder = text[len(art_match.group(0)):].strip()
                    if remainder:
                         current_article['content'].append(remainder)
                         current_article['references'].extend(self.extract_references(elem))
                elif state == "BODY" and current_article:
                    current_article['content'].append(text)
                    current_article['references'].extend(self.extract_references(elem))

            elif state == "FINAL":
                if len(text) < 60:
                    sigs = self.data['document']['final_provisions'].get('signatures', [])
                    sigs.append(text)
                    self.data['document']['final_provisions']['signatures'] = sigs
                else:
                    prev = self.data['document']['final_provisions'].get('text', "")
                    self.data['document']['final_provisions']['text'] = (prev + " " + text).strip()

            elif state == "ANNEX":
                if current_annex:
                    if not current_annex["title"] and not current_annex["content"]:
                        current_annex["title"] = text
                    else:
                        current_annex["content"].append(text)
                        current_annex["references"].extend(self.extract_references(elem))

        if current_article: self.data['document']['body'].append(current_article)
        if current_annex: self.data['document']['annexes'].append(current_annex)
    
    # ---------------------------------------------------------
    # 5. CONSOLIDATED INLINE PARSER (Old consolidated, inline CSS)
    # ---------------------------------------------------------
    def parse_consolidated_inline(self):
        self.data['document']['metadata']['original_format'] = 'consolidated_inline'
        
        # 1. Titel
        titles = self.tree.xpath("//table//p[contains(@style, 'font-weight: bold') and not(contains(., '▼'))]")
        if titles:
            valid_titles = [self.clean_text(t) for t in titles if len(self.clean_text(t)) > 5]
            self.data['document']['metadata']['title'] = " ".join(valid_titles)

        # 2. Preamble
        preamble_div = self.tree.xpath("//div[contains(@style, '#CCCCCC')]")
        if preamble_div:
            node = preamble_div[0]
            intro_parts = []
            in_recitals = False
            recital_count = 1
            
            split_pattern = re.compile(r'med beaktande av följande', re.IGNORECASE)
            body_trigger = re.compile(r'(HÄRIGENOM FÖRESKRIVS|HÄRMED FÖRESKRIVS|HÄRIGENOM FÖRESKRIVS FÖLJANDE)', re.IGNORECASE)

            if node.text and split_pattern.search(node.text):
                intro_parts.append(node.text.strip())
                in_recitals = True

            for p in node.xpath(".//p"):
                p_text = self.clean_text(p)
                if not p_text: continue

                if body_trigger.search(p_text):
                    break 

                if split_pattern.search(p_text):
                    intro_parts.append(p_text)
                    in_recitals = True
                    continue 

                if in_recitals:
                    self.data['document']['preamble']['recitals'].append({
                        "id": str(recital_count),
                        "text": p_text,
                        "references": self.extract_references(p)
                    })
                    recital_count += 1
                else:
                    intro_parts.append(p_text)
                
                if p.tail and split_pattern.search(p.tail):
                    intro_parts.append(p.tail.strip())
                    in_recitals = True

            self.data['document']['preamble']['intro_text'] = " ".join(intro_parts)

        # 3. Artiklar och Bilagor
        all_paras = self.tree.xpath("//body//p")
        
        current_article = None
        current_annex = None
        
        REGEX_ART_HEADER = re.compile(r'^Artikel\s+(\d+[a-z]*)', re.IGNORECASE)
        REGEX_ANNEX_HEADER = re.compile(r'^BILAGA(\s+([IVX0-9A-Z]+))?', re.IGNORECASE)

        in_body = False
        
        for p in all_paras:
            if p.xpath("ancestor::div[contains(@style, '#CCCCCC')]"):
                continue
            
            style = p.get('style', '').lower()
            text = self.clean_text(p)
            if not text: continue
            
            is_italic = 'italic' in style
            art_match = REGEX_ART_HEADER.match(text)
            annex_match = REGEX_ANNEX_HEADER.match(text)

            if art_match and is_italic:
                in_body = True
                if current_article:
                    self.data['document']['body'].append(current_article)
                    current_article = None
                if current_annex:
                    self.data['document']['annexes'].append(current_annex)
                    current_annex = None
                
                art_id = art_match.group(1)
                current_article = {
                    "type": "article", "id": art_id, "title": "",
                    "content": [], "references": []
                }
                continue

            if annex_match and is_italic:
                in_body = True
                if current_article:
                    self.data['document']['body'].append(current_article)
                    current_article = None
                if current_annex:
                    self.data['document']['annexes'].append(current_annex)
                    current_annex = None
                
                raw_id = annex_match.group(2)
                anx_id = raw_id if raw_id else str(len(self.data['document']['annexes']) + 1)
                current_annex = {
                    "id": anx_id, "title": "", "content": [], "references": []
                }
                continue

            if in_body:
                is_bold = 'bold' in style
                
                if current_article:
                    if is_bold and not current_article['title'] and not current_article['content']:
                         current_article['title'] = text
                    else:
                        current_article['content'].append(text)
                        current_article['references'].extend(self.extract_references(p))
                
                elif current_annex:
                    if is_bold and not current_annex['title'] and not current_annex['content']:
                         current_annex['title'] = text
                    else:
                        current_annex['content'].append(text)
                        current_annex['references'].extend(self.extract_references(p))

        if current_article: self.data['document']['body'].append(current_article)
        if current_annex: self.data['document']['annexes'].append(current_annex)


    def run(self):
        has_eli = len(self.tree.xpath("//*[contains(@class, 'eli-container')]")) > 0
        has_title_article_norm = len(self.tree.xpath("//*[contains(@class, 'title-article-norm')]")) > 0
        has_ti_art = len(self.tree.xpath("//*[contains(@class, 'ti-art')]")) > 0
        has_grey_preamble = len(self.tree.xpath("//div[contains(@style, '#CCCCCC')]")) > 0
        
        if has_eli:
            self.parse_modern_eli()
        elif has_title_article_norm:
            self.parse_modern_flat() 
        elif has_grey_preamble:
            self.parse_consolidated_inline()
        elif has_ti_art:
            self.parse_transitional()
        else:
            self.parse_legacy()
            
        return self.data

def save_json(data, celex_id):
    if not os.path.exists(DEST_DIR):
        os.makedirs(DEST_DIR)
    dest_path = os.path.join(DEST_DIR, f"{celex_id}.json")
    with open(dest_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return dest_path

def main():
    parser = argparse.ArgumentParser(description="Konvertera EU-rättsakter till JSON.")
    parser.add_argument("--file", help="Sökväg till en enskild fil att testa", type=str)
    args = parser.parse_args()
    
    validator = Validator()
    results = []

    if args.file:
        if not os.path.exists(args.file):
            print(f"Filen {args.file} hittades inte.")
            return
        print(f"Bearbetar enskild fil: {args.file}")
        try:
            extractor = LegalActParser(args.file)
            data = extractor.run()
            
            val_res = validator.validate(extractor, data)
            print(f"Valideringsstatus: {val_res['status']}")
            if val_res['flags']:
                print(f"Varningar: {val_res['flags']}")
                
            saved_path = save_json(data, extractor.celex_id)
            print(f"✅ Resultat sparat till: {saved_path}")
        except Exception as e:
            print(f"❌ Fel vid bearbetning: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"Startar batch-bearbetning från {SOURCE_DIR}...")
        count = 0
        for root, dirs, files in os.walk(SOURCE_DIR):
            for filename in files:
                if filename.endswith((".xhtml", ".html", ".xml")):
                    filepath = os.path.join(root, filename)
                    try:
                        extractor = LegalActParser(filepath)
                        data = extractor.run()
                        
                        val_res = validator.validate(extractor, data)
                        results.append(val_res)
                        
                        save_json(data, extractor.celex_id)
                        count += 1
                        if count % 100 == 0:
                            print(f"Bearbetat {count} filer...")
                    except Exception as e:
                        print(f"Fel med {filename}: {e}")
                        results.append({
                            "celex": filename, 
                            "status": "CRASH", 
                            "flags": str(e)
                        })

        if results:
            keys = results[0].keys()
            with open(LOG_FILE, 'w', newline='', encoding='utf-8-sig') as f:
                dict_writer = csv.DictWriter(f, keys)
                dict_writer.writeheader()
                dict_writer.writerows(results)
            
            crashes = [r for r in results if r['status'] == 'CRASH']
            failures = [r for r in results if r['status'] == 'FAIL']
            
            print("\n" + "="*50)
            print(f"SAMMANFATTNING AV KÖRNING ({count} filer)")
            print("="*50)
            print(f"✅ Lyckade:   {count - len(crashes) - len(failures)}")
            print(f"⚠️  Varningar: {len(failures)}")
            print(f"❌ Kraschar:  {len(crashes)}")
            print("-" * 50)
            
            if crashes:
                print("\n❌ FILER SOM KRASCHADE:")
                for c in crashes:
                    print(f"  {c['celex']}: {c.get('flags', 'Okänt fel')}")
            
            if failures:
                print("\n⚠️  FILER MED VALIDERINGSVARNINGAR:")
                for f in failures:
                     print(f"  {f['celex']}: {f['flags']}")
            
            print(f"\nValideringsrapport sparad till: {LOG_FILE}")

if __name__ == "__main__":
    main()