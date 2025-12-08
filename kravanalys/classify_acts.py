import os
import argparse
from lxml import etree
from collections import Counter, defaultdict

# Konfiguration
SOURCE_DIR = "data/eurlexacts/parsed"

def get_file_type(tree):
    """
    Analyserar DOM-trädet och returnerar vilken parser-kategori filen tillhör.
    Returnerar en tuple: (Kategori, Detaljer)
    """
    
    # 1. MODERN ELI (Nestlad struktur)
    # Trigger: class="eli-container"
    if len(tree.xpath("//*[contains(@class, 'eli-container')]")) > 0:
        is_consolidated = len(tree.xpath("//*[contains(@class, 'disclaimer')]")) > 0
        if is_consolidated:
            return "MODERN_ELI_CONSOLIDATED", "Har eli-container + disclaimer"
        return "MODERN_ELI_OJ", "Har eli-container"

    # 2. MODERN FLAT (Platt struktur, konsoliderad)
    # Trigger: class="title-article-norm" (unik för dessa filer)
    if len(tree.xpath("//*[contains(@class, 'title-article-norm')]")) > 0:
        return "MODERN_FLAT_CONSOLIDATED", "Har title-article-norm"

    # 3. CONSOLIDATED INLINE (Äldre konsoliderade med inline styles)
    # Trigger: div med grå bakgrund (preamble) ELLER p med italic style (artiklar)
    # Vi kollar specifikt efter style="#CCCCCC" eller "font-style: italic" i kombination med "Artikel"
    if len(tree.xpath("//div[contains(@style, '#CCCCCC')]")) > 0:
        return "CONSOLIDATED_INLINE", "Har grå preamble-div"
    
    # 4. TRANSITIONAL (Övergångsformat)
    # Trigger: class="ti-art" (användes innan ELI men efter Legacy)
    if len(tree.xpath("//*[contains(@class, 'ti-art')]")) > 0:
        return "TRANSITIONAL", "Har ti-art"

    # 5. LEGACY (Det gamla formatet)
    # Trigger: id="TexteOnly" (standardbehållaren för gamla filer)
    # Vi kollar även efter <txt_te> för säkerhets skull
    if len(tree.xpath("//*[@id='TexteOnly']")) > 0 or len(tree.xpath("//txt_te")) > 0:
        return "LEGACY_CONFIRMED", "Har id='TexteOnly' eller <txt_te>"

    # 6. OKÄND / GAP
    # Om vi hamnar här har filen INGEN av våra kända triggers.
    # Detta är troligen en 'Legacy to Transitional'-variant eller en trasig fil.
    return "UNCATEGORIZED", "Saknar kända triggers"

def main():
    parser = argparse.ArgumentParser(description="Diagnostisera och klassificera EU-rättsakter.")
    parser.add_argument("--dir", default=SOURCE_DIR, help="Mapp att söka i")
    args = parser.parse_args()

    if not os.path.exists(args.dir):
        print(f"Mappen {args.dir} hittades inte.")
        return

    print(f"🔍 Startar analys av filer i {args.dir}...")
    
    stats = Counter()
    uncategorized_files = []
    
    # För html-parsern
    html_parser = etree.HTMLParser(recover=True, remove_comments=True)

    file_count = 0
    for root, dirs, files in os.walk(args.dir):
        for filename in files:
            if filename.endswith((".xhtml", ".html", ".xml")):
                filepath = os.path.join(root, filename)
                file_count += 1
                
                try:
                    tree = etree.parse(filepath, html_parser)
                    category, reason = get_file_type(tree)
                    
                    stats[category] += 1
                    
                    if category == "UNCATEGORIZED":
                        uncategorized_files.append((filename, reason))
                        
                except Exception as e:
                    stats["ERROR"] += 1
                    print(f"Fel vid läsning av {filename}: {e}")

                if file_count % 500 == 0:
                    print(f"   ...analyserat {file_count} filer")

    print("\n" + "="*40)
    print("RESULTAT AV KLASSIFICERING")
    print("="*40)
    
    # Sortera och skriv ut statistik
    for cat, count in stats.most_common():
        print(f"{cat:<25}: {count}")

    print("-" * 40)
    print(f"Totalt antal filer     : {file_count}")
    
    # Skriv ut varningslistan
    if uncategorized_files:
        print("\n" + "!"*40)
        print(f"VARNING: {len(uncategorized_files)} filer kunde inte identifieras!")
        print("Dessa riskerar att bli tomma eller felaktiga i JSON-konverteringen.")
        print("!"*40)
        print(f"{'FILNAMN':<40} | ORSAK")
        print("-" * 60)
        
        # Visa de första 50 för att inte dränka terminalen om det är många
        for fname, reason in uncategorized_files[:50]:
            print(f"{fname:<40} | {reason}")
            
        if len(uncategorized_files) > 50:
            print(f"... och {len(uncategorized_files) - 50} till.")
            
        print("\nREKOMMENDATION: Undersök xhtml-koden för ovanstående filer.")
    else:
        print("\n✅ Alla filer föll inom en känd kategori!")
        print("    (Inga 'UNCATEGORIZED' hittades)")

if __name__ == "__main__":
    main()