import re
import os
import atexit
from ferenda import util, errors, decorators
from lxml import etree
from . import EURLex, CDM

# kör med ./ferenda-build.py eurlexacts parse --all --force eller tex ./ferenda-build.py eurlexacts parse 32004L0025 --force

class EURLexActs(EURLex):
    alias = "eurlexacts"
    expertquery_template = "DTS_SUBDOM = EU_LAW_ALL AND (DTT = R OR DTT = L)"
    celexfilter = re.compile("(3\d{4}[RL]\d{4}(|\(\d+\)))$").match
    rdf_type = (CDM.directive, CDM.regulation)
    xslt_template = "xsl/eurlexacts.xsl"

    # --- STATISTIK-HANTERING ---
    # Vi sparar statistik i en klassvariabel så den lever över alla dokument
    stats = {
        'total': 0,
        'ok': 0,
        'fail': 0,
        'failed_list': []
    }

    @classmethod
    def print_report(cls):
        """Skriver ut slutrapport när scriptet är klart."""
        print("\n" + "="*40)
        print("       SLUTRAPPORT PARSNING")
        print("="*40)
        print(f"Antal bearbetade filer: {cls.stats['total']}")
        print(f"Lyckade (OK):           {cls.stats['ok']}")
        print(f"Misslyckade (FAIL):     {cls.stats['fail']}")
        
        if cls.stats['failed_list']:
            print("-" * 40)
            print("Lista över misslyckade filer:")
            for item in cls.stats['failed_list']:
                print(f" - {item}")
        print("="*40 + "\n")

    def __init__(self, config=None, **kwargs):
        super().__init__(config, **kwargs)
        # Registrera rapport-funktionen så den körs när programmet avslutas
        # Vi kollar en flagga så vi inte registrerar den flera gånger
        if not getattr(EURLexActs, '_atexit_registered', False):
            atexit.register(EURLexActs.print_report)
            EURLexActs._atexit_registered = True

    # --- PARSNING ---

    def parse_html(self, doc, source):
        # Vår robusta inläsare som vi vet fungerar
        with open(source, "rb") as fp:
            if source.endswith(".html"):
                # Legacy HTML: kasta bort kommentarer
                parser = etree.HTMLParser(encoding="utf-8", remove_comments=True, remove_pis=True)
                return etree.parse(fp, parser)
            else:
                # XHTML
                parser = etree.XMLParser(encoding="utf-8", remove_comments=True, remove_pis=True)
                return etree.parse(fp, parser)

    @decorators.managedparsing
    def parse(self, doc):
        EURLexActs.stats['total'] += 1
        
        try:
            doc.meta = self.metadata_from_basefile(doc)
            
            # --- PRIORITERING AV FILTYP ---
            # Istället för att bara ta första bästa, letar vi i vår prioritetsordning.
            # Prioritet: 1. XHTML, 2. HTML, 3. FMX4
            source = None
            
            # Kolla explicit efter filerna i ordning
            for suffix in ['.xhtml', '.html', '.fmx4']:
                path = self.store.path(doc.basefile, 'downloaded', suffix)
                if os.path.exists(path):
                    source = path
                    break
            
            # Fallback om vi inte hittade någon av dem (osannolikt om download lyckades)
            if not source:
                source = self.store.downloaded_path(doc.basefile)

            # --- INLÄSNING ---
            if source.endswith(".html") or source.endswith(".xhtml"):
                doc.body = self.parse_html(doc, source)
            elif source.endswith(".fmx4"):
                doc.body = self.parse_formex(doc, source)
            else:
                raise errors.ParseError("Can't yet parse %s" % source)
            
            # Spara
            self.parse_entry_update(doc)
            
            # Logga framgång
            EURLexActs.stats['ok'] += 1
            return True

        except Exception as e:
            # Logga misslyckande och kasta felet vidare så Ferenda ser det
            error_msg = f"{doc.basefile} ({type(e).__name__}: {str(e)})"
            EURLexActs.stats['fail'] += 1
            EURLexActs.stats['failed_list'].append(error_msg)
            raise e