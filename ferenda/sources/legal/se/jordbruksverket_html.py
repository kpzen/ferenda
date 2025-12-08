# Exempel ./ferenda-build.py sjvfs_html download
# ./ferenda-build.py sjvfs_html parse sjvfs_html/2022:27 --force
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from .myndfskr import MyndFskrBase
from .sfs_parser import make_parser
from ferenda import TextReader
from ferenda.decorators import managedparsing
from ferenda.elements import Body
from .elements import Kapitel, Paragraf, Stycke, Rubrik, Listelement, Bilaga
from bs4 import BeautifulSoup
from rdflib import Literal, URIRef
import os
import shutil
import re
import datetime
import logging

class JordbruksverketHTML(MyndFskrBase):
    # Vi byter alias så att denna inte krockar med din PDF-version (sjvfs)
    alias = "sjvfs_html"
    start_url = "http://localhost/dummy"
    downloaded_suffix = ".html"

    # Definiera predikat för ID-generering
    ordinalpredicates = {
        Kapitel: "rpubl:kapitelnummer",
        Paragraf: "rpubl:paragrafnummer",
        Stycke: "rinfoex:styckenummer",
        Rubrik: "rinfoex:rubriknummer",
        Listelement: "rinfoex:punktnummer",
        Bilaga: "rinfoex:bilaganummer"
    }
    rootnode = Body

    def download(self, basefile=None):
        if basefile:
            return self.download_single(basefile)
        
        self.log.info("Startar manuell import av HTML från staging-mappen")
        for basefile, params in self.download_get_basefiles([]):
            self.download_single(basefile, params)

    def download_get_basefiles(self, source):
        # Vi letar i samma staging-mapp som förut
        staging_dirs = [os.path.abspath("staging/sjvfs"), os.path.abspath("../staging/sjvfs")]
        found_dir = None
        for d in staging_dirs:
            if os.path.exists(d):
                found_dir = d
                break
        
        if not found_dir:
            self.log.warning("Staging directory saknas.")
            return

        self.log.info("Letar efter HTML-filer i %s" % found_dir)
        for filename in os.listdir(found_dir):
            if filename.lower().endswith(".html") or filename.lower().endswith(".htm"):
                m = re.search(r"(\d{4})[_\-:](\d+)", filename)
                if m:
                    # Vi använder det nya aliaset i basefile: sjvfs_html/2022:27
                    basefile = "%s/%s:%s" % (self.alias, m.group(1), m.group(2))
                    self.log.info("Hittade HTML för %s: %s" % (basefile, filename))
                    yield basefile, {"local_path": os.path.join(found_dir, filename)}

    def download_single(self, basefile, params=None):
        if params and "local_path" in params:
            dest = self.store.downloaded_path(basefile)
            # Tvinga filändelse till .html
            if dest.endswith(".pdf"):
                dest = dest.replace(".pdf", ".html")
            
            import ferenda.util
            ferenda.util.ensure_dir(dest)
            shutil.copy(params["local_path"], dest)
            self.log.info("%s: Importerad HTML till %s" % (basefile, dest))
            return True
        return False

    @managedparsing
    def parse(self, doc):
        attribs = self.metadata_from_basefile(doc.basefile)
        resource = self.polish_metadata(attribs, doc.basefile)
        doc.meta = resource.graph
        doc.uri = str(resource.identifier)

        infile = self.store.downloaded_path(doc.basefile)
        if infile.endswith(".pdf"):
             infile = infile.replace(".pdf", ".html")
             
        with open(infile, "rb") as fp:
            soup = BeautifulSoup(fp, "lxml")
            for script in soup(["script", "style"]):
                script.extract()
            # Extrahera text med dubbla radbrytningar för att SFS-parsern ska hitta stycken
            text = soup.get_text(separator="\n\n")

        reader = TextReader(string=text, linesep=TextReader.UNIX)
        
        if not hasattr(self, 'trace'):
            self.trace = {}
            for logname in ('paragraf', 'tabell', 'numlist', 'rubrik'):
                 self.trace[logname] = logging.getLogger('dummy')

        # Använd SFS-logiken för att strukturera texten
        parser_func = make_parser(reader, doc.basefile, self.log, self.trace)
        doc.body = parser_func(reader)

        # Traversera och skapa ID:n (länkar)
        state = {'basefile': doc.basefile, 'uris': set(), 'parent': []}
        self.traverse(doc.body, self.construct_id, state)

        # Fyll i saknad metadata
        if not doc.meta.value(self.ns['dcterms'].title):
            title_tag = soup.find("title")
            title_text = title_tag.text.strip() if title_tag else "Föreskrift utan titel"
            doc.meta.add((URIRef(doc.uri), self.ns['dcterms'].title, Literal(title_text, lang="sv")))

        if not doc.meta.value(self.ns['dcterms'].identifier):
             # Skapa snyggt ID: SJVFS 2022:27 (utan _html suffixet i visningsnamnet)
             short_id = doc.basefile.replace(self.alias, "SJVFS").upper().replace("/", " ")
             doc.meta.add((URIRef(doc.uri), self.ns['dcterms'].identifier, Literal(short_id)))

        # Försök slå upp Jordbruksverket, fallback till hårdkodad URI
        try:
            org_uri = self.lookup_resource("Jordbruksverket")
        except KeyError:
            org_uri = URIRef("http://rinfo.lagrummet.se/org/statens_jordbruksverk")
        doc.meta.add((URIRef(doc.uri), self.ns['rpubl'].beslutadAv, org_uri))

        today = datetime.date.today()
        for pred in ['beslutsdatum', 'ikrafttradandedatum', 'utkomFranTryck']:
            if not doc.meta.value(self.ns['rpubl'][pred]):
                doc.meta.add((URIRef(doc.uri), self.ns['rpubl'][pred], Literal(today)))

        self.parse_entry_update(doc)
        return True

    def metadata_from_basefile(self, basefile):
        if "/" not in basefile:
            basefile = self.alias + "/" + basefile
        return super(JordbruksverketHTML, self).metadata_from_basefile(basefile)

    def traverse(self, node, func, state):
        child_state = func(node, state)
        if child_state and hasattr(node, '__iter__'):
            for child in node:
                if not isinstance(child, str):
                    self.traverse(child, func, child_state)

    # Kopierad construct_id för att vara oberoende
    def construct_id(self, node, state):
        state = dict(state)
        if isinstance(node, self.rootnode):
            attributes = self.metadata_from_basefile(state['basefile'])
            state.update(attributes)
        
        ordinalpredicate = self.ordinalpredicates.get(node.__class__)
        if ordinalpredicate:
            if hasattr(node, 'ordinal') and node.ordinal:
                ordinal = node.ordinal
            else:
                ordinal = 0
                if 'parent' in state:
                    for othernode in state['parent']:
                        if type(node) == type(othernode):
                            ordinal += 1
                        if node == othernode:
                            break
            
            if ordinalpredicate == "rinfoex:punktnummer":
                while ordinalpredicate in state:
                    ordinalpredicate = ("rinfoex:sub" + ordinalpredicate.split(":")[1])
            
            state[ordinalpredicate] = ordinal
            
            if 'parent' in state:
                del state['parent']
                
            res = self.attributes_to_resource(state)
            try:
                uri = self.minter.space.coin_uri(res)
            except Exception:
                uri = None
            
            if uri:
                if uri not in state['uris']:
                    node.uri = uri
                    state['uris'].add(uri)
                else:
                    return None 
                
                if "#" in uri:
                    node.id = uri.split("#", 1)[1]
        
        state['parent'] = node
        return state