# base class that abstracts acess to the EUR-Lex web services and the
# Cellar repository. Uses CELEX ids for basefiles, but stores them
# sharded per year
from lxml import etree
from io import BytesIO
import requests
import os
import re
from math import ceil
from html import escape
import email
import tempfile

import requests
from bs4 import BeautifulSoup
from rdflib import Graph, Namespace, URIRef, Literal, RDF
from rdflib.resource import Resource
from rdflib.namespace import OWL
from lxml.etree import XSLT

from ferenda import util, decorators, errors
from ferenda import DocumentRepository, DocumentStore, Describer
from . import CDM

class EURLexStore(DocumentStore):
    downloaded_suffixes = [".fmx4", ".xhtml", ".html", ".pdf"]
    def basefile_to_pathfrag(self, basefile):
        if basefile.startswith("."):
            return basefile
        # Shard all files under year, eg "32017R0642" => "2017/32017R0642"
        year = basefile[1:5]
        assert year.isdigit(), "%s doesn't look like a legit CELEX" % basefile
        return "%s/%s" % (year, basefile)

    def pathfrag_to_basefile(self, pathfrag):
        if pathfrag.startswith("."):
            return pathfrag
        year, basefile = pathfrag.split("/", 1)
        return basefile
    

# this implements some common request.Response properties/methods so
# that it can be used in plpace of a real request.Response object
class FakeResponse(object):

    def __init__(self, status_code, text, headers):
        self.status_code = status_code
        self.text = text
        self.headers = headers

    @property
    def content(self):
        default = "text/html; encoding=utf-8"
        encoding = self.headers.get("Content-type", default).split("encoding=")[1]
        return self.text.encode(encoding)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise ValueError(self.status_code)
        
        
    
class EURLex(DocumentRepository):
    alias = "eurlex"
    start_url = "http://eur-lex.europa.eu/eurlex-ws?wsdl"
    pagesize = 100 # 100 max allowed by the web service
    expertquery_template = "" # sub classes adjust this
    download_iterlinks = False
    lang = "sv"
    languages = ["swe", "eng"]
    documentstore_class = EURLexStore
    downloaded_suffix = ".xhtml"
    download_accept_406 = True
    contenttype = "application/xhtml+xml" 
    namespace = "{http://eur-lex.europa.eu/search}"
    download_archive = False
    namespaces = ['rdf', 'rdfs', 'xsd', 'dcterms', 'prov',
                  ('cdm', str(CDM))]
    sparql_annotations = None
    
    @classmethod
    def get_default_options(cls):
        opts = super(EURLex, cls).get_default_options()
        opts['languages'] = ['eng']
        opts['curl'] = True  # if True, the web service is called
                              # with command-line curl, not the
                              # requests module (avoids timeouts)
        return opts

    def dump_graph(self, celexid, graph):
        return # <--- LÄGG TILL DETTA FÖR ATT STOPPA SKRIVANDET TILL DISK
        with self.store.open_intermediate(celexid, "wb", suffix=".ttl") as fp:
             fp.write(graph.serialize(format="ttl"))

    def query_webservice(self, query, page):
        # this is the only soap template we'll need, so we include it
        # verbatim to avoid having a dependency on a soap module like
        # zeep.
        endpoint = 'https://eur-lex.europa.eu/EURLexWebService'
        envelope = """<soap-env:Envelope xmlns:soap-env="http://www.w3.org/2003/05/soap-envelope">
  <soap-env:Header>
    <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
      <wsse:UsernameToken>
        <wsse:Username>%s</wsse:Username>
        <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">%s</wsse:Password>
      </wsse:UsernameToken>
    </wsse:Security>
  </soap-env:Header>
  <soap-env:Body>
    <sear:searchRequest xmlns:sear="http://eur-lex.europa.eu/search">
      <sear:expertQuery>%s</sear:expertQuery>
      <sear:page>%s</sear:page>
      <sear:pageSize>%s</sear:pageSize>
      <sear:searchLanguage>%s</sear:searchLanguage>
    </sear:searchRequest>
  </soap-env:Body>
</soap-env:Envelope>
""" % (self.config.username, self.config.password, escape(query, quote=False), page, self.pagesize, self.lang)
        headers = {'Content-Type': 'application/soap+xml; charset=utf-8; action="https://eur-lex.europa.eu/EURLexWebService/doQuery"',
                   'SOAPAction': 'https://eur-lex.europa.eu/EURLexWebService/doQuery'}
        if self.config.curl:
            # dump the envelope to a tempfile
            headerstr = ""
            for k, v in headers.items():
                assert "'" not in v  # if it is, we need to work on escaping it
                headerstr += " --header '%s: %s'" % (k, v)
            with tempfile.NamedTemporaryFile() as fp:
                fp.write(envelope.encode("utf-8"))
                fp.flush()
                envelopename = fp.name
                headerfiledesc, headerfilename = tempfile.mkstemp()
                cmd = 'curl -L -X POST -D %(headerfilename)s --data-binary "@%(envelopename)s" %(headerstr)s %(endpoint)s' % locals()
                (ret, stdout, stderr) = util.runcmd(cmd)
            headerfp = os.fdopen(headerfiledesc)
            header = headerfp.read()
            headerfp.close()
            util.robust_remove(headerfilename)
            status, headers = header.split('\n', 1)
            prot, code, msg = status.split(" ", 2)
            headers = dict(email.message_from_string(headers).items())
            res = FakeResponse(int(code), stdout, headers)
        else:
            res = util.robust_fetch(self.session.post, endpoint, self.log,
                                    raise_for_status=False,
                                    data=envelope, headers=headers,
                                    timeout=10)
            
        if res.status_code == 500:
            tree = etree.parse(BytesIO(res.content))
            statuscode = tree.find(".//{http://www.w3.org/2003/05/soap-envelope}Subcode")[0].text
            statusmsg = tree.find(".//{http://www.w3.org/2003/05/soap-envelope}Text").text
            raise errors.DownloadError("%s: %s" % (statuscode, statusmsg))
        elif res.status_code == 301:
            # the call to robust_fetch or curl should have followed
            # the redirect, but at this point we'll just have to
            # report the error
            raise errors.DownloadError("%s: was redirected to %s" % (endpoint, res.headers['Location']))
        return res
        
    def construct_expertquery(self, query_template):
        if 'lastdownload' in self.config and not self.config.refresh:
            query_template += self.config.lastdownload.strftime(" AND DD >= %d/%m/%Y")
        query_template += " ORDER BY DD ASC"
        self.log.info("Query: %s" % query_template)
        return query_template
    
    def download_get_first_page(self):
        return self.query_webservice(self.construct_expertquery(self.expertquery_template), 1)

    def get_treenotice_graph(self, cellarurl, celexid):
        # avoid HTTP call if we already have the data
        if os.path.exists(self.store.intermediate_path(celexid, suffix=".ttl")):
            self.log.info("%s: Opening existing TTL file" % celexid)
            with self.store.open_intermediate(celexid, suffix=".ttl") as fp:
                return Graph().parse(data=fp.read(), format="ttl")
        # FIXME: read the rdf-xml data line by line and construct a
        # graph by regex-parsing interesting lines with a very simple
        # state machine, rather than doing a full parse, to speed
        # things up
        resp = util.robust_fetch(self.session.get, cellarurl, self.log, headers={"Accept": "application/rdf+xml;notice=tree"}, timeout=10)
        if not resp:
            return None
        with util.logtime(self.log.info,
                          "%(basefile)s: parsing the tree notice took %(elapsed).3f s",
                          {'basefile': celexid}):
            graph = Graph().parse(data=resp.content)
        return graph
    
    def find_manifestation(self, cellarid, celexid):
        # 1. Konfigurera språk
        target_languages = self.config.languages
        if isinstance(target_languages, str):
            target_languages = target_languages.split()

        cellarurl = "http://publications.europa.eu/resource/cellar/%s?language=%s" % (cellarid, target_languages[0])
        graph = self.get_treenotice_graph(cellarurl, celexid)
        if graph is None:
            return None, None, None, None
        
        CDM = Namespace("http://publications.europa.eu/ontology/cdm#")
        CMR = Namespace("http://publications.europa.eu/ontology/cdm/cmr#")
        
        # 2. Hitta rätt språkuttryck (Startpunkter)
        candidateexpressions = {}
        for s, o in graph.subject_objects(CDM.expression_uses_language):
            lang_code = str(o).rsplit("/", 1)[-1].lower()
            if lang_code in target_languages:
                candidateexpressions[lang_code] = s

        if not candidateexpressions:
            self.log.warning("%s: Found no suitable languages (checked %s)" % (celexid, target_languages))
            return None, None, None, None

        # 3. Leta filer
        for lang in target_languages:
            if lang in candidateexpressions:
                start_expression_uri = candidateexpressions[lang]
                
                # --- OMNI-SEARCH EXPRESSION LEVEL ---
                # Samla alla alias för UTTRYCKET (IMMC, OJ, etc)
                expression_aliases = set([start_expression_uri])
                # Vad pekar den på?
                for obj in graph.objects(start_expression_uri, OWL.sameAs): 
                    expression_aliases.add(obj)
                # Vad pekar PÅ den?
                for subj in graph.subjects(OWL.sameAs, start_expression_uri): 
                    expression_aliases.add(subj)
                
                self.log.info(f"DEBUG: Letar filer för språk '{lang}' via {len(expression_aliases)} st uttryck-alias")

                candidateitem = {}

                # Loopa över ALLA uttryckets alias för att hitta manifestationer
                manifestations = set()
                for expr_uri in expression_aliases:
                    for man in graph.objects(expr_uri, CDM.expression_manifested_by_manifestation):
                        manifestations.add(man)
                
                for man_uri in manifestations:
                    man_str = str(man_uri)
                    
                    # --- OMNI-SEARCH MANIFESTATION LEVEL ---
                    # Samla alias för MANIFESTATIONEN
                    man_aliases = set([man_uri])
                    for obj in graph.objects(man_uri, OWL.sameAs): man_aliases.add(obj)
                    for subj in graph.subjects(OWL.sameAs, man_uri): man_aliases.add(subj)
                    
                    found_items = []
                    for alias in man_aliases:
                        found_items.extend(list(graph.subjects(CDM.item_belongs_to_manifestation, alias)))

                    # Identifiera typ
                    mtype = None
                    for alias in man_aliases:
                        for t in graph.objects(alias, CDM.type):
                            mtype = str(t)
                            break
                        if mtype: break
                    
                    # Gissa typ från URL
                    if not mtype:
                        if ".xhtml" in man_str: mtype = "xhtml"
                        elif ".fmx4" in man_str: mtype = "fmx4"
                        elif ".pdf" in man_str: mtype = "pdf"
                        else: mtype = "unknown"

                    # Välj URL
                    selected_url = None
                    if found_items:
                        best_item = found_items[0]
                        if len(found_items) > 1:
                            for item in found_items:
                                try:
                                    sameas_url = str(item.value(OWL.sameAs).identifier)
                                    if sameas_url.endswith(".xml") and not sameas_url.endswith(".doc.xml"):
                                        best_item = item
                                        break
                                except:
                                    pass
                        selected_url = str(best_item)
                    else:
                        # Fallback: Använd manifestationen direkt
                        selected_url = man_str

                    if selected_url:
                        candidateitem[mtype] = selected_url

                # Prioritera format och returnera
                if candidateitem:
                    for t in ("xhtml", "fmx4", "html", "pdf", "pdfa1a", "unknown"):
                        if t in candidateitem:
                            url = candidateitem[t]
                            
                            mimetype = "application/octet-stream"
                            if t == "xhtml": mimetype = "application/xhtml+xml"
                            elif t == "fmx4": mimetype = "application/xml"
                            elif t == "pdf": mimetype = "application/pdf"
                            elif t == "html": mimetype = "text/html"
                            
                            self.log.info("%s: Has manifestation %s (%s) in language %s (URL: %s)" % (celexid, t, mimetype, lang, url))
                            self.dump_graph(celexid, graph) 
                            return lang, t, mimetype, url
        
        self.log.warning("%s: Failed to find manifestation for %s" % (celexid, target_languages))
        self.dump_graph(celexid, graph)
        return None, None, None, None

    
    def download_single(self, basefile, url=None):
        if url is None:
            result = self.query_webservice("DN = %s" % basefile, page=1)
            result.raise_for_status()
            tree = etree.parse(BytesIO(result.content))
            results = tree.findall(".//{http://eur-lex.europa.eu/search}result")
            assert len(results) == 1
            result = results[0]
            cellarid = result.find(".//{http://eur-lex.europa.eu/search}reference").text
            cellarid = re.split("[:_]", cellarid)[2]

            celex = result.find(".//{http://eur-lex.europa.eu/search}ID_CELEX")[0].text
            match = self.celexfilter(celex)
            assert match
            celex = match.group(1)
            assert celex == basefile
            
            lang, filetype, mimetype, url = self.find_manifestation(cellarid, celex)
            
            # BUGGFIX: Kontrollera om vi fick tillbaka något giltigt format
            if not filetype:
                self.log.error("%s: Download failed - no suitable filetype found" % basefile)
                return False 

            # FIXME: This is an ugly way of making sure the downloaded
            # file gets the right suffix
            downloaded_path = self.store.path(basefile, 'downloaded', '.'+filetype)
            if not os.path.exists(downloaded_path):
                util.writefile(downloaded_path, "")
        return super(EURLex, self).download_single(basefile, url)

    @decorators.downloadmax
    def download_get_basefiles(self, source):
        totalhits = None
        done = False
        page = 1
        processedhits = 0
        while not done:
            tree = etree.parse(BytesIO(source.encode("utf-8")))
            if totalhits is None:
                totalhits = int(tree.find(".//{http://eur-lex.europa.eu/search}totalhits").text)
                self.log.info("Total hits: %s" % totalhits)
            results = tree.findall(".//{http://eur-lex.europa.eu/search}result")
            self.log.info("Page %s: %s results" % (page, len(results)))
            for idx, result in enumerate(results):
                processedhits += 1
                cellarid = result.find(".//{http://eur-lex.europa.eu/search}reference").text
                cellarid = re.split("[:_]", cellarid)[2]
                celex = result.find(".//{http://eur-lex.europa.eu/search}ID_CELEX")[0].text
                try:
                    title = result.find(".//{http://eur-lex.europa.eu/search}EXPRESSION_TITLE")[0].text
                except TypeError:
                    self.log.info("%s: Lacks title, the resource might not be available in %s" % (celex, self.lang))
                match = self.celexfilter(celex)
                if not match:
                    self.log.info("%s: Not matching current filter, skipping" % celex)
                    continue
                celex = match.group(1)
                self.log.debug("%3s: %s %.55s %s" % (idx + 1, celex, title, cellarid))
                lang, filetype, mimetype, url = self.find_manifestation(cellarid, celex)
                if filetype:
                    # FIXME: This is an ugly way of making sure the downloaded
                    # file gets the right suffix (due to
                    # DocumentStore.downloaded_path choosing a filename from among
                    # several possible suffixes based on what file already exists
                    downloaded_path = self.store.path(celex, 'downloaded', '.'+filetype)
                    if not os.path.exists(downloaded_path):
                        util.writefile(downloaded_path, "")
                    yield celex, url
            page += 1
            done = processedhits >= totalhits
            if not done:
                self.log.info("Getting page %s (out of %s)" % (page, ceil(totalhits/self.pagesize)))
                result = self.query_webservice(self.construct_expertquery(self.expertquery_template), page)
                result.raise_for_status()
                source = result.text

    # since doc.body is a etree object, not a tree of CompoundElement
    # objects, the job for render_xhtml_doc is already done
    def render_xhtml_tree(self, doc):
        return doc.body

    def metadata_from_basefile(self, doc):
        desc = Describer(doc.meta, doc.uri)
        desc.rel(CDM.resource_legal_id_celex, Literal(doc.basefile))
        # the sixth letter in 
        rdftype = {"R": CDM.regulation,
                   "L": CDM.directive,
                   "C": CDM.decision_cjeu}[doc.basefile[5]]
        desc.rel(RDF.type, rdftype)
        return doc.meta
        
    
    @decorators.managedparsing
    def parse(self, doc):
        doc.meta = self.metadata_from_basefile(doc)
        source = self.store.downloaded_path(doc.basefile)
        
        if source.endswith(".fmx4"):
            doc.body = self.parse_formex(doc, source)
        # BUGGFIX: Vi lägger till .xhtml här så att den inte kraschar
        elif source.endswith(".html") or source.endswith(".xhtml"):
            doc.body = self.parse_html(doc, source)
        else:
            raise errors.ParseError("Can't yet parse %s" % source)
        self.parse_entry_update(doc)
        return True

    def parse_html(self, doc, source):
        with open(source, "rb") as fp:
            # Fall 1: Gammal HTML (ofta slarvig kod, kräver "förlåtande" parser)
            if source.endswith(".html"):
                parser = etree.HTMLParser(encoding="utf-8")
                return etree.parse(fp, parser)
            
            # Fall 2: XHTML (Ska vara strikt XML, men vi säkrar upp teckenkodningen)
            else:
                parser = etree.XMLParser(encoding="utf-8")
                return etree.parse(fp, parser)

    def parse_formex(self, doc, source):
        parser = etree.XMLParser(remove_blank_text=True)
        sourcetree = etree.parse(source, parser).getroot()
        fp = self.resourceloader.openfp("xsl/formex.xsl")
        xslttree = etree.parse(fp, parser)
        transformer = etree.XSLT(xslttree)
        params = etree.XSLT
        resulttree = transformer(sourcetree,
                                 about=XSLT.strparam(doc.uri),
                                 rdftype=XSLT.strparam(str(doc.meta.value(URIRef(doc.uri), RDF.type))))
        return resulttree
        # re-parse to fix whitespace
        buffer = BytesIO(etree.tostring(resulttree, encoding="utf-8"))
        return etree.parse(buffer, parser)

    def render_xhtml_validate(self, xhtmldoc):
        def checknode(node):
            if node.tag.split("}")[-1].isupper():
                raise errors.InvalidTree("Node %s has not been properly transformed from Formex to XHTML" % node.tag)
            for child in node:
                if type(child).__name__ == "_Element":
                    checknode(child)
        try:
            checknode(xhtmldoc.getroot())
        except errors.InvalidTree as e:
            return str(e)
        return super(EURLex, self).render_xhtml_validate(xhtmldoc)

        
    def tabs(self):
        return []

#    def _addheaders(self, url, filename=None):
#        headers = super(EURLex, self)._addheaders(filename)
#        headers["Accept"] = self.contenttype
#        key, lang = url.split("?")[1].split("=")
#        assert key == "language"
#        headers["Accept-Language"] = lang
#        return headers

