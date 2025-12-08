# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from .myndfskr import MyndFskrBase
import os
import shutil
import re

class Jordbruksverket(MyndFskrBase):
    alias = "sjvfs"
    start_url = "http://localhost/dummy"
    
    # Vi anger inga speciella suffix, så den faller tillbaka på .pdf som är default i MyndFskrBase

    def download(self, basefile=None):
        if basefile:
            return self.download_single(basefile)
        
        self.log.info("Startar manuell import av PDF från staging-mappen")
        for basefile, params in self.download_get_basefiles([]):
            self.download_single(basefile, params)

    def download_get_basefiles(self, source):
        staging_dirs = [
            os.path.abspath("staging/sjvfs"),
            os.path.abspath("../staging/sjvfs")
        ]
        
        found_dir = None
        for d in staging_dirs:
            if os.path.exists(d):
                found_dir = d
                break
        
        if not found_dir:
            self.log.warning("Staging directory saknas.")
            return

        self.log.info("Letar efter PDF-filer i %s" % found_dir)

        for filename in os.listdir(found_dir):
            if filename.lower().endswith(".pdf"):
                # T.ex. SJVFS_2022_27.pdf -> sjvfs/2022:27
                m = re.search(r"(\d{4})[_\-:](\d+)", filename)
                if m:
                    basefile = "sjvfs/%s:%s" % (m.group(1), m.group(2))
                    self.log.info("Hittade PDF för %s: %s" % (basefile, filename))
                    yield basefile, {"local_path": os.path.join(found_dir, filename)}

    def download_single(self, basefile, params=None):
        if params and "local_path" in params:
            dest = self.store.downloaded_path(basefile)
            import ferenda.util
            ferenda.util.ensure_dir(dest)
            shutil.copy(params["local_path"], dest)
            self.log.info("%s: Importerad PDF till %s" % (basefile, dest))
            return True
        return False
    
    # Vi tar bort parse() och parse_body() så att den använder MyndFskrBase intelligenta PDF-parser