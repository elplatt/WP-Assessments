# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: crawler
# Date: 11/10/16

import codecs
from collections import OrderedDict
import datetime
from dateutil.parser import parse
import logging
from multiprocessing import Pool, Process, Queue
import os
import os.path
from Queue import Empty
import re
import shutil
import sys
import subprocess
import time
import traceback

from bs4 import BeautifulSoup
import urllib

# Config
num_workers = 25
project_tsv = "data/projects-2016-10-12.utf-16-le.tsv"
output_dir = "output/projects/%s"
project_log = "output/projects/%s/project.log"
to_crawl = "output/to_crawl/%s"
to_parse = "output/to_parse/%s"
done_parse = "output/done_parse/%s"
cache_dir = "output/projects/%s/cache"
cache_tar = "output/projects_crawled/%s-cache.tgz"
base_url = "https://en.wikipedia.org/"
assessment_history_url = (
    "https://en.wikipedia.org/w/index.php?title=Wikipedia:Version_1.0_Editorial_Team/%s_articles_by_quality_log&offset=%s&limit=500&action=history"
)

# Load project names, ignore duplicates
project_names = []
unique_names = set()
with open(project_tsv, "rb") as f:
    lines = enumerate(f.read().decode('utf-16-le').split(u"\n"))
    lines.next()
    for i, line in lines:
        if len(line) == 0:
            continue
        name, unique = line.split(u"\t")
        if unique not in unique_names:
            unique_names.add(unique)
            project_names.append(name)

report_to_crawl = set()
report_to_parse = set()

for project_name in sorted(project_names):
    clean_name = project_name.replace('/', '_')
    # Try to open file in to_crawl
    try:
        os.stat(to_crawl % clean_name)
        report_to_crawl.add(project_name)
        continue
    except OSError:
        # File has been crawled
        pass
    # Try to open file in done_parse
    try:
        os.stat(done_parse % clean_name)
    except OSError:
        # File hasn't been parsed yet
        report_to_parse.add(project_name)

print "To Crawl (%d)" % len(report_to_crawl)
for p in sorted(list(report_to_crawl)):
    print "  %s" % p
print "To Parse (%d)" % len(report_to_parse)
for p in sorted(list(report_to_parse)):
    print "  %s" % p