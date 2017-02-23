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
import os
import sys
from subprocess import call
import traceback

from bs4 import BeautifulSoup
import urllib

# Config
project_tsv = "data/projects-2016-10-12.utf-16-le.tsv"
project_log = "output/%s.log"
output = "output/"
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

def crawl(project):

    # Create log
    logging.basicConfig(filename=(project_log % project), level=logging.DEBUG, filemode='a')
    
    logging.info("Getting assessment revision urls")
    try:
        assessment_urls = []
        next_url = assessment_history_url % (project, "")
        while next_url is not None:
            # Request next page of history and parse
            logging.info("Requesting: %s" % next_url)
            handle = urllib.urlopen(next_url)
            if handle.getcode() != 200:
                logging.error("HTTP %d when fetching: %s" % (handle.getcode(), next_url))
                raise IOError
            gunk_bytes = handle.read()
            soup = BeautifulSoup(gunk_bytes, 'html.parser')
    
            # Add url for each revision
            revisions = soup.findAll("a", {"class": "mw-changeslist-date"})
            for rev in revisions:
                assessment_urls.append("%s%s" % (base_url, rev.get('href')))
            
            # Get next page url
            try:
                next_url = base_url + soup.find("a", {"class": "mw-nextlink"}).get('href')
            except AttributeError:
                next_url = None

    except:
        logging.error(traceback.format_exc())
        raise
    logging.info("Parsed %d assessment revision urls" % len(assessment_urls))

crawl("Malaysia")
