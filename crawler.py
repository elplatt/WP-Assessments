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
from multiprocessing import Pool, Process
import os
import os.path
import re
import sys
from subprocess import call
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
cache_dir = "output/projects/%s/cache"
base_url = "https://en.wikipedia.org/"
assessment_history_url = (
    "https://en.wikipedia.org/w/index.php?title=Wikipedia:Version_1.0_Editorial_Team/%s_articles_by_quality_log&offset=%s&limit=500&action=history"
)
logging.basicConfig(level=logging.DEBUG, filemode='a')

def crawl(project_name):
    
    # Create log
    clean_name = project_name.replace("/", "_")
    logger = logging.getLogger(project_name)
    fh = logging.FileHandler(project_log % clean_name)
    logger.addHandler(fh)
    
    # Make sure project hasn't already been crawled
    try:
        os.stat(to_crawl % clean_name)
    except OSError:
        logging.info("Already crawled, skipping")
        return
    
    try:
        revision_urls = get_assessment_revisions(project_name, logging)
    except IOError:
        # Unable to get urls, return without marking finished
        return
    crawl_revisions(project_name, revision_urls, logging)
    
    # Mark finished
    os.remove(to_crawl % clean_name)
    logging.info("Crawling complete")
    return project_name

def get_assessment_revisions(project, logging):
    
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
    
    return assessment_urls

def crawl_revisions(project_name, revision_urls, logging):
    logging.info("Crawling revisions")
    # Create dir if necessary
    clean_name = project_name.replace('/', '_')
    try:
        os.stat(cache_dir % clean_name)
    except OSError:
        os.mkdir(cache_dir % clean_name)
    for url in revision_urls:
        logging.info("Crawling: %s" % url)
        oldid = re.search(r'oldid=(\d+)', url).group()
        output_file = os.path.join(cache_dir % clean_name, "%s.html" % oldid)
        urllib.urlretrieve(url, output_file)
        logging.info("Cached: %s" % url)

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

# Create project folders if necessary
for project_name in project_names:
    clean_name = project_name.replace('/','_')
    project_dir = output_dir % clean_name
    try:
        os.stat(project_dir)
    except OSError:
        os.mkdir(project_dir)
        with open(to_crawl % clean_name, "wb") as f:
            f.write(project_name.encode('utf-8'))
        with open(to_parse % clean_name, "wb") as f:
            f.write(project_name.encode('utf-8'))

# Create pool and start crawling
project_pool = Pool(num_workers)
for result in project_pool.imap(crawl, sorted(project_names)):
    print "Finished: %s" % result
    sys.stdout.flush()
