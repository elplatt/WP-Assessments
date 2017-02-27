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
cache_dir = "output/projects/%s/cache"
cache_tar = "output/projects_crawled/%s-cache.tgz"
base_url = "https://en.wikipedia.org/"
assessment_history_url = (
    "https://en.wikipedia.org/w/index.php?title=Wikipedia:Version_1.0_Editorial_Team/%s_articles_by_quality_log&offset=%s&limit=500&action=history"
)
logger = logging.getLogger('crawler_main')
handler = logging.FileHandler('output/main.log')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

def crawl_worker(project_q, logger):
    while not project_q.empty():
        try:
            project_name = project_q.get(1)
        except Empty:
            pass
        logger.info("Starting: %s" % project_name)
        crawl(project_name)
        logger.info("Finished: %s" % project_name)

def crawl(project_name):
    
    # Create project folder and log
    clean_name = project_name.replace("/", "_")
    project_dir = output_dir % clean_name
    try:
        os.stat(project_dir)
    except OSError:
        os.mkdir(project_dir)
        with open(to_crawl % clean_name, "wb") as f:
            f.write(project_name.encode('utf-8'))
        with open(to_parse % clean_name, "wb") as f:
            f.write(project_name.encode('utf-8'))
    logger = logging.getLogger(project_name)
    fh = logging.FileHandler(project_log % clean_name)
    logger.addHandler(fh)
    logger.setLevel(logging.DEBUG)
    
    # Make sure project hasn't already been crawled
    try:
        os.stat(to_crawl % clean_name)
    except OSError:
        logger.info("Already crawled, skipping")
        return
    
    try:
        revision_urls = get_assessment_revisions(project_name, logger)
    except IOError:
        # Unable to get urls, return without marking finished
        return
    crawl_revisions(project_name, revision_urls, logger)
    
    # Mark finished
    os.remove(to_crawl % clean_name)
    logger.info("Compressing results")
    project_cache_tar = cache_tar % clean_name
    project_cache_dir = cache_dir % clean_name
    subprocess.call(["tar", "-czf", project_cache_tar, project_cache_dir])
    logger.info("Removing uncompressed results")
    shutil.rmtree(project_cache_dir)
    logger.info("Crawling complete")
    return project_name

def get_assessment_revisions(project, logger):
    
    logger.info("Getting assessment revision urls")
    assessment_urls = []
    next_url = assessment_history_url % (project, "")
    while next_url is not None:
        # Request next page of history and parse
        logger.info("Requesting: %s" % next_url)
        handle = urllib.urlopen(next_url)
        if handle.getcode() != 200:
            logger.error("HTTP %d when fetching: %s" % (handle.getcode(), next_url))
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

    logger.info("Parsed %d assessment revision urls" % len(assessment_urls))
    
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

# Create pool and start crawling
project_q = Queue()
for project_name in sorted(project_names):
    project_q.put(project_name)
try:
    logger.info("Creating workers")
    workers = []
    for i in range(num_workers):
        p = Process(target=crawl_worker, args=(project_q, logger))
        p.start()
        workers.append(p)
    while not project_q.empty():
        time.sleep(1)
except:
    logger.error("Exception: %s" % str(sys.exc_info()))
    logger.info("Stopping workers")
    for p in workers:
        p.terminate()
    raise
