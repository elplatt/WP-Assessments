# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: crawler
# Date: 11/10/16

import calendar
from datetime import datetime
from dateutil.parser import parse
import logging
import os
import re
import sys
import traceback
import urllib

from bs4 import BeautifulSoup
from bs4 import element
import pandas as pd

# Config
project_tsv = "data/projects-2016-10-12.utf-16-le.tsv"
project_log = "output/projects/%s/parse.log"
cache_dir = "output/projects/%s/cache"
assessment_dir = "output/assessments"

# Fields
columns = [
    'Project', 'Date', 'Action', 'ArticleName', 'OldQual',
    'NewQual', 'OldImp', 'NewImp', 'NewArticleName', 'OldArticleLink',
    'OldTalkLink'
]

# Continuation message
contd_text = "This log entry was truncated because it was too long. This entry is a continuation of the entry in the next revision of this log page."

# Regular expressions
date_pattern = re.compile(
    "(January|February|March|April|May|June|July|August|September|October|November|December)"
    "\_\d{1,2}\.2C_\d{4}")
reassessed_simple_re = re.compile(
    "(.+) reassessed from (.+) \((.+)\) to (.+) \((.+)\)"
)
reassessed_re = re.compile(
    "(.+) \(talk\) reassessed."
)
reassessed_qual_re = re.compile(
    "Quality rating changed from (\S+) to (\S+)"
)
reassessed_imp_re = re.compile(
    "Importance rating changed from (\S+) to (\S+)"
)
added_re = re.compile(
    "(.+) \(talk\) (.+) \((.+)\) added"
)
removed_re = re.compile(
    "(.+) \(talk\) (.+) \((.+)\) removed"
)
renamed_simple_re = re.compile(
    "(.+) renamed to (.+)\."
)
renamed_talk_re = re.compile(
    "(.+) \(talk\) (.+) \((.+)\) renamed to (.+)"
)
renamed_re = re.compile(
    "(.+) \(.+?|talk\) (.+) \((.+)\) renamed to (.+)"
)
def get_entry(project_name, date, item, logger):
    text = item.get_text()
    action = ""
    old_qual = ""
    new_qual = ""
    old_imp = ""
    new_imp = ""
    article_name = ""
    article_new_name = ""
    article_old_link = ""
    talk_old_link = ""
    
    m = re.match(reassessed_re, text)
    if m:
        action = "Reassessed"
        article_name, = m.groups()
        m = re.search(reassessed_qual_re, text)
        if m:
            old_qual, new_qual = m.groups()
        m = re.search(reassessed_imp_re, text)
        if m:
            old_imp, new_imp = m.groups()            
        return [
            project_name, date, action, article_name, old_qual, new_qual,
            old_imp, new_imp, article_new_name, article_old_link, talk_old_link]
    
    m = re.match(reassessed_simple_re, text)
    if m:
        action = "Reassessed"
        article_name, old_qual, old_imp, new_qual, new_imp = m.groups()
        
        return [
            project_name, date, action, article_name, old_qual, new_qual,
            old_imp, new_imp, article_new_name, article_old_link, talk_old_link]
    
    m = re.match(added_re, text)
    if m:
        action = "Assessed"
        article_name, new_qual, new_imp = m.groups()
        return [
            project_name, date, action, article_name, old_qual, new_qual,
            old_imp, new_imp, article_new_name, article_old_link, talk_old_link]
    
    m = re.match(removed_re, text)
    if m:
        action = "Removed"
        article_name, old_class, old_imp = m.groups()
        return [
            project_name, date, action, article_name, old_qual, new_qual,
            old_imp, new_imp, article_new_name, article_old_link, talk_old_link]
    
    m = re.match(renamed_talk_re, text)
    if m:
        action = "Renamed"
        article_name, old_class, old_imp, article_new_name = m.groups()
        return [
            project_name, date, action, article_name, old_qual, new_qual,
            old_imp, new_imp, article_new_name, article_old_link, talk_old_link]
        
    m = re.match(renamed_re, text)
    if m:
        action = "Renamed"
        article_name, old_class, old_imp, article_new_name = m.groups()
        return [
            project_name, date, action, article_name, old_qual, new_qual,
            old_imp, new_imp, article_new_name, article_old_link, talk_old_link]
        
    m = re.match(renamed_simple_re, text)
    if m:
        action = "Renamed"
        article_name, article_new_name = m.groups()
        return [
            project_name, date, action, article_name, old_qual, new_qual,
            old_imp, new_imp, article_new_name, article_old_link, talk_old_link]
    
    logger.error("Unrecognized format: %s" % text)
    raise ValueError

def parse_date(date_string):
    '''Convert date_string (assumed UTC) to UTC timestamp.'''
    fmt = "%B %d, %Y"
    try:
        timestamp = calendar.timegm(datetime.strptime(date_string, fmt).timetuple())
    except TypeError:
        raise ValueError
    return timestamp

def parse(project_name):
    clean_name = project_name.replace("/", "_")
    logger = logging.getLogger(project_name)
    fh = logging.FileHandler(project_log % clean_name)
    logger.addHandler(fh)
    logger.setLevel(logging.DEBUG)
    logger.info("Beggining parse")
    
    entries = {}
    
    # Loop through cached history pages
    # Go newest to oldest for correct order in multi-page entries
    pages = sorted(os.listdir(cache_dir % clean_name), reverse=True)
    for i, page in enumerate(pages):
        if i > 0 and i % 1000 == 0:
            print "%d: %2.2f" % (i, (float(i) / float(len(pages))))
        # Load html into beautifulsoup
        with open(os.path.join(cache_dir % clean_name, page)) as f:
            page_tree = BeautifulSoup(f.read(), 'html.parser')
            
        # Check whether this page is a continuation
        p = page_tree.find(text=contd_text)
        # If not, find the first date header
        if p is None:
            try:
                header_tags = page_tree.find_all("h3")
                date_tags = [x for x in header_tags
                                if x.span and x.span.get('class') == ["mw-headline"]
                                    and 'id' in x.span.attrs
                                    and date_pattern.match(x.span.get('id'))]
                try:
                    current_tag = date_tags[0]
                except IndexError:
                    logger.exception("No headers match pattern: %s" % page)
                    continue
            except Exception as TagNotFoundException:
                logger.exception("No headers found: %s" % page)
                continue
            try:
                date_text = current_tag.span.get_text()
                current_date = parse_date(date_text)
            except ValueError:
                logger.error("Unable to parse date: %s" % str(current_tag))
                return

        while True:            
            # Move to next tag
            try:
                current_tag = current_tag.next_sibling
            except AttributeError:
                break
            
            try:
                if current_tag.name == "h3":
                    date_text = current_tag.span.get_text()
                    current_date = parse_date(date_text)
                elif current_tag.name == "ul":
                    for item in current_tag.find_all('li'):
                        try:
                            entry = get_entry(project_name, current_date, item, logger)
                        except ValueError:
                            logger.error("Error parsing: %s" % item.get_text())
                            raise
                            continue
                        entries[entry[1]] = entry
            except AttributeError:
                continue
    logger.info("Parse complete")
    logger.info("Sortintg results")
    sorted_keys = sorted(entries.keys())
    logger.info("Writing results")
    clean_name = urllib.quote(project_name.replace(" ", "_").encode('utf-8'))
    with open(os.path.join(assessment_dir, "%s.csv" % clean_name)) as f:
        f.write((u",".join(columns) + u"\n").encode('utf-8'))
        for k in sorted_keys:
            f.write((u",".join(entries[k]) + u"\n").encode('utf-8'))
    logger.info("Project %s complete" % project_name)

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

parse("Computing")
