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
import shutil
import subprocess
import sys
import traceback
import urllib

from bs4 import BeautifulSoup
from bs4 import element

# Config
project_tsv = "data/projects-2016-10-12.utf-16-le.tsv"
project_log = "output/projects/%s/parse.log"
cache_dir = "output/projects/%s/cache"
cache_tar = "output/projects_crawled/%s-cache.tgz"
to_parse = "output/to_parse/%s"
assessment_file = "output/assessments/%s.utf8.tsv"

# Main log
logger = logging.getLogger('parser_main')
handler = logging.FileHandler('output/parser_main.log')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

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
cache_re = re.compile(
    "oldid=(\d+)\.html"
)
assessed_re = re.compile(
    "(.+) \(.+\) assessed."
)
assessed_qual_re = re.compile(
    "Quality assessed as (.+?) \(.+?\)\."
)
assessed_imp_re = re.compile(
    "Importance assessed as (.+?) \(.+?\)\."
)
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
reassessed_quote_rev_re = re.compile(
    "(.+) rev"
)
reassessed_quote_t_re = re.compile(
    "(.+) t"
)
reassessed_moved_re = re.compile(
    "(.+) moved from (.+) \((.+)\) to (.+) \((.+)\)"
)
added_simple_re = re.compile(
    "(.+) \(talk\) added"
)
added_re = re.compile(
    "(.+) \(talk\) (.+) \((.+)\) added"
)
removed_simple_re = re.compile(
    "(.+) \(talk\) (.+) \((.+)\) removed"
)
removed_re = re.compile(
    "(.+) \(talk\) removed"
)
renamed_simple_re = re.compile(
    "(.+) renamed to (.+)\."
)
renamed_talk_re = re.compile(
    "(.+) \(talk\) (.+) \((.+)\) renamed to (.+)"
)
renamed_simple_talk_re = re.compile(
    "(.+) \(talk\) renamed to (.+) \(talk\)"
)
renamed_re = re.compile(
    "(.+) \(.+?\) (.+) \((.+)\) renamed to (.+)"
)
renamed_assessment_re = re.compile(
    "(.+) \((.+)\)"
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
        article_name, = m.groups()
        m = re.search(reassessed_qual_re, text)
        if m:
            old_qual, new_qual = m.groups()
        m = re.search(reassessed_imp_re, text)
        if m:
            old_imp, new_imp = m.groups()
        # Get old revision link
        try:
            rev = item.find('a', text="rev")
            article_old_link = rev.get('href').split("?")[1]
        except AttributeError:
            # Couldn't parse, check whether it's a quoting problem
            try:
                rev = item.find('a', text=reassessed_quote_rev_re)
                text_part = re.match(reassessed_quote_rev_re, rev.get_text()).groups()[0]
                href_part = rev.get('href').split("?")[1]
                article_old_link = href_part + text_part.replace('"', "%22")
            except AttributeError:
                logger.error("  Couldn't parse: %s" % item.get_text())
                raise AssertionError
        # Get old talk link
        try:
            t = item.find('a', text="t")
            talk_old_link = t.get('href').split("?")[1]
        except AttributeError:
            # Couldn't parse, check whether it's a quoting problem
            try:
                t = item.find('a', text=reassessed_quote_t_re)
                text_part = re.match(reassessed_quote_t_re, t.get_text()).groups()[0]
                href_part = t.get('href').split("?")[1]
                talk_old_link = href_part + text_part.replace('"', "%22")
            except AttributeError:
                logger.error("  Couldn't parse: %s" % item.get_text())
                raise AssertionError
        # We made it, somehow
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
    
    m = re.match(assessed_re, text)
    if m:
        action = "Assessed"
        links = item.find_all('a')
        article_name = links[0].get_text()
        qual_m = re.search(assessed_qual_re, text)
        if qual_m:
            new_qual, = qual_m.groups()
        imp_m = re.search(assessed_imp_re, text)
        if imp_m:
            new_imp, = imp_m.groups()
        if qual_m or imp_m:
            if len(links) == 4 or len(links) == 6:
                # Not missing a talk link
                article_old_link = links[2].get('href').split("?")[1]
                talk_old_link = links[3].get('href').split("?")[1]
            elif len(links) == 3 or len(links) == 5:
                # Missing a talk link
                article_old_link = links[1].get('href').split("?")[1]
                talk_old_link = links[2].get('href').split("?")[1]
            else:
                logger.error("Unrecognized number of links: %s" % text)
                raise ValueError
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
        links = item.find_all('a')
        # Get article names and talk link
        # Can't trust regex because of nested parentheses
        # Instead trim links from ends of string
        action = "Renamed"
        article_name = links[0].get_text()
        if len(links) == 2:
            article_new_name = links[1].get_text()
        elif len(links) > 2:
            talk_text = links[1].get_text()
            article_new_name = links[2].get_text()
            front = len(article_name) + len(talk_text) + len(" () ")
            back = len(article_new_name) + len(" ")
            assessment_part = text[front:-back]
            old_class, old_imp = re.match(renamed_assessment_re, assessment_part).groups()
        else:
            logger.error("renamed_re unrecognized link count")
            raise ValueError
        return [
            project_name, date, action, article_name, old_qual, new_qual,
            old_imp, new_imp, article_new_name, article_old_link, talk_old_link]
    
    m = re.match(renamed_simple_talk_re, text)
    if m:
        links = item.find_all('a')
        action = "Renamed"
        article_name = links[0].get_text()
        talk_text = links[1].get_text()
        article_new_name = links[2].get_text()
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
    
    m = re.match(removed_re, text)
    if m:
        action = "Removed"
        article_name, = m.groups()
        return [
            project_name, date, action, article_name, old_qual, new_qual,
            old_imp, new_imp, article_new_name, article_old_link, talk_old_link]
    
    m = re.match(removed_simple_re, text)
    if m:
        action = "Removed"
        article_name, old_class, old_imp = m.groups()
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
    
    m = re.match(added_simple_re, text)
    if m:
        action = "Assessed"
        article_name, = m.groups()
        return [
            project_name, date, action, article_name, old_qual, new_qual,
            old_imp, new_imp, article_new_name, article_old_link, talk_old_link]
    
    m = re.match(reassessed_moved_re, text)
    if m:
        action = "Reassessed"
        article_name, old_qual, old_imp, new_qual, new_imp = m.groups()
        return [
            project_name, date, action, article_name, old_qual, new_qual,
            old_imp, new_imp, article_new_name, article_old_link, talk_old_link]

    logger.error("Unrecognized format: %s" % text)
    raise ValueError

def parse_date(date_string):
    '''Convert date_string (assumed UTC) to UTC timestamp.'''
    fmt1 = "%B %d, %Y"
    fmt2 = "%Y-%m-%d"    
    try:
        try:
            dt = datetime.strptime(date_string, fmt1)
        except ValueError:
            dt = datetime.strptime(date_string, fmt2)
    except ValueError:
        raise ValueError
    try:
        timestamp = calendar.timegm(dt.timetuple())
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
    
    # Make sure project hasn't already been crawled
    try:
        os.stat(to_parse % clean_name)
    except OSError:
        logger.info("Already parsed, skipping")
        return 1

    entries = {}
    
    # Loop through cached history pages
    # Go newest to oldest for correct order in multi-page entries
    pages = os.listdir(cache_dir % clean_name)    
    pages = sorted(os.listdir(cache_dir % clean_name), reverse=True)
    page_ids = [int(re.match(cache_re, page).groups()[0]) for page in pages]
    page_ids = sorted(page_ids, reverse=True)
    for i, page in enumerate(page_ids):
        if i > 0 and i % 100 == 0:
            print "%d: %2.2f%%" % (i, (float(100*i) / float(len(pages))))
        # Load html into beautifulsoup
        with open(os.path.join(cache_dir % clean_name, "oldid=%d.html" % page)) as f:
            page_tree = BeautifulSoup(f.read(), 'html.parser')
        try:
            # Prevent running out of filehandlers if python procrastinates
            f.close()
        except:
            pass
        
        # Check whether this page is a continuation
        p = page_tree.find(text=contd_text)
        # If not, find the first date header
        if p is None:
            contd = False
            try:
                header_tags = page_tree.find_all("h3")
                date_tags = [x for x in header_tags
                                if x.span and x.span.get('class') == ["mw-headline"]
                                    and 'id' in x.span.attrs
                                    and date_pattern.match(x.span.get('id'))]
                try:
                    current_tag = date_tags[0]
                except IndexError:
                    logger.info("No headers match pattern: %s" % page)
                    continue
            except Exception as TagNotFoundException:
                logger.info("No headers found: %s" % page)
                continue
            try:
                date_text = current_tag.span.get_text()
                current_date = parse_date(date_text)
            except ValueError:
                logger.error("Unable to parse date: %s" % str(current_tag))
                raise
        else:
            contd = True
            logger.info("  Continuing date: %d" % current_date)
            current_tag = page_tree.find(id="mw-content-text").find('ul')

        entry_count = 0
        skip_count = 0
        while True:
            # Move to next tag
            # If we're continuing from the last page, we're already on the right
            # tag so we don't go to the next one.
            if contd:
                contd = False
            else:
                try:
                    current_tag = current_tag.next_sibling
                    current_tag.name
                except AttributeError:
                    if entry_count == 0 and skip_count == 0:
                        huge = page_tree.find(text="The log for today is too huge to upload to the wiki.")
                        if huge is None:
                            logger.error("Found no entries in: %s" % page)
                        else:
                            logger.warning("Log too large to upload: %s" % page)
                    #logger.info("  Parsed(skipped) %d(%d) counts in %s" % (entry_count, skip_count, page))
                    break
            
            if current_tag.name == "h3":
                date_text = current_tag.span.get_text()
                try:
                    current_date = parse_date(date_text)
                except ValueError:
                    logger.error("Unable to parse date: %s" % str(current_tag))
                    raise
            elif current_tag.name == "ul":
                for item in current_tag.find_all('li'):
                    try:
                        entry = get_entry(project_name, current_date, item, logger)
                    except ValueError:
                        logger.error("Error parsing: %s" % item.get_text())
                        raise
                    except AssertionError:
                        raise
                    k = (entry[1], entry[3], entry[2])
                    try:
                        prev = entries[k]
                        if prev != entry:
                            logger.error("  Contradictory entries:")
                            logger.error("    Keeping: " + str(prev))
                            logger.error("    Discarding: " + str(entry))
                        skip_count += 1
                    except KeyError:
                        entries[k] = entry
                        entry_count += 1
    logger.info("Parse complete")
    logger.info("Sortintg results")
    sorted_keys = sorted(entries.keys())
    logger.info("Writing results")
    quoted_name = urllib.quote(project_name.replace(" ", "_").encode('utf-8'))
    with open(os.path.join(assessment_file % quoted_name), "wb") as f:
        f.write((u"\t".join(columns) + u"\n").encode('utf-8'))
        for k in sorted_keys:
            entry = entries[k]
            row = u"\t".join([unicode(x) for x in entry]) + u"\n"
            f.write(row.encode('utf-8'))
    try:
        # Prevent running out of filehandlers if python procrastinates
        f.close()
    except:
        pass
    logger.info("Marking complete")
    os.remove(to_parse % clean_name)
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
try:
    # Prevent running out of filehandlers if python procrastinates
    f.close()
except:
    pass

#parse("test")
#sys.exit()

for project_name in sorted(project_names):
    # If the first arg is a project name, skip to that arg
    try:
        if sys.argv[1] > project_name:
            logger.info("Skipping from arg: %s" % project_name)
            continue
    except IndexError:
        pass
    logger.info("Beginning %s" % project_name)
    logger.info("  Decompressing cache")
    clean_name = clean_name = project_name.replace("/", "_")
    project_cache_tar = cache_tar % clean_name
    project_cache_dir = cache_dir % clean_name
    subprocess.call(["tar", "-xzf", project_cache_tar])
    logger.info("  Beginning parse")
    try:
        status = parse(project_name)
        if status == 1:
            logger.info("  Already parsed")
        else:
            logger.info("  Parsed successfully")
    except:
        logger.error(str(sys.exc_info()))
    logger.info("  Cleaning up")
    try:
       shutil.rmtree(project_cache_dir)
    except:
        # Will error if we were unable to successfully decompress
        pass

