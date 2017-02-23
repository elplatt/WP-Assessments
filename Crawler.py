# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: crawler
# Date: 11/10/16

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import urllib
from collections import OrderedDict

from bs4 import BeautifulSoup
from dateutil.parser import parse
from subprocess import call
import datetime
import logging
import traceback

from continuity.FileSystem import filesystem
from continuity.csvReader import csvreader


class crawler(object):

    def __init__(self, filepath):

        dataframe = csvreader(filepath)
        projectData = dataframe.readtable('\t')
        self.projects = projectData["Title"].tolist()
        self.limit = 1500

    def crawl(self):

        for count, project in enumerate(self.projects):

            today_date = datetime.datetime.today()
            today = today_date.strftime('%Y-%m-%d')
            logPath = os.path.join(filesystem.LogFileSystem, today + '.log')
            logging.basicConfig(filename=logPath, level=logging.DEBUG, filemode='a',
                                format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
            logging.info("Project: {0}".format(project))
            logging.info("Crawling project : {0}".format(project))
            outpath = os.path.join(filesystem.CrawledFileSystem, project + '.tsv')
            crawldir = os.path.join(filesystem.OutputFileSystem, project)
            call(["mkdir", crawldir])

            try:
                url = "https://en.wikipedia.org/w/index.php?title=Wikipedia:Version_1.0_Editorial_Team/" \
                      + project + "_articles_by_quality_log&offset=&limit=" + str(self.limit) + "&action=history"
                try:
                    handle = urllib.urlopen(url)
                except Exception as URLNotFoundException:
                    logging.exception("URL not found for project {0}".format(project))
                try:
                    gunk = handle.read()
                except Exception as CannotReadException:
                    logging.exception("Cannot read the html file for the project {0}".format(project))

                try:
                    soup = BeautifulSoup(gunk, 'html.parser')
                except Exception as SoupNotMadeException:
                    logging.exception("Unable to parse")

                logs = soup.findAll("a", {"class": "mw-changeslist-date"})
                date_dict = {}
                for log in logs:
                    link = log.get('href')
                    logURL = "https://en.wikipedia.org/" + link
                    try:
                        date = parse(log.get_text().encode('utf-8').split(',')[1])
                    except Exception as DateNotParseException:
                        logging.exception("Unable to parse the date.")

                    date_dict[date] = logURL

                try:
                    history = filesystem.HistoryFileSystem
                    project_file = project + '.tsv'
                    project_hist = os.path.join(history, project_file)
                except Exception as FileNotFoundException:
                    logging.exception("File not found")

                csvHistory = csvreader(project_hist)

                try:
                    history_data = csvHistory.readtable('\t')
                except Exception as CannotReadException:
                    logging.exception("Pandas unable to read the tsv file")

                if not history_data.empty:
                    lastdate = parse(history_data['LatestTimeStamp'].tolist()[-1])
                else:
                    lastdate = parse("January 1, 1980")

                temp = [x for x in sorted(date_dict.items(), key=lambda t: t[0], reverse=True) if x[0] >= lastdate]
                required = OrderedDict(temp)
                if required:
                    for key, value in required.iteritems():

                        requiredURL = value
                        try:
                            requiredHandle = urllib.urlopen(requiredURL)
                        except Exception as URLNotFoundException:
                            logging.exception("Cannot get the history html files for the project {0}".format(project))

                        try:
                            requiredGunk = requiredHandle.read()
                        except Exception as CannotReadException:
                            logging.exception("Cannot read the data fromt he html page")

                        try:
                            requiredSoup = BeautifulSoup(requiredGunk, 'html.parser')
                        except Exception as SoupNotMadeException:
                            logging.exception("Cannot parse the html file")

                        try:
                            htmlpath = os.path.join(filesystem.CrawlHTMLFileSystem,str(key) + '.html')
                            fileout = open(htmlpath, 'w')
                            fileout.write(requiredSoup.prettify(encoding='utf-8', formatter="minimal"))
                            fileout.close()
                        except Exception as FileNotFoundException:
                            logging.exception("Path not found")
            except:
                traceback.print_exc(file=logPath)



