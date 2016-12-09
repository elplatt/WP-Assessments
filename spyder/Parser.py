# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: crawler
# Date: 11/10/16

import os
import re
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from bs4 import BeautifulSoup
from bs4 import element
from dateutil.parser import parse
import datetime
import pandas as pd
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

    def parse(self):

        for count, project in enumerate(self.projects):

            today_date = datetime.datetime.today()
            today = today_date.strftime('%Y-%m-%d')
            logPath = os.path.join(filesystem.LogFileSystem, today + '.log')
            logging.basicConfig(filename=logPath, level=logging.DEBUG, filemode='a',
                                format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
            logging.info("Project: {0}".format(project))
            logging.info("Parsing project : {0}".format(project))
            outpath = os.path.join(filesystem.CrawledFileSystem, project + '.tsv')
            crawldir = os.path.join(filesystem.OutputFileSystem, project)
            crawl_logpath = os.path.join(filesystem.CrawlLogFileSystem, project+'.tsv')

            try:
                history = filesystem.HistoryFileSystem
                crawlLog = filesystem.CrawlLogFileSystem
                project_file = project + '.tsv'
                project_hist = os.path.join(history, project_file)
                project_crawl = os.path.join(crawlLog, project_file)
            except Exception as FileNotFoundException:
                logging.exception("File not found")

            csvHistory = csvreader(project_hist)
            csvCrawl = csvreader(project_crawl)

            try:
                history_data = csvHistory.readtable('\t')
                crawldata = csvCrawl.readtable('\t')
            except Exception as CannotReadException:
                logging.exception("Pandas unable to read the tsv file")

            if not history_data.empty:
                lastdate = parse(history_data['LatestTimeStamp'].tolist()[-1])
                last_crawled = parse(crawldata['LatestTimeStamp'].tolist()[-1])
            else:
                lastdate = parse("January 1, 1980")
                last_crawled = parse("January 1, 1980")

            try:
                for file in os.listdir(crawldir):
                    crawlout = open(project_crawl, 'a')
                    if parse(file.split('.')[0]) >= last_crawled:
                        inpath = os.path.join(crawldir, file)
                        filein = open(inpath, 'w')
                        requiredSoup = BeautifulSoup(filein.read(), 'html.parser')
                        pattern = re.compile("(January|February|March|April|May|June|July|August|September|October|November|December)"
                            "\_\d{1,2}\.2C_\d{4}")

                        try:
                            requiredTag = requiredSoup.find_all("h3")
                        except Exception as TagNotFoundException:
                            logging.exception("h3 tag not found")

                        requiredTag = [x for x in requiredTag if x.span and x.span.get('class') == ["mw-headline"]
                                       and 'id' in x.span.attrs and pattern.match(x.span.get('id'))]

                        for i, tag in enumerate(requiredTag):
                            requiredDate = tag.span.string
                            project_assessment = []
                            if requiredDate:
                                while tag.next_sibling and parse(requiredDate) >= lastdate:
                                    if isinstance(tag.next_sibling, element.Tag) and tag.next_sibling.name != "h3":
                                        next_tag = tag.next_sibling
                                        if next_tag.name == "ul":
                                            for tag_li in next_tag.find_all("li"):
                                                sentence = str(tag_li)
                                                for item in tag_li.find_all("span"):
                                                    sentence = sentence.replace(str(item), '')
                                                for item in tag_li.find_all("b"):
                                                    sentence = sentence.replace(str(item), '')
                                                for item in tag_li.find_all("a"):
                                                    sentence = sentence.replace(str(item), '')
                                                sentence = sentence.replace('<li>', '').replace('</li>', '')
                                                sentence = sentence.replace('()', '')
                                                sentence = ' '.join(sentence.split())

                                                bs = tag_li("b")
                                                if bs:
                                                    article_name = bs[0].text
                                                if sentence == 'assessed. Importance assessed as':
                                                    scores = [article_name, 'Assessed', '', '', '', bs[1].text, '']
                                                elif sentence == 'assessed. Quality assessed as':
                                                    scores = [article_name, 'Assessed', '', bs[1].text, '', '', '']
                                                elif sentence == 'assessed. Quality assessed as Importance assessed as':
                                                    scores = [article_name, 'Assessed', '', bs[1].text, '', bs[2].text,
                                                              '']
                                                elif sentence == 'reassessed. Importance rating changed from to':
                                                    scores = [article_name, 'Reassessed', '', '', bs[1].text,
                                                              bs[2].text, '']
                                                elif sentence == 'reassessed. Quality assessed as Importance rating changed from to':
                                                    scores = [article_name, 'Reassessed', '', bs[1].text, bs[2].text,
                                                              bs[3].text, '']
                                                elif sentence == 'reassessed. Quality rating changed from to':
                                                    scores = [article_name, 'Reassessed', bs[1].text, bs[2].text, '',
                                                              '', '']
                                                elif sentence == 'reassessed. Quality rating changed from to Importance assessed as':
                                                    scores = [article_name, 'Reassessed', bs[1].text, bs[2].text, '',
                                                              bs[3].text, '']
                                                elif sentence == 'reassessed. Quality rating changed from to Importance rating changed from to':
                                                    scores = [article_name, 'Reassessed', bs[1].text, bs[2].text,
                                                              bs[3].text, bs[4].text, '']
                                                elif sentence == 'removed. Importance rating was':
                                                    scores = [article_name, 'Removed', '', '', bs[1].text, '', '']
                                                elif sentence == 'removed. Quality rating was':
                                                    scores = [article_name, 'Removed', bs[1].text, '', '', '', '']
                                                elif sentence == 'removed. Quality rating was Importance rating was':
                                                    scores = [article_name, 'Removed', bs[1].text, '', bs[2].text, '',
                                                              '']
                                                elif sentence == 'renamed to .':
                                                    scores = [article_name, 'Renamed', '', '', '', '', bs[1].text]

                                                article = scores[0]
                                                action = scores[1]
                                                old_qual = scores[2]
                                                new_qual = scores[3]
                                                old_imp = scores[4]
                                                new_imp = scores[5]
                                                new_article_name = scores[6]

                                                for a in tag_li("a"):
                                                    if a.text == "t":
                                                        old_talk_link = a['href'].replace('https://en.wikipedia.org',
                                                                                          '')
                                                    elif a.text == "rev":
                                                        old_article_link = a['href'].replace('https://en.wikipedia.org',
                                                                                             '')
                                                    else:
                                                        old_talk_link = a['href'].replace('https://en.wikipedia.org',
                                                                                          '')
                                                        old_article_link = a['href'].replace('https://en.wikipedia.org',
                                                                                             '')

                                                article_assessment = [project, requiredDate, action, article, old_qual,
                                                                      new_qual, old_imp,
                                                                      new_imp, new_article_name, old_article_link,
                                                                      old_talk_link]
                                                project_assessment.append(article_assessment)
                                    tag = tag.next_sibling
                                project_assessment = sorted(project_assessment, key=lambda x: parse(x[1]))
                                df = pd.DataFrame(project_assessment)
                                with open(outpath, 'a') as f:
                                    df.to_csv(f, sep='\t', encoding='utf-8', index=False, header=False)
                                f.close()

                        project_assessment = pd.read_csv(outpath, sep='\t')
                        if project_assessment:
                            project_assessment = project_assessment.drop_duplicates()
                        project_assessment.to_csv(outpath, sep='\t', encoding='utf-8', index=False, header=False)
                        project_assessment = project_assessment.sort('Date')

                        history_outpath = os.path.join(filesystem.HistoryFileSystem, project + '.tsv')
                        with open(history_outpath, 'a') as outfile:
                            if project_assessment:
                                outfile.write(project_assessment[-1][1] + '\n')
                        outfile.close()

                        print "The project parsed is: {0}, The cumulative total is: {1}".format(project, str(count))

                    crawlout.write(last_crawled + '\n')

                crawlout.close()
            except:
                traceback.print_exc(file=logPath)
