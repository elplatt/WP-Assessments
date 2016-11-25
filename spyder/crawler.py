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
import pandas as pd

from continuity.FileSystem import filesystem
from continuity.csvReader import csvreader


class crawler(object):
    def __init__(self, filepath):

        dataframe = csvreader(filepath)
        projectData = dataframe.readtable('\t')
        self.projects = projectData["Title"].tolist()
        self.limit = 1500

    def crawl(self):

        for project in self.projects:
            url = "https://en.wikipedia.org/w/index.php?title=Wikipedia:Version_1.0_Editorial_Team/" \
                  + project + "_articles_by_quality_log&offset=&limit=" + str(self.limit) + "&action=history"

            handle = urllib.urlopen(url)
            gunk = handle.read()
            soup = BeautifulSoup(gunk)
            logs = soup.findAll("a", {"class": "mw-changeslist-date"})
            date_dict = {}
            for log in logs:
                link = log.get('href')
                logURL = "https://en.wikipedia.org/" + link
                date = parse(log.get_text().encode('utf-8').split(',')[1])
                date_dict[date] = logURL

            history = filesystem.HistoryFileSystem
            project_file = project + '.tsv'
            project_hist = os.path.join(history, project_file)
            csvHistory = csvreader(project_hist)
            history_data = csvHistory.readcsv()

            if not history_data.empty:
                lastdate = parse(history_data['LatestTimeStamp'].tolist()[-1])
            else:
                lastdate = parse("January 1, 1980")

            temp = [x for x in sorted(date_dict.items(), key=lambda t: t[0], reverse=True) if x[0] >= lastdate]
            required = OrderedDict(temp)
            project_assessment = []
            print project

            if required:
                for key, value in required.iteritems():
                    requiredURL = value
                    requiredHandle = urllib.urlopen(requiredURL)
                    requiredGunk = requiredHandle.read()
                    requiredSoup = BeautifulSoup(requiredGunk)

                    # requiredDate = requiredSoup.findAll("span", {"class": "mw-headline", "id":
                    #     re.compile("(January|February|March|April|May|June|July|August|September|October|November|December)"
                    #                "\_\d{1,2}\.2C_\d{4}")})
                    #
                    # for tag in requiredDate:
                    #     time = tag.get_text()
                    #     print tag.parent
                    #     if tag.parent.name =="h3":
                    #         for element in tag.parent.next_elements:
                    #             if element.name == "h4":
                    #                 action = element.get_text()
                    #
                    #             elif element.name == "ul":
                    #                 content = element.contents
                    #
                    #             else:
                    #                 break

                    requiredTag = requiredSoup.find_all("h3")
                    requiredTag = [x for x in requiredTag if x.span and x.span.get('class') == ["mw-headline"]]

                    for i, tag in enumerate(requiredTag):
                        # print i
                        requiredDate = tag.span.string
                        while tag.next_sibling and tag.next_sibling.name != "h3" and parse(requiredDate) >= lastdate:

                            next_tag = tag.next_sibling
                            if next_tag.name == "h4" and next_tag.span and next_tag.span.get('class') == ['mw-headline']:
                                action = next_tag.string

                            if next_tag.name == "ul":
                                tag_li = next_tag("li")
                                sentence = str(tag_li[0])
                                for item in tag_li[0].find_all("span"):
                                    sentence = sentence.replace(str(item), '')
                                for item in tag_li[0].find_all("b"):
                                    sentence = sentence.replace(str(item), '')
                                for item in tag_li[0].find_all("a"):
                                    sentence = sentence.replace(str(item), '')
                                sentence = sentence.replace('<li>', '').replace('</li>', '')
                                sentence = sentence.replace('()', '')
                                sentence = ' '.join(sentence.split())

                                bs = tag_li[0]("b")
                                if bs:
                                    article_name = bs[0].text
                                if sentence == 'assessed. Importance assessed as':
                                    scores = [article_name, 'Assessed', 'NA', 'NA', 'NA', bs[1].text, 'NA']
                                elif sentence == 'assessed. Quality assessed as':
                                    scores = [article_name, 'Assessed', 'NA', bs[1].text, 'NA', 'NA', 'NA']
                                elif sentence == 'assessed. Quality assessed as Importance assessed as':
                                    scores = [article_name, 'Assessed', 'NA', bs[1].text, '', bs[2].text, 'NA']
                                elif sentence == 'reassessed. Importance rating changed from to':
                                    scores = [article_name, 'Reassessed', 'NA', 'NA', bs[1].text, bs[2].text, 'NA']
                                elif sentence == 'reassessed. Quality assessed as Importance rating changed from to':
                                    scores = [article_name, 'Reassessed', 'NA', bs[1].text, bs[2].text, bs[3].text, 'NA']
                                elif sentence == 'reassessed. Quality rating changed from to':
                                    scores = [article_name, 'Reassessed', bs[1].text, bs[2].text, 'NA', 'NA', 'NA']
                                elif sentence == 'reassessed. Quality rating changed from to Importance assessed as':
                                    scores = [article_name, 'Reassessed', bs[1].text, bs[2].text, 'NA', bs[3].text, 'NA']
                                elif sentence == 'reassessed. Quality rating changed from to Importance rating changed from to':
                                    scores = [article_name, 'Reassessed', bs[1].text, bs[2].text, bs[3].text, bs[4].text, 'NA']
                                elif sentence == 'removed. Importance rating was':
                                    scores = [article_name, 'Removed', 'NA', 'NA', bs[1].text, 'NA', 'NA']
                                elif sentence == 'removed. Quality rating was':
                                    scores = [article_name, 'Removed', bs[1].text, 'NA', 'NA', 'NA', 'NA']
                                elif sentence == 'removed. Quality rating was Importance rating was':
                                    scores = [article_name, 'Removed', bs[1].text, 'NA', bs[2].text, 'NA', 'NA']
                                elif sentence == 'renamed to .':
                                    scores = [article_name, 'Renamed', 'NA', 'NA', 'NA', 'NA', bs[1].text]

                                article = scores[0]
                                action = scores[1]
                                old_qual = scores[2]
                                new_qual = scores[3]
                                old_imp = scores[4]
                                new_imp = scores[5]
                                new_article_name = scores[6]

                                for a in tag_li[0]("a"):
                                    if a.text == "t":
                                        old_talk_link = a['href'].replace('https://en.wikipedia.org', '')
                                    elif a.text == "rev":
                                        old_article_link = a['href'].replace('https://en.wikipedia.org', '')
                                    elif a.text == "talk":
                                        talk_link = "https://en.wikipedia.org/" + a['href']

                                article_assessment = [project, requiredDate, action, article, old_qual, new_qual, old_imp,
                                                      new_imp, new_article_name, old_article_link, old_talk_link]
                                project_assessment.append(article_assessment)

                            tag = tag.next_sibling

                project_assessment = sorted(project_assessment, key=lambda x: parse(x[1]))
                df = pd.DataFrame(project_assessment)
                df = df.drop_duplicates()

                outpath = os.path.join(filesystem.CrawledFileSystem, project+'.tsv')
                with open(outpath, 'a') as f:
                    df.to_csv(f, sep='\t', encoding='utf-8', index=False, header=['Project', 'Date', 'Action', 'ArticleName', 'OldQual',
                                     'NewQual', 'OldImp', 'NewImp', 'NewArticleName', 'OldArticleLink', 'OldTalkLink'])
                f.close()

                history_outpath = os.path.join(filesystem.HistoryFileSystem, project+'.tsv')
                with open(history_outpath, 'a') as outfile:
                    if project_assessment:
                        outfile.write(project_assessment[-1][1] + '\n')
                outfile.close()

                # with open(outpath, 'a') as outcsv:
                #     writer = csv.writer(outcsv,  delimiter = '\t', lineterminator='\n')
                #     writer.writerow(['Project', 'Date', 'Action', 'ArticleName', 'OldQual',
                #                      'NewQual', 'OldImp', 'NewImp', 'NewArticleName', 'OldArticleLink', 'OldTalkLink'])
                #     for item in project_assessment:
                #         item = [x.encode('utf-8') for x in item]
                #         writer.writerow([item[0], item[1], item[2], item[3], item[4], item[5], item[6],
                #                         item[7], item[8], item[9], item[10]])
                # outcsv.close()

