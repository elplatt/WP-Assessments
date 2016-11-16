# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: crawler
# Date: 11/10/16

from continuity.FileSystem import filesystem
from continuity.csvReader import csvreader
from bs4 import BeautifulSoup
from dateutil.parser import parse
import urllib
import os


class crawler (object):

    def __init__(self,filepath):

        dataframe = csvreader(filepath)
        projectData = dataframe.readtable('\t')
        self.projects = projectData["Title"].tolist()
        self.limit = 1500

    def getLatestTime(self):

        crawled = filesystem.Data
        for file in os.listdir(crawled):
            if ".csv" in file:
                filename = os.path.join(crawled, file)
                csvfile = csvreader(filename)
                data = csvfile.readcsv()
                for i in data.index.tolist():
                    data.loc[i, 'Date'] = parse(data.ix[i]['Date'])
                data = data.sort('Date')

                history = filesystem.HistoryFileSystem
                outpath = os.path.join(history, file)
                fileout = open(outpath, 'w')
                fileout.write('LatestTimeStamp' + '\n')
                fileout.write(str(data['Date'].tolist()[-1])+'\n')
                fileout.close()

    def crawl(self):

        for project in self.projects:
            url = "https://en.wikipedia.org/w/index.php?title=Wikipedia:Version_1.0_Editorial_Team/" \
                + project + "_articles_by_quality_log&offset=&limit=" + str(self.limit) +"&action=history"

            handle = urllib.urlopen(url)
            gunk = handle.read()
            soup = BeautifulSoup(gunk,'lxml')
            logs = soup.findAll("a", {"class": "mw-changeslist-date"})
            date_dict = {}
            for log in logs:
                link = log.get('href')
                logURL = "https://en.wikipedia.org/" + link
                date = parse(log.get_text().encode('utf-8').split(',')[1])
                date_dict[date] = logURL
            date_dict = {x[0]: x[1] for x in sorted(date_dict.items(), key=lambda t: t[0])}

            history = filesystem.HistoryFileSystem
            project_file = project + '.csv'
            project_hist = os.path.join(history, project_file)
            history_data = csvreader(project_hist)
            lastdate = parse(history_data[-1])

            required = {k: v for k, v in date_dict.iteritems() if k >= lastdate}

            for key, value in required.iteritems():
                requiredURL = value
                requiredHandle = urllib.urlopen(requiredURL)
                requiredGunk = requiredHandle.read()




















