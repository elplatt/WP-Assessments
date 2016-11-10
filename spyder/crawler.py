# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: crawler
# Date: 11/10/16

from csvReader import csvreader
from bs4 import BeautifulSoup
import urllib


class crawler (object):

    def __init__(self,filepath):

        dataframe = csvreader(self.filepath)
        projectData = dataframe.readcsv("\t")
        self.projects = projectData["Title"].tolist()
        self.limit=1500

    def crawl(self):

        for project in self.projects:
            url = "https://en.wikipedia.org/w/index.php?title=Wikipedia:Version_1.0_Editorial_Team/" \
                + project + "_articles_by_quality_log&offset=&limit=" + self.limit +"&action=history"

            handle = urllib.urlopen(url)
            gunk = handle.read()

            print gunk
            break










