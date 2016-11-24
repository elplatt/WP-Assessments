# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: seeder
# Date: 11/16/16

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from continuity.FileSystem import filesystem
from continuity.csvReader import csvreader
from dateutil.parser import parse
import os


class seeder(object):
    def __init__(self, filepath):

        dataframe = csvreader(filepath)
        projectData = dataframe.readtable('\t')
        self.projects = projectData["Title"].tolist()
        self.limit = 1500

    def getLatestTime(self):

        crawled = filesystem.Data
        for file in os.listdir(crawled):
            if ".csv" in file:
                print file
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

                if not data.empty:
                    fileout.write(str(data['Date'].tolist()[-1]) + '\n')
                fileout.close()
