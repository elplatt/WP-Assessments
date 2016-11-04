# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: RepeatedStat
# Date: 11/4/16

from continuity.csvReader import csvreader
from continuity.FileSystem import filesystem
import rpy2.robjects.lib.ggplot2 as ggplot2
import pandas as pd
import os

class jumpstat (object):

    def __init__(self):
        pass

    def statistics (self):

        dir = filesystem()
        repeated = dir.GapFileSystem
        jumpCount = {}

        for file in os.listdir(repeated):
            if ".csv" in file:
                filepath = os.path.join(repeated,file)
                Istream = csvreader(filepath)
                data = Istream.readcsv()

                if not data.empty:
                    for i in data.index.tolist():
                        tup = (data.ix[i]['PrevOldQual'], data.ix[i]['PrevNewQual'], data.ix[i]['NextOldQual'], data.ix[i]['NextNewQual'])
                        if tup not in jumpCount:
                            jumpCount[tup] = 1
                        else:
                            jumpCount[tup] += 1

        df = pd.DataFrame(jumpCount.items(), columns = ['Transition','Count'])













