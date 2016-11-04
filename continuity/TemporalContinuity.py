# Project: Identifying shocks in Wikipedia projects
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: TemporalContinuity
# Date: 10/16/16

import os
import re

import pandas as pd
from dateutil.parser import parse

from continuity.FileSystem import filesystem


class temporalcontinuity (object):

    def __init__(self, threshold):

        self.threshold = threshold
        io = filesystem()
        self.filesystem = io.Data
        self.outdir = io.ContFileSystem

    def timerange(self):

        for file in os.listdir(self.filesystem):
            if '.csv' in file:

                required = []
                inputfile = os.path.join(self.filesystem, file)
                projectData = pd.read_csv(inputfile)
                datedict = {}

                for i in projectData.index.tolist():
                    datedict[i] = parse(projectData.ix[i]['Date'])
                dates = sorted(datedict.items(), key=lambda x: x[1])

                for i in range(len(dates)-1):
                    diff = (dates[i+1][1] - dates[i][1]).days
                    if diff >= self.threshold:
                        required.append([str(projectData.ix[dates[i][0]]['Project']),
                                         str(re.sub(',', ' ', projectData.ix[dates[i][0]]['Date'])),
                                         str(re.sub(',', ' ', projectData.ix[dates[i+1][0]]['Date']))])

                outpath = os.path.join(self.outdir, file)
                fileout = open(outpath, 'w')
                fileout.write("Project" + "," + "From" + "," + "To" + "\n")
                for element in required:
                    fileout.write(",".join(element) + '\n')
                fileout.close()
















