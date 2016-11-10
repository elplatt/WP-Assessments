# Project: Identifying shocks in Wikipedia projects
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: csvReader
# Date: 10/16/16

import pandas as pd

class csvreader (object):

    def __init__(self, file):
        self.file = file

    def readcsv(self):
        projectData = pd.read_csv(self.file)
        return projectData

    def readcsv(self,sep):
        if self.sep == "\t":
            projectData = pd.read_csv(self.file, sep="\t")
            return projectData