# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: replaceWS
# Date: 11/24/16

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from continuity.csvReader import csvreader
from continuity.FileSystem import filesystem

import re

class replacews(object):
    def __init__(self, filepath):
        dataframe = csvreader(filepath)
        self.projectData = dataframe.readtable('\t')
        self.filepath = filepath

    def replace(self):
        for i in self.projectData.index.tolist():
            self.projectData.loc[i, 'Title'] = re.sub(' ', '_', self.projectData.loc[i, 'Title'])

        self.projectData.to_csv(self.filepath, index=False, sep='\t', encoding='utf-8')

if __name__ == '__main__':

    replaced = replacews(filesystem.ProjectTSV)
    replaced.replace()
