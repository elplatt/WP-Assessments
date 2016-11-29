# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: Project
# Date: 11/29/16

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from continuity.FileSystem import filesystem

class project(object):

    def __init__(self):
        pass

    @staticmethod
    def list():

        filein = open(filesystem.ProjectTSV, 'w')
        filein.write('Title'+'\n')

        for file in os.listdir(filesystem.Data):
            if '.csv' in file:
                name = file.split('.')
                filein.write(name[0] + '\n')
        filein.close()

if __name__ == '__main__':

    project.list()

