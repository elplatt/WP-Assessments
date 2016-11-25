# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: seed
# Date: 11/16/16

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from Seeder import seeder
from continuity.FileSystem import filesystem

if __name__ == '__main__':

    latest = seeder(filesystem.ProjectTSV)
    latest.getLatestTime()
