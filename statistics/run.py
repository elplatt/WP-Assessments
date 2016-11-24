# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: run
# Date: 11/4/16

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from JumpStat import jumpstat

if __name__ == '__main__':

    stat = jumpstat()
    stat.statistics()