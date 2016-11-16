# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: seed
# Date: 11/16/16

from Seeder import seeder

if __name__ == '__main__':

    latest = seeder("/Users/ishan/Desktop/WikiProject/projects.tsv")
    latest.getLatestTime()



