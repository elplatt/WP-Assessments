# Project: Identifying shocks in Wikipedia projects
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: run
# Date: 10/16/16

from TemporalContinuity import temporalcontinuity
from GradeContinuity import gradecontinuity

if __name__ == '__main__':

    # threshold = 60
    # timegap = temporalcontinuity(threshold)
    # timegap.timerange()

    multiple = gradecontinuity()
    multiple.repeatedEntires()

    # grade = gradecontinuity()
    # grade.gradejump()


