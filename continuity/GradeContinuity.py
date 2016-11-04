# Project: Identifying shocks in Wikipedia projects
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: GradeContinuity
# Date: 10/16/16

from csvReader import csvreader
from FileSystem import filesystem
from dateutil.parser import parse
import pandas as pd
import os
import re


class gradecontinuity (object):

    def __init__(self):
        io = filesystem()
        self.filesystem = io.Data
        self.outdirRepeat = io.RepeatFileSystem
        self.outdirGap = io.GapFileSystem
        self.gradelist = ["fa-class", "ga-class", "a-class", "b-class", "c-class", "start-class", "stub-class", "na"]

    def dataframe(self, filename):
        inputfile = os.path.join(self.filesystem, filename)
        csvfile = csvreader(inputfile)
        data = csvfile.readcsv()

        return data


    def repeatedEntires(self):

        wiki = gradecontinuity()
        for file in os.listdir(self.filesystem):
            if ".csv" in file:

                data = wiki.dataframe(file)
                data = data[data['Action'] == 'Reassessed']
                data['OldQual'].fillna("NA", inplace=True)
                data['NewQual'].fillna("NA", inplace=True)
                data = data.drop_duplicates()
                repeated = {}

                for i in data.index.tolist():
                    if data.ix[i]['OldQual'].lower() in self.gradelist and data.ix[i]['NewQual'].lower() in self.gradelist:
                        if (data.ix[i]['Project'], data.ix[i]['ArticleName'], data.ix[i]['Action'],
                                    data.ix[i]['OldQual'], data.ix[i]['NewQual']) not in repeated:
                            repeated[(data.ix[i]['Project'], re.sub(',', ' ', data.ix[i]['ArticleName']), data.ix[i]['Action'],
                                      data.ix[i]['OldQual'], data.ix[i]['NewQual'])] = 1
                        else:
                            repeated[(data.ix[i]['Project'], re.sub(',', ' ', data.ix[i]['ArticleName']),
                                      data.ix[i]['Action'], data.ix[i]['OldQual'], data.ix[i]['NewQual'])] += 1

                outpath = os.path.join(self.outdirRepeat, file)
                fileout = open(outpath, 'w')
                fileout.write("Project" + "," + "ArticleName" + "," + "Action" + "," + "OldQual"
                                              + "," + "NewQual" + ',' + "#Entires" + '\n')
                for key, value in repeated.iteritems():
                    if value > 1:

                        string = ','.join(str(v) for v in list(key)) + ',' + str(value)
                        fileout.write(string + '\n')
                fileout.close()

    def gradejump(self):

        pd.options.mode.chained_assignment = None
        wiki = gradecontinuity()
        for file in os.listdir(self.filesystem):
            if ".csv" in file:

                data = wiki.dataframe(file)
                data['OldQual'].fillna("NA", inplace = True)
                data['NewQual'].fillna("NA", inplace = True)

                outpath = os.path.join(self.outdirGap, file)
                fileout = open(outpath, 'w')
                fileout.write("Project" + "," + "ArticleName" + "," + "PrevDate" + "," + "PrevOldQual"
                              + "," + "PrevNewQual" + "," + "NextDate" + ',' + "NextOldQual" + ',' + "NextNewQual" +'\n')

                for name, group in data.groupby('ArticleName'):
                    for i in group.index.tolist():
                        group.loc[i,'Date'] = parse(group.ix[i]['Date'])
                    group = group.sort('Date')
                    group = group.drop_duplicates()

                    notcont = []
                    index = group.index.tolist()
                    if len(index) > 1:
                        for i in range(len(index)-1):

                            if group.ix[index[i]]['Action'] == 'Reassessed' and (group.ix[index[i]]['OldQual'].lower() in self.gradelist) \
                            and (group.ix[index[i]]['NewQual'].lower() in self.gradelist) and (group.ix[index[i+1]]['OldQual'].lower() in self.gradelist)\
                            and (group.ix[index[i+1]]['NewQual'].lower() in self.gradelist) and group.ix[index[i+1]]['Action'] == 'Reassessed':

                                if group.ix[index[i+1]]['OldQual'] != group.ix[index[i]]['NewQual']:
                                    notcont.append([group.ix[index[i]]['Project'], re.sub(',',' ',name), str(group.ix[index[i]]['Date']), group.ix[index[i]]['OldQual'],
                                                    group.ix[index[i]]['NewQual'], str(group.ix[index[i+1]]['Date']), group.ix[index[i+1]]['OldQual'],
                                                    group.ix[index[i]]['NewQual']])

                    if notcont:
                        for x in notcont:
                            x =[str(entry) for entry in x]
                            fileout.write(','.join(x) + '\n')
                fileout.close()





