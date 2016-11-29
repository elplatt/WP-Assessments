# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: CreateHeader
# Date: 11/25/16

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from continuity.FileSystem import filesystem
import pandas as pd

class createheader (object):

    def __init__(self, filepath):
        pass

    def create(self):

        df = pd.DataFrame(columns=['Project', 'Date', 'Action', 'ArticleName', 'OldQual',
                                     'NewQual', 'OldImp', 'NewImp', 'NewArticleName', 'OldArticleLink', 'OldTalkLink'])

        for project in os.listdir(filesystem.Data):
            outpath = os.path.join(filesystem.CrawledFileSystem, project.split('.')[0]+'.tsv')
            df.to_csv(outpath, sep='\t', encoding='utf-8', index=False)

if __name__ == '__main__':

    header = createheader(filesystem.ProjectTSV)
    header.create()