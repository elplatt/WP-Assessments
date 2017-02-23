# Project: Identifying shocks in Wikipedia projects
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: AbstractIO
# Date: 10/16/16

columns = [
    'Project', 'Date', 'Action', 'ArticleName', 'OldQual',
    'NewQual', 'OldImp', 'NewImp', 'NewArticleName', 'OldArticleLink',
    'OldTalkLink'
]

Data = "/Users/ishan/Desktop/WikiProject/Data1"

ContFileSystem = "/mnt/turbo/dromdata/wikipedia/wiki-code/output/Time"

RepeatFileSystem = "/mnt/turbo/dromdata/wikipedia/wiki-code/output/Repeated"

GapFileSystem = "/mnt/turbo/dromdata/wikipedia/wiki-code/output/Gap"

CrawledFileSystem = "/Users/ishan/Desktop/WikiProject/Crawled"

HistoryFileSystem = "/Users/ishan/Desktop/WikiProject/History"

ProjectTSV = "/Users/ishan/Desktop/WikiProject/projects1.tsv"

# ProjectTSV = "/mnt/turbo/dromdata/wikipedia/projects_modified.tsv"

LogFileSystem = "/Users/ishan/Desktop/WikiProject/LOG"

CrawlLogFileSystem = "/Users/ishan/Desktop/WikiProject/Crawl-Log"

CrawlHTMLFileSystem = "/Users/ishan/Desktop/WikiProject/Crawl-HTML"

OutputFileSystem = "/Users/ishan/Desktop/WikiProject/output"
