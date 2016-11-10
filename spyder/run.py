# Project: Identifying shocks in Wikipedia Project
# Author: Shailesh Vedula
# Advisors: Dr.Daniel Romero, Dr. Ceren Budak
# Affiliation: Industrial and Operations Engineering, University of Michigan, Ann Arbor
# File Name: run
# Date: 11/10/16

from Crawler import crawler

if __name__ == '__main__':

    webCrawler = crawler()
    webCrawler.crawl()