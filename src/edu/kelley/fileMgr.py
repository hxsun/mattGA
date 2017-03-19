'''
Created on Feb 10, 2017

@author: haoxsun
'''
# import urllib.request
# import shutil
from pymongo import MongoClient
import os
import logging
import ssl
##from time import time
# import gzip
import requests
# from lxml import html
from bs4 import BeautifulSoup
# from pip._vendor.distlib.locators import Page
import re
from re import RegexFlag
from _ast import Num
import locale
import pandas as pd
import numpy as np
# import sys


logger = logging.getLogger(__name__)
logging.getLogger('requests').setLevel(logging.CRITICAL)

logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('fileMgr_error.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

SEC_DOMAIN = "https://www.sec.gov"
FOLDER_LOCATION = "../../../data/download/"
REGEXP = r"schedule II[^I]"
compiled_re = re.compile(REGEXP, RegexFlag.IGNORECASE | RegexFlag.UNICODE | RegexFlag.MULTILINE)
NON_DIGIT = r"[^\d.,]"
compile_nondigit_re = re.compile(NON_DIGIT, RegexFlag.IGNORECASE | RegexFlag.UNICODE | RegexFlag.MULTILINE)
# Download the file from `url` and save it locally under `file_name`:
def downloadFile():
    
    client = MongoClient("localhost", 27017)
    db = client.gadb
    linkCollection = db.linkCollection
    
    context = ssl._create_unverified_context()
    
    os.chdir(FOLDER_LOCATION)
    companyFKeys = linkCollection.find({"companyFKey": {"$in": ["0000001923", "0000002098"]}}).distinct("companyFKey")
    #companyFKeys = linkCollection.distinct("companyFKey")
    folderList = os.listdir()
    for key in companyFKeys:
        logger.debug("companyFKey: " + key)
        if (key not in folderList):
            logger.debug("Creating a new folder named: " + key)
            os.mkdir(key)
        os.chdir(key)
        #logger.debug("Change to dir: " + key)
        docs = linkCollection.find( {'companyFKey': key} )
        #fileNames = os.listdir()
        for doc in docs:
            urls = doc['10KURL']
            for url in urls:
                #logger.debug("Original url: " + url)
                completeUrl = SEC_DOMAIN + url
                response = requests.get(completeUrl)
                response.encoding = "UTF-8"
                if (url.endswith("htm")):
                    proc10KHTMLFile(response)
                elif (url.endswith("txt")):
                    proc10KTXTFile(response)
                else:
                    logger.debug("Unhandled file: " + url)
                
                
#                 fileName = fileNameGenerator(url, doc['periodOfReport'])
#                 if (fileName not in fileNames and ((fileName + ".gz") not in fileNames)):
#                     logger.debug("Creating compressed file named: " + fileName + ".gz")
#                     with urllib.request.urlopen(completeUrl, context=context) as response, gzip.open(fileName + ".gz", 'wb') as out_file:
#                         shutil.copyfileobj(response, out_file)
#                 else:
#                     logger.debug("File already exists: " + fileName)
#                     with gzip.open(fileName + ".gz", 'rb') as f:
#                         file_content = f.read()
#                     if (fileName.endswith("htm.gz")):
#                         proc10KHTMLFile(file_content)
#                     elif (fileName.endswith("txt.gz")):
#                         proc10KTXTFile(file_content)
                #logger.debug(fileName + " deleted.")
        os.chdir("..")
        client.close()

def proc10KHTMLFile(response):
    soup = BeautifulSoup(response.content, 'lxml')
    schedule2Nodes = soup.body.findAll(text=compiled_re)
    for bNode in schedule2Nodes:
        bNode = bNode.parent
        while (bNode.name is None or bNode.name != "p"):
            #logger.debug(bNode.name)
            bNode = bNode.parent
        else:
            bNode = bNode.next_sibling
            while bNode.name is None or (bNode.name != "center" and bNode.name != "div"):
                bNode = bNode.next_sibling
            else:
                #logger.debug(str(str(bNode.prettify()).encode(encoding='utf_8', errors='strict')))
                #logger.debug(str(bNode.find("table")[0]))
                transferHtmlTable2Arrays(bNode.find("table"))
        #logger.debug("tagName: " + str(str(bNode.parent).encode(encoding='utf_8', errors='strict')))
        #leftPNode = bNode.parent.parent.parent
        #table = leftPNode.next_sibling.next_sibling
        
    #logger.debug("If the page has 'SCHEDULE II':" + str(str(soup.body.findAll(text=compiled_re)).encode("UTF-8")))
    # schedule2Nodes = soup.xpath("//p[b[contains(text(), 'SCHEDULE II ')]]")
    
#     if (schedule2Nodes is not None and len(schedule2Nodes) > 0):
#         logger.debug(html.tostring(schedule2Nodes[0], encoding='UTF-8'))
#         tables = tree.xpath("//p[b[contains(text(), 'SCHEDULE II ')]]/following-sibling::div/table")
#         logger.debug("html table:" + html.tostring(tables[0], encoding='UTF-8'))
    #table = tree.xpath("//p[b[contains(text(), 'SCHEDULE II ')]]/following-sibling::div/table")[0]
def transferHtmlTable2Arrays(table):
    tableValues = []
    rowsWithHr = table.find_all("tr")
    hrIdx = 0
    hrCursors = []
    for row in rowsWithHr:
        hasHr = row.find_all("hr")
        if hasHr is not None and len(hasHr):
            hrCursors.append(hrIdx)
        hrIdx = hrIdx + 1
        
    procColHeaders(table.find_all("tr")[:(hrCursors[len(hrCursors) - 1] + 1)], hrCursors)
    for tr in table.find_all("tr"):
        # { rowStarts: # of first cell starts, { cell value: {starts, span}} }
        rowValues = []
        index = 0
        for td in tr.find_all("td"):
            cell = {}
            cell['startPos'] = index;
            span = 1
            if td.has_attr("colspan"):
                span = int(td['colspan'])
            cell['span'] = span
            textContent = str(td.get_text().encode(encoding='ascii', errors='ignore'))
            textContent = textContent[2:(len(textContent)-1)].strip()
            if len(textContent) != 0:
                if textContent != '$':
                    if textContent.startswith("$"):
                        textContent = textContent[1:len(textContent)].replace(",","").strip()
                    if textContent == '-':
                        textContent = '0'
                    cell['value'] = textContent
            index = index + span
            if 'value' in cell:
                hasHr = td.find_all('hr')
                if hasHr is not None and len(hasHr) > 0:
                    # If there is hr element under td, meaning it starts next row, only append
                    rowValues.append(cell)
                else:
                    if len(tableValues) > 0:
                        lastRow = tableValues[len(tableValues) - 1]
                        for cellInLastRow in lastRow:
                            if cellInLastRow['startPos'] == index and cellInLastRow['span'] == span:
                                cellInLastRow['value'] = cellInLastRow['value'] + ' ' + textContent
                                
        if len(rowValues) > 0:
            tableValues.append(rowValues)
    logger.debug(str(tableValues))
    return tableValues

def procColHeaders(headerRows, hrRowIndexArr):
    trIdx = 0
    rawTrRows = []
    cellDf = pd.DataFrame()
    for tr in headerRows:
        index = 0
        cellArr = []
        for td in tr.find_all('td'):
            cell = {}
            cell['startPos'] = index;
            span = 1
            if td.has_attr("colspan"):
                span = int(td['colspan'])
            cell['span'] = span
            textContent = str(td.get_text().encode(encoding='ascii', errors='ignore'))
            textContent = textContent[2:(len(textContent)-1)].strip()
            cell['value'] = textContent
            if ('value' in cell and cell['value'] != '') or hasElemment(td, 'hr'):
                cellArr.append(cell)
                if ((str(cell['startPos']) in cellDf.index) == False) or ((str(cell['span']) in cellDf.columns) == False):
                    cellDf.set_value(str(cell['startPos']), str(cell['span']), cell['value'])
                else:
                    origVal = cellDf.get_value(str(cell['startPos']), str(cell['span']))
                    if pd.isnull(origVal):
                        origVal = ""
                    cellDf.set_value(str(cell['startPos']), str(cell['span']), (str(origVal) + " " + cell['value']).strip())
                
            index = index + span
        if len(cellArr) > 0:
            rawTrRows.append(cellArr)
        #if len(hrRowIndexArr) > 0 and index == hrRowIndexArr[1]:
            # Merge the rawTrRows.
            
        trIdx = trIdx + 1
        cellDf.sort_index(0, 1)        
    print(cellDf)
    print(cellDf.columns.values.to_array().sort(key=float))
    print(cellDf.index.values.to_array().sort(key=float))
                    
            
def hasElemment(element, tagName):
    return element.find(tagName) is not None

def consolidateHtmlTable(tableValues):
    index = []
    numOfCols = len(tableValues)
    tablePositions = []
    rows = []
    for idxOfRow in range(1, numOfCols):
        rowPositions = []
        cellValues = []
        for row in tableValues[numOfCols - idxOfRow]['cellValues']:
            firstCol = True
            if firstCol and re.search(compile_nondigit_re, row[0]):
                index.insert(0, row[0])
                firstCol = False
            else:
                cellValues.insert(len(cellValues),float(row[0].replace(",", "")))
            rowPositions.append(row[1])
        if (len(cellValues) > 0):
            rows.insert(0, cellValues)
        tablePositions.insert(0, rowPositions)
        
    logger.debug(index)
    logger.debug(rows)
    logger.debug(tablePositions)


def proc10KTXTFile(response):
    soup = BeautifulSoup(response.content, 'lxml')
#     scheduleNodes = soup.findAll(text=re.compile("SCHEDULE II"))
    logger.debug("If the page has 'SCHEDULE II':" + str(str(soup.findAll(text=compiled_re)).encode("UTF-8")))
    #table = tree.xpath("//table[contains(text(), 'SCHEDULE II')]")[0]
    #logger.debug("text table: " + table.text_content())
    
def addHasHtmlFile():
    client = MongoClient("localhost", 27017)
    db = client.gadb
    linkCollection = db.linkCollection
    for record in linkCollection.find({}):
        hasHTML = False
        for fileLink in record['10KURL']:
            if (fileLink.endswith(".htm") or fileLink.endswith(".html")):
                hasHTML = True
                continue
        db.linkCollection.update({'_id': record['_id']}, {'$set' : {'hasHTML' : hasHTML}})
    client.close()

def main():
    downloadFile()
    #addHasHtmlFile()
    # Go thru all the 10K links in mongo db
    # with urllib.request.urlopen("http://www.gunnerkrigg.com//comics/00000001.jpg") as response, open("00000001.jpg", 'wb') as out_file:
        # shutil.copyfileobj(response, out_file)
    
def fileNameGenerator(url, dateStr):
    index = url.rindex("/")
    #actualLength = len(url) - (index + 1)
    #logger.debug("index: %d, actualLength: %d" % (index, actualLength))
    return dateStr + "_" + url[(index + 1):len(url)]


if __name__ == '__main__':
    main()