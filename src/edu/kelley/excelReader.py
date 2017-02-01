'''
Created on Jan 27, 2017

@author: haoxsun
'''
from openpyxl import load_workbook
from openpyxl.utils import coordinate_from_string, column_index_from_string
from pymongo import MongoClient

class ExcelReader(object):
    '''
    classdocs
    '''
    def __init__(self, fileLocation):
        '''
        Constructor
        '''
        self.fileLocation = fileLocation
        
    def loadLnkFile(self):
        # "../../../data/RAIND1.xlsx"
        wb = load_workbook(self.fileLocation)

        ws = wb.get_sheet_by_name("raind1")
        columns = ["B", "K", "M"]
        row_counter = 0

        # Initiate the mongodb client
        client = MongoClient("localhost", 27017)
        db = client.gadb
        linkCollection = db.linkCollection
        # dataRecords = []
        
        for row in ws.iter_rows(row_offset=1):

            row_counter = row_counter + 1
            if row[column_index_from_string(columns[0])-1].value is not None:
                dateStr = '{:%Y-%m-%d}'.format(row[column_index_from_string(columns[0])-1].value)
                CIKStr = row[column_index_from_string(columns[1])-1].value
                url = row[column_index_from_string(columns[2])-1].value
                result = linkCollection.update({'Date':dateStr, 'CIK': CIKStr}, 
                                      {"$set": {'Date':dateStr, 'CIK': CIKStr, 'URL':url}}, upsert = True)
                print("%s %s %s"%(dateStr, CIKStr, url))
                
                #print(ws[columns[0] + row] + " " + ws[columns[1] + row] + " "+ ws[columns[2] + row])
        print("This sheet has %d rows" % (row_counter))
        

linkFileReader = ExcelReader( "../../../data/RAIND1.xlsx")
linkFileReader.loadLnkFile()