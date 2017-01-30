'''
Created on Jan 27, 2017

@author: haoxsun
'''
from openpyxl import load_workbook

wb = load_workbook("../../../data/RAIND1.xlsx")
print(wb.get_sheet_names())

# import os 
# dir_path = os.path.dirname(os.path.realpath(__file__))
# print(dir_path)

#from openpyxl.compat import range
#from openpyxl.utils import get_column_letter
ws = wb.worksheets[0]
columns = ["B", "K", "M"]
column_names = []
for name in columns:
    column_names.append(ws[name + "1"].value)
    
print(column_names)


# wb = Workbook()

# ws = wb.active

class ExcelReader(object):
    '''
    classdocs
    '''


    def __init__(self, params):
        '''
        Constructor
        '''
        