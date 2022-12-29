#!/usr/bin/python3
"""
Program that extracts data from flat_out data files and creates csv files under raw data

Authors:
1. Ravikiran Jois Yedur Prabhakar
2. Karanjit Singh
3. Suhas Vijayakumar
Yes they are the original authors but their code is so shitty i changed nearly all relevant things in this
Seriously did the authors even run this code once???? They should have recognized how bad it is
"""


import glob
import threading


class myThread (threading.Thread):
   def __init__(self, path):
      threading.Thread.__init__(self)
      self.path = path
   def run(self):
      print ("Starting " + self.path)
      parse_file(self.path)
      print ("Exiting " + self.path)

def parse_file(path):
    file_name=path[18:]
    size = len(file_name)
    file_name = file_name[:size-4]
    table_name=file_name.strip('.')
    try:
        with open(path) as data:
            rows=data.readlines()
            csv_rows=[]
            for row in rows:
                csv_row=row.replace('|','","')
                csv_row = '"' + csv_row + '"'
                csv_rows.append(csv_row)
            with open('../benchmark/csv_data/'+table_name+'.csv','w') as output:
                output.writelines(csv_rows)
    except:
        print('File: ' + file_name + ' not found')



threads = []
for file in glob.glob("../TPC-E/flat_out/*.txt"):
    thread = myThread(file)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

