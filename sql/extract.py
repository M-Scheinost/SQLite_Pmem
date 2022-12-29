"""
Program that extracts data from flat_out data files and creates csv files under raw data

Authors:
1. Ravikiran Jois Yedur Prabhakar
2. Karanjit Singh
3. Suhas Vijayakumar
Yes they are the original authors but their code is so shitty i changed nearly all relevant things in this
"""


import glob
#import sqlite3

# conn=sqlite3.connect('tpce')
# cur=conn.cursor()

for file in glob.glob("../TPC-E/flat_out/*.txt"):
    file_name=file[18:]
    file_name = file_name.strip('.txt')
    print(file_name)
    table_name=file_name.strip('.')
    try:
        with open(file) as data:
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
