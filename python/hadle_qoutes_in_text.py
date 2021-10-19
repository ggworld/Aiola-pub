import csv
with open('/Users/geva/Downloads/ZSD_OH01_202109.csv') as csv_file, open('/Users/geva/Documents/cust/dura/csv/out1.csv', 'w') as fout:
    csv_reader = csv.reader(csv_file, delimiter=',')
    csv_writer = csv.writer(fout, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for n,row in enumerate(csv_reader):   
        csv_writer.writerow(row)
