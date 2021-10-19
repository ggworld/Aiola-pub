import csv
with open('/Users/geva/Downloads/ZSD_OH01_202109.csv') as csv_file, open('/Users/geva/Documents/cust/dura/csv/ph1.csv', 'w') as fout:
    for myl in csv_file :
        t = myl.replace('""','"')
        t = t.replace('TUCKERSMITH 1.25", 1", 3/4" SDR 13.5','TUCKERSMITH 1.25~, 1~, 3/4~ SDR 13.5')
        fout.write(t)

with open('/Users/geva/Documents/cust/dura/csv/ph1.csv') as csv_file, open('/Users/geva/Documents/cust/dura/csv/ph2.csv', 'w') as fout:
    csv_reader = csv.reader(csv_file, delimiter=',')
    csv_writer = csv.writer(fout, delimiter=',', quotechar='|',quoting=csv.QUOTE_ALL)
    for n,row in enumerate(csv_reader):   
        csv_writer.writerow(row)
