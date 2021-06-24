
with open(“Quote-Order Int.XML”) as xml_file
    data_dict = xmltodict.parse(xml_file.read())
#assuming there are redundent hirarchy of INPUT leve 
df = pd.DataFrame.from_dict(data_dict[‘INPUT’][‘ITEMS’])
