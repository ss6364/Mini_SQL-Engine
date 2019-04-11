import csv
import sys
import re
from collections import OrderedDict

def main():
	dictionary = {}
	readMetadata(dictionary)
	processQuery(str(sys.argv[1]),dictionary)

def readMetadata(dictionary):
	f = open('./metadata.txt','r')
	check = 0
	for line in f:
		if line.strip() == "<begin_table>":
			check = 1
			continue
		if check == 1:
			tableName = line.strip()
			dictionary[tableName] = [];
			check = 0
			continue
		if not line.strip() == '<end_table>':
			dictionary[tableName].append(line.strip());		

def processQuery(query,dictionary):
	query = (re.sub(' +',' ',query)).strip();

	if "from" in query:
		obj1 = query.split('from');
	else:
		sys.exit("Incorrect Syntax")

	obj1[0] = (re.sub(' +',' ',obj1[0])).strip();

	if "select" not in obj1[0].lower():
		sys.exit("Incorrect Syntax")
	object1 = obj1[0][7:]

	object1 = (re.sub(' +',' ',object1)).strip();
	l = []
	l.append("select")

	if "distinct" in object1 and "distinct(" not in object1:
		object1 = object1[9:]
		l.append("distinct")

	l.append(object1)
	object1 = l 

	# select distinct List<colnames> from <table>
	object3 = ""
	if "distinct" in object1[1] and "distinct(" not in object1[1]:
		object3 = object1[1];
		object3 = (re.sub(' +',' ',object3)).strip()
		object1[1] = object1[2]

	colStr = object1[1];
	colStr = (re.sub(' +',' ',colStr)).strip()
	columnNames = colStr.split(',');
	for i in columnNames:
		columnNames[columnNames.index(i)] = (re.sub(' +',' ',i)).strip();

	obj1[1] = (re.sub(' +',' ',obj1[1])).strip();
	object2 = obj1[1].split('where');
	
	tableStr = object2[0]
	tableStr = (re.sub(' +',' ',tableStr)).strip();
	tableNames = tableStr.split(',')
	for i in tableNames:
		tableNames[tableNames.index(i)] = (re.sub(' +',' ',i)).strip();
	for i in tableNames:
		if i not in dictionary.keys():
			sys.exit("Table not found")

	if len(object2) > 1 and len(tableNames) == 1:
		object2[1] = (re.sub(' +',' ',object2[1])).strip();
		processWhere(object2[1],columnNames,tableNames,dictionary)
		return
	elif len(object2) > 1 and len(tableNames) > 1:
		object2[1] = (re.sub(' +',' ',object2[1])).strip();
		processWhereJoin(object2[1],columnNames,tableNames,dictionary)
		return

	if(len(tableNames) > 1):
		join(columnNames,tableNames,dictionary)
		return

	if object3 == "distinct":
		distinctMany(columnNames,tableNames,dictionary)
		return
	
	if len(columnNames) == 1:
		#aggregate -- Assuming (len(columnNames) == 1) i.e aggregate function
		for col in columnNames:
			if '(' in col and ')' in col:
				funcName = ""
				colName = ""
				a1 = col.split('(');
				funcName = (re.sub(' +',' ',a1[0])).strip()
				colName = (re.sub(' +',' ',a1[1].split(')')[0])).strip()
				aggregate(funcName,colName,tableNames[0],dictionary)
				return
			elif '(' in col or ')' in col:
				sys.exit("Syntax error")

	selectColumns(columnNames,tableNames,dictionary);

def processWhere(whereStr,columnNames,tableNames,dictionary):
	a = whereStr.split(" ")

	# print a

	if(len(columnNames) == 1 and columnNames[0] == '*'):
		columnNames = dictionary[tableNames[0]]

	printHeader(columnNames,tableNames,dictionary)

	tName = tableNames[0] + '.csv'
	fileData = []
	readFile(tName,fileData)

	check = 0
	for data in fileData:
		string = evaluate(a,tableNames,dictionary,data)
		for col in columnNames:
			if eval(string):
				check = 1
				print data[dictionary[tableNames[0]].index(col)],
		if check == 1:
			check = 0
			print

def evaluate(a,tableNames,dictionary,data):
	string = ""
	for i in a:
		# print i
		if i == '=':
			string += i*2
		elif i in dictionary[tableNames[0]] :
			string += data[dictionary[tableNames[0]].index(i)]
		elif i.lower() == 'and' or i.lower() == 'or':
			string += ' ' + i.lower() + ' '
		else:
			string += i
		# print string
	return string

def processWhereJoin(whereStr,columnNames,tableNames,dictionary):
	tableNames.reverse()

	l1 = []
	l2 = []
	readFile(tableNames[0] + '.csv',l1)
	readFile(tableNames[1] + '.csv',l2)

	fileData = []
	for item1 in l1:
		for item2 in l2:
			fileData.append(item2 + item1)

	# dictionary["sample"] = dictionary[b] + dictionary[a]
	dictionary["sample"] = []
	for i in dictionary[tableNames[1]]:
		dictionary["sample"].append(tableNames[1] + '.' + i)
	for i in dictionary[tableNames[0]]:
		dictionary["sample"].append(tableNames[0] + '.' + i)

	dictionary["test"] = dictionary[tableNames[1]] + dictionary[tableNames[0]]

	tableNames.remove(tableNames[0])
	tableNames.remove(tableNames[0])
	tableNames.insert(0,"sample")

	if(len(columnNames) == 1 and columnNames[0] == '*'):
		columnNames = dictionary[tableNames[0]]

	# print header
	for i in columnNames:
		print i,
	print

	a = whereStr.split(" ")

	# check = 0
	# for data in fileData:
	# 	string = evaluate(a,tableNames,dictionary,data)
	# 	for col in columnNames:
	# 		if eval(string):
	# 			check = 1
	# 			print data[dictionary[tableNames[0]].index(col)],
	# 	if check == 1:
	# 		check = 0
	# 		print

	check = 0
	for data in fileData:
		string = evaluate(a,tableNames,dictionary,data)
		for col in columnNames:
			if eval(string):
				check = 1
				if '.' in col:
					print data[dictionary[tableNames[0]].index(col)],
				else:
					print data[dictionary["test"].index(col)],
		if check == 1:
			check = 0
			print

	del dictionary['sample']

def selectColumns(columnNames,tableNames,dictionary):

	if len(columnNames) == 1 and columnNames[0] == '*':
		columnNames = dictionary[tableNames[0]]

	for i in columnNames:
		if i not in dictionary[tableNames[0]]:
			sys.exit("error")

	printHeader(columnNames,tableNames,dictionary)

	tName = tableNames[0] + '.csv'
	fileData = []
	readFile(tName,fileData)
	
	printData(fileData,columnNames,tableNames,dictionary)

def aggregate(func,columnName,tableName,dictionary):

	if columnName == '*':
		sys.exit("error")
	if columnName not in dictionary[tableName]:
		sys.exit("error")

	tName = tableName + '.csv'
	fileData = []
	readFile(tName,fileData)
	colList = []
	for data in fileData:
		colList.append(int(data[dictionary[tableName].index(columnName)]))

	if func.lower() == 'max':
		print max(colList)
	elif func.lower() == 'min':
		print min(colList)
	elif func.lower() == 'sum':
		print sum(colList)
	elif func.lower() == 'avg':
		print sum(colList)/len(colList)
	elif func.lower() == 'distinct':
		distinct(colList,columnName,tableName,dictionary);
	else :
		print "ERROR"
		print "Unknown function : ", '"' + func + '"'

def distinct(colList,columnName,tableName,dictionary):
	print "OUTPUT :"
	string = tableName + '.' + columnName
	print string
	
	colList = list(OrderedDict.fromkeys(colList))
	for col in range(len(colList)):
		print colList[col]

def distinctMany(columnNames,tableNames,dictionary):
	printHeader(columnNames,tableNames,dictionary)

	temp = []
	check = 0
	for tab in tableNames:
		tName = tab + '.csv'
		with open(tName,'rb') as f:
			reader = csv.reader(f)
			for row in reader:
				for col in columnNames:
					x = row[dictionary[tableNames[0]].index(col)]
					if x not in temp:
						temp.append(x)
						check =1
						print x,
				if check == 1 :
					check = 0
					print

def join(columnNames,tableNames,dictionary):
	tableNames.reverse()

	l1 = []
	l2 = []
	readFile(tableNames[0] + '.csv',l1)
	readFile(tableNames[1] + '.csv',l2)

	fileData = []
	for item1 in l1:
		for item2 in l2:
			fileData.append(item2 + item1)

	# dictionary["sample"] = dictionary[b] + dictionary[a]
	dictionary["sample"] = []
	for i in dictionary[tableNames[1]]:
		dictionary["sample"].append(tableNames[1] + '.' + i)
	for i in dictionary[tableNames[0]]:
		dictionary["sample"].append(tableNames[0] + '.' + i)

	dictionary["test"] = dictionary[tableNames[1]] + dictionary[tableNames[0]]
	# print dictionary["test"]

	tableNames.remove(tableNames[0])
	tableNames.remove(tableNames[0])
	tableNames.insert(0,"sample")

	if(len(columnNames) == 1 and columnNames[0] == '*'):
		columnNames = dictionary[tableNames[0]]

	# print header
	for i in columnNames:
		print i,
	print

	# printData(fileData,columnNames,tableNames,dictionary)

	for data in fileData:
		for col in columnNames:
			if '.' in col:
				print data[dictionary[tableNames[0]].index(col)],
			else:
				print data[dictionary["test"].index(col)],
		print

	# del dictionary[tableNames[0]]

def printHeader(columnNames,tableNames,dictionary):
	
	print "OUTPUT : "
	# Table headers
	string = ""
	for col in columnNames:
		for tab in tableNames:
			if col in dictionary[tab]:
				if not string == "":
					string += ','
				string += tab + '.' + col
	print string

def printData(fileData,columnNames,tableNames,dictionary):
	for data in fileData:
		for col in columnNames:
			print data[dictionary[tableNames[0]].index(col)],
		print

def readFile(tName,fileData):
	with open(tName,'rb') as f:
		reader = csv.reader(f)
		for row in reader:
			fileData.append(row)

if __name__ == "__main__":
	main()
