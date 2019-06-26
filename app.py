import sys
import os
import oci
import cx_Oracle
import csv
import ast
import psycopg2



os.environ['TNS_ADMIN'] = 'wallet/'
connection = cx_Oracle.connect('admin', 'WElcome_12345_', 'jokerdb_medium')

f = open('data/demo2.csv', 'r')
reader = csv.reader(f)

longest, headers, type_list = [], [], []

datatypes = []

def dataType(val, current_type):
    try:
        # Evaluates numbers to an appropriate type, and strings an error
        t = ast.literal_eval(val)
    except ValueError:
        return 'varchar2(50)'
    except SyntaxError:
        return 'varchar2(50)'
    if type(t) in [int, float]:
        if (type(t) in [int]) and current_type not in ['float', 'varchar']:
            if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                return 'number(10)'
            elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                return 'number(10)'
            else:
                return 'bigint'
        if type(t) is float and current_type not in ['varchar']:
            return 'float(2)'
    else:
        return 'varchar2(50)'



for row in reader:
    if len(headers) == 0:
        headers = row
        for col in row:
            longest.append(0)
            type_list.append('')
    else:
        for i in range(len(row)):
            # NA is the csv null value
            if type_list[i] == 'varchar' or row[i] == 'NA':
                pass
            else:
                var_type = dataType(row[i], type_list[i])
                type_list[i] = var_type
        if len(row[i]) > longest[i]:
            longest[i] = len(row[i])
f.close()

statement = 'create table data2 ('

for i in range(len(headers)):
    if type_list[i] == 'varchar':
        statement = (statement + '\n{} varchar({}),').format(headers[i].lower().replace(" ","_"), str(longest[i]))
    else:
        statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower().replace(" ","_").replace("-","_"), type_list[i])

statement = statement[:-1] + ')'

cursor = connection.cursor()

#print(statement)

#executre create table statement
#cursor.execute(statement)
connection.commit()

# try:
#     cursor.execute(statement)
#     connection.commit()
#     # cursor.close()
#     # connection.close()
#     print("Table created!")
# except:
#     print("Error!")


header_row = []



def checkX(x):
    try:
        output = (type(ast.literal_eval(x)))
        output = x
        return output
        #values = map((lambda x: ''+x+''), col)
    except SyntaxError:
        output = '"'+x+'"'
        return output
    except ValueError:
        output = '"'+x+'"'
        return output
    except TypeError:
        output = '"'+x+'"'
        return output





with open ('data/demo2.csv', 'r') as f:
    reader = csv.reader(f)
    header = next(reader)
    headers = map((lambda x: ''+x+''), header)
    insert = 'INSERT INTO DATA2 (' + ",".join(headers).lower().lstrip().replace(" ","_").replace("-","_") + ") VALUES "
    #print(insert)
    # for row in reader:
    #     values = map((lambda x: '"'+x+'"'), row)
    #     #statement = statement + (insert +"("+ ", ".join(values) +");" )
    #     statement = insert +"("+ ", ".join(values) +")"
    #     print(statement)
    #     cursor.execute(statement)
    lines = f.readlines()[1:]
    for row in lines:
        for col in row:
            output = checkX(row)
            #print(output)
            statement =  insert + '(' + output +',);'
            print(statement)

        # statement = insert +"("+ ", ".join(values) +")"
        # #print(statement)
        # #cursor.execute(statement)

print("end")

# connection.commit()
# cursor.close()
# connection.close()
# print("Data inserted!")