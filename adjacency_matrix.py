'''
A program that multiplies the adjacency matrix of a graph once. DBMS used is vertica.
Author: Sikder Tahsin Al-Amin
'''


import vertica_python
import string
import sys
import math
import time


######connection string to vertica #####
try:
    conn_info = {'host':'hostname/ip',
                 'port': 5433,
                 'user': 'username',
                 'password': 'password',
                 'database':'database_name',
                 'read_timeout': 60000000,
                 'unicode_error': 'strict',
                 'ssl': False,
                 'connection_timeout': 50000
                 }
 
    # simple connection, with manual close
    connection = vertica_python.connect(**conn_info)
    cur = connection.cursor()
 
except:
    print("Database connection error")
    sys.exit()

sys.argv = ["adjacency_matrix.py", "dataset=cliquegeometric.csv"]  ##not needed when running from commandline

####check the command line arguments ####
if len(sys.argv) != 2:
    print("Not correct arguments. Call by script_name.py dataset=xyz.csv");
    sys.exit()

arg4=sys.argv[1]
arg4=arg4.split('=')
input_dataset=arg4[1] ##name of the dataset


#####initialize variables ####
#group_by_flag='N'
file = open("adjmat.sql","w")
tc_table='R'

#####drop table######
def drop_table(table_name):
    sql_string="DROP TABLE IF EXISTS "+table_name+" ;"
    cur.execute(sql_string)
    file.write(sql_string+'\n')
 
####Create E####
drop_table('E')
sql_string="CREATE TABLE E (i int ENCODING RLE, j int ENCODING RLE, v int ENCODING RLE, PRIMARY KEY(i,j)) PARTITION BY i;"
#sql_string="CREATE TABLE E (i int , j int , v int , PRIMARY KEY(i,j)) ;"  ##not optimized
cur.execute(sql_string)
file.write(sql_string+'\n')

####Load Dataset#####
print("loading table...")
sql_string="COPY E FROM '/home/vertica/tahsin/TC_programs/"+input_dataset+"' parser fcsvparser();"
cur.execute(sql_string)
file.write(sql_string+'\n')

###maintaiining 2nd versions of E
drop_table('E2')
sql_string="CREATE TABLE E2 (i int ENCODING RLE, j int ENCODING RLE, v int ENCODING RLE, PRIMARY KEY(i,j)) PARTITION BY j;"
#sql_string="CREATE TABLE E2 (i int , j int , v int , PRIMARY KEY(i,j)) ;"
cur.execute(sql_string)
file.write(sql_string+'\n')
sql_string="INSERT INTO E2 SELECT i AS i, j AS j, v AS v  FROM E;"
cur.execute(sql_string)
file.write(sql_string+'\n')

drop_table('A1')
print("matrix multiplication....")
start_time=time.time()

###Create R1###
sql_string="CREATE TABLE  A1 AS SELECT E.i as i, E2.j as j,E.v as v  FROM E JOIN E2 on E.j=E2.i GROUP BY E.i, E2.j,E.v;"
cur.execute(sql_string)
#file.write(sql_string+'\n')

drop_table('AdjMat')
sql_string="CREATE TABLE AdjMat AS SELECT * FROM E UNION ALL SELECT * FROM A1;"
cur.execute(sql_string)
#file.write(sql_string+'\n')

print('Total time=',time.time()-start_time) ##total time

##drop the TC tables
file.write('\n')

sql_string="SELECT COUNT(*) FROM AdjMat;"
cur.execute(sql_string)
total_triangle = cur.fetchone()
file.write(sql_string+'\n')
print("Total edges=",total_triangle[0])
 
file.close()
connection.close()
