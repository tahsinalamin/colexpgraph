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
                 'database':'fournodes',
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
 
sys.argv = ["Paths_reacbability.py", "dataset=web-Google.csv","source=6"] ##This line is not needed when running from command line
 
####check the command line arguments ####
if len(sys.argv) != 3:
    print("Not correct arguments. ");
    sys.exit()
 
arg2=sys.argv[1]
arg2=arg2.split('=')
input_dataset=arg2[1]

arg3=sys.argv[2]
arg3=arg3.split('=')
source_vertex=arg3[1]

dummy_table="test"
path_length=6
file = open("path_rechability.sql","w")
 
#####drop table######
def drop_table(table_name):
    sql_string="DROP TABLE IF EXISTS "+table_name+" ;"
    cur.execute(sql_string)
    file.write(sql_string+'\n')

for i in range(1,path_length+1):
    drop_table_name="P"+str(i)
    drop_table(drop_table_name)
   
####Create E####
drop_table('E')
sql_string="CREATE TABLE E (i int ENCODING RLE, j int ENCODING RLE, v int ENCODING RLE);"
cur.execute(sql_string)
file.write(sql_string+'\n')
 
####Load Dataset#####
print("Loading the CSV file..")
sql_string="COPY E FROM '/home/vertica/tahsin/TC_programs/"+input_dataset+"' parser fcsvparser();"
cur.execute(sql_string)
file.write(sql_string+'\n')
 
#####Counting the number of triangles ######
#drop_table('traingle_count')
drop_table('P')
print("Creaintg table P..")
start_time=time.time()
sql_string="CREATE TABLE P1 AS SELECT i AS i,j AS j FROM E WHERE i="+str(source_vertex)+";"
cur.execute(sql_string)
#file.write(sql_string+'\n')

print("Exploring Paths...")
for i in range (2,path_length+1,1):
    sql_string="CREATE TABLE P"+str(i)+" AS SELECT P"+str(i-1)+".i AS i,E.j as j FROM P"+str(i-1)+" JOIN E ON P"+str(i-1)+".j=E.i GROUP BY P"+str(i-1)+".i, E.j;"
    #sql_string="CREATE TABLE P"+str(i)+" AS SELECT P"+str(i-1)+".i AS i,E.j as j FROM P"+str(i-1)+" JOIN E ON P"+str(i-1)+".j=E.i; "   ###without GROUPBY
    cur.execute(sql_string)
    #file.write(sql_string+'\n')

#file.write('\n\n\n')

print('Creating final table...')
###create the final path table
drop_table('Reachability')
sql_string="CREATE TABLE Reachability AS "
for i in range(1,path_length+1):
    sql_string+=" SELECT * FROM P"+str(i)+" UNION "+'\n'
sql_string+=" SELECT * FROM P"+str(path_length)+";"  
cur.execute(sql_string)
#file.write(sql_string+'\n')
 
print('Total time=',time.time()-start_time)  ##total time 

sql_string=" SELECT COUNT(*) FROM Reachability;"  
cur.execute(sql_string)
res=cur.fetchone()
print("Total paths=",res)

##drop the TC tables
file.write('\n')
for i in range(1,path_length+1):
    drop_table("P"+str(i))
  
file.close()
connection.close()
