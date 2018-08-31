import vertica_python
import string
import sys
import math
import time
 
 
######connection string to vertica #####

try:
    conn_info = {'host':'192.168.1.11',
                 'port': 5433,
                 'user': 'vertica',
                 'password': '12512Marlive',
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
 
sys.argv = ["connected_component.py", "dataset=tree10m.csv","source=6"]
 
 
####check the command line arguments ####
if len(sys.argv) != 3:
    print("Not correct arguments. ");
    sys.exit()
 
arg2=sys.argv[1]
arg2=arg2.split('=')
input_dataset=arg2[1]

path_length=6
###initialize the variables####
file = open("connected_component.sql","w")
#input_dataset='web-Google.csv'
 
#####drop table######
def drop_table(table_name):
    sql_string="DROP TABLE IF EXISTS "+table_name+" ;"
    cur.execute(sql_string)
    file.write(sql_string+'\n')

for i in range(1,path_length+1):
    drop_table_name="S"+str(i)
    drop_table(drop_table_name)
  
 
####Create E####
drop_table('E')
sql_string="CREATE TABLE E (i int ENCODING RLE, j int ENCODING RLE, v int ENCODING RLE, PRIMARY KEY(i,j)) ;"
cur.execute(sql_string)
file.write(sql_string+'\n')
 
####Load Dataset#####
print("Loading the CSV file..")
sql_string="COPY E FROM '/home/vertica/tahsin/TC_programs/"+input_dataset+"' parser fcsvparser();"
cur.execute(sql_string)
file.write(sql_string+'\n')
 

#####Counting the number of triangles ######
#drop_table('traingle_count')
drop_table('S0')
print("Creaintg table S0..")
start_time=time.time()
sql_string="CREATE TABLE S0 AS SELECT DISTINCT i AS i FROM E;"
cur.execute(sql_string)
file.write(sql_string+'\n')
sql_string="SELECT COUNT(*) FROM S0;"
cur.execute(sql_string)
Si_prev=cur.fetchone()


print("E[i,i]=1")
sql_string="INSERT INTO E SELECT S0.i AS i, S0.i as j, 1 as v FROM S0;"
cur.execute(sql_string)
file.write(sql_string+'\n')


print("computing cc...")
#for i in range (2,path_length+1,1):
i=1
while 1:
    #sql_string="CREATE TABLE P"+str(i)+" AS SELECT P"+str(i-1)+".i AS i,E.j as j FROM P"+str(i-1)+" JOIN E ON P"+str(i-1)+".j=E.i GROUP BY P"+str(i-1)+".i, E.j;"
    sql_string="CREATE TABLE S"+str(i)+" AS SELECT E.i AS i FROM S"+str(i-1)+" JOIN E ON S"+str(i-1)+".i=E.j GROUP BY E.i; "
    cur.execute(sql_string)
    file.write(sql_string+'\n')
    drop_table('S_temp')
    sql_string="CREATE TABLE S_temp AS SELECT * FROM S"+str(i-1)+" UNION SELECT * FROM S"+str(i)+";"
    cur.execute(sql_string)
    file.write(sql_string+'\n')
    sql_string="SELECT COUNT(*) FROM S_temp"+";"
    file.write(sql_string+'\n')
    cur.execute(sql_string)
    Si_next=cur.fetchone()
    sql_string="SELECT COUNT(*) FROM S"+str(i)+";"
    cur.execute(sql_string)
    file.write(sql_string+'\n')
    Si_prev=cur.fetchone()
    print(Si_prev[0],Si_next[0])
    if Si_prev[0]==Si_next[0] and i!=1:
        break
    i=i+1
    file.write('\n\n\n')

 
print('Total time=',time.time()-start_time)


#cur.execute(sql_string)
#res=cur.fetchone()
#print("Total paths=",res)


##drop the TC tables
file.write('\n')
for i in range(0,i+1):
    drop_table("S"+str(i))

  
file.close()
connection.close()
