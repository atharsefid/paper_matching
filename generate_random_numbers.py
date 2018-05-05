from random import  randint
import pymysql
import mysql.connector
cmd_paper= "select * from papers where rid = %s"
db = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01',pool_name = "mypool",
                                  pool_size = 10, database='citeseerx2', charset='utf8', use_unicode=True)
cursor = db.cursor( dictionary=True)
cmd_paper= "select id from papers where rid = %s"
f1 = open('remaining_ground_truth.txt','r')
f = open('remaining_ground_truth2.txt','w')
for line in  f1:
    cursor.execute(cmd_paper % (line.strip()))
    row = cursor.fetchone()
    f.write(row['id']+'\n')
    #f.write(str(randint(0,9000000))+'\n')
      
