import ibm_db

# connectction details
dsn_hostname = "764264db-9824-4b7c-82df-40d1b13897c2.bs2io90l08kqb1od8lcg.databases.appdomain.cloud" # e.g.: "dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net"
dsn_uid = ""      
dsn_pwd = ""    
dsn_port = "32536"               
dsn_database = "bludb"            
dsn_driver = "{IBM DB2 ODBC DRIVER}"           
dsn_protocol = "TCPIP"            
dsn_security = "SSL"              

dsn = ( 
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)

conn = ibm_db.connect(dsn, "", "")
print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

# create table
SQL = """CREATE TABLE IF NOT EXISTS 
products(rowid INTEGER PRIMARY KEY NOT NULL,product varchar(255) NOT NULL,
category varchar(255) NOT NULL)"""

create_table = ibm_db.exec_immediate(conn, SQL)
print("Table created")

# insert data
SQL = "INSERT INTO products(rowid,product,category)  VALUES(?,?,?);"
stmt = ibm_db.prepare(conn, SQL)

row1 = (1,'Television','Electronics')
ibm_db.execute(stmt, row1)

row2 = (2,'Laptop','Electronics')
ibm_db.execute(stmt, row2)

row3 = (3,'Mobile','Electronics')
ibm_db.execute(stmt, row3)


# query data
SQL="SELECT * FROM products"
stmt = ibm_db.exec_immediate(conn, SQL)
tuple = ibm_db.fetch_tuple(stmt)
while tuple != False:
    print (tuple)
    tuple = ibm_db.fetch_tuple(stmt)

# export the fields name and type from collection test into the file data.csv



# close connection
ibm_db.close(conn)

