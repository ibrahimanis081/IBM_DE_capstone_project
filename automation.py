
import mysql.connector
import ibm_db

# Connect to MySQL
connection = mysql.connector.connect(user='root', password='mysql',host='localhost',port='3306',database='sales')
cursor = connection.cursor()

# Connect to DB2
dsn_hostname = "764264db-9824-4b7c-82df-40d1b13897c2.bs2io90l08kqb1od8lcg.databases.appdomain.cloud" # e.g.: "dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net"
dsn_uid = "mbc06121"        
dsn_pwd = "kf59QdDF2GNrcjh4"     
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

# Find out the last rowid from DB2 data warehouse
def get_last_rowid():
    '''This function returns the last rowid of the table sales_data on the IBM DB2 database.
    '''
    SQL = '''SELECT MAX(rowid) from sales_data;'''
    last_row = ibm_db.fetch_tuple(ibm_db.exec_immediate(conn, SQL))
    return int(last_row[0])


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)


def get_latest_records(rowid):
    '''This function returns a list of all records that have a rowid greater
    than the last_row_id in the sales_data table in the sales 
    database on the MySQL staging data warehouse.
    '''
    SQL = '''SELECT * FROM sales_data WHERE rowid > %s'''
    cursor.execute(SQL, (rowid,))
    records = cursor.fetchall()
    return records


new_records = get_latest_records(last_row_id)
print("New rows on staging datawarehouse = ", len(new_records))


def insert_records(records):
    '''This function inserts all the records passed 
    to it into the sales_data table in IBM DB2 database.
    '''
    for record in records:
        query = '''INSERT INTO sales_data (rowid, product_id, customer_id, quantity) VALUES (?, ?, ?, ?)'''
        stmt = ibm_db.prepare(conn, query)
        ibm_db.execute(stmt, (record[0], record[1], record[2], record[3]))


insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connection.close()

# disconnect from DB2 data warehouse
ibm_db.close(conn)


