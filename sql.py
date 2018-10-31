import os 
path = "your path"
os.chdir(path)
from mysql.connector import MySQLConnection, Error 
from python_mysql_dbconfig import read_db_config


def connect(): 
    """ Connect to MySQL database """
    
    db_config = read_db_config()
    
    try: 
        print('Connecting to MySQL database...')
        conn = MySQLConnection(**db_config[0]) #must use the db_config[0] format if you want to use the 
        
        
        if conn.is_connected(): 
            print('connection established')
        else: 
            print('connection failed')
    except Error as error: 
        print(error)

    finally: 
        conn.close()
        print('connection closed')

if __name__ == '__main__': 
    connect()
              
def query_with_fetchone(): 
    try: 
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig[0])
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM books")
        
        row = cursor.fetchone()
        
        while row is not None: 
            print(row)
            row = cursor.fetchone()
    except Error as e: 
        print(e)
    
    finally: 
        cursor.close()
        conn.close()

if __name__ == '__main__': 
    query_with_fetchone()
    
def query_with_fetchall(): 
    try: 
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig[0])
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM books")
        rows = cursor.fetchall()
        
        print('Total Row(s):', cursor.rowcount)
        for row in rows: 
            print(row)
    except Error as e : 
        print(e)
    
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__' : 
    query_with_fetchall()

def iter_row(cursor, size = 10): 
    while True: 
        rows = cursor.fetchmany(size)
        if not rows: 
            break
        for row in rows: 
            yield row 


def query_with_fetchmany(): 
    try: 
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig[0])
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM books")
        
        for row in iter_row(cursor, 10):
            print(row)
    
    except Error as e: 
        print(e)
        
    finally: 
        cursor.close()
        conn.close()

if __name__ == "__main__" : 
    query_with_fetchmany()


#creation of the table 
def create_book():
    
    query = """CREATE TABLE books (title VARCHAR(255), 
                isbn INT)"""
    
    query1 = "SHOW TABLES "
    try: 
        db_config = read_db_config()
        conn = MySQLConnection(**db_config[0])
        
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.execute(query1)
        for x in cursor:
            print(x)
        
        
    except Error as error : 
        print(error)
    finally: 
        cursor.close()
        conn.close()
if __name__ == "__main__": 
    create_book()

#modify the table 

def modify_table():
    
    query = """ALTER TABLE books 
                MODIFY isbn BIGINT;"""
    
    try: 
        db_config = read_db_config()
        conn = MySQLConnection(**db_config[0])
        
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.execute(query)
        for x in cursor:
            print(x)
        
        
    except Error as error : 
        print(error)
    finally: 
        cursor.close()
        conn.close()
if __name__ == "__main__": 
    modify_table()


#check the existing tables
def check_tables():
    
    
    query1 = "SHOW TABLES "
    try: 
        db_config = read_db_config()
        conn = MySQLConnection(**db_config[0])
        
        cursor = conn.cursor()
        cursor.execute(query1)
        for x in cursor:
            print(x)
        
        
    except Error as error : 
        print(error)
    finally: 
        cursor.close()
        conn.close()
if __name__ == "__main__": 
    check_tables()    

#inserting new lines 
def insert_book(title, isbn): 
    
    
    query = "INSERT INTO books(title,isbn)" \
            "VALUES(%s,%s)" 
    
    args = (title, isbn)      
    
    try:
        db_config = read_db_config()
        conn = MySQLConnection(**db_config[0])
        
        cursor = conn.cursor()
        cursor.execute(query, args)
        
        if cursor.lastrowid: 
            print('last insert id', cursor.lastrowid)
        else: 
            print('last insert id not found')
        
        conn.commit()
    except Error as error : 
        print(error)
    finally: 
        cursor.close()
        conn.close()
def main(): 
    insert_book('A Sudden Light', '9765554656363763')

if __name__ == '__main__': 
    main()        

#inserting multiple new lines 

def insert_books(books): 
    query= """INSERT INTO books(title, isbn)
            VALUE(%s,%s)"""
    
    try: 
        db_config = read_db_config()
        conn = MySQLConnection(**db_config[0])
        
        cursor = conn.cursor()
        cursor.executemany(query, books)
        
        conn.commit()
    except Error as e: 
        print("Error:", e)
    finally: 
        cursor.close()
        conn.close()
        
def main():
    books = [('Harry Potter And The Order Of The Phoenix', '9780439358071'),
             ('Gone with the Wind', '9780446675536'),
             ('Pride and Prejudice (Modern Library Classics)', '9780679783268')]
    insert_books(books)
if __name__ == "__main__": 
    main()            

#update the table 
    

def update_book(book_id, title): 
    db_config = read_db_config()
    
    query = """UPDATE books 
            SET title = %s
            WHERE isbn = %s"""
    
    data = (title, book_id)
    
    try: 
        conn = MySQLConnection(**db_config[0])
        
        cursor = conn.cursor()
        cursor.execute(query, data)
        
        conn.commit()
        
    except Error as error: 
        print(error)
    
    finally: 
        cursor.close()
        conn.close()

if __name__ == '__main__': 
    update_book('9780679783268', 'The Giant on the Hill')


#delete an item 

def delete_book(isbn): 
    db_config = read_db_config()
    
    query = "DELETE FROM books WHERE isbn = %s"
    
    try: 
        conn = MySQLConnection(**db_config[0])
        
        cursor = conn.cursor()
        cursor.execute(query, (isbn,))
        
        conn.commit()
       
        
    except Error as error: 
        print(error)
    
    finally: 
        cursor.close()
        conn.close()

if __name__ == '__main__': 
    delete_book('9780679783268')

#delete a table 

def delete_table(): 
    db_config = read_db_config()
    
    query = "Drop TABLE visiteurs"
    
    try: 
        conn = MySQLConnection(**db_config[0])
        
        cursor = conn.cursor()
        cursor.execute(query)
        
        conn.commit()
       
        
    except Error as error: 
        print(error)
    
    finally: 
        cursor.close()
        conn.close()


if __name__ == '__main__': 
    delete_table()

 
