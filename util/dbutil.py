# 로컬
def db_connector():
    # Connect to MariaDB Platform
    conn = mariadb.connect(
        user="root",
        password="1234",
        host="127.0.0.1",
        port=3306,
        database="book_analysis"
    )
    return conn
