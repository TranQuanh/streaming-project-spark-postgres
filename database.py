import psycopg2


def create_db():
    conn = psycopg2.connect(
    host="postgres", 
    port="5432",
    user="postgres",
    password="UnigapPostgres@123"
    )

    # Tạo cursor để thực hiện các lệnh SQL
    conn.autocommit = True
    cursor = conn.cursor()

    # Tạo database mới
    try:
        cursor.execute("CREATE DATABASE view_product")
        print("Database đã được tạo thành công!")
    except psycopg2.errors.DuplicateDatabase:
        print("Database đã tồn tại.")
    finally:
        cursor.close()
        conn.close()