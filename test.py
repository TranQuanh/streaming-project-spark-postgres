import psycopg2
from util.config import Config
from util.logger import Log4j
from pyspark.sql import SparkSession
DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'UnigapPostgres@123'
}

def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn


def process_partition(columns,insert_query,partition):
    conn =None
    try:
        conn = get_connection()       
        cursor = conn.cursor()             
        for row in partition:             
            try:    
                values = [getattr(row, col) for col in columns]
                cursor.execute(insert_query, values)
            except Exception as e:
                print(f"Error inserting row {row}: {e}")
        conn.commit()  
    except Exception as e:
        print(f"Error in partition: {e}")
        if conn:
            conn.rollback()
    finally:
        # Đóng cursor và connection khi hoàn thành
        if conn:
            cursor.close()
            conn.close()
def test():
    conn = get_connection()       
    cursor = conn.cursor()  
    insert_query = '''INSERT INTO dim_browser (browser_id, browser_name) 
                    VALUES (2, 'Cssh') ON CONFLICT (browser_id) DO NOTHING'''
    cursor.execute(insert_query)
    cursor.close()
    conn.close()
def upsert_to_dim_browser(df_browser):
    columns = ['browser_id','browser_name']
    column_names = ",".join(columns)
    placeholders = ','.join(['%s']*len(columns))
    insert_query = f"INSERT INTO dim_browser ({column_names}) VALUES ({placeholders}) ON CONFLICT (browser_id) DO NOTHING"

    df_browser.foreachPartition(lambda partition:process_partition(columns,insert_query,partition))

def create_table():
    conn = get_connection()
    cur = conn.cursor()  
    query = '''
    DROP TABLE IF EXISTS Dim_Date CASCADE;
    CREATE TABLE Dim_Date(
        date_id INT PRIMARY KEY,
        full_date DATE,
        day_of_week VARCHAR(10),
        day_of_week_short VARCHAR(10),
        day_of_month INT,
        year INT,
        month INT,
        hour INT
    );

    DROP TABLE IF EXISTS Dim_Product CASCADE;
    CREATE TABLE Dim_Product(
        product_id INT PRIMARY KEY,
        product_name VARCHAR(50)
    );

    DROP TABLE IF EXISTS Dim_Territory CASCADE;
    CREATE TABLE Dim_Territory(
        territory_id INT PRIMARY KEY,
        country_code INT,
        Country_name VARCHAR(100),         
        iso_3166_2 VARCHAR(30),          
        region VARCHAR(100),              
        sub_region VARCHAR(100),          
        intermediate_region VARCHAR(100)   
    );

    DROP TABLE IF EXISTS Dim_Os CASCADE;          
    CREATE TABLE Dim_Os(
        os_id INT PRIMARY KEY,
        os_name VARCHAR(100)
    );

    DROP TABLE IF EXISTS Dim_Browser CASCADE;
    CREATE TABLE Dim_Browser(
        browser_id INT PRIMARY KEY,
        browser_name VARCHAR(100)
    );
    DROP TABLE IF EXISTS Fact_View CASCADE;
    CREATE TABLE Fact_View(
        id VARCHAR(100) PRIMARY KEY,      
        product_id INT NOT NULL,
        territory_id INT NOT NULL,
        date_id INT NOT NULL,
        os_id INT NOT NULL,
        browser_id INT NOT NULL,
        current_url VARCHAR(255),         
        referee_url VARCHAR(255),        
        store_id INT NOT NULL,
        total_view INT,
        FOREIGN KEY (product_id) REFERENCES Dim_Product(product_id),
        FOREIGN KEY (territory_id) REFERENCES Dim_Territory(territory_id),
        FOREIGN KEY (date_id) REFERENCES Dim_Date(date_id),
        FOREIGN KEY (os_id) REFERENCES Dim_Os(os_id),
        FOREIGN KEY (browser_id) REFERENCES Dim_Browser(browser_id)
    );
    '''
    # Sử dụng self.cur và self.conn thay vì cur và conn
    cur.execute(query)
    conn.commit()

    # Đóng kết nối
    cur.close()
    conn.close()
    print("Table created successfully.")



# conf = Config()
# spark_conf = conf.spark_conf
# kafka_conf = conf.kafka_conf

# spark = SparkSession \
#         .builder \
#         .config(conf=spark_conf) \
#         .getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")


# # Test data
# data = [(1, "Chrome"), (2, "Firefox")]
# df = spark.createDataFrame(data, ["browser_id", "browser_name"])
# df.show()
# # Thực hiện upsert
# upsert_to_dim_browser(df)
# print("success !!")


    