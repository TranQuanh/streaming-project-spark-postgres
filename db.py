import psycopg2

class Postgres:
    def get_connection(self):
        conn = psycopg2.connect(
            host="postgres",  # Tên container hoặc địa chỉ IP
            port=5432,
            database="postgres",
            user="postgres",
            password="UnigapPostgres@123"
        )
        return conn
    def upsert_to_table(insert_query,partition,columns):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            for row in partition:
                values = list(row)
                print(values)
                cursor.execute(insert_query,values)
        except Exception as e:
            print(f"Error inserting row {row} : {e}")
        finally:
            cursor.close()
            conn.close()

    def upsert_to_dim_browser(self,df_browser):
        columns = ['browser_id','browser_name']
        insert_query = '''INSERT INTO dim_browser (browser_key, browser_name) 
                        VALUES (%s, %s) ON CONFLICT (browser_key) DO NOTHING'''
        df_browser.foreachPartition(lambda partiton:self.upsert_to_table(partiton,insert_query,columns))
    def create_table(self):
        conn = self.get_connection()
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

