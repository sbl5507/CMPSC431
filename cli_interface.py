#PostgreSQL adapter for Python
import psycopg2
#Library for reading and writing CSV files
import csv

# Constants for table names
TABLE_NAMES = [
    "Location","Merchant","Cardholder", "Transaction", 
    "City", "Date", "Amount", "AgeGroup", "Zip_Code"
]

# Function to get connection to the PostgreSQL Database
def sql_connect():
    try:
        conn = psycopg2.connect(
            dbname="mydb", #database name
            user="postgres", #username
            password="1234qwer", #password
            host="localhost",
            port="5432"
        )
        #Checking if the Databse has connected
        print("Database Connected")

        # Creating tables and foreign key constraints if they don't exist
        create_tables(conn)
        add_foreign_key(conn)
        return conn
    
    #Error Handling if you can't connect to the database
    except psycopg2.Error as e: 
        print("Cannot connect to the database")
        print(e)
        return None
    
def add_foreign_key(conn):
    try:
        cur = conn.cursor()

        # Transaction table referencing Cardholder table
        query = """
        ALTER TABLE Transaction
        ADD CONSTRAINT FK_Transaction_Cardholder
        FOREIGN KEY (Cc_num) REFERENCES Cardholder(Cc_num);
        """
        cur.execute(query)

        # Transaction table referencing Merchant table
        query = """
        ALTER TABLE Transaction
        ADD CONSTRAINT FK_Transaction_Merchant
        FOREIGN KEY (Merchant) REFERENCES Merchant(Merchant);
        """
        cur.execute(query)

        # Cardholder table referencing Location table
        query = """
        ALTER TABLE Cardholder
        ADD CONSTRAINT FK_Cardholder_Location
        FOREIGN KEY (Lat, Long) REFERENCES Location(Lat, Long);
        """
        cur.execute(query)

        # Transaction table referencing Amount table
        query = """
        ALTER TABLE Transaction
        ADD CONSTRAINT fk_transaction_amount
        FOREIGN KEY (amt) REFERENCES Amount(amt)
        ON UPDATE CASCADE;
        """
        cur.execute(query)
        conn.commit()


        print("Foreign key constraints added successfully.")

        #Error Handling for Adding Foreign Constraints
    except psycopg2.Error as e:
        conn.rollback()
        if "already exists" in str(e):  # Check if the constraint already exists
            print("Foreign key constraint already exists.")
        else:
            print("Error adding foreign key constraints:")
            print(e)


# Function to create tables
def create_tables(conn):
    try:
        cur = conn.cursor()
        for table_name in TABLE_NAMES:
            ddl_command = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            if table_name == "Cardholder":
                ddl_command += """
                Cc_num NUMERIC PRIMARY KEY,
                First VARCHAR(255),
                Last VARCHAR(255),
                Gender CHAR(1),
                Street VARCHAR(255),
                City VARCHAR(255), 
                State CHAR(2),
                Zip VARCHAR(10),
                Lat FLOAT,
                Long FLOAT,
                City_pop INT,
                Job VARCHAR(255),
                Dob DATE,
                FOREIGN KEY (Lat, Long) REFERENCES Location(Lat, Long)
                """
            elif table_name == "Transaction":
                ddl_command += """
                Trans_num VARCHAR(36) PRIMARY KEY,
                Trans_date_trans_time TIMESTAMP,
                Cc_num NUMERIC,
                Merchant VARCHAR(255),
                Category VARCHAR(255),
                Amt DECIMAL(10, 2),
                Unix_time NUMERIC,
                Is_fraud INT,
                FOREIGN KEY (Cc_num) REFERENCES Cardholder(Cc_num),
                FOREIGN KEY (Merchant) REFERENCES Merchant(Merchant)
                """
            elif table_name == "Merchant":
                ddl_command += """
                Merchant VARCHAR(255) PRIMARY KEY,
                Merch_lat DECIMAL(10, 8),
                Merch_long DECIMAL(11, 8)
                """
            elif table_name == "Location":
                ddl_command += """
                Lat FLOAT,
                Long FLOAT,
                PRIMARY KEY (Lat, Long)
                """
            elif table_name == "City":
                ddl_command += """
                City VARCHAR(255),
                State CHAR(2),
                City_pop INT,
                PRIMARY KEY (City)
                """
            elif table_name == "Date":
                ddl_command += """
                Trans_date_trans_time TIMESTAMP PRIMARY KEY
                """
            elif table_name == "Amount":
                ddl_command += """
                Amt DECIMAL(10, 2) PRIMARY KEY
                """
            elif table_name == "AgeGroup":
                ddl_command += """
                Dob DATE PRIMARY KEY
                """
            elif table_name == "Zip_Code":
                ddl_command += """
                Zip VARCHAR(10) PRIMARY KEY
                """
            ddl_command += ");"
            cur.execute(ddl_command)
        conn.commit()
        print("Tables created successfully")
    except psycopg2.Error as e:
        conn.rollback()
        print("Error creating tables:")
        print(e)


# Function to execute SQL query
def execute_query(conn, query, fetch_results=True):
    try:
        cur = conn.cursor()
        cur.execute(query)
        if fetch_results and cur.description:
            rows = cur.fetchall()
            for row in rows:
                print(row)
        elif query.strip().upper().startswith('COMMIT'):
            print("Transaction committed successfully")
        else:
            print("Query executed successfully")

    except psycopg2.Error as e:
        conn.rollback()  # Rollback
        print("Error executing query:")
        print(e)


# Insert data into a table
def insert_data(conn, table_name, columns, values):
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
    execute_query(conn, query, fetch_results=False)
# Delete data from a table
def delete_data(conn, table_name, condition):
    query = f"DELETE FROM {table_name} WHERE {condition};"
    execute_query(conn, query, fetch_results=False)
# Update data in a table
def update_data(conn, table_name, column, new_value, condition):
    query = f"UPDATE {table_name} SET {column} = {new_value} WHERE {condition};"
    execute_query(conn, query, fetch_results=False)
# Search data in a table
def search_data(conn, table_name, condition):
    query = f"SELECT * FROM {table_name} WHERE {condition};"
    execute_query(conn, query)
# Aggregate functions
def aggregate_functions(conn, column, table_name, function):
    query = f"SELECT {function}({column}) FROM {table_name};"
    execute_query(conn, query)
# Sorting
def sorting(conn, table_name, column, order):
    query = f"SELECT * FROM {table_name} ORDER BY {column} {order};"
    execute_query(conn, query)
# Joins
def joins(conn, table1, table2, key):
    query = f"SELECT * FROM {table1} INNER JOIN {table2} ON {table1}.{key} = {table2}.{key};"
    execute_query(conn, query)
# Grouping
def grouping(conn, table_name, column):
    query = f"SELECT {column}, COUNT(*) FROM {table_name} GROUP BY {column};"
    execute_query(conn, query)
# Subqueries
def subqueries(conn, table_name, column, subquery):
    query = f"SELECT * FROM {table_name} WHERE {column} IN ({subquery});"
    execute_query(conn, query)
# Transactions
def transactions(conn):
    print("Starting transaction...")
    execute_query(conn,'COMMIT')
# Error Handling
def error_handling(e,conn):
    print(e)
    print("Rolling back changes...")
    conn.rollback()

# Function to read CSV file and insert data into Tables into the SQL database
def insert_transaction_data_from_csv(conn, csv_file_path):
    try:
        with open(csv_file_path, 'r') as file:
            reader = csv.reader(file)
            for row in reader:

                # Location
                lat= row[0]
                long = row[1]

                # #Cardholder
                # cc_num = row[0]
                # first = row[1]
                # last = row[2]
                # gender = row[3]
                # street = row[4]
                # city = row[5]
                # state = row[6]
                # zip = row[7]
                # lat = row[8]    
                # long = row[9]
                # city_pop = row[10]
                # job = row[11]
                # dob = row[12]    

                # #Merchant
                # Merchant = row[0]
                # Merch_lat = row[1]
                # Merch_long = row[2]

                #Transaction
                # trans_num = row[0]
                # trans_date_trans_time = row[1]
                # cc_num = row[2]
                # merchant = row[3]
                # category = row[4]
                # amt = row[5]
                # unix_time = row[6]
                # is_fraud = row[7]

                # #City
                # City = row[0]
                # State = row[1]
                # city_pop = row[2]

                # #Date
                # Trans_date_trans_time = row[0]

                #Amount
                # amt = row[0]

                #AgeGroup
                Dob = row[0]

                #Zip Code
                # Zip = row[0]


                # # Inserting data into Cardholer table
                insert_data(conn, "Location", "Lat, Long", f"'{lat}', '{long}'")
                # insert_data(conn, "Cardholder", "cc_num, first, last, gender, street, city, state, zip, lat, long, city_pop, job, dob",f"'{cc_num}', '{first}', '{last}', '{gender}', '{street}', '{city}', '{state}', '{zip}', '{lat}', '{long}', '{city_pop}', '{job}', '{dob}'")
                # insert_data(conn, "Merchant", "Merchant, Merch_lat, Merch_long", f"'{Merchant}', '{Merch_lat}', '{Merch_long}'")
                # insert_data(conn, "Transaction", "Trans_num, trans_date_trans_time, cc_num, merchant, category, amt, unix_time, is_fraud", f"'{trans_num}', '{trans_date_trans_time}', '{cc_num}', '{merchant}', '{category}', '{amt}', '{unix_time}', '{is_fraud}'")
                # insert_data(conn, "City", "City, State, city_pop", f"'{City}', '{State}', '{city_pop}'")
                # insert_data(conn, "Date","Trans_date_trans_time", f"'{Trans_date_trans_time}'")
                # insert_data(conn, "Amount","amt", f"'{amt}'")
                insert_data(conn, "Agegroup","Dob", f"'{Dob}'")
                # insert_data(conn, "zip_code","Zip", f"'{Zip}'")
                   
                
        print("Data inserted successfully from CSV file.")
        conn.commit()
    except Exception as e:
        print("Error inserting data from CSV file:")
        print(e)

# Main function to display CLI
def main():
    with sql_connect() as conn:
        if conn is None:
            return

    while True: 
        print("Welcome to the Databse CLI Interface!\n\nPlease select an option: \n1.Insert Data\n2.Delete Data\n3.Update Data\n4.Search Data\n5.Aggregate Functions\n6.Sorting\n7.Joins\n8.Grouping\n9.Subqueries\n10.Transactions\n11.Error Handling\n12.Exit")
        select = input("\nEnter your choice (1-12):")

        if select == "12":
            print("Exiting...")
            break

        if select not in ["1","2","3","4","5","6","7","8","9","10","11","Import"]:
            print("Please Try Again")
            continue

        if select == "1": #Insert
            table_name = input("Enter table name: ")
            columns = input("Enter column names: ")
            values = input("Enter values: ")
            insert_data(conn, table_name, columns, values)
            execute_query(conn, 'COMMIT')  # Commit after inserting data

        elif select == "2": #Delete
            table_name = input("Enter table name: ")
            condition = input("Enter condition: ")
            delete_data(conn, table_name, condition)
            execute_query(conn, 'COMMIT')  # Commit after inserting data

        elif select == "3": #Update
            table_name = input("Enter table name: ")
            column = input("Enter column to update: ")
            new_value = input("Enter new value: ")
            condition = input("Enter condition: ")
            update_data(conn, table_name, column, new_value, condition)
            execute_query(conn, 'COMMIT')  # Commit after inserting data

        elif select == "4": #Search
            table_name = input("Enter table name: ")
            condition = input("Enter condition: ")
            search_data(conn, table_name, condition)

        elif select == "5": #Aggregation
            table_name = input("Enter Table Name:")
            column = input("Enter column: ")
            function = input("Enter Function (SUM, AVG, COUNT, MIN, MAX): ")
            aggregate_functions(conn, column, table_name, function)
 
        elif select == "6": #Sort
            table_name = input("Enter table name: ")
            column = input("Enter column name: ")
            order = input("Enter sorting order (ASC/DESC): ")
            sorting(conn, table_name, column, order)

        elif select == "7": #Joins
            table1 = input("Enter first table name: ")
            table2 = input("Enter second table name: ")
            key = input("Enter join key: ")
            joins(conn, table1, table2, key)

        elif select == "8": #Grouping
            table_name = input("Enter table name: ")
            column = input("Enter column name to group by: ")
            grouping(conn, table_name, column)

        elif select == "9": #Subquery
            table_name = input("Enter table name: ")
            column = input("Enter column name: ")
            subquery = input("Enter subquery: ")
            subqueries(conn, table_name, column, subquery)

        elif select == "10": #Transaction
            transactions(conn)

        elif select == "11": #Error handling
            error_handling('Starting Error Handling..', conn)

        elif select == "Import": #Importing csv files to SQL
            csv_file_path = input("Path: ")
            insert_transaction_data_from_csv(conn, csv_file_path)            

    conn.close()

if __name__ == "__main__":
    main()
