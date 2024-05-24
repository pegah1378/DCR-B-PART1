import os
import mysql.connector
import time
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(filename='file_monitor.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

def setup_database(cursor, db_name):
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        cursor.execute(f"USE {db_name}")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS files_info (
                id INT AUTO_INCREMENT,
                file_name VARCHAR(255),
                full_path VARCHAR(255) UNIQUE,
                file_extension VARCHAR(50),
                file_size BIGINT,
                file_type VARCHAR(50),
                content LONGTEXT,
                PRIMARY KEY (id, file_name),
                INDEX idx_file_name (file_name),
                INDEX idx_full_path (full_path),
                FULLTEXT(content)
            ) ENGINE=InnoDB
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS search_results_user (
                id INT AUTO_INCREMENT,
                file_name VARCHAR(255),
                full_path VARCHAR(255),
                occurrences INT,
                file_type VARCHAR(50),
                PRIMARY KEY (id, file_name)
            ) ENGINE=InnoDB
        """)
        logging.info("Database and tables created or already exist.")
    except mysql.connector.Error as err:
        logging.error("Error while setting up database: %s", err)

def get_file_type(file_path):
    if os.path.isdir(file_path):
        return "Directory"
    _, file_extension = os.path.splitext(file_path)
    return file_extension[1:].upper() if file_extension else "Unknown"

def get_file_content(file_path):
    try:
        if os.path.isdir(file_path):
            return ""
        with open(file_path, 'r', encoding='utf-8') as file_content:
            return file_content.read()
    except Exception as e:
        logging.error("Error reading file content: %s", e)
        return "Unreadable"

def file_exists(cursor, file_path):
    cursor.execute("SELECT COUNT(*) FROM files_info WHERE full_path = %s", (file_path,))
    return cursor.fetchone()[0] > 0

def insert_file_info(directory, cursor, connection, db_name):
    start_time = datetime.now()
    try:
        cursor.execute(f"USE {db_name}")
        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                if file_exists(cursor, file_path):
                    continue  # Skip the file if it already exists in the database
                file_name = os.path.basename(file)
                file_extension = os.path.splitext(file)[1]
                file_size = os.path.getsize(file_path)
                file_type = get_file_type(file_path)
                content = get_file_content(file_path)
                cursor.execute("""
                    INSERT INTO files_info (file_name, full_path, file_extension, file_size, file_type, content)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (file_name, file_path, file_extension, file_size, file_type, content))
        connection.commit()
    except mysql.connector.Error as err:
        logging.error("Error while inserting file info: %s", err)
        connection.rollback()
    end_time = datetime.now()
    logging.info(f"Inserted file info for directory {directory}. Time taken: {end_time - start_time}")

def create_search_table(cursor, db_name, table_name):
    try:
        cursor.execute(f"USE {db_name}")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT,
                file_name VARCHAR(255),
                full_path VARCHAR(255),
                occurrences INT,
                file_type VARCHAR(50),
                PRIMARY KEY (id, file_name)
            ) ENGINE=InnoDB
        """)
        logging.info(f"Search results table {table_name} created or already exists.")
    except mysql.connector.Error as err:
        logging.error("Error while creating search results table: %s", err)

def search_for_term(cursor, connection, search_term, db_name, table_name):
    start_time = datetime.now()
    create_search_table(cursor, db_name, table_name)
    try:
        cursor.execute(f"USE {db_name}")
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        cursor.execute("SELECT file_name, full_path, file_type, content FROM files_info")
        files = cursor.fetchall()
        for file in files:
            file_name, full_path, file_type, content = file
            occurrences = content.lower().count(search_term.lower())
            if search_term.lower() in file_name.lower():
                cursor.execute(f"""
                    INSERT INTO {table_name} (file_name, full_path, occurrences, file_type)
                    VALUES (%s, %s, %s, %s)
                """, (file_name, full_path, 0 if occurrences == 0 else occurrences, file_type))
            elif occurrences > 0:
                cursor.execute(f"""
                    INSERT INTO {table_name} (file_name, full_path, occurrences, file_type)
                    VALUES (%s, %s, %s, %s)
                """, (file_name, full_path, occurrences, file_type))
        connection.commit()

        # Retrieve and display search results
        cursor.execute(f"SELECT * FROM {table_name}")
        results = cursor.fetchall()
        if results:
            print(f"\nSearch results in table {table_name}:")
            for result in results:
                print(f"File Name: {result[1]}, Full Path: {result[2]}, Occurrences: {result[3]}, File Type: {result[4]}")
        else:
            print("\nNo matches found for the search term.")
    except mysql.connector.Error as err:
        logging.error("Error while searching for term: %s", err)
    end_time = datetime.now()
    logging.info(f"Search for term '{search_term}' completed. Time taken: {end_time - start_time}")

def monitor_directory(directory, cursor, connection, db_name):
    print(f"Monitoring directory {directory}... Press Ctrl+C to stop and access options.")
    try:
        while True:
            insert_file_info(directory, cursor, connection, db_name)
            logging.info("Directory monitored. Press Ctrl+C to stop monitoring.")
            time.sleep(60)  # Check for changes every 60 seconds
    except KeyboardInterrupt:
        logging.info("Monitoring stopped.")
        print("\nMonitoring stopped. Entering options menu.")

def user_interface(cursor, connection, db_name):
    try:
        while True:
            print("Options:")
            print("1. Search for term")
            print("2. Exit")
            choice = input("Enter your choice: ")

            if choice == '1':
                search_term = input("Enter the search term: ")
                search_for_term(cursor, connection, search_term, db_name, "search_results_user")
            elif choice == '2':
                break
            else:
                print("Invalid choice. Please try again.")
    except mysql.connector.Error as err:
        logging.error("Error in user interface: %s", err)

if __name__ == "__main__":
    try:
        db_name = 'file_storage'
        connection = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password='Pegah77183561',  # Replace with your password
            autocommit=False  # Disable autocommit to manage transactions manually
        )
        cursor = connection.cursor()

        setup_database(cursor, db_name)
        
        # Automatically start monitoring the directory
        monitor_directory(r"D:\1", cursor, connection, db_name)
        
        # Start the user interface
        user_interface(cursor, connection, db_name)

    except mysql.connector.Error as err:
        logging.error("MySQL error: %s", err)

    finally:
        cursor.close()
        connection.close()
