import sqlite3 as sq
import logging

class parser_db():

    def __init__(self):
        self.conn = sq.connect("db.db")
    

    def create_rabota_ua(self):
        """Crate the 'rabota' table if it doesn't exits"""
        cursor = self.conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS rabota(
            url INTEGER PRIMARY KEY,  
            name TEXT,
            data_create DATETIME DEFAULT CURRENT_TIMESTAMP,
            sent BOOLEAN DEFAULT 0
        )''')
        self.conn.commit()

    def create_djini(self):
        """Create the 'djini' table if it doesn't exist. """
        cursor = self.conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS djini(
        id INT AUTO_INCREMENT PRIMARY KEY,
        url VARCHAR(255) NOT NULL UNIQUE,
        name VARCHAR(255) NOT NULL,
        data_create DATETIME DEFAULT CURRENT_TIMESTAMP,
        sent BOOLEAN DEFAULT 0
        )
        ''')
        self.conn.commit()

    def create_dou(self):
        """Create the 'job.dou' table if it doesn't exist. """
        cursor = self.conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT dou(
        id INT AUTO_INCREMENT PRIMARY KEY,
        url VARCHAR(255) NOT NULL UNIQUE,
        name VARCHAR(255) NOT NULL,
        data_create DATETIME DEFAULT CURRENT_TIMESTAMP,
        sent BOOLEAN DEFAULT 0 
        )
        ''')

    def insert_rabota(self, data):
        """Insert data into the 'rabota' table."""
        cursor = self.conn.cursor()
        for item in data:
            if all(key in item for key in ('url', 'name')):  # Change 'id' to 'url'
                try:
                    cursor.execute("INSERT INTO rabota (url, name) VALUES (?, ?)", (item['url'], item['name']))
                except sq.IntegrityError as e:
                    logging.error(f"Error inserting data: {e}")
                except Exception as e:
                    logging.error(f'Unexpected error during insertion: {e}')
            else:
                logging.warning(f"Missing keys in item: {item}")
        self.conn.commit()
        
    def insert_djini(self,data):
        """Insert job data into the 'djini' table"""
        cursor = self.conn.cursor()
        required_keys = ('Url', 'name')
        for item in data:
            if not all(key in item for key in required_keys):
                logging.warning(f"Отсутствуют необходимые ключи в элементе: {item}")
                continue

            # Формируем SQL-запрос динамически
            columns = ', '.join(required_keys)
            placeholders = ', '.join(['?'] * len(required_keys))
            sql = f"INSERT INTO djini ({columns}) VALUES ({placeholders})"

            try:
                cursor.execute(sql, tuple(item[key] for key in required_keys))
            except sq.IntegrityError as e:
                logging.error(f"Ошибка при вставке данных в таблицу djini: {e}")
            except Exception as e:
                logging.error(f"Непредвиденная ошибка при вставке данных: {e}")

        self.conn.commit()

    def insert_dou(self,data):
        """Inserts job data into the djini table"""
        cursor = self.conn.cursor()
        required_keys = ('Url', 'name')
        for item in data:
            if not all(key in item for key in required_keys):
                logging.warning(f"Отсутствуют необходимые ключи в элементе: {item}")
                continue

            # Формируем SQL-запрос динамически
            columns = ', '.join(required_keys)
            placeholders = ', '.join(['?'] * len(required_keys))
            sql = f"INSERT INTO dou ({columns}) VALUES ({placeholders})"

            try:
                cursor.execute(sql, tuple(item[key] for key in required_keys))
            except sq.IntegrityError as e:
                logging.error(f"Ошибка при вставке данных в таблицу dou: {e}")
            except Exception as e:
                logging.error(f"Непредвиденная ошибка при вставке данных: {e}")
        self.conn.commit()       
    def get_unsent_data(self, table_name):
        """Gets unsent data from the specified table"""
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT url, name FROM {table_name} WHERE sent = 0")
        return cursor.fetchall()

    def update_sent_flag(self, message_id, table_name):
        """Updates the sent field for the record with the specified id in the specified table"""
        cursor = self.conn.cursor()
        cursor.execute(f"UPDATE {table_name} SET sent = 1 WHERE url = ?", (message_id,))
        self.conn.commit()   