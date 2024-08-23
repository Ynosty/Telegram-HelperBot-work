import sqlite3
import logging

def connect_to_db(db_file='pars.db'):
    """Connects to the specified SQLite database file."""
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database: {e}")
        raise

def create_table(conn):
    """Creates the 'jobs' table if it doesn't exist."""
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY,
        name TEXT,
        company_name TEXT,
        salary TEXT,
        salary_from TEXT,
        date TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.commit()
def create_djini(conn):
    """Creates the 'jobs' table if it doesn't exist."""
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS djini (
        id INT AUTO_INCREMENT PRIMARY KEY,
        url VARCHAR(255) NOT NULL UNIQUE,
        name VARCHAR(255) NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )''')
    conn.commit()

def insert_djini(conn, data):
    """Inserts job data into the djini table

    Args:
        conn: A database connection object
        data: A list of dictionaries containing job information (Url and name)
    """
    cursor = conn.cursor()

    # Проверяем, что в данных есть необходимые ключи
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
        except sqlite3.IntegrityError as e:
            logging.error(f"Ошибка при вставке данных в таблицу djini: {e}")
        except Exception as e:
            logging.error(f"Непредвиденная ошибка при вставке данных: {e}")
            # Рассмотрите возможность логирования проблемного элемента для дальнейшего анализа

    conn.commit()
def create_dou(conn):
    """Creates the 'jobs' table if it doesn't exist."""
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS dou (
        id INT AUTO_INCREMENT PRIMARY KEY,
        url VARCHAR(255) NOT NULL UNIQUE,
        name VARCHAR(255) NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )''')
    conn.commit()

def insert_dou(conn, data):
    """Inserts job data into the djini table

    Args:
        conn: A database connection object
        data: A list of dictionaries containing job information (Url and name)
    """
    cursor = conn.cursor()

    # Проверяем, что в данных есть необходимые ключи
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
        except sqlite3.IntegrityError as e:
            logging.error(f"Ошибка при вставке данных в таблицу dou: {e}")
        except Exception as e:
            logging.error(f"Непредвиденная ошибка при вставке данных: {e}")
            # Рассмотрите возможность логирования проблемного элемента для дальнейшего анализа

    conn.commit()
def insert_data(conn, data):
    """Inserts data into the 'jobs' table."""
    cursor = conn.cursor()
    for item in data:
        if all(key in item for key in ('id', 'name', 'companyName', 'salary', 'salaryFrom', 'date')):
            try:
                cursor.execute('''INSERT INTO jobs (id, name, company_name, salary, salary_from, date)
                                 VALUES (?, ?, ?, ?, ?, ?)''', (
                                     item['id'], item['name'], item['companyName'], item['salary'], item['salaryFrom'], item['date']
                                 ))
            except sqlite3.IntegrityError as e:
                logging.error(f"Error inserting data: {e}")
            except Exception as e:
                logging.error(f"Unexpected error during insertion: {e}")
                # Consider logging the problematic item for further analysis
        else:
            logging.warning(f"Missing keys in item: {item}")


# def get_jobs(conn):
#     """Получает все данные из таблицы jobs"""
#     cursor = conn.cursor()
#     cursor.execute("SELECT * FROM jobs")
#     rows = cursor.fetchall()
#     return rows

# def get_djini(conn):
#     """Получает все данные из таблицы djini"""
#     cursor = conn.cursor()
#     cursor.execute("SELECT * FROM djini")
#     rows = cursor.fetchall()
#     return rows

# def get_dou(conn):
#     """Получает все данные из таблицы dou"""
#     cursor = conn.cursor()
#     cursor.execute("SELECT * FROM dou")
#     rows = cursor.fetchall()
#     return rows

def get_rabota(conn):
    """Give urls and name"""
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM jobs")
    rows = cursor.fetchall()
    return rows

def main():
    conn = connect_to_db()
    data = get_rabota(conn)

    for row in data:
        print(row)
if __name__ == "__main__":
    main()