import pymysql


class DatabaseContextManager:
    def __init__(self, is_select=False):
        self.is_select = is_select

    def __enter__(self):
        self.connection = pymysql.connect(host="localhost", user="root", password="38467173", database="task")
        self.cursor = self.connection.cursor()
        self.connection.autocommit(False)
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.is_select:
            self.connection.commit()
        self.connection.close()
