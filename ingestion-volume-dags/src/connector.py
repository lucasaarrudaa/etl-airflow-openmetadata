import psycopg2

class Connector:
    def __init__(self, conn_str):
        self.conn_str = conn_str
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(self.conn_str)
            self.conn.autocommit = True  
            print("Connection successfully established.")
            return self.conn
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return None

    def disconnect(self):
        if self.conn:
            self.conn.close()
            print("Connection successfully terminated.")
