import pandas as pd
import os

class Loader:
    def __init__(self, connector, directory='/opt/csv_data'):
        self.connector = connector
        self.directory = directory
        self.conn = self.connector.connect()
        self.cursor = self.conn.cursor() if self.conn else None

    def check_table_exists(self, table_name):
        try:
            with self.connector.connect() as conn:
                cursor = conn.cursor()
                query = f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'staging' 
                        AND table_name = '{table_name}'
                    );
                """
                cursor.execute(query)
                exists = cursor.fetchone()[0]
                print(f"The table '{table_name}' exists: {exists}")
                return exists
        except Exception as e:
            print(f"Error checking existence of the table {table_name}: {e}")
            return False

    def load_to_db(self, df, schema, table):
        os.makedirs(self.directory, exist_ok=True)
        bytes_per_row = df.memory_usage(index=True, deep=True).sum() / len(df)
        target_file_size = 1e9  # 1GB
        chunk_size = int(target_file_size / (bytes_per_row * 2))

        for i, chunk in enumerate(range(0, len(df), chunk_size)):
            file_path = os.path.join(self.directory, f'output_chunk_{schema}_{table}_{i}.csv')
            df.iloc[chunk:chunk+chunk_size].to_csv(file_path, index=False, header=False)
            print(f"File {file_path} saved successfully.")

            # Execute Copy From
            self.copy_from(schema, table, file_path)

            # Check if Copy From was successful before deleting
            os.remove(file_path)
            print(f"File {file_path} removed successfully.")

    def copy_from(self, schema, table, file_path):
        # Building the table path with correct double quotes
        table_path = f'{schema}.{table}'
        # Building the COPY query
        copy_query = f"COPY {table_path} FROM '{file_path}' WITH (FORMAT csv, DELIMITER ',', HEADER FALSE, NULL '');"

        try:
            # Executing the COPY query
            self.cursor.execute(copy_query)
            self.conn.commit()
            print(f"Data successfully inserted into the table {table_path}.")
        except Exception as e:
            self.conn.rollback()
            print(f"Failure to execute copy on the table {table_path}: {e}")

    def run_query_from_file(self, file_path):
        with open(file_path, 'r') as file:
            sql_query = file.read()
        try:
            self.cursor.execute(sql_query)
            self.conn.commit()
            print("Query successfully executed from the file.")
        except Exception as e:
            self.conn.rollback()
            print(f"Failure to execute the query from the file {file_path}: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
