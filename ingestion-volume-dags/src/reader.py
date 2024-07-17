import pandas as pd
from azure.storage.blob import BlobServiceClient

class BlobStorageReader:
    def __init__(self, conn_str, container_name):
        self.blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        self.container_client = self.blob_service_client.get_container_client(container_name)
        print(f"BlobStorageReader initialized for container: {container_name}")

    def read_csv(self, blob_name, delimiter=',', header=0, columns=None):
        print(f"Reading CSV from blob: {blob_name}, with delimiter: '{delimiter}', header: {header}")

        blob_client = self.container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()
        df = pd.read_csv(pd.io.common.BytesIO(blob_data), delimiter=delimiter, header=header)
        if columns is not None:
            df.columns = columns
        print(f"DataFrame columns after setting custom names: {df.columns}")
        return df

    def read_json(self, blob_name, lines=True):
        print(f"Reading JSON from blob: {blob_name}")
        blob_client = self.container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()
        return pd.read_json(pd.io.common.BytesIO(blob_data), lines=lines)