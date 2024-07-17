class Extractor:
    def __init__(self, reader, file_type, blob_name, delimiter=',', header=0, columns=None):
        self.reader = reader
        self.file_type = file_type
        self.blob_name = blob_name
        self.delimiter = delimiter
        self.header = header
        self.columns = columns
        print(f"Extractor initialized with file_type: {file_type}, blob_name: {blob_name}, header: {header}")

    def extract(self):
        if self.file_type == 'csv':
            print(f"Extracting CSV with delimiter: '{self.delimiter}', header: {self.header}")
            return self.reader.read_csv(self.blob_name, delimiter=self.delimiter, header=self.header, columns=self.columns)
        elif self.file_type == 'json':
            print(f"Extracting JSON")
            return self.reader.read_json(self.blob_name)