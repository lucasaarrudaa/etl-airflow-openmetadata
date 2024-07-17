# etl-airflow-openmetadata

# Documentation of the ETL and Data Analysis Project

## Project Overview

### Data Sources

Data was extracted from four different sources, all stored in a blob storage on Azure and in various formats, essential for the analysis of marketing campaign performance:
- **Google Ads** (JSON): Data related to advertising costs.
- **Facebook Ads** (JSON): Similar data to Google's, focusing on campaign costs.
- **User Accessed Ads** (TXT): Records of user ad clicks.
- **User Leader Analytical Table** (CSV): Consolidated information about user interactions with campaigns.

### ETL Process

#### Extraction
Data was extracted using Airflow with custom classes:
- `Extractor`: Class to extract data from blob storage and load it into dataframes.
- `Transformer`: Class to apply necessary transformations to the data.
- `Loader`: Class to load the transformed data into the database schemas.

### Design Patterns Utilized

#### Factory Method
This pattern is used to create objects without specifying the exact class to be instantiated. In your code, the Factory Method pattern is implemented to instantiate different types of readers and data processors based on file types.

Eg:
```py
class Extractor:
    def __init__(self, reader, file_type, blob_name, delimiter=',', header=0, columns=None):
        self.reader = reader
        self.file_type = file_type
        # more initialization
```

#### Decorator
Decorators are used to dynamically modify the behavior of functions or methods. In Airflow, the @task decorator transforms a regular Python function into an Airflow task, while @dag defines a complete DAG.
Eg:
```py
@dag(default_args=default_args, schedule_interval=None, description='etl', catchup=False)
def etl():
    @task
    def extract_data(path, file_type, columns=None, delimiter=','):
        # Function implementation
```
In this section, the Extractor is configured to use a reader object that may vary depending on the type of data storage (such as BlobStorageReader). The choice of reader is abstracted and can be changed without modifying the Extractor's code.

#### Strategy
The Strategy pattern is applied to select the transformation algorithm based on the type of data processed. This allows the transformation behavior to be selected at runtime.

```py
class Transformer:
    def __init__(self, dataframe, dataframe_type):
        self.dataframe = dataframe
        self.dataframe_type = dataframe_type

    def transform(self):
        if self.dataframe_type == 'leads':
            self._transform_leads()
        elif self.dataframe_type == 'pageview':
            self._transform_pv()
        # Additional conditions
```
This section shows how the Transformer adjusts its transformation logic according to the dataframe_type, using different internal methods for different data types.


### Systems and Tools Used

- **PostgreSQL**: Chosen for its robustness and efficiency in managing large volumes of data.
- **Airflow (OpenMetadata)**: Used to automate and manage the ETL pipeline, providing refined control over each process stage.

## Container Network Configuration

All services and tools used are configured to operate on the same Docker network, named `openmetadata-docker_app_net`. This includes:
- `openmetadata_ingestion`: Responsible for data ingestion.
- `openmetadata_postgresql`: PostgreSQL database server.
- `openmetadata_elasticsearch`: Elasticsearch server for searches and analysis.
- `openmetadata_server`: Main OpenMetadata server.
- `docker-etl-postgres_db_1`: Database container used for ETL.
- `docker-etl-postgres_pgadmin_1`: Graphical interface for PostgreSQL administration.

This configuration ensures that all components can communicate efficiently without the need for external network configurations, facilitating network administration and security.

## Schema Pattern

### Staging (`staging`)

- **Purpose**: Serves as a preparation area for data that has already undergone pre-transformations before being loaded. These transformations include:
  - **Data type conversion**
  - **Column renaming**
  - **Date formatting**
  - **Null value and standard treatment**
  - **URL extraction and cleaning**

- **Benefits**:
  - **Initial Preparation**: Facilitates more complex transformations and integration of data from various sources.
  - **Efficiency**: Improves the efficiency of the ETL process by reducing complexity in subsequent schemas.

### Transformed (`transformed`)

- **Purpose**: Contains data transformed and normalized according to specific business rules for detailed analysis.

### Data Warehouse (`dw`)

- **Purpose**: Stores final data modeled in dimensional and factual schemas, ready for analysis and business intelligence reporting.

## Detailed ETL Implementation

### Transformations in `staging`

Before loading data into the `staging` schema, the following pre-transformations are performed:
- **Data types**: Conversion to formats compatible with analysis.
- **Column renaming**: Adjustment for clarity and consistency.
- **Date formatting**: Standardization in PostgreSQL's date/time format.
- **Null handling**: Correction or filling of missing values.
- **URL cleaning**: Decomposition and removal of unnecessary parameters.

### Transformations in `transformed`

- **Aggregation**: Organization of data by relevant dimensions to facilitate analysis.
- **Enrichment and Normalization**: Additional adjustments to ensure consistency and utility of the data.

### Data Warehouse (`dw`)

- **Dimensional Modeling**: Structuring of data in dimensional and factual tables to optimize queries and analysis.

## Analyses Performed

Analyses focused on identifying the effectiveness of campaigns and ads based on cost, revenue, clicks, and lead generation.

## OpenMetadata: What It Is and Why Use It

### What Is OpenMetadata

OpenMetadata is a metadata management platform designed to centralize, manage, and make available information about data and data processes within an organization. This tool offers features for cataloging, managing, and governing data automatically, providing a clear view of how data is used and facilitating the administration of security and quality policies.

### Objectives of Using It in the Project

- **Data Automation and Governance**: Use OpenMetadata to automate the data cataloging process and manage the metadata associated with the ETL project. This helps maintain a consistent and up-to-date view of data usage.
- **Metadata Monitoring and Management**: Monitor data usage and manage metadata to ensure compliance with data policies and improve data quality.
- **Integration and Collaboration**: Facilitate integration and collaboration among teams, providing easy and controlled access to metadata information.

## Conclusion

The schema structure adopted, along with the pre-transformations in `staging`, provides a solid foundation for effective analysis, data governance, and reliable insights from the DW. The inclusion of OpenMetadata in the project enriches this environment, bringing an additional layer of automation, monitoring, and governance crucial for success in modern data environments.
