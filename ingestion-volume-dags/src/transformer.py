import pandas as pd
import numpy as np
from urllib.parse import urlparse, parse_qs

class Transformer:
    def __init__(self, dataframe, dataframe_type):
        self.dataframe = dataframe
        self.dataframe_type = dataframe_type
        print(f"Initialized Transformer with type: {self.dataframe_type}")

    def _strip_columns(self):
        print("Stripping columns...")
        for col in self.dataframe.select_dtypes(include=['object']):
            self.dataframe[col] = self.dataframe[col].str.strip()
        print("Columns stripped.")

    def transform(self):
        print("Starting transformation...")
        self._strip_columns()

        if self.dataframe_type == 'leads':
            self._transform_leads()
        elif self.dataframe_type == 'pageview':
            self._transform_pv()
        elif self.dataframe_type == 'facebook' or self.dataframe_type == 'google':
            self._transform_campaign_data()
        print("Transformation complete.")

    def _transform_leads(self):
        date_columns = ['registered_at', 'credit_decision_at', 'signed_at']
        for col in date_columns:
            self.dataframe[col] = pd.to_datetime(self.dataframe[col], errors='coerce')
        self.dataframe['revenue'] = self.dataframe['revenue'].fillna(0)

    def _transform_pv(self):
        self.apply_transforms_pv()
        self.dataframe['campaign_domain'] = self.dataframe['campaign_link'].apply(
            lambda x: urlparse(x).netloc if pd.notna(x) else np.nan)
        self.dataframe['campaign_path'] = self.dataframe['campaign_link'].apply(
            lambda x: urlparse(x).path if pd.notna(x) else np.nan)
        self.dataframe['advertising_domain'] = self.dataframe['advertising'].apply(
            lambda x: urlparse(x).netloc if pd.notna(x) else np.nan)
        self.dataframe['advertising_path'] = self.dataframe['advertising'].apply(
            lambda x: urlparse(x).path if pd.notna(x) else np.nan)

    def _transform_campaign_data(self):
        print(f"Transforming campaign data for: {self.dataframe_type}")
        self.apply_rename_campaign_date()
        if self.dataframe_type == 'facebook':
            self.apply_rename_facebook_campaign_name()
            campaign_id_column = 'facebook_campaign_id'
        elif self.dataframe_type == 'google':
            self.apply_rename_google_campaign_name()
            campaign_id_column = 'google_campaign_id'
        else:
            raise ValueError("Invalid dataframe type provided")
        print("Transformation complete.")

        if campaign_id_column not in self.dataframe.columns:
            print(f"Error: Column {campaign_id_column} not found in dataframe. Available columns: {self.dataframe.columns}")
            print('não tem')
            
        self.dataframe['campaign_date'] = pd.to_datetime(self.dataframe['campaign_date'], errors='coerce')
        print(f"Campaign date converted. Current columns: {self.dataframe.columns}")
        self.dataframe[campaign_id_column] = self.dataframe[campaign_id_column].astype(str)
        print(f"Campaign ID column: {campaign_id_column} transformed to string.")

    def apply_transforms_pv(self):
        transforms = [
            ('ip', 'ips', r'(\d{3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'),
            ('device_id', 'device_id', r'device_id:\s([^\s]+)'),
            ('click', 'ips', r'(http.+)'),
            ('referer', 'refer', r'(http.+)'),
            ('data', 'ips', r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})')
        ]
        for new_col, old_col, regex in transforms:
            self.dataframe[new_col] = self.extract_string(self.dataframe[old_col], regex)
        delete_cols_pv = ['ips', 'refer']
        for col in delete_cols_pv:
            self.dataframe = self.delete_col(self.dataframe, col)
        self.apply_renames_pv()

    def apply_renames_pv(self):
        renames = [
            ('data', 'data_click'),
            ('click', 'campaign_link'),
            ('referer', 'advertising')
        ]
        for old_name, new_name in renames:
            self.rename(old_name, new_name)

    def apply_rename_campaign_date(self):
        print("Renaming date to campaign_date...")
        self.rename('date', 'campaign_date')
        print("Date column renamed.")

    def apply_rename_facebook_campaign_name(self):
        print("Renaming Facebook campaign name...")
        self.rename('facebook_campaign_name', 'campaign_name')
        print("Facebook campaign name renamed.")

    def apply_rename_google_campaign_name(self):
        print("Renaming Google campaign name...")
        self.rename('google_campaign_name', 'campaign_name')
        print("Google campaign name renamed.")

    def extract_string(self, series, regex):
        return series.str.extract(regex, expand=False)

    def delete_col(self, df, column):
        return df.drop(columns=[column], errors='ignore')

    def rename(self, old_name, new_name):
        self.dataframe.rename(columns={old_name: new_name}, inplace=True)

    def get_transformed_df(self):
        return self.dataframe
    
    def calculate_time_differences(self):
        self.dataframe['time_to_decision'] = self.dataframe['credit_decision_at'] - self.dataframe['registered_at']
        self.dataframe['time_to_sign'] = self.dataframe['signed_at'] - self.dataframe['registered_at']

    def enrich_device_id_from_ip(self, pv_df, leads_df):
        if 'ip' in leads_df.columns and 'device_id' in leads_df.columns:
            ip_to_device_id = leads_df.dropna(subset=['ip', 'device_id']).set_index('ip')['device_id'].to_dict()
            pv_df['device_id'] = pv_df['ip'].map(ip_to_device_id).fillna(pv_df['device_id'])
        else:
            print("Error: 'ip' and/or 'device_id' columns are missing in leads_df")
