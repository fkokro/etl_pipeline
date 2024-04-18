import pandas as pd
import requests
import os
from dataclasses import dataclass
import sys
from src.exception import CustomException
from src.logger import logging


@dataclass
class DataIngestionConfig:
    raw_data_json_path: str=os.path.join('artifacts','raw_data.json')
    train_data_path: str=os.path.join('artifacts','train.csv')
    test_data_path: str=os.path.join('artifacts','test.csv')
    train_data_txt_path: str=os.path.join('artifacts','train.txt')
    test_data_txt_path: str=os.path.join('artifacts','test.txt')
    raw_data_path: str=os.path.join('artifacts','data.csv')   
    

class DataETL:
    def __init__(self) -> None:
        self.ingestion_config = DataIngestionConfig()
        
    def api_extract(self, API_URL) -> dict:
        """Extract data from an API source. Store and return"""
        try:
            self.data = requests.get(API_URL)
            logging.info('Data successful extracted from {}'.format(API_URL))
            return self.data

        except Exception as e:
            raise CustomException(e, sys)
        
    def csv_extract(self, filename):
        """Import csv files"""
        try:
            df = pd.read_csv(filename)
            logging.info("{} imported".format(filename))
            return df
        except Exception as e:
            raise CustomException(e, sys)
    
    def fill_none_housing_data(self, df):
        """Fillna values for the housing data set, both categorical and numeric features."""
        for col in df.columns:
            check = df[col].dtypes
            if check == 'object':
                df.fillna({col:'None'}, inplace=True)
            else:
                df.fillna({col:df[col].mean()}, inplace=True)
        
        return df
        logging.info('Null values filled.')

    def process_housing_file(self):
        """Must enter data path from DataIngestionConfig"""
        try:
            train_df = self.csv_extract('notebooks/data/train.csv')
            test_df = self.csv_extract('notebooks/data/test.csv')
            trained_processed_df = self.fill_none_housing_data(train_df)
            test_processed_df = self.fill_none_housing_data(test_df)
            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path), exist_ok=True)
            os.makedirs(os.path.dirname(self.ingestion_config.test_data_path), exist_ok=True)
            os.makedirs(os.path.dirname(self.ingestion_config.train_data_txt_path), exist_ok=True)
            os.makedirs(os.path.dirname(self.ingestion_config.test_data_txt_path), exist_ok=True)
            trained_processed_df.to_csv(self.ingestion_config.train_data_path, index=False, header=True)
            trained_processed_df.to_csv(self.ingestion_config.train_data_txt_path, index=False, header=True)
            test_processed_df.to_csv(self.ingestion_config.test_data_path, sep='|', index=False)
            test_processed_df.to_csv(self.ingestion_config.test_data_txt_path, sep='|', index=False)
            logging.info('Data processed and stored in artifacts.')
            
        except Exception as e:
            raise CustomException(e, sys)
        

if __name__=="__main__":
   data_etl = DataETL()
   data_etl.process_housing_file()