import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
from utils.datasetup import *
from utils.dimension_classes import *

class MainETL():
    # List of columns need to be replaced
    def __init__(self) -> None:
        self.drop_columns = []
        self.dimension_tables = []
        self.fact_table = pd.DataFrame()

    def extract(self, csv_file="hotel_bookings.csv"):
        # Step 1: Extract: use pandas read_csv to open the csv file and extract data
        print(f'Step 1: Extracting data from csv file')
        self.fact_table = df
        print(f'We find {len(self.fact_table.index)} rows and {len(self.fact_table.columns)} columns in csv file: {csv_file}')
        print(f'Step 1 finished')

    def transform(self):
        #transform data types, here the .fillna(0) function is called and used for any columns having NaN values and creating errors
        str_cols = ['arrival_date_day_of_month', 'arrival_date_month', 'arrival_date_year', 'meal', 'country', 'reservation_status']
        int_cols = ['adults', 'children', 'babies','is_repeated_guest']
        self.fact_table[str_cols] = self.fact_table[str_cols].astype(str)
        self.fact_table[int_cols] = self.fact_table[int_cols].fillna(0).astype(int)

        #fetch dimension tables
        dim_guest = DimGuest()
        self.drop_columns += dim_guest.columns
        self.dimension_tables.append(dim_guest)

        dim_booking = DimBooking()
        self.drop_columns += dim_booking.columns
        self.dimension_tables.append(dim_booking)

        dim_timeanddate = DimTimeAndDate()
        self.drop_columns += dim_timeanddate.columns
        new_timeanddatedim = dim_timeanddate.dimension_table.copy()
        #concat day month and year values and make format d/m/y
        month_mapping = {
            "January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6, "July": 7, "August": 8, "September": 9, "October": 10, "November": 11, "December": 12}
        dim_timeanddate.dimension_table['arrival_date_month'] = dim_timeanddate.dimension_table['arrival_date_month'].map(month_mapping)
        new_timeanddatedim['arrival_date'] = range(1, len(new_timeanddatedim) + 1)
        new_timeanddatedim['arrival_date'] = (dim_timeanddate.dimension_table['arrival_date_day_of_month'].astype(str) + '/' + 
            dim_timeanddate.dimension_table['arrival_date_month'].astype(str) + '/' +
            dim_timeanddate.dimension_table['arrival_date_year'].astype(str))
        new_timeanddatedim['arrival_date'] = pd.to_datetime(new_timeanddatedim['arrival_date'], format = '%d/%m/%Y')
        dim_timeanddate.dimension_table = new_timeanddatedim
        self.dimension_tables.append(dim_timeanddate)

        dim_marketing = DimMarketing()
        self.drop_columns += dim_marketing.columns
        self.dimension_tables.append(dim_marketing)

        dim_rooms = DimRooms()
        self.drop_columns += dim_rooms.columns
        self.dimension_tables.append(dim_rooms)

        for dim in self.dimension_tables:
            print(f'Before merging {dim.__class__.__name__}, column types:')
            print('fact_table:')
            print(self.fact_table[dim.columns].dtypes)
            print('dimension_table:')
            print(dim.dimension_table[dim.columns].dtypes)
            
            for col in dim.columns:
                if col in self.fact_table.columns:
                    if pd.api.types.is_numeric_dtype(self.fact_table[col]):
                        self.fact_table[col] = self.fact_table[col].astype(dim.dimension_table[col].dtype)
                    else:
                        self.fact_table[col] = self.fact_table[col].astype(str)

        #keeping columns which need to stay in the fact table
        columns_to_drop = [col for col in self.drop_columns if col in self.fact_table.columns and col not in [
            'arrival_date_month', 'meal', 'country', 'reservation_status', 'adults', 'children', 'babies', 'is_repeated_guest']]
        
        for dim in self.dimension_tables:
            self.fact_table = pd.merge(self.fact_table, dim.dimension_table, on=dim.columns, how='left')
        self.fact_table = self.fact_table.drop(columns=columns_to_drop)

        print(self.fact_table.columns)
        
        print(f'Step 2 finished')

    def load(self):
        for table in self.dimension_tables:
            table.load()
        with engine.connect() as con:
            trans = con.begin()
            self.fact_table['Hotel_fact_id'] = range(1, len(self.fact_table) + 1)
            database.upload_dataframe_sqldatabase(f'Hotel_fact', blob_data=self.fact_table)
            
            # self.fact_table['Total_Pay_Fact_id'] = range(len(self.fact_table) + 2, 2*(len(self.fact_table) + 1))
            # database.append_dataframe_sqldatabase(f'Total_Pay_Fact', blob_data=self.fact_table)
            self.fact_table.to_csv('./data/Hotel_fact.csv')

            for table in self.dimension_tables:
                con.execute(text(f'ALTER TABLE [dbo].[Hotel_fact] WITH NOCHECK ADD CONSTRAINT [FK_{table.name}_dim] FOREIGN KEY ([{table.name}_id]) REFERENCES [dbo].[{table.name}_dim] ([{table.name}_id]) ON UPDATE CASCADE ON DELETE CASCADE;'))
            trans.commit()
            
        print(f'Step 3 finished')

    def mainLoop(self):    
        # Step 1
        self.extract()
        # Step 2
        self.transform()
        # Step 3
        database.delete_sqldatabase('Hotel_fact')
        self.load()
                
def main():
    # create an instance of MainETL
    main = MainETL()
    main.mainLoop()
    
if __name__ == '__main__':
    main() 