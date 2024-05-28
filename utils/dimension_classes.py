from utils.datasetup import *
import pandas as pd

blob_name = "hotel_bookings.csv"
database = AzureDB()
database.access_container("sukobookeasecontainer")
df = database.access_blob_csv(blob_name=blob_name)

class ModelAbstract():
    def __init__(self):
        self.columns = None
        self.dimension_table = None

    def dimension_generator(self, name:str, columns:list):
        dim = df[columns]
        dim = dim.drop_duplicates()
        # Creating primary key for dimension table
        dim[f'{name}_id'] = range(1, len(dim) + 1)

        self.dimension_table = dim
        self.name = name
        self.columns = columns


    def load(self): 
        if self.dimension_table is not None:
            # Upload dimension table to data warehouse
            database.upload_dataframe_sqldatabase(f'{self.name}_dim', blob_data=self.dimension_table)
        
            # Saving dimension table as separate file
            self.dimension_table.to_csv(f'./data/{self.name}_dim.csv')
        else:
            print("Please create a dimension table first using dimension_generator") 

class DimGuest(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('Guest', ['adults', 'children', 'babies', 'country'])

class DimBooking(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('Booking', ['is_canceled', 'stays_in_week_nights', 'stays_in_weekend_nights', 'previous_cancellations', 'booking_changes', 'deposit_type', 'required_car_parking_spaces', 'total_of_special_requests', 'meal', 'previous_bookings_not_canceled'])

class DimTimeAndDate(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('TimeAndDate', ['lead_time', 'arrival_date_year', 'arrival_date_month', 'arrival_date_week_number', 'arrival_date_day_of_month', 'reservation_status', 'reservation_status_date', 'days_in_waiting_list'])

class DimMarketing(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('Marketing', ['market_segment', 'distribution_channel', 'is_repeated_guest', 'agent', 'company', 'customer_type', 'adr'])

class DimRooms(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('Rooms', ['reserved_room_type', 'assigned_room_type'])