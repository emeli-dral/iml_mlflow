import pickle
import pandas as pd

from prefect import flow, task
from sklearn.metrics import root_mean_squared_error

@task(name='data load', log_prints=True)
def load_data(path):
    data = pd.read_parquet(path)
    return data

@task(name='data transform', log_prints=True)
def transform_data(data):
    data['duration'] = data['lpep_dropoff_datetime'] - data['lpep_pickup_datetime']
    data.duration = data.duration.apply(lambda td: td.total_seconds()/60)
    data = data[(data.duration >= 3.) & (data.duration <= 90.)] 
    data.fillna(0, inplace=True) #maybe debug this later
    return data

@task(name='model load', log_prints=True)
def load_model(path):
    with open(path, 'rb') as f_in:
        model = pickle.load(f_in)
    return model

@task(name='predictions generation', log_prints=True)
def generate_predictions(model, dataframe):
    num_features = ['total_amount', 'trip_distance', 'passenger_count']
    cat_features = ['PULocationID', 'DOLocationID']
    return model.predict(dataframe[num_features + cat_features])

@task(name='model validation', log_prints=True)
def validate_model(preds, actual):
    return root_mean_squared_error(preds, actual)

@flow(name='taxi ride duration prediction')
def the_flow():
    train = load_data('data/green_tripdata_2024-10.parquet')
    test = load_data('data/green_tripdata_2024-11.parquet')
    train_dataset = transform_data(train)
    test_dataset = transform_data(test)
    model = load_model('models/model.bin')
    train_preds = generate_predictions(model, train_dataset)
    test_preds = generate_predictions(model, test_dataset)
    validate_model(train_preds, train_dataset['duration'])
    validate_model(test_preds, test_dataset['duration'])

if __name__ == '__main__':
    the_flow()

    
    
    
    
    