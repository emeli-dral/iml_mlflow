from prefect import flow, task

@task(name='data load', log_prints=True)
def load_data(path):
    print(f'loaded: {path}')

@task(name='data transform', log_prints=True)
def transform_data(path):
    print(f'transformed: {path}')

def load_transform_data(path):
   print(path)

def train_test_split(path):
   print(path)

def dataset_visualization(dataframe):
    pass

def train_model(dataframe):
    pass

@task(name='model load', log_prints=True)
def load_model(model):
    print(f'load model {model}')

@task(name='model validation', log_prints=True)
def validate_model(preds, actual):
    print(f'validation for {preds}, {actual}')

@task(name='predictions generation', log_prints=True)
def generate_predictions(model, dataframe):
    print(f'generate predictions {model}, {dataframe}')

@flow(name='taxi ride duration prediction')
def the_flow():
    load_data('train_data')
    load_data('test_data')
    transform_data('train_data')
    transform_data('test_data')
    load_model('best_model')
    generate_predictions('model', 'train_data')
    generate_predictions('model', 'test_data')
    validate_model('preds_train', 'y_train')
    validate_model('preds_test', 'y_test')

if __name__ == '__main__':
    the_flow()

    
    
    
    
    