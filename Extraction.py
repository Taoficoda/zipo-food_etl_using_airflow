import pandas as pd

def extraction():
    try:
        data = pd.read_csv(r'raw_data\zipco_transaction.csv')
        print("Data Extracted Successfully")
    except Exception as e:
        print(f"An error occured {e}")
