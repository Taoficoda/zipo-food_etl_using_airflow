import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

def run_loading():
    # Loading datasets
    data = pd.read_csv(r'cleaned_data\cleaned.csv')
    product = pd.read_csv(r"cleaned_data\products.csv")
    staff = pd.read_csv(r"cleaned_data\staff.csv")
    customer = pd.read_csv(r"cleaned_data\customer.csv")
    transcation_fact = pd.read_csv(r"cleaned_data\transaction_fact.csv")

    # Load the environmental variables from .env files
    load_dotenv()

    # Get the environmental variables
    connect_str = os.getenv("AZURE_CONNECTION_STRING_VALUE")
    container_name = os.getenv("CONTAINER_NAME")

    # Create an Azure service connection
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    # Define the files to upload
    files = [
        (data, r"raw_data\cleaned.csv"),
        (customer, r"cleaned_data\customer.csv"),
        (product, r"cleaned_data\products.csv"),
        (staff, r"cleaned_data\staff.csv"),
        (transcation_fact, r"cleaned_data\transaction_fact.csv")
    ]

    # Upload files to Azure Blob Storage
    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f'{blob_name} loaded into Azure Blob Storage')
