import pandas as pd


def run_transformation():
    data = pd.read_csv(r'raw_data\zipco_transaction.csv')

    # Changing the date data type
    data['Date'] = pd.to_datetime(data['Date'])


 # Handling missing values
    numeric_columns = data.select_dtypes(include=['Float64', 'int64']).columns
    for cols in numeric_columns:
        data.fillna({cols:data[cols].mean()},inplace=True)

# Handling missing values(Fill Missing strings/Objects with 'Unknown')
    string_columns = data.select_dtypes(include=['object']).columns
    for str_cols in string_columns:
        data.fillna({str_cols:'Unknown'}, inplace=True)


# Creating the fact and dimensions table.
# Creating the product table
    product = data[['ProductName']].copy().drop_duplicates().reset_index(drop=True)
    product.index.name = 'Product_ID'
    product = product.reset_index()

# Creating the Customer dimensio table

    customer = data[['CustomerName','CustomerAddress','Customer_PhoneNumber','CustomerEmail']].copy().drop_duplicates().reset_index(drop=True)
    customer.index.name = 'Customer_ID'
    customer = customer.reset_index()

# Creating the staff table

    staff = data[['Staff_Name', 'Staff_Email']].copy().drop_duplicates().reset_index(drop=True)
    staff.index.name = 'Staff_ID'
    staff = staff.reset_index()

# Creating the transaction fact table

# Merging all the tables to create a fact table
    transcation_fact = data.merge(product, on=['ProductName'], how='left')\
        .merge(customer, on=['CustomerName','CustomerAddress','Customer_PhoneNumber','CustomerEmail'], how='left')\
                .merge(staff, on=['Staff_Name', 'Staff_Email'], how='left')

    transcation_fact.index.name = 'Transaction_ID'
    transcation_fact.reset_index()

# Slicing the columns of the act tables
    transcation_fact = transcation_fact[['Customer_ID','Product_ID','Staff_ID','Date', 'Quantity', 'UnitPrice', 'StoreLocation', 'PaymentType',
        'PromotionApplied', 'Weather', 'Temperature', 'StaffPerformanceRating',
        'CustomerFeedback', 'DeliveryTime_min', 'OrderType', 'DayOfWeek',
        'TotalSales', 'ProductName','CustomerName', 'CustomerAddress', 'Customer_PhoneNumber',
        'CustomerEmail','Staff_Name', 'Staff_Email']]
    
# Save files to csv locally.
    data.to_csv(r'cleaned_data\cleaned.csv',index=False)
    product.to_csv(r'cleaned_data\products.csv',index=False)
    customer.to_csv(r'cleaned_data\customer.csv',index=False)
    staff.to_csv(r'cleaned_data\staff.csv',index=False)
    transcation_fact.to_csv(r'cleaned_data\transaction_fact.csv',index=False)
    
    