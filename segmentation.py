import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import boto3
import csv
color = sns.color_palette()
from IPython.display import display, HTML
# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id = 'AKIAI7M5MGZEAWGEXLNA',
    aws_secret_access_key = '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',
    region_name = 'us-east-2'
)
clientResponse = client.list_buckets()
    
# Print the bucket names one by one
print('Printing bucket names...')
for bucket in clientResponse['Buckets']:
    print(f'Bucket Name: {bucket["Name"]}')

# Create the S3 object
obj = client.get_object(
    Bucket = 'segmentationaws12',
    Key = 'Online_Retail.xlsx'
)
    
# Read data from the S3 object
df = pd.read_excel(obj['Body'])
print(df.head(10))
df.shape
df['Total_Price'] = df['UnitPrice']*df['Quantity']

tst = df.groupby(['InvoiceDate','InvoiceNo'])
        
tst.size()

# converting 
df['date'] = df['InvoiceDate'].map(lambda x: 100*x.year + x.month)

tst = df.groupby(['date'])
tst.size()

# checking country-wise sales 
Cust_country=df[['Country','CustomerID']].drop_duplicates()

#Calculating the distinct count of customer for each country
Cust_country_count=Cust_country.groupby(['Country'])['CustomerID'].\
aggregate('count').reset_index().sort_values('CustomerID', ascending=False)

#Plotting the count of customers
country=list(Cust_country_count['Country'])
Cust_id=list(Cust_country_count['CustomerID'])
plt.figure(figsize=(12,8))
sns.barplot(country, Cust_id, alpha=0.8, color=color[2])
plt.xticks(rotation='60')
plt.show()

Cust_date_UK=df[df['Country']=='United Kingdom']
Cust_date_UK=Cust_date_UK[['CustomerID','date']].drop_duplicates()

def recency(row):
    if row['date']>201110:
        val = 5
    elif row['date'] <= 201110 and row['date'] > 201108:
        val = 4
    elif row['date'] <= 201108 and row['date'] > 201106:
        val = 3
    elif row['date'] <= 201106 and row['date'] > 201104:
        val = 2
    else:
        val = 1
    return val

Cust_date_UK['Recency_Flag'] = Cust_date_UK.apply(recency, axis=1)
Cust_date_UK.head()
tst = Cust_date_UK.groupby('Recency_Flag')
tst.size()
Cust_freq=df[['Country','InvoiceNo','CustomerID']].drop_duplicates()
Cust_freq.head()

#Calculating the count of unique purchase for each customer and his buying freq in descending order
Cust_freq_count=Cust_freq.groupby(['Country','CustomerID'])['InvoiceNo'].aggregate('count').\
reset_index().sort_values('InvoiceNo', ascending=False)


Cust_freq_count_UK=Cust_freq_count[Cust_freq_count['Country']=='United Kingdom']
Cust_freq_count_UK.head()
unique_invoice=Cust_freq_count_UK[['InvoiceNo']].drop_duplicates()
unique_invoice['Freqency_Band'] = pd.qcut(unique_invoice['InvoiceNo'], 5)
unique_invoice=unique_invoice[['Freqency_Band']].drop_duplicates()
unique_invoice

def frequency(row):
    if row['InvoiceNo'] <= 13:
        val = 1
    elif row['InvoiceNo'] > 13 and row['InvoiceNo'] <= 25:
        val = 2
    elif row['InvoiceNo'] > 25 and row['InvoiceNo'] <= 38:
        val = 3
    elif row['InvoiceNo'] > 38 and row['InvoiceNo'] <= 55:
        val = 4
    else:
        val = 5
    return val

Cust_freq_count_UK['Freq_Flag'] = Cust_freq_count_UK.apply(frequency, axis=1)
Cust_freq_count_UK.groupby(['Freq_Flag']).size()
plt.figure(figsize=(12,8))
sns.countplot(x='Freq_Flag', data=Cust_freq_count_UK, color=color[1])
plt.ylabel('Count', fontsize=12)
plt.xlabel('Freq_Flag', fontsize=12)
plt.xticks(rotation='vertical')
plt.title('Frequency of Freq_Flag', fontsize=15)
plt.show()
Cust_monetary = df.groupby(['Country','CustomerID'])['Total_Price'].aggregate('sum').\
reset_index().sort_values('Total_Price', ascending=False)
Cust_monetary_UK=Cust_monetary[Cust_monetary['Country']=='United Kingdom']

unique_price=Cust_monetary_UK[['Total_Price']].drop_duplicates()
unique_price=unique_price[unique_price['Total_Price'] > 0]
unique_price['monetary_Band'] = pd.qcut(unique_price['Total_Price'], 5)
unique_price=unique_price[['monetary_Band']].drop_duplicates()
unique_price
def monetary(row):
    if row['Total_Price'] <= 243:
        val = 1
    elif row['Total_Price'] > 243 and row['Total_Price'] <= 463:
        val = 2
    elif row['Total_Price'] > 463 and row['Total_Price'] <= 892:
        val = 3
    elif row['Total_Price'] > 892 and row['Total_Price'] <= 1932:
        val = 4
    else:
        val = 5
    return val
Cust_monetary_UK['Monetary_Flag'] = Cust_monetary_UK.apply(monetary, axis=1)
Cust_monetary_UK.groupby(['Monetary_Flag']).size()

plt.figure(figsize=(12,8))
sns.countplot(x='Monetary_Flag', data=Cust_monetary_UK, color=color[1])
plt.ylabel('Count', fontsize=12)
plt.xlabel('Monetary_Flag', fontsize=12)
plt.xticks(rotation='vertical')
plt.title('Frequency of Monetary_Flag', fontsize=15)
plt.show()

Cust_UK_All=pd.merge(Cust_date_UK,Cust_freq_count_UK[['CustomerID','Freq_Flag']],\
on=['CustomerID'],how='left')
Cust_UK_All=pd.merge(Cust_UK_All,Cust_monetary_UK[['CustomerID','Monetary_Flag']],\
on=['CustomerID'],how='left')


Cust_UK_All.head(10)