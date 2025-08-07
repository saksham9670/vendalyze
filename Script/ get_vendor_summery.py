import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vendor_summery.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summery(conn):
    '''thise function will merge the different tables to get the overall vendor summary and adding new colums int the resultant data'''
    #now the task is to merge all the tables that we have created for final data set.
    vendor_sales_summery = pd.read_sql_query("""WITH FreightSummary AS (
     SELECT
         VendorNumber,
         SUM(Freight) AS FreightCost
     From vendor_invoice
     group by VendorNumber
    ),
    
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        From purchases p
        join purchase_prices pp
            ON p.Brand=pp.Brand
        WHERE p.PurchasePrice > 0
        Group by p.VendorNumber,p.VendorName,p.Brand,p.Description,p.PurchasePrice,pp.Price,pp.Volume
    ),
    
    SalesSummary AS (
     SELECT
         VendorNo,
         Brand,
         SUM(SalesQuantity) AS TotalSalesQuantity,
         SUM(SalesDollars) AS TotalSalesDollars,
         SUM(SalesPrice) AS TotalSalesPrice,
         SUM(ExciseTax) AS TotalExciseTax
     FROM sales
     group by VendorNo,Brand
    )
    
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExcisetax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand=ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC""",conn)

    return vendor_sales_summery

def clean_data(df):
    '''thise function will clean the data'''
    #changing data tyoe to float
    df['Volume'] = df['Volume'].astype('float64')
 
    #filling missing values with 0
    df.fillna(0,inplace=True)

    # removing spaces from catagorical colums
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description']= df['Description'].str.strip()

    #creating new colums for better analysis.
    vendor_sales_summary['GrossProfit']=vendor_sales_summary['TotalSalesDollars']-vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit'] / vendor_sales_summary['TotalSalesDollars'])*100
    vendor_sales_summary['StockTurnover'] =vendor_sales_summary['TotalSalesQuantity'] / vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary['SalestoPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars'] / vendor_sales_summary['TotalPurchaseDollars']

    return df

if __name__=='__main__':
    #creating database connection
    conn = sqlite3.connect('inventory.db')

    logg.info('Creating vendor summary table....')
    summary_df= create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('cleaning data....')
    clean_df= clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data.....')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    loggin.info('completed')
    
    
        
