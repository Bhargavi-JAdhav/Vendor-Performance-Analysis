import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode="a")

def create_vendor_summary(conn):
    vendor_sales_summary=pd.read_sql_query("""with 
    freightsummary as (select VendorNumber, sum(freight) as freight_cost
        from vendor_invoice
        group by VendorNumber),
        
    purchasesummary as (select p.VendorNumber, p.VendorName, p.Brand, p.PurchasePrice, 
        p.description, 
        pp.volume, pp.price as actual_price,
        Sum(p.Quantity) as total_purchase_quantity,
        sum(p.dollars) as total_purchase_dollars,
        sum(p.purchaseprice) as total_purchase_price
        from purchases p
        join purchase_prices pp
        on p.brand=pp.brand
        where p.purchaseprice>0
        group by p.VendorName, p.VendorNumber, p.Brand, p.description, p.purchaseprice, pp.price, 
        pp.volume),
        
    salessummary as (select 
        vendorno, brand, sum(salesdollars) as total_sales_dollars,
        sum(SalesPrice) as total_sales_price,
        sum(SalesQuantity) as total_sales_quantity,
        sum(excisetax) as total_excise_tax 
        from sales
        group by VendorNo, Brand)
        
        select
            ps.vendornumber,
            ps.vendorname,
            ps.brand,
            ps.description,
            ps.purchaseprice,
            ps.actual_price,
            ps.total_purchase_price,
            ps.total_purchase_dollars,
            ps.total_purchase_quantity,
            ps.volume,
            ss.total_sales_dollars,
            ss.total_sales_price,
            ss.total_excise_tax,
            ss.total_sales_quantity,
            fs.freight_cost
        from purchasesummary ps
        left join salessummary ss
            on ps.vendornumber=ss.vendorno
            and ps.brand=ss.brand
        left join freightsummary fs
            on ps.vendornumber=fs.vendornumber
        order by ps.total_purchase_dollars desc
        
        """, conn)
    return vendor_sales_summary

def clean_data(df):
    df['volume']=df['volume'].astype('float')
    df.fillna(0,inplace=True)
    df['VendorName']=df['VendorName'].str.strip()
    df['description']=df['description'].str.strip()

    vendor_sales_summary['GrossProfit']=vendor_sales_summary['total_sales_dollars']-vendor_sales_summary['total_purchase_dollars']
    vendor_sales_summary['ProfitMargine']=(vendor_sales_summary['GrossProfit']/vendor_sales_summary['total_sales_dollars'])*100
    vendor_sales_summary['StockTurnover']=vendor_sales_summary['total_sales_quantity']/vendor_sales_summary['total_purchase_quantity']
    vendor_sales_summary['sales_to_purchase_ratio']=vendor_sales_summary['total_sales_dollars']/vendor_sales_summary['total_purchase_dollars']

    return df

if __name__=='__main__':
    #creaating database connection
    conn=sqlite3.connect('inventory.db')

    logging.info('creating vendor summary table...')
    summary_df=create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning Data...')
    clean_df=clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data...')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('completed')
    