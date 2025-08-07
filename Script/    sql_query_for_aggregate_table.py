#now the task is to merge all the tables that we have created for final data set.
vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
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
    
