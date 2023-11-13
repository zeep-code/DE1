
-- Create Database

DROP DATABASE IF EXISTS Addidas_Sales;
CREATE DATABASE Addidas_Sales;
USE Addidas_Sales;

-- OPERATIONAL LAYER

DROP TABLE IF EXISTS SalesTransaction;
DROP TABLE IF EXISTS Location;
DROP TABLE IF EXISTS Product;
DROP TABLE IF EXISTS Retailer;

-- Create Retailer Table
CREATE TABLE retailer (
    Retailer_Entry_ID INT AUTO_INCREMENT PRIMARY KEY,
    Retailer_ID INT,
    Retailer_Name VARCHAR(255)
);

-- Create Product Table
CREATE TABLE product (
    Product_ID INT AUTO_INCREMENT PRIMARY KEY,
    Product_Name VARCHAR(255),
    Price_per_Unit DECIMAL(10, 2)
);

-- Create Location Table
CREATE TABLE location (
    Location_ID INT AUTO_INCREMENT PRIMARY KEY,
    Region VARCHAR(255),
    State VARCHAR(255),
    City VARCHAR(255)
);

-- Create Sales Transaction Table
CREATE TABLE salestransaction (
    Transaction_ID INT AUTO_INCREMENT PRIMARY KEY,
    Retailer_Entry_ID INT,
    Product_ID INT,
    Location_ID INT,
    Invoice_Date DATETIME,
    Units_Sold INT,
    Total_Sales DECIMAL(10, 2),
    Operating_Profit DECIMAL(10, 2),
    Operating_Margin DECIMAL(5, 2),
    Sales_Method VARCHAR(255),
    FOREIGN KEY (Retailer_Entry_ID) REFERENCES retailer(Retailer_Entry_ID),
    FOREIGN KEY (Product_ID) REFERENCES product(Product_ID),
    FOREIGN KEY (Location_ID) REFERENCES location(Location_ID)
);

-- For loading data into operational layer i have used a python script since i had to take a subset of the dataset and make sure
-- it was representative of the dataset

-- ANALYTICAL LAYER

-- Drop existing tables if they exist
DROP TABLE IF EXISTS ProductAnalytics;
DROP TABLE IF EXISTS MarketSalesPerformance;

-- ProductAnalytics Table
CREATE TABLE ProductAnalytics (
    ProductAnalytics_ID INT AUTO_INCREMENT PRIMARY KEY,
    Product_ID INT,
    Date_First_Sold DATE,
    Total_Units_Sold INT,
    Total_Sales DECIMAL(10,2),
    Total_Profit DECIMAL(10,2),
    Avg_Unit_Price DECIMAL(10,2),
    Avg_Unit_Cost DECIMAL(10,2),  -- Assuming this is equivalent to 'Price per Unit' in the dataset
    Lifecycle_Stage VARCHAR(50),
    FOREIGN KEY (Product_ID) REFERENCES product(Product_ID)
);

-- MarketSalesPerformance Table
CREATE TABLE MarketSalesPerformance (
    MarketSalesPerformance_ID INT AUTO_INCREMENT PRIMARY KEY,
    Region VARCHAR(255),
    Total_Sales DECIMAL(10,2),
    Total_Units_Sold INT,
    Avg_Sales_Per_Unit DECIMAL(10,2),
    Avg_Margin_Per_Unit DECIMAL(10,2),
    Market_Share DECIMAL(10,2)
);

-- ETL for ProductAnalytics
DROP PROCEDURE IF EXISTS ETL_ProductAnalytics;

DELIMITER //
CREATE PROCEDURE ETL_ProductAnalytics()
BEGIN
	DELETE FROM ProductAnalytics;
    INSERT INTO ProductAnalytics (Product_ID, Date_First_Sold, Total_Units_Sold, Total_Sales, Total_Profit, Avg_Unit_Price, Avg_Unit_Cost, Lifecycle_Stage)
    SELECT 
        st.Product_ID,
        MIN(st.Invoice_Date) AS Date_First_Sold,
        SUM(st.Units_Sold),
        SUM(st.Total_Sales),
        SUM(st.Total_Sales) - SUM(st.Units_Sold * p.Price_per_Unit),
        AVG(st.Total_Sales / st.Units_Sold) AS Avg_Unit_Price,
        p.Price_per_Unit AS Avg_Unit_Cost,
        CASE 
            WHEN SUM(st.Units_Sold) < 500 THEN 'Introduction'
            WHEN SUM(st.Units_Sold) BETWEEN 500 AND 2000 THEN 'Growth'
            WHEN SUM(st.Units_Sold) BETWEEN 2001 AND 5000 THEN 'Maturity'
            ELSE 'Decline'
        END AS Lifecycle_Stage
    FROM 
        salestransaction st
    JOIN 
        product p ON st.Product_ID = p.Product_ID
    GROUP BY 
        st.Product_ID;
END //
DELIMITER ;

-- ETL for MarketSalesPerformance
DROP PROCEDURE IF EXISTS ETL_MarketSalesPerformance;

DELIMITER //
CREATE PROCEDURE ETL_MarketSalesPerformance()
BEGIN
    INSERT INTO MarketSalesPerformance (Region, Total_Sales, Total_Units_Sold, Avg_Sales_Per_Unit, Avg_Margin_Per_Unit, Market_Share)
    SELECT 
        l.Region,
        SUM(st.Total_Sales),
        SUM(st.Units_Sold),
        AVG(st.Total_Sales / st.Units_Sold),
        AVG((st.Total_Sales - st.Units_Sold * p.Price_per_Unit) / st.Units_Sold),  -- Corrected to use p.Price_per_Unit
        (SUM(st.Total_Sales) / (SELECT SUM(Total_Sales) FROM salestransaction) * 100)
    FROM 
        salestransaction st
    JOIN 
        location l ON st.Location_ID = l.Location_ID
    JOIN 
        product p ON st.Product_ID = p.Product_ID  -- Join with product table to access Price_per_Unit
    GROUP BY 
        l.Region;
END //
DELIMITER ;


-- RUN ETL PROCEDURES
CALL ETL_ProductAnalytics();
CALL ETL_MarketSalesPerformance();

-- Views

-- View for Product Lifecycle and Profitability Analysis
DROP VIEW IF EXISTS ProductLifecycleProfitability;

CREATE VIEW ProductLifecycleProfitability AS
SELECT 
    p.Product_Name,
    pa.Date_First_Sold,
    pa.Total_Units_Sold,
    pa.Total_Sales,
    pa.Total_Profit,
    pa.Lifecycle_Stage,
    pa.Avg_Unit_Price,
    pa.Avg_Unit_Cost
FROM 
    ProductAnalytics pa
JOIN 
    product p ON pa.Product_ID = p.Product_ID;

-- View for Market Penetration and Sales Performance
DROP VIEW IF EXISTS MarketPenetrationSalesPerformance;

CREATE VIEW MarketPenetrationSalesPerformance AS
SELECT 
    m.Region,
    m.Total_Sales,
    m.Total_Units_Sold,
    m.Avg_Sales_Per_Unit,
    m.Avg_Margin_Per_Unit,
    m.Market_Share
FROM 
    MarketSalesPerformance m;

-- Materialized View and Triggers
DROP TABLE IF EXISTS DailySalesSummary;

CREATE TABLE DailySalesSummary AS
SELECT 
    CAST(st.Invoice_Date AS DATE) AS SaleDate,
    SUM(st.Total_Sales) AS TotalDailySales
FROM 
    salestransaction st
GROUP BY 
    SaleDate;

-- Trigger to Update Daily Sales Summary
DROP TRIGGER IF EXISTS trg_AfterInsertSalesTransaction;

DELIMITER //
CREATE TRIGGER trg_AfterInsertSalesTransaction
AFTER INSERT ON salestransaction
FOR EACH ROW
BEGIN
    UPDATE DailySalesSummary
    SET TotalDailySales = TotalDailySales + NEW.Total_Sales
    WHERE SaleDate = CAST(NEW.Invoice_Date AS DATE);
END //
DELIMITER ;


-- Event to Refresh Market Share Materialized View
DELIMITER //
CREATE EVENT evt_RefreshMarketShareSummary
ON SCHEDULE EVERY 1 DAY
DO
BEGIN
    CALL ETL_MarketSalesPerformance();
END //
DELIMITER ;



