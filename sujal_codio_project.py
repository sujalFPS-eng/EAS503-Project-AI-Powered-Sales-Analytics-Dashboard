### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
# WRITE YOUR CODE HERE
    u_regions = set()
    with open(data_filename, 'r', encoding='utf-8') as f:
        next(f)
        for line in f:
            cols = line.strip().split('\t')
            if len(cols) > 4:
                reg = cols[4].strip()
                if reg: u_regions.add(reg)
    sorted_reg = sorted(list(u_regions))

    con = create_connection(normalized_database_filename)
    sql = """
    CREATE TABLE Region (
        RegionID INTEGER PRIMARY KEY,
        Region TEXT NOT NULL
    );
    """
    create_table(con, sql, drop_table_name="Region")
    payload = [(r,) for r in sorted_reg]

    with con:
        con.executemany("INSERT INTO Region (Region) VALUES (?)", payload)
    con.close()
    pass
def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    con = create_connection(normalized_database_filename)
    cur = con.cursor()
    cur.execute("SELECT Region, RegionID FROM Region")
    rows = cur.fetchall()
    con.close()

    return {r[0]: r[1] for r in rows}
    
# WRITE YOUR CODE HERE

    pass
def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
# WRITE YOUR CODE HERE
    reg_map = step2_create_region_to_regionid_dictionary(normalized_database_filename)
    u_countries = set()
    with open(data_filename, 'r', encoding='utf-8') as f:
        next(f)
        for line in f:
            cols = line.strip().split('\t')
            if len(cols) > 4:
                ctry = cols[3].strip()
                reg = cols[4].strip()
                u_countries.add((ctry, reg))

    sorted_cnt = sorted(list(u_countries), key = lambda x: x[0])

    data_to_load = []
    for c_name, r_name in sorted_cnt:
        rid = reg_map.get(r_name)
        if rid:
            data_to_load.append((c_name, rid))

    con = create_connection(normalized_database_filename)
    sql = """
    CREATE TABLE Country (
        CountryID INTEGER PRIMARY KEY,
        Country TEXT NOT NULL,
        RegionID INTEGER NOT NULL,
        FOREIGN KEY (RegionID) REFERENCES Region(RegionID)
    )
    """
    create_table(con, sql, drop_table_name="Country")
    with con:
        con.executemany("INSERT INTO Country (Country, RegionID) VALUES (?,?)", data_to_load)
    con.close()
    pass
def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    
    con = create_connection(normalized_database_filename)
    cur = con.cursor()
    cur.execute("SELECT Country, CountryID FROM Country")
    rows = cur.fetchall()
    con.close()
    return {r[0]: r[1] for r in rows}

# WRITE YOUR CODE HERE
        
    pass        
def step5_create_customer_table(data_filename, normalized_database_filename):
    ctry_map = step4_create_country_to_countryid_dictionary(normalized_database_filename)
    customers = []
    
    with open(data_filename, 'r', encoding='utf-8') as f:
        next(f)
        for line in f:
            cols = line.strip().split('\t')
            if len(cols) < 4: continue

            raw_name = cols[0].strip()
            addr = cols[1].strip()
            city = cols[2].strip()
            ctry_name = cols[3].strip()

            name_parts = raw_name.split()
            if len(name_parts) < 2: continue

            fname = name_parts[0]
            lname = " ".join(name_parts[1:])

            cid = ctry_map.get(ctry_name)
            if cid:
                customers.append((fname, lname, addr, city, cid))
    customers.sort(key=lambda x: x[0] + " " + x[1])
    con = create_connection(normalized_database_filename)
    sql = """
    CREATE TABLE Customer (
        CustomerID INTEGER PRIMARY KEY,
        FirstName TEXT NOT NULL,
        LastName TEXT NOT NULL,
        Address TEXT NOT NULL,
        City TEXT NOT NULL,
        CountryID INTEGER NOT NULL,
        FOREIGN KEY (CountryID) REFERENCES Country (CountryID)

    )
    """
    create_table(con, sql, drop_table_name= "Customer")
    with con:
        con.executemany("""
            INSERT INTO Customer (FirstName, LastName, Address, City, CountryID)
            VALUES (?,?,?,?,?)""", customers)
    con.close()
        
# WRITE YOUR CODE HERE

    pass
def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    con = create_connection(normalized_database_filename)
    rows = con.execute("SELECT CustomerID, FirstName, LastName FROM Customer").fetchall()
    con.close()

    return {f"{r[1]} {r[2]}": r[0] for r in rows}

    
# WRITE YOUR CODE HERE
    pass        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    cat_set = {}
    with open(data_filename, 'r', encoding='utf-8') as f:
        next(f)
        for line in f:
            cols = line.strip().split('\t')
            if len(cols) < 8: continue

            cats = cols[6].split(';')
            descs = cols[7].split(';')

            for c, d in zip(cats, descs):
                c_clean = c.strip()
                d_clean = d.strip()
                if c_clean:
                    cat_set[c_clean] = d_clean
    sorted_cats = sorted(cat_set.keys())
    final_data = [(c, cat_set[c])for c in sorted_cats]

    con = create_connection(normalized_database_filename)
    sql = """
    CREATE TABLE ProductCategory (
        ProductCategoryID INTEGER PRIMARY KEY,
        ProductCategory TEXT NOT NULL,
        ProductCategoryDescription TEXT NOT NULL
    )
    """
    create_table(con, sql, drop_table_name="ProductCategory")

    with con:
        con.executemany("INSERT INTO ProductCategory (ProductCategory, ProductCategoryDescription) VALUES (?,?)", final_data)
    con.close()
# WRITE YOUR CODE HERE
    pass
def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    con = create_connection(normalized_database_filename)
    rows= con.execute("SELECT ProductCategory, ProductCategoryID FROM ProductCategory").fetchall()
    con.close()
    return {r[0]: r[1] for r in rows}

    
# WRITE YOUR CODE HERE
        
    pass
def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
# WRITE YOUR CODE HERE
    cat_map = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
    prod_map = {}

    with open(data_filename, 'r', encoding='utf-8') as f:
        next(f)
        for line in f:
            cols = line.strip().split('\t')
            if len(cols) < 9: continue

            p_names = cols[5].split(';')
            p_cats = cols[6].split(';')
            p_prices = cols[8].split(';')

            for name, cat, price in zip(p_names, p_cats, p_prices):
                n_clean = name.strip()
                c_clean = cat.strip()

                cat_id = cat_map.get(c_clean)
                if not cat_id: continue

                try:
                    p_val = float(price.strip())
                    prod_map[n_clean] = (p_val, cat_id)
                except ValueError:
                    continue
    sorted_prods = sorted(prod_map.keys())

    insert_data = [(name, prod_map[name][0], prod_map[name][1])for name in sorted_prods]

    con = create_connection(normalized_database_filename)
    sql = """
    CREATE TABLE Product (
        ProductID INTEGER PRIMARY KEY,
        ProductName TEXT NOT NULL,
        ProductUnitPrice REAL NOT NULL,
        ProductCategoryID INTEGER NOT NULL,
        FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)

    )
    """
    create_table(con, sql, drop_table_name="Product")

    with con:
        con.executemany("INSERT INTO Product (ProductName, ProductUnitPrice, ProductCategoryID) VALUES (?,?,?)", insert_data)
    con.close()
    pass
def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
# WRITE YOUR CODE HERE
    con = create_connection(normalized_database_filename)
    rows = execute_sql_statement("SELECT ProductName, ProductID FROM Product", con)
    con.close()
    return {r[0]: r[1] for r in rows}
    pass             
    
def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    import datetime

    p_map = step10_create_product_to_productid_dictionary(normalized_database_filename)
    c_map = step6_create_customer_to_customerid_dictionary(normalized_database_filename)

    orders = []
    with open(data_filename, 'r', encoding='utf-8') as f:
        next(f)
        for line in f:
            cols = line.strip().split('\t')
            if len(cols) < 11: continue

            n_parts = cols[0].strip().split()
            if len(n_parts) < 2: continue
            c_key = f"{n_parts[0]} {' '.join(n_parts[1:])}"

            cid = c_map.get(c_key)
            if not cid: continue

            prods = cols[5].split(';')
            qtys = cols[9].split(';')
            dates = cols[10].split(';')

            for p, q, d in zip(prods, qtys, dates):
                pid = p_map.get(p.strip())
                if not pid: continue

                try:
                    q_val = int(q.strip())
                    d_obj = datetime.datetime.strptime(d.strip(), '%Y%m%d')
                    d_fmt = d_obj.strftime('%Y-%m-%d')

                    orders.append((cid, pid, d_fmt, q_val))
                except ValueError:
                    continue
    con = create_connection(normalized_database_filename)
    sql = """
    CREATE TABLE OrderDetail (
        OrderID INTEGER PRIMARY KEY,
        CustomerID INTEGER NOT NULL,
        ProductID INTEGER NOT NULL,
        OrderDate TEXT NOT NULL,
        QuantityOrdered INTEGER NOT NULL,
        FOREIGN KEY(CustomerID) REFERENCES Customer(CustomerID),
        FOREIGN KEY(ProductID) REFERENCES Product(ProductID)
    );
    """
    create_table(con, sql, drop_table_name="OrderDetail")
    with con:
        con.executemany("INSERT INTO OrderDetail (CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES (?,?,?,?)", orders)
    con.close()
    pass
    
# WRITE YOUR CODE HERE

    pass
def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    sql_statement = f"""
    SELECT 
        Cust.FirstName || ' ' || Cust.LastName as Name,
        Prod.ProductName,
        Ord.OrderDate,
        Prod.ProductUnitPrice,
        Ord.QuantityOrdered,
        Round(Prod.ProductUnitPrice * Ord.QuantityOrdered, 2) as Total
    FROM OrderDetail Ord
    JOIN Customer Cust ON Ord.CustomerID = Cust.CustomerID
    JOIN Product Prod ON Ord.ProductID = Prod.ProductID
    WHERE Cust.FirstName || ' ' || Cust.LastName = '{CustomerName}'
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    sql_statement = f"""
    SELECT 
        Cust.FirstName || ' ' || Cust.LastName as Name,
        ROUND(SUM(Prod.ProductUnitPrice * Ord.QuantityOrdered), 2) as Total
    FROM OrderDetail Ord
    JOIN Customer Cust ON Ord.CustomerID = Cust.CustomerID
    JOIN Product Prod ON Ord.ProductID = Prod.ProductID
    WHERE Cust.FirstName || ' ' || Cust.LastName = '{CustomerName}'
    GROUP BY Name 

    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    
    sql_statement = """
    SELECT 
        Cust.FirstName || ' ' || Cust.LastName as Name,
        ROUND(SUM(Prod.ProductUnitPrice * Ord.QuantityOrdered), 2) as Total
    FROM OrderDetail Ord
    JOIN Customer Cust ON Ord.CustomerID = Cust.CustomerID
    JOIN Product Prod ON Ord.ProductID = Prod.ProductID
    GROUP BY Name
    ORDER BY Total DESC
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    
    sql_statement = """
    SELECT 
        Reg.Region,
        ROUND(SUM(Prod.ProductUnitPrice * Ord.QuantityOrdered), 2) as Total
    FROM OrderDetail Ord
    JOIN Customer Cust ON Ord.CustomerID = Cust.CustomerID
    JOIN Product Prod ON Ord.ProductID = Prod.ProductID
    JOIN Country Cntry ON Cust.CountryID = Cntry.CountryID
    JOIN Region Reg ON Cntry.RegionID = Reg.RegionID
    GROUP BY Reg.Region
    ORDER BY Total DESC
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex5(conn):
    
    # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 

    sql_statement = """
    SELECT Co.Country as Country,
    ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS Total
    FROM OrderDetail O
    JOIN Customer C ON O.CustomerID = C.CustomerID
    JOIN Product P ON O.ProductID = P.ProductID
    JOIN Country Co ON C.CountryID = Co.CountryID
    GROUP BY Co.Country
    ORDER BY Total DESC
    """

# WRITE YOUR CODE HERE
    return sql_statementpd.read_sql_query(sql_statement, conn)


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region

  sql_statement = """
  SELECT R.Region, Co.Country, ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS CountryTotal,
  RANK() OVER(PARTITION BY R.Region ORDER BY SUM(P.ProductUnitPrice * O.QuantityOrdered) DESC) AS TotalRank
  FROM OrderDetail O
  JOIN Customer C ON O.CustomerID = C.CustomerID
  JOIN Product P ON O.ProductID = P.ProductID
  JOIN Country Co ON C.CountryID = Co.CountryID
  JOIN Region R ON Co.RegionID = R.RegionID
  GROUP BY R.Region, Co.Country
  ORDER BY R.Region ASC
  """

# WRITE YOUR CODE HERE
  df = pd.read_sql_query(sql_statement, conn)
  return sql_statement



def ex7(conn):
    
    # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, Total, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"

    sql_statement = """
    WITH RankedCountries AS (
      SELECT 
        R.Region,
        Co.Country,
        ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) as CountryTotal,
        RANK() OVER (
          PARTITION BY R.Region 
          ORDER BY SUM(P.ProductUnitPrice * O.QuantityOrdered) DESC
        ) as CountryRank
      FROM OrderDetail O
      JOIN Customer C ON O.CustomerID = C.CustomerID
      JOIN Product P ON O.ProductID = P.ProductID
      JOIN Country Co ON C.CountryID = Co.CountryID
      JOIN Region R ON Co.RegionID = R.RegionID
      GROUP BY R.Region, Co.Country
    )
    SELECT Region, Country, CountryTotal, CountryRank AS CountryRegionalRank
    FROM RankedCountries
    WHERE CountryRank = 1
    ORDER BY Region ASC, CountryRegionalRank ASC
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!

    sql_statement = """
    WITH CustomerSales AS (
      SELECT
         CASE
            WHEN CAST(SUBSTR(O.OrderDate,6,2) AS INTEGER) BETWEEN 1 AND 3 THEN 'Q1'
            WHEN CAST(SUBSTR(O.OrderDate,6,2) AS INTEGER) BETWEEN 4 AND 6 THEN 'Q2'
            WHEN CAST(SUBSTR(O.OrderDate,6,2) AS INTEGER) BETWEEN 7 AND 9 THEN 'Q3'
            ELSE'Q4'
        END AS Quarter,
        CAST(SUBSTR(O.OrderDate,1,4)AS INTEGER)AS Year,
        O.CustomerID,
        ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS Total
      FROM OrderDetail O
      JOIN Product P ON O.ProductID = P.ProductID
      GROUP BY Quarter, Year, O.CustomerID
    )
    SELECT Quarter, Year, CustomerID, Total
    FROM CustomerSales
    ORDER BY Year ASC, Quarter ASC, CustomerID ASC

    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()

    sql_statement = """
    WITH CustomerSales AS (
      SELECT 
        CASE
            WHEN CAST(SUBSTR(O.OrderDate,6,2) AS INTEGER) BETWEEN 1 AND 3 THEN 'Q1'
            WHEN CAST(SUBSTR(O.OrderDate,6,2) AS INTEGER) BETWEEN 4 AND 6 THEN 'Q2'
            WHEN CAST(SUBSTR(O.OrderDate,6,2) AS INTEGER) BETWEEN 7 AND 9 THEN 'Q3'
            ELSE'Q4'
        END AS Quarter,
        CAST(SUBSTR(O.OrderDate,1,4)AS INTEGER)AS Year,
        O.CustomerID,
        ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS Total
      FROM OrderDetail O
      JOIN Product P ON O.ProductID = P.ProductID
      GROUP BY Quarter, Year, O.CustomerID
    ),
    RankedSales AS (
      SELECT 
          Quarter, Year, CustomerID, Total,
          RANK() OVER(PARTITION BY Year, Quarter ORDER BY Total DESC) AS CustomerRank
      FROM CustomerSales
    )
    SELECT Quarter, Year, CustomerID, Total, CustomerRank
    FROM RankedSales
    WHERE CustomerRank <= 5
    ORDER BY Year ASC
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex10(conn):
    
    # Rank the monthy sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total

    sql_statement = """
    WITH MonthlySales AS (
      SELECT
          CASE SUBSTR(O.OrderDate,6,2)
              WHEN '01' THEN 'January'
              WHEN '02' THEN 'February'
              WHEN '03' THEN 'March'
              WHEN '04' THEN 'April'
              WHEN '05' THEN 'May'
              WHEN '06' THEN 'June'
              WHEN '07' THEN 'July'
              WHEN '08' THEN 'August'
              WHEN '09' THEN 'September'
              WHEN '10' THEN 'October'
              WHEN '11' THEN 'November'
              WHEN '12' THEN 'December'
          END AS Month,
          SUM(ROUND(P.ProductUnitPrice * O.QuantityOrdered)) AS Total
      FROM Product P
      JOIN OrderDetail O ON O.ProductID = P.ProductID
      GROUP BY Month
    )
    SELECT Month, Round(Total) AS Total,
           RANK() OVER (ORDER BY Total DESC) AS TotalRank
    FROM MonthlySales
    
    """

# WRITE YOUR CODE HERE
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag
    sql_statement = """
    WITH CustomerOrders AS (
      SELECT
          C.CustomerID,
          C.FirstName,
          C.LastName,
          Co.Country,
          O.OrderDate,
          LAG(O.OrderDate) OVER (PARTITION BY C.CustomerID ORDER BY O.OrderDate) AS PreviousOrderDate
      FROM OrderDetail O
      JOIN Customer C ON O.CustomerID = C.CustomerID
      JOIN Country Co ON C.CountryID = Co.CountryID
    ),
    DaysBetweenOrders AS (
      SELECT
          CustomerID,
          FirstName,
          LastName,
          Country,
          OrderDate,
          PreviousOrderDate,
          JULIANDAY(OrderDate) - JULIANDAY(PreviousOrderDate) AS DaysWithoutOrder
      FROM CustomerOrders
      WHERE PreviousOrderDate IS NOT NULL
    ),
    MaxDays AS (
      SELECT CustomerID, MAX(DaysWithoutOrder) AS MaxDaysWithoutOrder
      FROM DaysBetweenOrders
      GROUP BY CustomerID
    )
    SELECT d.CustomerID, d.FirstName, d.LastName, d.Country, d.OrderDate, d.PreviousOrderDate, m.MaxDaysWithoutOrder
    FROM DaysBetweenOrders d
    JOIN MaxDays m ON d.CustomerID = m.CustomerID AND d.DaysWithoutOrder = m.MaxDaysWithoutOrder
    WHERE d.OrderDate = (
      SELECT MIN(db.OrderDate)
      FROM DaysBetweenOrders db
      JOIN MaxDays md on db.CustomerID = md.CustomerID and db.DaysWithoutOrder = md.MaxDaysWithoutOrder
      WHERE db.CustomerID = d.CustomerID AND db.DaysWithoutOrder = m.MaxDaysWithoutOrder

    )
    ORDER BY m.MaxDaysWithoutOrder DESC, d.CustomerID DESC
    """

    
    
# WRITE YOUR CODE HERE
    return sql_statement