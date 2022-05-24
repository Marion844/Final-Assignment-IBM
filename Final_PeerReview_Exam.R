# Final-Assignment-IBM

# Establish database connection
dsn_driver <- "{IBM DB2 ODBC Driver}"
dsn_database <- "bludb"            
dsn_hostname <- "0c77d6f2-5da9-48a9-81f8-86b520b87518.bs2io90l08kqb1od8lcg.databases.appdomain.cloud"
dsn_port <- "31198"  
dsn_protocol <- "TCPIP"            
dsn_uid <- "tzv89628"        
dsn_pwd <- "qtYmnIgCCNjLjN96"       
dsn_security <- "ssl"

conn_path <- paste("DRIVER=",dsn_driver,
                  ";DATABASE=",dsn_database,
                  ";HOSTNAME=",dsn_hostname,
                  ";PORT=",dsn_port,
                  ";PROTOCOL=",dsn_protocol,
                  ";UID=",dsn_uid,
                  ";PWD=",dsn_pwd,
                  ";SECURITY=",dsn_security,        
                    sep="")
conn <- odbcDriverConnect(conn_path)
conn 

sql.info <- sqlTypeInfo(conn)
conn.info <- odbcGetInfo(conn)
conn.info["DBMS_Name"]
conn.info["DBMS_Ver"]
conn.info["Driver_ODBC_Ver"]

# Solution 1 : Create the tables

# CROP_DATA:


df1 <- sqlQuery(conn, "CREATE TABLE CROP_DATA (
                            CD_ID INTEGER NOT NULL, 
                            YEAR DATE, 
                            CROP_TYPE VARCHAR(10) NOT NULL, 
                            GEO VARCHAR(10) NOT NULL,
                            SEEDED_AREA VARCHAR (50),
                            HARVERSTED_AREA VARCHAR (50),
                            PRODUCTION VARCHAR (50),
                            AVG_YIELD VARCHAR (10),
                            PRIMARY KEY (CD_ID))", 
                errors=FALSE)
if (df1 == -1){
  cat ("An error has occurred.\n")
  msg <- odbcGetErrMsg(conn)
  print (msg)
} else {
  cat ("Table was created successfully.\n")
}

# FARM_PRICES:

df2 <- sqlQuery(conn, "CREATE TABLE FARM_PRICES (
                            CD_ID INTEGER NOT NULL, 
                            YEAR DATE, 
                            CROP_TYPE VARCHAR(20) NOT NULL, 
                            GEO VARCHAR(20) NOT NULL,
                            PRICE_PRERMT DECIMAL(6,2),
                            PRIMARY KEY (CD_ID))", 
                errors=FALSE)
if (df2 == -1){
  cat ("An error has occurred.\n")
  msg <- odbcGetErrMsg(conn)
  print (msg)
} else {
  cat ("Table was created successfully.\n")
}

# DAILY_FX:

df3 <- sqlQuery(conn, "CREATE TABLE DAILY_FX (
                            DFX_ID INTEGER NOT NULL, 
                            DATE DATE, #
                            FXUSDCAD DECIMAL (6,2), 
                            PRIMARY KEY (DFX_ID))", 
                errors=FALSE)

if (df3 == -1){
  cat ("An error has occurred.\n")
  msg <- odbcGetErrMsg(conn)
  print (msg)
} else {
  cat ("Table was created successfully.\n")
}

# MONTHLY_FX:

df4 <- sqlQuery(conn, "CREATE TABLE MONTHLY_FX (
                            DFX_ID INTEGER NOT NULL, 
                            DATE DATE, 
                            FXUSDCAD DECIMAL (6,2), 
                            PRIMARY KEY (DFX_ID))", 
                errors=FALSE)

if (df4 == -1){
  cat ("An error has occurred.\n")
  msg <- odbcGetErrMsg(conn)
  print (msg)
} else {
  cat ("Table was created successfully.\n")
}

# Solution 2 : Read datasets and load tables

tab.frame <- sqlTables(conn, schema=myschema)
nrow(tab.frame)
tab.frame$CROP_DATA$FARM_PRICES$DAILY_FX$MONTHLY_FX

crop_datadf <- read.csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud
/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Annual_Crop_Data.csv", header = FALSE)
head(crop_datadf)
farm_pricesdf <- read.csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud
/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Monthly_Farm_Prices.csv", header = FALSE)
head(farm_pricesdf)
daily_fxdf <- read.csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud
/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Daily_FX.csv", header = FALSE)
head(daily_fxdf)
monthly_fxdf <- read.csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud
/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Monthly_FX.csv", header = FALSE)
head(monthly_fxdf)

sqlSave(conn, crop_datadf, "CROP_DATA", append=TRUE, fast=FALSE, rownames=FALSE, colnames=FALSE, verbose=FALSE)
sqlSave(conn, farm_pricesdf, "FARM_PRICES", append=TRUE, fast=FALSE, rownames=FALSE, colnames=FALSE, verbose=FALSE)
sqlSave(conn, daily_fxdf, "DAILY_FX", append=TRUE, fast=FALSE, rownames=FALSE, colnames=FALSE, verbose=FALSE)
sqlSave(conn, monthly_fxdf, "MONTHLY_FX", append=TRUE, fast=FALSE, rownames=FALSE, colnames=FALSE, verbose=FALSE)

# Solution 3

query <- "SELECT COUNT(*) FROM FARM_PRICES;"
view sqlQuery(conn, query)
view

# Solution 4

query <- "SELECT DISCTINCT GEO FROM FARM_PRICES;"
view sqlQuery(conn, query)
view

# Solution 5

query <- "SELECT SUM(HARVESTED_AREA) FROM CROP_DATA WHERE CROP_TYPE = "RYE" AND YEAR = 1968;"
view sqlQuery(conn, query)
view

# Solution 6

query <- "SELECT * FROM FARM_PRICES WHERE CROP_TYPE = "RYE" LIMIT 6;"
view sqlQuery(conn, query)
view

# Solution 7

query <- "SELECT DICTINCT GEO, CROP_TYPE FROM CROP_DATA WHERE CROP_TYPE = "BARLEY";"
view sqlQuery(conn, query)
view

# Solution 8

query <- "SELECT MAX(DATE) FROM FARM_PRICES;"
view sqlQuery(conn, query)
view

query <- "SELECT MIN(DATE) FROM FARM_PRICES;"
view sqlQuery(conn, query)
view

# Solution 9

query <- "SELECT DISTINCT CROP_TYPE, PRICE_PRERMT FROM FARM_PRICES WHERE PRICE_PRERMT >= "350;"
view sqlQuery(conn, query)
view

# Solution 10

query <- "SELECT CROP_TYPE, GEO, HARVESTED_AREA, AVG_YIELD FROM CROP_DATA 
WHERE GEO = "Saskatchewan" AND YEAR = 2000
ORDER BY AVG_YIELD;"
view sqlQuery(conn, query)
view

# Solution 11

query <- "SELECT CROP_TYPE, GEO, AVG_YIELD FROM CROP_DATA
WHERE YEAR >= 2000
ORDER BY AVG_YIELD;"
view sqlQuery(conn, query)
view

# Solution 12

query <- "SELECT CROP_TYPE, YEAR, (SELECT SUM(HARVESTED_AREA) FROM CROP_DATA)
FROM CROP_DATA
WHERE CROP_TYPE = "wheat"
ORDER BY YEAR;"
view sqlQuery(conn, query)
view

# Solution 13

query <- "SELECT DATE, CROP_TYPE, PRICE_PRERMT AS US DOLLARS, PRICE_PRERMT*FXUSDCAD AS CANADIAN DOLLARS FROM CROP_DATA, MONTHLY_PRICES 
WHERE CROP_DATA.DATE=MONTHLY_PRICES.DATE
and CROP_TYPE = "canola" and GEO = "Saskatchewan"
ORDER BY YEAR
;"
view sqlQuery(conn, query)
view
