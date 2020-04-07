library(tidyverse)
library(dplyr)
library(dbplyr)
library(RSQLite)
library(DBI)
library(data.table)




#create database ----
neds = dbConnect(RSQLite::SQLite(), "Data/NEDS_DB.sqlite")
dbSendQuery(neds, "PRAGMA foreign_keys=ON")





#demographics ----
files = list.files("Data/", pattern = "demos", full.names = T)

for(i in 1:length(files)){
	file = files[i]
	temp = readRDS(file)
	temp$key_ed = as.character(temp$key_ed)
	dbWriteTable(neds, "demos", temp, append = TRUE)
	rm(temp)
	print(file)
}


query = dbSendQuery(neds, "SELECT * from demos limit 20")
print(dbFetch(query))
dbColumnInfo(query)
rm(query)


# to update table format (from https://www.techonthenet.com/sqlite/primary_keys.php)
# Let's look at an example of how to add a primary key to an existing table in SQLite. 
# So say, we already have an employees table with the following definition:
# 
# CREATE TABLE employees
# ( employee_id INTEGER,
#   last_name VARCHAR NOT NULL,
#   first_name VARCHAR,
#   hire_date DATE
# );
# 
# And we wanted to add a primary key to the employees table that consists of the employee_id.
# We could run the following commands:
# 
# PRAGMA foreign_keys=off;
# 
# BEGIN TRANSACTION;
# 
# ALTER TABLE employees RENAME TO old_employees;
# 
# CREATE TABLE employees
# (
#   employee_id INTEGER,
#   last_name VARCHAR NOT NULL,
#   first_name VARCHAR,
#   hire_date DATE,
#   CONSTRAINT employees_pk PRIMARY KEY (employee_id)
# );
# 
# INSERT INTO employees SELECT * FROM old_employees;
# 
# COMMIT;
# 
# PRAGMA foreign_keys=on;
# 
# In this example, we've created a primary key on the employees table 
# called employees_pk which consists of the employee_id column. 
# The original table will still exist in the database called old_employees. 
# You can drop the old_employees table once you have verified that your 
# employees table and data are as expected.
#   
#   DROP TABLE old_employees;
#   


# temp = readRDS(files[1])
# str(temp)
# rm(temp)
# 
# dbSendQuery(neds,
# 						"CREATE TABLE demos(
#     age INTEGER,
#     discwt REAL,
#     female INTEGER,
#     hosp_ed TEXT,
#     key_ed TEXT PRIMARY KEY,
#     year TEXT
#   );"
# )




#dx ----
files = list.files("Data/", pattern = "dx", full.names = T)

for(i in 1:length(files)){
	file = files[i]
	temp = readRDS(file)
	temp$key_ed = as.character(temp$key_ed)
	if(file == "Data//NEDS_2015Q1Q3_dx.RDS"){temp$year = "2015Q1Q3"}
	if(file == "Data//NEDS_2015Q4_dx.RDS"){temp$year = "2015Q4"}
	dbWriteTable(neds, "dx", temp, append = TRUE)
	rm(temp)
	print(file)
}



query = dbSendQuery(neds, "SELECT * from dx limit 20")
print(dbFetch(query))
dbColumnInfo(query)
rm(query)


# temp = readRDS(files[1])
# str(temp)
# rm(temp)
# 
# dbSendQuery(neds,
# 						"CREATE TABLE dx(
#     age INTEGER,
#     discwt REAL,
#     female INTEGER,
#     hosp_ed TEXT,
#     key_ed TEXT PRIMARY KEY,
#     year TEXT
#   );"
# )








#ecodes ----
files = list.files("Data/", pattern = "ecode", full.names = T)

for(i in 1:length(files)){
	file = files[i]
	temp = readRDS(file)
	temp$key_ed = as.character(temp$key_ed)
	dbWriteTable(neds, "ecodes", temp, append = TRUE)
	rm(temp)
	print(file)
}



query = dbSendQuery(neds, "SELECT * from ecodes limit 20")
print(dbFetch(query))
dbColumnInfo(query)
rm(query)


# temp = readRDS(files[1])
# str(temp)
# rm(temp)
# 
# dbSendQuery(neds,
# 						"CREATE TABLE ecodes(
#     age INTEGER,
#     discwt REAL,
#     female INTEGER,
#     hosp_ed TEXT,
#     key_ed TEXT PRIMARY KEY,
#     year TEXT
#   );"
# )








#outcomes ----
files = list.files("Data/", pattern = "outcome", full.names = T)

for(i in 1:length(files)){
	file = files[i]
	temp = readRDS(file)
	temp$key_ed = as.character(temp$key_ed)
	dbWriteTable(neds, "outcomes", temp, append = TRUE)
	rm(temp)
	print(file)
}



query = dbSendQuery(neds, "SELECT * from outcomes limit 20")
print(dbFetch(query))
dbColumnInfo(query)
rm(query)


# temp = readRDS(files[1])
# str(temp)
# rm(temp)
# 
# dbSendQuery(neds,
# 						"CREATE TABLE outcomes(
#     age INTEGER,
#     discwt REAL,
#     female INTEGER,
#     hosp_ed TEXT,
#     key_ed TEXT PRIMARY KEY,
#     year TEXT
#   );"
# )









#IP ----
files = list.files("Data/", pattern = "IP", full.names = T)

for(i in 1:length(files)){
	file = files[i]
	temp = readRDS(file)
	temp$key_ed = as.character(temp$key_ed)
	dbWriteTable(neds, "ip", temp, append = TRUE)
	rm(temp)
	print(file)
}




query = dbSendQuery(neds, "SELECT * from ip limit 20")
print(dbFetch(query))
dbColumnInfo(query)
rm(query)


# temp = readRDS(files[1])
# str(temp)
# rm(temp)
# 
# dbSendQuery(neds,
# 						"CREATE TABLE ip(
#     age INTEGER,
#     discwt REAL,
#     female INTEGER,
#     hosp_ed TEXT,
#     key_ed TEXT PRIMARY KEY,
#     year TEXT
#   );"
# )






#hospital ----
files = list.files("Data/", pattern = "Hospital", full.names = T)

for(i in 1:length(files)){
	file = files[i]
	temp = readRDS(file)
	dbWriteTable(neds, "hosp", temp, append = TRUE)
	rm(temp)
	print(file)
}


query = dbSendQuery(neds, "SELECT * from hosp limit 20")
print(dbFetch(query))
dbColumnInfo(query)
rm(query)


# temp = readRDS(files[1])
# str(temp)
# rm(temp)
# 
# dbSendQuery(neds,
# 						"CREATE TABLE hosp(
#     age INTEGER,
#     discwt REAL,
#     female INTEGER,
#     hosp_ed TEXT,
#     key_ed TEXT PRIMARY KEY,
#     year TEXT
#   );"
# )








dbDisconnect(neds)
