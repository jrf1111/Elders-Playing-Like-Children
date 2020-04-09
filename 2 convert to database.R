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










dbDisconnect(neds)










# create indexes ----------------------------------------------------------


neds = dbConnect(RSQLite::SQLite(), "Data/NEDS_DB.sqlite")

dbListTables(neds)
dbListFields(neds, "demos")
dbListFields(neds, "hosp")


dbExecute(neds, "CREATE INDEX index_key_ed ON demos (key_ed)")
dbGetQuery(neds, "PRAGMA index_list(demos)")


dbExecute(neds, "CREATE INDEX index_hosp_ed ON hosp (hosp_ed)")
dbGetQuery(neds, "PRAGMA index_list(hosp)")






# standardize dx strings --------------------------------------------------
query = dbSendQuery(neds, "SELECT dx1 FROM dx LIMIT 20")
print(dbFetch(query))


# dx1 with length of 3
query = dbSendQuery(neds, 
										"SELECT dx1 || '00' AS dx1s
										FROM dx
										WHERE LENGTH(dx1) = 3
										LIMIT 50")
print(dbFetch(query))



dbSendQuery(neds, 
						"UPDATE dx
						 SET dx1 = (dx1 || '00')
						 WHERE LENGTH(dx1) = 3")

query = dbSendQuery(neds, "SELECT dx1 FROM dx LIMIT 50")
print(dbFetch(query))






# dx1 with length of 4
query = dbSendQuery(neds, 
										"SELECT dx1 || '0' AS dx1s
										FROM dx
										WHERE LENGTH(dx1) = 4
										LIMIT 50")
print(dbFetch(query))


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx1 = (dx1 || '0')
						 WHERE LENGTH(dx1) = 4")

query = dbSendQuery(neds, "SELECT dx1 FROM dx LIMIT 50")
print(dbFetch(query))





# make table of injury dxs for filtering ----------------------------------



# make table of ecodes for filtering ----------------------------------





# filters -------------------------------------------------------



# joins -------------------------------------------------------


query = dbSendQuery(neds, 
										"SELECT demos.key_ed AS key_ed,
										        demos.age AS age,
										        demos.discwt AS demos_discwt,
										        demos.hosp_ed AS hosp_ed,
										        demos.year AS year,
										        dx.dx1 AS dx1,
										FROM demos
										INNER JOIN dx
										ON (demos.key_ed = dx.key_ed AND demos.age > 49)")


print(dbFetch(query))
dbColumnInfo(query)









# setup keys --------------------------------------------------------------
#Dont think this is going to work...or that is needs to. See
#https://stackoverflow.com/questions/26308134/left-join-on-non-primary-key
#and
#https://mode.com/sql-tutorial/sql-joins-where-vs-on/


neds = dbConnect(RSQLite::SQLite(), "Data/NEDS_DB.sqlite")


dbListTables(neds)
dbListFields(neds, "demos")
dbListFields(neds, "dx")
dbListFields(neds, "ecodes")
dbListFields(neds, "hosp")
dbListFields(neds, "ip")
dbListFields(neds, "outcomes")

dbSendQuery(neds, "ALTER TABLE demos 
						MODIFY 'key_ed' VARCHAR NOT NULL")


dbSendQuery(neds, "ALTER TABLE demos ADD PRIMARY KEY ('key_ed');")

dbSendQuery(neds, 
						"ALTER TABLE demos
						ADD CONSTRAINT pk_ed PRIMARY KEY (key_ed);")










query = dbSendQuery(neds, "SELECT * from demos limit 20")
print(dbFetch(query))
dbColumnInfo(query)
rm(query)




dbSendQuery(neds,
"PRAGMA foreign_keys=off;

BEGIN TRANSACTION;

ALTER TABLE demos RENAME TO old_demos;

CREATE TABLE demos
(age INTEGER,
discwt DOUBLE,
female DOUBLE,
hosp_ed DOUBLE,
key_ed VARCHAR,
year DOUBLE,
PRIMARY KEY (key_ed) );

INSERT INTO demos SELECT * FROM old_demos;

COMMIT;

PRAGMA foreign_keys=on;")


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

# In this example, we've created a primary key on the employees table 
# called employees_pk which consists of the employee_id column. 
# The original table will still exist in the database called old_employees. 
# You can drop the old_employees table once you have verified that your 
# employees table and data are as expected.
#   
#   DROP TABLE old_employees;
#   










# disconnect from DB ------------------------------------------------------



dbDisconnect(neds)



