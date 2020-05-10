library(tidyverse)
library(dplyr)
library(dbplyr)
# library(RSQLite) # starts throwing "Error: database or disk is full" after updating to R v4, use postgresql instead
library(RPostgreSQL)
library(DBI)
library(data.table)




#create database ----
# neds = dbConnect(RSQLite::SQLite(), "Data/NEDS_DB.sqlite")

#need to start the server in the Postgresql app first
pg = dbDriver("PostgreSQL")
neds = dbConnect(pg, 
								 user="jr-f", 
								 password="",
								 host="localhost",
								 port=5430,
								 dbname="jr-f")






# ~ dx ----
files = list.files("Data/", pattern = "dx", full.names = T)

for(i in 5:length(files)){
	file = files[i]
	temp = readRDS(file)
	temp$key_ed = as.character(temp$key_ed)
	if(file == "Data//NEDS_2015Q1Q3_dx.RDS"){temp$year = "2015Q1Q3"}
	if(file == "Data//NEDS_2015Q4_dx.RDS"){temp$year = "2015Q4"}
	temp$year = as.character(temp$year)
	dbWriteTable(neds, "dx", temp, append = TRUE, row.names=FALSE)
	rm(temp)
	print(file)
}

###########################################################


# issues with NEDS_2015Q1Q3_dx.RDS below


###########################################################


# dbWriteTable(neds, "dx", temp[, c("discwt", "dx3")]  , append = TRUE, row.names=FALSE); dbSendQuery(neds, "DROP TABLE dx")
# Error in postgresqlgetResult(new.con) : 
# 	RS-DBI driver: (could not Retrieve the result : ERROR:  invalid byte sequence for encoding "UTF8": 0xf0
# 									CONTEXT:  COPY dx, line 11864529
# 	)
# 
# dbWriteTable(neds, "dx", temp[, c("discwt", "dx4")]  , append = TRUE, row.names=FALSE); dbSendQuery(neds, "DROP TABLE dx")
# Error in postgresqlgetResult(new.con) : 
# 	RS-DBI driver: (could not Retrieve the result : ERROR:  invalid byte sequence for encoding "UTF8": 0xe5 0x7f 0xe5
# 									CONTEXT:  COPY dx, line 20844363
# 	)
# 
# 
# 
# dbWriteTable(neds, "dx", temp[, c("discwt", "dx5")]  , append = TRUE, row.names=FALSE); dbSendQuery(neds, "DROP TABLE dx")
# Error in postgresqlgetResult(new.con) : 
# 	RS-DBI driver: (could not Retrieve the result : ERROR:  invalid byte sequence for encoding "UTF8": 0xe5 0x7f 0xe5
# 									CONTEXT:  COPY dx, line 11864529
# 	)
# 
# dbWriteTable(neds, "dx", temp[, c("discwt", "dx6")]  , append = TRUE, row.names=FALSE)
# Error in postgresqlgetResult(new.con) : 
# 	RS-DBI driver: (could not Retrieve the result : ERROR:  invalid byte sequence for encoding "UTF8": 0xe5 0x7f 0xe5
# 									CONTEXT:  COPY dx, line 11864529
# 	)
# 
# 
# 
# dbWriteTable(neds, "dx", temp[, c("discwt", "dx7")]  , append = TRUE, row.names=FALSE)
# Error in postgresqlgetResult(new.con) : 
# 	RS-DBI driver: (could not Retrieve the result : ERROR:  invalid byte sequence for encoding "UTF8": 0xe5 0x7f 0xe5
# 									CONTEXT:  COPY dx, line 20841524
# 	)
# 
# 
# 
# dbWriteTable(neds, "dx", temp[, c("discwt", "dx8")]  , append = TRUE, row.names=FALSE)
# Error in postgresqlgetResult(new.con) : 
# 	RS-DBI driver: (could not Retrieve the result : ERROR:  invalid byte sequence for encoding "UTF8": 0xe5 0x7f 0xe5
# 									CONTEXT:  COPY dx, line 20841524
# 	)
# 
# 
# 
# 
# dbWriteTable(neds, "dx", temp[, c("discwt", "dx9")]  , append = TRUE, row.names=FALSE)
# Error in postgresqlgetResult(new.con) : 
# 	RS-DBI driver: (could not Retrieve the result : ERROR:  invalid byte sequence for encoding "UTF8": 0xe5 0x7f 0xe5
# 									CONTEXT:  COPY dx, line 20841524
# 	)
# 
# 
# dbWriteTable(neds, "dx", temp[, c("discwt", "dx10")]  , append = TRUE, row.names=FALSE)
# Error in postgresqlgetResult(new.con) : 
# 	RS-DBI driver: (could not Retrieve the result : ERROR:  invalid byte sequence for encoding "UTF8": 0xe5 0x7f 0xe5
# 									CONTEXT:  COPY dx, line 20841524
# 	)
# 
# 
# 
# dbWriteTable(neds, "dx", temp[, c("discwt", "dx11")]  , append = TRUE, row.names=FALSE)
# Error in postgresqlgetResult(new.con) : 
# 	RS-DBI driver: (could not Retrieve the result : ERROR:  invalid byte sequence for encoding "UTF8": 0xe5 0x7f 0xe5
# 									CONTEXT:  COPY dx, line 20841524
# 	)
# 
# 
# dbWriteTable(neds, "dx", temp[, c("discwt", "dx12")]  , append = TRUE, row.names=FALSE)
# Error in postgresqlgetResult(new.con) : 
# 	RS-DBI driver: (could not Retrieve the result : ERROR:  invalid byte sequence for encoding "UTF8": 0xe5 0x7f 0xe5
# 									CONTEXT:  COPY dx, line 20841524
# 	)
# 
# 
# 
# dbWriteTable(neds, "dx", temp[, c("discwt", "dx13")]  , append = TRUE, row.names=FALSE)
# Error in postgresqlgetResult(new.con) : 
# 	RS-DBI driver: (could not Retrieve the result : ERROR:  invalid byte sequence for encoding "UTF8": 0xe5 0x7f 0xe5
# 									CONTEXT:  COPY dx, line 20841524
# 	)
# 
# 
# dbWriteTable(neds, "dx", temp[, c("discwt", "dx14")]  , append = TRUE, row.names=FALSE)
# Error in postgresqlgetResult(new.con) : 
# 	RS-DBI driver: (could not Retrieve the result : ERROR:  invalid byte sequence for encoding "UTF8": 0xe5 0x7f 0xe5
# 									CONTEXT:  COPY dx, line 20841524
# 	)
# 






















query = dbSendQuery(neds, "SELECT * FROM dx LIMIT 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)


n = dbGetQuery(neds, "SELECT COUNT(key_ed) FROM dx")
n = as.integer(n)
cat("Initial N_obs = ", n, "\n", file = "nobs log.txt")
gc()






# ~ demographics ----
files = list.files("Data/", pattern = "demos", full.names = T)

for(i in 1:length(files)){
	file = files[i]
	temp = readRDS(file)
	temp$key_ed = as.character(temp$key_ed)
	dbWriteTable(neds, "demos", temp, append = TRUE)
	rm(temp)
	print(file)
}


query = dbSendQuery(neds, "SELECT * FROM demos LIMIT 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)
gc()









# ~ ecodes ----
files = list.files("Data/", pattern = "ecode", full.names = T)

for(i in 1:length(files)){
	file = files[i]
	temp = readRDS(file)
	temp$key_ed = as.character(temp$key_ed)
	dbWriteTable(neds, "ecodes", temp, append = TRUE)
	rm(temp)
	print(file)
}



query = dbSendQuery(neds, "SELECT * FROM ecodes LIMIT 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)
gc()







# ~ outcomes ----
files = list.files("Data/", pattern = "outcome", full.names = T)

for(i in 1:length(files)){
	file = files[i]
	temp = readRDS(file)
	temp$key_ed = as.character(temp$key_ed)
	dbWriteTable(neds, "outcomes", temp, append = TRUE)
	rm(temp)
	print(file)
}



query = dbSendQuery(neds, "SELECT * FROM outcomes LIMIT 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)
gc()











# ~ IP ----
files = list.files("Data/", pattern = "IP", full.names = T)

for(i in 1:length(files)){
	file = files[i]
	temp = readRDS(file)
	temp$key_ed = as.character(temp$key_ed)
	temp = temp[, c("key_ed", "hosp_ed", "disp_ip")]
	dbWriteTable(neds, "ip", temp, append = TRUE)
	rm(temp)
	print(file)
}




query = dbSendQuery(neds, "SELECT * FROM ip LIMIT 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)
gc()




# ~ hospital ----
files = list.files("Data/", pattern = "Hospital", full.names = T)

for(i in 1:length(files)){
	file = files[i]
	temp = readRDS(file)
	dbWriteTable(neds, "hosp", temp, append = TRUE)
	rm(temp)
	print(file)
}


query = dbSendQuery(neds, "SELECT * FROM hosp LIMIT 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)
gc()






















# add primary keys and create indexes ----------------------------------------------------------
#indexes take a while to create but they speed up the joins and filters



dbExecute(neds, "ALTER TABLE demos ADD PRIMARY KEY (key_ed)")
dbExecute(neds, "ALTER TABLE dx ADD PRIMARY KEY (key_ed)")
dbExecute(neds, "ALTER TABLE ecodes ADD PRIMARY KEY (key_ed)")
dbExecute(neds, "ALTER TABLE ip ADD PRIMARY KEY (key_ed)")
dbExecute(neds, "ALTER TABLE outcomes ADD PRIMARY KEY (key_ed)")





dbExecute(neds, "CREATE INDEX index_key_ed_demos ON demos (key_ed)")
dbExecute(neds, "CREATE INDEX index_key_ed_dx ON dx (key_ed)")
dbExecute(neds, "CREATE INDEX index_key_ed_ecodes ON ecodes (key_ed)")
dbExecute(neds, "CREATE INDEX index_key_ed_ip ON ip (key_ed)")
dbExecute(neds, "CREATE INDEX index_key_ed_outcomes ON outcomes (key_ed)")




# filters -------------------------------------------------------

# ~ injury dx filter ---------------------------------------
# ~~ make table of injury dxs for filtering ----------------------------------

#sequence of ICD9 injury codes
injury_codes = c(seq(800, 904.9, 0.01),
								 # skip 905-909: Late Effects of Injuries, Poisonings, Toxic Effects, & Other External Causes
								 seq(910, 939.9, 0.01),
								 #skip 940-949: Burns
								 seq(950, 959.9, 0.01),
								 #skip 960-979: Poisoning by Drugs, Medicinals & Biological Substances
								 #skip 980-989: Toxic Effects of Substances Chiefly Nonmedicinal as to Source
								 seq(990, 999.9, 0.01)
)

#convert to character
injury_codes = as.character(injury_codes)

#remove periods
injury_codes = gsub(".", "", injury_codes, fixed = T)


#standardize the length of the strings
dx_recode = function(dx){
	case_when( 
		nchar(dx) == 3 ~ paste0(dx, "00"),
		nchar(dx) == 4 ~ paste0(dx, "0"),
		TRUE ~ dx
	)
}

injury_codes = dx_recode(injury_codes)


#make table of injury codes in DB
dbWriteTable(neds, "injury_dx",  data.frame(injury_dx = injury_codes)  )

#confirm it was created
dbListTables(neds)
dbListFields(neds, "injury_dx")
dbGetQuery(neds, "SELECT * FROM injury_dx LIMIT 50")

rm(injury_codes)





# ~~ standardize dx1 strings --------------------------------------------------
dbGetQuery(neds, "SELECT dx1 FROM dx LIMIT 20")


# dx1 with length of 3
dbGetQuery(neds, 
					 "SELECT dx1 || '00' AS dx1s
										FROM dx
										WHERE LENGTH(dx1) = 3
										LIMIT 20")



dbSendQuery(neds, 
						"UPDATE dx
						 SET dx1 = (dx1 || '00')
						 WHERE LENGTH(dx1) = 3")

dbGetQuery(neds, "SELECT dx1 FROM dx LIMIT 50")






# dx1 with length of 4
dbGetQuery(neds, 
					 "SELECT dx1 || '0' AS dx1s
										FROM dx
										WHERE LENGTH(dx1) = 4
										LIMIT 20")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx1 = (dx1 || '0')
						 WHERE LENGTH(dx1) = 4")

dbGetQuery(neds, "SELECT dx1 FROM dx LIMIT 50")






# ~~ filter on injury dx ----
dbSendQuery(neds, 
						"DELETE FROM dx
										WHERE dx1 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")


dbSendQuery(neds, 
						"DELETE FROM demos
										WHERE key_ed NOT IN (SELECT f.key_ed FROM dx AS f)")


dbSendQuery(neds, 
						"DELETE FROM ip
										WHERE key_ed NOT IN (SELECT f.key_ed FROM dx AS f)")


dbSendQuery(neds, 
						"DELETE FROM outcomes
										WHERE key_ed NOT IN (SELECT f.key_ed FROM dx AS f)")


dbSendQuery(neds, 
						"DELETE FROM ecodes
										WHERE key_ed NOT IN (SELECT f.key_ed FROM dx AS f)")



n = dbGetQuery(neds, "SELECT COUNT(key_ed) FROM dx")
n = as.integer(n)
cat("N_obs after filtering on injury dx = ", n, "\n", file="nobs log.txt", append=T)


# ~~ standardize other dx strings ----------------


for(i in 2:15){
	
	var = paste0("dx", i)
	command = paste0("UPDATE dx set ", var, 
									 " = (", var, " || '00') WHERE LENGTH(", var, ") = 3")
	dbSendQuery(neds, command)
	
	
	command = paste0("UPDATE dx set ", var, 
									 " = (", var, " || '0') WHERE LENGTH(", var, ") = 4")
	dbSendQuery(neds, command)
	print(var)
}




# ~~ NULL out non-injury dxs to reduce file size -----

#skip dx1 since already filtered above



for(i in 2:15){
	
	var = paste0("dx", i)
	command = paste0("UPDATE dx SET ", var, " = NULL WHERE ", var, " NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")
	dbSendQuery(neds, command)
	print(var)
	
}




#confirm filter (should both be zero)
# the `WHERE dx1 LIKE "2%"` part is to only look at if the dx starts with "2".
dbGetQuery(neds, 
					 'SELECT COUNT(*), dx1
					 FROM dx
					 WHERE dx1 LIKE "2%"
					 GROUP BY dx1')


dbGetQuery(neds, 
					 'SELECT COUNT(*), dx2
					 FROM dx
					 WHERE dx2 LIKE "2%"
					 GROUP BY dx2')



# ~ age filter ----
dbSendQuery(neds, 
						"DELETE FROM demos
										WHERE age < 50")

#confirm filter (should be zero)
dbGetQuery(neds, 
					 'SELECT COUNT(age)
					 FROM demos
					 WHERE age < 50')

dbGetQuery(neds, 
					 'SELECT MIN(age)
					 FROM demos')



dbSendQuery(neds, 
						"DELETE FROM dx
										WHERE key_ed NOT IN (SELECT f.key_ed FROM demos AS f)")


dbSendQuery(neds, 
						"DELETE FROM ip
										WHERE key_ed NOT IN (SELECT f.key_ed FROM demos AS f)")


dbSendQuery(neds, 
						"DELETE FROM outcomes
										WHERE key_ed NOT IN (SELECT f.key_ed FROM demos AS f)")


dbSendQuery(neds, 
						"DELETE FROM ecodes
										WHERE key_ed NOT IN (SELECT f.key_ed FROM demos AS f)")




n = dbGetQuery(neds, "SELECT COUNT(key_ed) FROM dx")
n = as.integer(n)
cat("N_obs after filtering on age = ", n, "\n", file="nobs log.txt", append-T)





# ~ ecode filter ----------------------------------------
# ~~ make table of ecodes for filtering ------------------



#Exclude: injuries including burns, bites/stings, overexertion, 
#                  poisoning, or misadventures of medical/surgical care


exclude = c(
	
	# E850-E858  Accidental Poisoning By Drugs, Medicinal Substances, And Biologicals
	paste0("E", 850:858),
	
	# E860-E869  Accidental Poisoning By Other Solid And Liquid Substances, Gases, And Vapors
	paste0("E", 860:869),
	
	
	# E870-E876  Misadventures To Patients During Surgical And Medical Care
	paste0("E", 870:876),
	
	# E878-E879  Surgical And Medical Procedures As The Cause Of Abnormal Reaction Of Patient
	# 						Or Later Complication, Without Mention Of Misadventure At The Time Of Procedure
	paste0("E", 878:879),
	
	# E890-E899  Accidents Caused By Fire And Flames
	paste0("E", 890:899),
	
	
	
	# E990 Injury due to war operations by fires and conflagrations
	paste0("E", 990),
	
	# E905 Venomous animals and plants as the cause of poisoning and toxic reactions
	paste0("E", 905), 
	
	# E906 Other injury caused by animals
	paste0("E", 906),
	
	
	# E924 Accident caused by hot substance or object caustic or corrosive material and steam
	paste0("E", 924), 
	
	
	# E927 Overexertion and strenuous movements
	paste0("E", 927),
	
	
	# E930-E949  Drugs, Medicinal And Biological Substances Causing Adverse Effects In Therapeutic Use
	paste0("E", 930:949),
	
	# E950 Suicide and self-inflicted poisoning by solid or liquid substances
	# E951 Suicide and self-inflicted poisoning by gases in domestic use
	# E952 Suicide and self-inflicted poisoning by other gases and vapors
	paste0("E", 950:952), 
	
	# E958.1 Suicide and self-inflicted injury by burns, fire
	# E958.2 Suicide and self-inflicted injury by scald
	# E958.7 Suicide and self-inflicted injury by caustic substances, except poisoning
	paste0("E", 958.1), 
	paste0("E", 958.2), 
	paste0("E", 958.7), 
	
	# E961 Assault by corrosive or caustic substance, except poisoning
	paste0("E", 961), 
	
	# E962 Assault by poisoning
	paste0("E", 962),
	
	#E968.0 Assault by fire
	paste0("E", 968.0), 
	#E968.3 Assault by hot liquid
	paste0("E", 968.3), 
	
	#E972 Injury due to legal intervention by gas (poisoning mech)
	paste0("E", 972),
	
	
	# E980 Poisoning by solid or liquid substances undetermined whether accidentally or purposely inflicted
	# E981 Poisoning by gases in domestic use undetermined whether accidentally or purposely inflicted
	# E982 Poisoning by other gases undetermined whether accidentally or purposely inflicted
	paste0("E", 980:982),
	
	
	# E988.1 Injury by burns or fire, undetermined whether accidentally or purposely inflicted
	# E988.2 Injury by scald, undetermined whether accidentally or purposely inflicted
	# E988.7 Injury by caustic substances, except poisoning, undetermined whether accidentally or purposely inflicted
	paste0("E", 988.1),
	paste0("E", 988.2), 
	paste0("E", 988.7)
	
)


exclude = gsub(".", "", exclude, fixed = T) #remove periods




#make table of ecodes to exclude in DB
dbWriteTable(neds, "ecode_exclude",  data.frame(exclude = exclude)  )

#confirm it was created
dbListTables(neds)
dbListFields(neds, "ecode_exclude")
dbGetQuery(neds, "SELECT * FROM ecode_exclude LIMIT 20")
rm(exclude)



# ~~ filter on ecodes ----


#Per Alan, only filter on ecode1
dbSendQuery(neds,
						"DELETE FROM ecodes
										WHERE SUBSTR(ecode1, 1, 4) IN (SELECT f.exclude FROM ecode_exclude AS f)")
dbSendQuery(neds,
						"DELETE FROM ecodes
										WHERE ecode1 IN (SELECT f.exclude FROM ecode_exclude AS f)")


#confirm filter (should be zero)
dbGetQuery(neds, 
					 'SELECT COUNT(ecode1)
					 FROM ecodes
					 WHERE SUBSTR(ecode1, 1, 4) = "E890"')

dbGetQuery(neds, 
					 'SELECT COUNT(ecode1)
					 FROM ecodes
					 WHERE SUBSTR(ecode1, 1, 4) = "E850"')







dbSendQuery(neds, 
						"DELETE FROM dx
										WHERE key_ed NOT IN (SELECT f.key_ed FROM ecodes AS f)")


dbSendQuery(neds, 
						"DELETE FROM ip
										WHERE key_ed NOT IN (SELECT f.key_ed FROM ecodes AS f)")


dbSendQuery(neds, 
						"DELETE FROM outcomes
										WHERE key_ed NOT IN (SELECT f.key_ed FROM ecodes AS f)")


dbSendQuery(neds, 
						"DELETE FROM demos
										WHERE key_ed NOT IN (SELECT f.key_ed FROM ecodes AS f)")







n = dbGetQuery(neds, "SELECT COUNT(key_ed) FROM dx")
n = as.integer(n)
cat("N_obs after filtering on ecodes = ", n, "\n", file="nobs log.txt", append=T)










# join statement -------------------------------------------------------



#get all column names and data types
tables = dbListTables(neds)
for(i in 1:length(tables)){
	print(tables[i])
	q = paste0("SELECT * FROM ", tables[i], " LIMIT 1")
	q = dbSendQuery(neds, q)
	print(dbColumnInfo(q))
	dbClearResult(q)
}
rm(tables, q, i)



#create table for join results to go into
dbSendQuery(neds, 
						"CREATE TABLE join_res (
						key_ed CHARACTER,
						hosp_ed DOUBLE,
						year CHARACTER,
						discwt DOUBLE,
						age INTEGER,
						female INTEGER,
						
						died_visit INTEGER,
						disp_ed INTEGER,
						edevent INTEGER,
						
						dx1 CHARACTER,
						dx2 CHARACTER,
						dx3 CHARACTER,
						dx4 CHARACTER,
						dx5 CHARACTER,
						dx6 CHARACTER,
						dx7 CHARACTER,
						dx8 CHARACTER,
						dx9 CHARACTER,
						dx10 CHARACTER,
						dx11 CHARACTER,
						dx12 CHARACTER,
						dx13 CHARACTER,
						dx14 CHARACTER,
						dx15 CHARACTER,
						
						ecode1 CHARACTER,
						ecode2 CHARACTER,
						ecode3 CHARACTER,
						ecode4 CHARACTER,
						
						disp_ip INTEGER,
						
						neds_stratum INTEGER
						)")








#get all the var names
tables = dbListTables(neds)
for(i in 1:length(tables)){
	print(tables[i])
	q = paste0("SELECT * FROM ", tables[i], " LIMIT 1")
	q = dbSendQuery(neds, q)
	r = dbColumnInfo(q)
	dbClearResult(q)
	cat(paste0(tables[i], ".", r$name, sep = ", "))
}
rm(tables, q, i, r)









#takes about 45 mins with the key_ed indexes and >6 hours without the indexes
#vars in SELECT command have to be in same order as in the creation of join_res
dbSendQuery(neds, 
						"INSERT INTO join_res
						 SELECT a.key_ed, a.hosp_ed, a.year, a.discwt, a.age, a.female,  
						 b.died_visit, b.disp_ed,  b.edevent,
						 c.dx1,  c.dx2,  c.dx3,  c.dx4,  c.dx5,  c.dx6,  c.dx7,  c.dx8,  c.dx9,  c.dx10, 
						 c.dx11,  c.dx12,  c.dx13,  c.dx14,  c.dx15,
						 d.ecode1,  d.ecode2,  d.ecode3,  d.ecode4, 
						 e.disp_ip,
						 f.neds_stratum
						 
						 
						 FROM demos AS a
						 LEFT JOIN outcomes AS b
						 ON a.key_ed = b.key_ed
						 
						 INNER JOIN dx AS c
						 ON b.key_ed = c.key_ed
						 
						 INNER JOIN ecodes AS d
						 ON c.key_ed = d.key_ed
						 
						 LEFT JOIN ip AS e
						 ON d.key_ed = e.key_ed
						 
						 LEFT JOIN hosp AS f
						 ON e.hosp_ed = f.hosp_ed")


# ROWS Fetched: 0 [complete]
# Changed: 15724244



# ~ delete duplicates that were added in the join -----------------------------------------------------

dbListFields(neds, "join_res")

dbExecute(neds, "CREATE INDEX index_key_ed_join_res ON join_res (key_ed)")


dbGetQuery(neds,
					 'SELECT COUNT(*), key_ed FROM join_res
					 GROUP BY key_ed
					 ORDER BY COUNT(*) DESC LIMIT 10')



dbSendQuery(neds,
						"DELETE FROM join_res
					 WHERE rowid NOT IN (SELECT MIN(rowid) FROM join_res GROUP BY key_ed)")
# ROWS Fetched: 0 [complete]
# Changed: 2954194

dbGetQuery(neds,
					 'SELECT COUNT(*), key_ed FROM join_res
					 GROUP BY key_ed
					 ORDER BY COUNT(*) DESC LIMIT 10')




# ~ confirm proper join -----------------------------------------------------


dbGetQuery(neds, 'SELECT * FROM join_res LIMIT 5')


dbGetQuery(neds, 'SELECT MIN(age) FROM join_res')


dbGetQuery(neds, 'SELECT COUNT(*), died_visit FROM join_res GROUP BY died_visit')


dbGetQuery(neds, 'SELECT COUNT(*), edevent FROM join_res GROUP BY edevent')


dbGetQuery(neds, 'SELECT COUNT(*), disp_ed FROM join_res GROUP BY disp_ed')


dbGetQuery(neds, 'SELECT COUNT(*), disp_ip FROM join_res GROUP BY disp_ip')


dbGetQuery(neds, 'SELECT COUNT(*), female FROM join_res GROUP BY female')


dbGetQuery(neds, 
					 'SELECT COUNT(*), dx2
					 FROM join_res
					 GROUP BY dx2
					 ORDER BY COUNT(*) DESC LIMIT 20')


#these last few should all give counts of zero
dbGetQuery(neds, 
					 'SELECT COUNT(*), dx1
					 FROM join_res
					 WHERE dx1 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)
					 GROUP BY dx1')



dbGetQuery(neds, 
					 'SELECT COUNT(*), ecode1
					 FROM join_res
					 WHERE SUBSTR(ecode1, 1, 4) IN (SELECT f.exclude FROM ecode_exclude AS f)
					 GROUP BY ecode1')








n = dbGetQuery(neds, "SELECT COUNT(key_ed) FROM join_res")
n = as.integer(n)
cat("N_obs after all filters = ", n, "\n", file="nobs log.txt", append = T)






# export dxs & calculate TMPM -------------------------------------------

dbSendQuery(neds, 
						"CREATE TABLE dx_final (
						key_ed CHARACTER,

						dx1 CHARACTER,
						dx2 CHARACTER,
						dx3 CHARACTER,
						dx4 CHARACTER,
						dx5 CHARACTER,
						dx6 CHARACTER,
						dx7 CHARACTER,
						dx8 CHARACTER,
						dx9 CHARACTER,
						dx10 CHARACTER,
						dx11 CHARACTER,
						dx12 CHARACTER,
						dx13 CHARACTER,
						dx14 CHARACTER,
						dx15 CHARACTER
						)")


dbSendQuery(neds, 
						"INSERT INTO dx_final
						 SELECT join_res.key_ed, 
						 join_res.dx1,  join_res.dx2,  join_res.dx3,  join_res.dx4,  join_res.dx5,  
						 join_res.dx6,  join_res.dx7,  join_res.dx8,  join_res.dx9,  join_res.dx10, 
						 join_res.dx11,  join_res.dx12,  join_res.dx13,  join_res.dx14,  join_res.dx15
						 FROM join_res")

# ROWS Fetched: 0 [complete]
# Changed: 12770050


dx = dbReadTable(neds, "dx_final")
dx = as.data.table(dx)
gc()


#save dx file just in case
fwrite(dx, "Data/final/dx_final.csv")

dx = fread("Data/final/dx_final.csv")


#get ICD9 dx codes in proper format for tmpm: numeric, ###.##
dx = dx %>% mutate_at(vars(starts_with("dx")), function(x){ as.numeric(x)/100  } )


#calculate TMPM
library(tmpm)







#lets see if I can't speed up tmpm::tmpm()

ICs = tmpm::marcTable[tmpm::marcTable$lexi == "icdIX", ]
ICs$lexi = NULL

tmpm2 = function(Pdat){
	xBeta <- NULL
	
	MortModel <- function(marc1, marc2, marc3, marc4, marc5, S_Region, Interaction) {
		xBeta <- (1.406958 * marc1) + (1.409992 * marc2) + 
			(0.5205343 * marc3) + (0.4150946 * marc4) + 
			(0.8883929 * marc5) + (-0.0890527 * S_Region) - 
			(0.7782696 * Interaction) - 2.217565
		return(xBeta)
	}
	
	app <- function(x){
		marclist <- ICs[match(x[-1], ICs$index), ]
		marclist <- marclist[order(marclist$marc, decreasing = T), ]
		TCs <- marclist[1:5, ]
		TCs$marc[is.na(TCs$marc)] = 0
		
		RegionCheck <- function(TCs) {
			if (TCs$marc[1] != 0 & TCs$marc[2] != 0 & TCs$bodyregion[1] == TCs$bodyregion[2]) {
				sr <- 1
			}
			else {
				sr <- 0
			}
			return(sr)
		}
		
		same_region <- RegionCheck(TCs)
		Imarc <- TCs$marc[1] * TCs$marc[2]
		Model_Calc <- MortModel(TCs$marc[1], TCs$marc[2], 
														TCs$marc[3], TCs$marc[4], TCs$marc[5], same_region, Imarc)
		probDeath <- pnorm(Model_Calc)
		return(probDeath)
	}
	
	
	pDeath <- apply(Pdat, 1, app)
	
	return(data.frame(pDeath = pDeath))
	
	
	
}


tmpm3 = function(Pdat){
	
	
	# MortModel <- function(marc1, marc2, marc3, marc4, marc5, S_Region, Interaction) {
	# 	xBeta <- (1.406958 * marc1) + (1.409992 * marc2) + 
	# 		(0.5205343 * marc3) + (0.4150946 * marc4) + 
	# 		(0.8883929 * marc5) + (-0.0890527 * S_Region) - 
	# 		(0.7782696 * Interaction) - 2.217565
	# 	return(xBeta)
	# }
	
	app <- function(x){
		marclist <- ICs[match(x[-1], ICs$index), ]
		marclist <- marclist[order(marclist$marc, decreasing = T), ]
		TCs <- marclist[1:5, ]
		TCs$marc[is.na(TCs$marc)] = 0
		
		# RegionCheck <- function(TCs) {
		# 	if (TCs$marc[1] != 0 & TCs$marc[2] != 0 & TCs$bodyregion[1] == TCs$bodyregion[2]) {
		# 		sr <- 1
		# 	}
		# 	else {
		# 		sr <- 0
		# 	}
		# 	return(sr)
		# }
		
		
		if (TCs$marc[1] != 0 & TCs$marc[2] != 0 & TCs$bodyregion[1] == TCs$bodyregion[2]) {
			same_region = 1
		} else same_region = 0
		
		
		Imarc <- TCs$marc[1] * TCs$marc[2]
		
		# Model_Calc <- MortModel(TCs$marc[1], TCs$marc[2], 
		# 												TCs$marc[3], TCs$marc[4], TCs$marc[5], same_region, Imarc)
		
		Model_Calc = {1.406958 * TCs$marc[1]} + {1.409992 * TCs$marc[2]} + 
			{0.5205343 * TCs$marc[3]} + {0.4150946 * TCs$marc[4]} + 
			{0.8883929 * TCs$marc[5]} + {-0.0890527 * same_region} - 
			{0.7782696 * Imarc} - 2.217565
		
		
		
		probDeath <- pnorm(Model_Calc)
		probDeath
	}
	
	
	pDeath <- apply(Pdat, 1, app)
	
	return(data.frame(pDeath = pDeath))
	
}


tmpm4 = function(Pdat){
	
	
	get_marcs = function(x){
		marclist <- ICs[match(x[-1], ICs$index), ]
		marclist <- marclist[order(marclist$marc, decreasing = T), ]
		marclist <- marclist[1:5, ]
		marclist$marc[is.na(marclist$marc)] = 0
		
		marclist$index = NULL
		
		if (marclist$marc[1] != 0 & marclist$marc[2] != 0 & marclist$bodyregion[1] == marclist$bodyregion[2]) {
			same_region = 1
		} else same_region = 0
		
		marclist = c(marclist$marc, same_region)
		names(marclist) = c(paste0("marc", 1:5), "same_region")
		
		marclist
	}
	
	
	# marcs = matrix(NA, nrow = nrow(Pdat), ncol = 6)
	# colnames(marcs) = c("marc1", "marc2", "marc3", "marc4", "marc5", "same_region")
	# marcs = as_tibble(marcs)
	
	# for(i in 1:nrow(Pdat)){
	# 	marcs[i, ] = get_marcs(Pdat[i, ])
	# }
	
	
	marcs = apply(Pdat, 1, get_marcs)
	marcs = as_tibble(t(marcs)) #transposing takes a little bit...
	
	
	
	marcs$Imarc = marcs$marc1 * marcs$marc2
	
	
	Model_Calc = 
		{1.406958 * marcs$marc1} + 
		{1.409992 * marcs$marc2} + 
		{0.5205343 * marcs$marc3} +
		{0.4150946 * marcs$marc4} + 
		{0.8883929 * marcs$marc5} + 
		{-0.0890527 * marcs$same_region} - 
		{0.7782696 * marcs$Imarc} - 2.217565
	
	
	pnorm(Model_Calc)
	
	
	
	
}




#parallel version of tmpm4, not really any faster
tmpm4p = function(Pdat, cores = 6, max_combine = max(c(ceiling(nrow(Pdat)*0.1), 100))){
	
	
	get_marcs = function(x){
		marclist <- ICs[match(x[-1], ICs$index), ]
		marclist <- marclist[order(marclist$marc, decreasing = T), ]
		marclist <- marclist[1:5, ]
		marclist$marc[is.na(marclist$marc)] = 0
		
		marclist$index = NULL
		
		if (marclist$marc[1] != 0 & marclist$marc[2] != 0 & marclist$bodyregion[1] == marclist$bodyregion[2]) {
			same_region = 1
		} else same_region = 0
		
		marclist = c(marclist$marc, same_region)
		names(marclist) = c(paste0("marc", 1:5), "same_region")
		
		marclist
	}
	
	
	library(foreach)
	library(doParallel)
	
	doParallel::registerDoParallel(cores = cores)
	marcs = foreach::foreach(i = 1:nrow(Pdat), .combine=dplyr::bind_rows, 
													 .multicombine = T, .maxcombine = max_combine) %dopar% {
													 	get_marcs(Pdat[i, ])
													 }
	doParallel::stopImplicitCluster()
	
	marcs = as.data.frame(marcs)
	marcs$Imarc = marcs$marc1 * marcs$marc2
	
	
	Model_Calc = 
		{1.406958 * marcs$marc1} + 
		{1.409992 * marcs$marc2} + 
		{0.5205343 * marcs$marc3} +
		{0.4150946 * marcs$marc4} + 
		{0.8883929 * marcs$marc5} + 
		{-0.0890527 * marcs$same_region} - 
		{0.7782696 * marcs$Imarc} - 2.217565
	
	
	pnorm(Model_Calc)
	
	
}


tmpm5 = function(Pdat){
	
	
	get_marcs = function(x){
		marclist <- ICs[match(x[-1], ICs$index), ]
		marclist <- marclist[order(marclist$marc, decreasing = T), ]
		marclist <- marclist[1:5, ]
		marclist$marc[is.na(marclist$marc)] = 0
		
		marclist$index = NULL
		
		if (marclist$marc[1] != 0 & marclist$marc[2] != 0 & marclist$bodyregion[1] == marclist$bodyregion[2]) {
			same_region = 1
		} else same_region = 0
		
		Imarc = marclist$marc[1]*marclist$marc[2]
		
		marclist = c(marclist$marc, same_region, Imarc)
		names(marclist) = c(paste0("marc", 1:5), "same_region", "Imarc")
		
		marclist
	}
	
	
	marcs = apply(Pdat, 1, get_marcs) 
	
	betas = c(1.406958,      #marc1
						1.409992,      #marc2
						0.5205343,     #marc3
						0.4150946,     #marc4
						0.8883929,     #marc5
						-0.0890527,    #same_region
						-0.7782696     #Imarc
	)
	
	
	Model_Calc = {betas %*% marcs} - 2.217565
	
	
	Model_Calc = pnorm(Model_Calc)
	
	c(Model_Calc)
	
	
}

#parallel version of tmpm5, not really any faster
tmpm5p = function(Pdat, cores = 6, max_combine = max(c(ceiling(nrow(Pdat)*0.1), 100))){
	
	get_marcs = function(x){
		marclist <- ICs[match(x[-1], ICs$index), ]
		marclist <- marclist[order(marclist$marc, decreasing = T), ]
		marclist <- marclist[1:5, ]
		marclist$marc[is.na(marclist$marc)] = 0
		
		marclist$index = NULL
		
		if (marclist$marc[1] != 0 & marclist$marc[2] != 0 & marclist$bodyregion[1] == marclist$bodyregion[2]) {
			same_region = 1
		} else same_region = 0
		
		Imarc = marclist$marc[1]*marclist$marc[2]
		
		marclist = c(marclist$marc, same_region, Imarc)
		names(marclist) = c(paste0("marc", 1:5), "same_region", "Imarc")
		
		marclist
	}
	
	
	library(foreach)
	library(doParallel)
	doParallel::registerDoParallel(cores = cores)
	marcs = foreach::foreach(i = 1:nrow(Pdat), .combine=dplyr::bind_rows, 
													 .multicombine = T, .maxcombine = max_combine) %dopar% {
													 	get_marcs(Pdat[i, ])
													 }
	doParallel::stopImplicitCluster()
	
	marcs = as.matrix(marcs)
	
	betas = c(1.406958,      #marc1
						1.409992,      #marc2
						0.5205343,     #marc3
						0.4150946,     #marc4
						0.8883929,     #marc5
						-0.0890527,    #same_region
						-0.7782696     #Imarc
	)
	
	betas = matrix(betas, ncol = 1)
	
	Model_Calc = {marcs %*% betas} - 2.217565
	
	Model_Calc = pnorm(Model_Calc)
	
	c(Model_Calc)
}




mb = microbenchmark::microbenchmark(
	{tmpm(dx[1:100, ], ILex = 9)},
	{tmpm2(dx[1:100, ])},
	{tmpm3(dx[1:100, ])},
	{tmpm4(dx[1:100, ])},
	{tmpm4p(dx[1:100, ])},
	{tmpm4p(dx[1:100, ], max_combine = 100)},
	{tmpm4p(dx[1:100, ], max_combine = 50)},
	{tmpm5(dx[1:100, ])},
	{tmpm5p(dx[1:100, ])},
	{tmpm5p(dx[1:100, ], max_combine = 100)},
	{tmpm5p(dx[1:100, ], max_combine = 50)},
	times = 200, 
	control = list(warmup = 5))

gc()
mb2 = microbenchmark::microbenchmark(
	{tmpm(dx[1:1000, ], ILex = 9)},
	{tmpm2(dx[1:1000, ])},
	{tmpm3(dx[1:1000, ])},
	{tmpm4(dx[1:1000, ])},
	{tmpm4p(dx[1:1000, ])},
	{tmpm4p(dx[1:1000, ], max_combine = 1000)},
	{tmpm4p(dx[1:1000, ], max_combine = 500)},
	{tmpm5(dx[1:1000, ])},
	{tmpm5p(dx[1:1000, ])},
	{tmpm5p(dx[1:1000, ], max_combine = 1000)},
	{tmpm5p(dx[1:1000, ], max_combine = 500)},
	times = 200, 
	control = list(warmup = 5))
gc()


mb
ggplot2::autoplot(mb)

mb2
ggplot2::autoplot(mb2)









temp = tmpm(dx[1:1000, ], ILex = 9)$pDeath
all.equal(temp, tmpm2(dx[1:1000, ])$pDeath )  #same results
all.equal(temp, tmpm3(dx[1:1000, ])$pDeath )  #same results
all.equal(temp, tmpm4(dx[1:1000, ]) )  #same results
all.equal(temp, tmpm4p(dx[1:1000, ]) )  #same results
all.equal(temp, tmpm5(dx[1:1000, ]) )  #same results
all.equal(temp, tmpm5p(dx[1:1000, ]) )  #same results



rm(temp, mb, tmpm2, tmpm3, tmpm5)









#convert everything to a factor to reduce object size
dx = dx%>% mutate_all(., factor)



#do in chunks to reduce memory pressure
#takes about 20 mins with 7 cores and chunk.size = 1000
dx$pDeath = NA_real_

chunk.size = 1000
nchunks = ceiling( nrow(dx)/chunk.size)

starts = seq(1, nrow(dx), chunk.size)
ends = c(seq(chunk.size, nrow(dx), chunk.size), nrow(dx))


max_cores = parallel::detectCores()
choices = c("No, don't use parallel processing",
						paste("Yes, use parallel processing with", 2:max_cores, "cores") )

res = menu(choices, 
					 title = "Use parallel processing (multiple processors) to calculate TMPM?")

writeLines(c(""), "Data/tmpm_log.txt") #make file to monitor progress

if(res>1){
	library(foreach)
	library(doParallel)
	
	doParallel::registerDoParallel(cores = res)
	print(Sys.time())
	dx$pDeath = foreach::foreach(i = 1:nchunks, .combine=c, .multicombine = T, .maxcombine = 1000) %dopar% {
		try(cat(paste(Sys.time(), ":\t\t chunk", i, "of", nchunks, "\t\t", round( (i/nchunks)*100,2), "%\n"),
						file="Data/tmpm_log.txt", append=TRUE))
		tmpm5(dx[ starts[i]:ends[i], ])
	}
	print(Sys.time())
	doParallel::stopImplicitCluster()
	
} else {
	print(Sys.time())
	for(i in 1:nchunks){
		dx$pDeath[ starts[i]:ends[i] ] = tmpm4(dx[ starts[i]:ends[i], ]) 
	}
	print(Sys.time())
}

rm(max_cores, res, starts, ends, choices, chunk.size, nchunks)




gc()


dx[, paste0("dx", 1:15) ] = NULL

dir.create("Data/final/")
saveRDS(dx, "Data/final/tmpm.RDS")

#don't need backup of all dxs anymore
file.remove("Data/final/dx_final.csv")

dbDisconnect(neds)
rm(list = ls())
gc()




# export ecodes, recode, and consolidate  --------------------------------------




neds = dbConnect(RSQLite::SQLite(), "Data/NEDS_DB.sqlite")



dbSendQuery(neds, 
						"DELETE FROM ecodes
										WHERE key_ed NOT IN (SELECT f.key_ed FROM join_res AS f)")



ecodes = dbReadTable(neds, "ecodes")
ecodes = as.data.table(ecodes)


#drop unneeded vars
ecodes[, c("discwt", "year")]= NULL







#to standardize the length of the strings
dx_recode = function(dx){
	case_when( 
		nchar(dx) == 3 ~ paste0(dx, "00"),
		nchar(dx) == 4 ~ paste0(dx, "0"),
		TRUE ~ dx
	)
}

ecodes = ecodes %>% mutate_at(vars(starts_with("ecode")), dx_recode)




#read in ICD mapping file
icd_map = read_csv("https://raw.githubusercontent.com/jrf1111/ICD10-to-ICD9/master/ICD10%20ecodes%20with%20ICD9%20links.csv")

#select needed vars
icd_map = icd_map[, c("icd9_ecode", "icd9_mechanism", "icd9_intent")]
icd_map = icd_map[!is.na(icd_map$icd9_ecode), ]



#reformat to match ecodes file
icd_map$icd9_ecode = icd_map$icd9_ecode %>% 
	as.character() %>%  #convert to character
	gsub(".", "", ., fixed = T) %>% #remove periods
	dx_recode() #standardize the length of the strings



#join mapping file to ecode1
colnames(icd_map) = c("ecode1", "mech1", "intent1")
ecodes = plyr::join(ecodes, icd_map,
										by = "ecode1",
										type = "left", 
										match = "first")


#join mapping file to ecode2
colnames(icd_map) = c("ecode2", "mech2", "intent2")
ecodes = plyr::join(ecodes, icd_map,
										by = "ecode2",
										type = "left", 
										match = "first")


#join mapping file to ecode3
colnames(icd_map) = c("ecode3", "mech3", "intent3")
ecodes = plyr::join(ecodes, icd_map,
										by = "ecode3",
										type = "left", 
										match = "first")


#join mapping file to ecode4
colnames(icd_map) = c("ecode4", "mech4", "intent4")
ecodes = plyr::join(ecodes, icd_map,
										by = "ecode4",
										type = "left", 
										match = "first")


#re-order vars
ecodes = ecodes[, c("key_ed", "hosp_ed", 
										"ecode1", "mech1", "intent1",
										"ecode2", "mech2", "intent2", 
										"ecode3", "mech3", "intent3", 
										"ecode4", "mech4", "intent4")]

#remove duplicates
ecodes = ecodes[!duplicated(ecodes$key_ed), ]

ecodes = ecodes[!is.na(ecodes$key_ed), ]


saveRDS(ecodes, "Data/final/ecodes_final.RDS")

rm(ecodes, icd_map, dx_recode)

# disconnect from DB ------------------------------------------------------



dbDisconnect(neds)
rm(neds)


