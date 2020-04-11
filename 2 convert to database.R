library(tidyverse)
library(dplyr)
library(dbplyr)
library(RSQLite)
library(DBI)
library(data.table)




#create database ----
neds = dbConnect(RSQLite::SQLite(), "Data/NEDS_DB.sqlite")
dbSendQuery(neds, "PRAGMA foreign_keys=ON")





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


query = dbSendQuery(neds, "SELECT * from demos limit 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)





# ~ dx ----
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
dbFetch(query)
dbColumnInfo(query)
rm(query)









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



query = dbSendQuery(neds, "SELECT * from ecodes limit 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)








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



query = dbSendQuery(neds, "SELECT * from outcomes limit 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)












# ~ IP ----
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
dbFetch(query)
dbColumnInfo(query)
rm(query)





# ~ hospital ----
files = list.files("Data/", pattern = "Hospital", full.names = T)

for(i in 1:length(files)){
	file = files[i]
	temp = readRDS(file)
	dbWriteTable(neds, "hosp", temp, append = TRUE)
	rm(temp)
	print(file)
}


query = dbSendQuery(neds, "SELECT * from hosp limit 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)










dbDisconnect(neds)










# create indexes ----------------------------------------------------------


neds = dbConnect(RSQLite::SQLite(), "Data/NEDS_DB.sqlite")

dbListTables(neds)
dbListFields(neds, "demos")
dbListFields(neds, "hosp")

#create index
dbExecute(neds, "CREATE INDEX index_key_ed ON demos (key_ed)")
dbGetQuery(neds, "PRAGMA index_list(demos)")     #verify index was created


dbExecute(neds, "CREATE INDEX index_hosp_ed ON hosp (hosp_ed)")
dbGetQuery(neds, "PRAGMA index_list(hosp)")






# standardize dx strings --------------------------------------------------
query = dbSendQuery(neds, "SELECT dx1 FROM dx LIMIT 20")
dbFetch(query)


# dx1 with length of 3
dbSendQuery(neds, 
						"SELECT dx1 || '00' AS dx1s
										FROM dx
										WHERE LENGTH(dx1) = 3
										LIMIT 50")



dbSendQuery(neds, 
						"UPDATE dx
						 SET dx1 = (dx1 || '00')
						 WHERE LENGTH(dx1) = 3")

dbSendQuery(neds, "SELECT dx1 FROM dx LIMIT 50")






# dx1 with length of 4
dbSendQuery(neds, 
						"SELECT dx1 || '0' AS dx1s
										FROM dx
										WHERE LENGTH(dx1) = 4
										LIMIT 50")



dbSendQuery(neds, 
						"UPDATE dx
						 SET dx1 = (dx1 || '0')
						 WHERE LENGTH(dx1) = 4")

dbSendQuery(neds, "SELECT dx1 FROM dx LIMIT 50")





# make table of injury dxs for filtering ----------------------------------

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

# make table of ecodes for filtering ----------------------------------



#Exclude: injuries including burns, bites/stings, overexertion, 
#                  poisoning, or misadventures of medical/surgical care


exclude = c(
	
	# E890-E899  Accidents Caused By Fire And Flames
	paste0("E", 890:899),
	
	# E990 Injury due to war operations by fires and conflagrations
	paste0("E", 990),
	
	# E906 Other injury caused by animals
	paste0("E", 906),
	
	# E927 Overexertion and strenuous movements
	paste0("E", 927),
	
	# E850-E858  Accidental Poisoning By Drugs, Medicinal Substances, And Biologicals
	paste0("E", 850:858),
	
	# E860-E869  Accidental Poisoning By Other Solid And Liquid Substances, Gases, And Vapors
	paste0("E", 860:869),
	
	# E905 Venomous animals and plants as the cause of poisoning and toxic reactions
	paste0("E", 905), 
	
	# E950 Suicide and self-inflicted poisoning by solid or liquid substances
	# E951 Suicide and self-inflicted poisoning by gases in domestic use
	# E952 Suicide and self-inflicted poisoning by other gases and vapors
	paste0("E", 950:952), 
	
	# E962 Assault by poisoning
	paste0("E", 962),
	
	# E980 Poisoning by solid or liquid substances undetermined whether accidentally or purposely inflicted
	# E981 Poisoning by gases in domestic use undetermined whether accidentally or purposely inflicted
	# E982 Poisoning by other gases undetermined whether accidentally or purposely inflicted
	paste0("E", 980:982),
	
	# E870-E876  Misadventures To Patients During Surgical And Medical Care
	paste0("E", 870:876),
	
	# E878-E879  Surgical And Medical Procedures As The Cause Of Abnormal Reaction Of Patient
	# 						Or Later Complication, Without Mention Of Misadventure At The Time Of Procedure
	paste0("E", 878:879),
	
	# E930-E949  Drugs, Medicinal And Biological Substances Causing Adverse Effects In Therapeutic Use
	paste0("E", 930:949))






#make table of ecodes to exclude in DB
dbWriteTable(neds, "ecode_exclude",  data.frame(exclude = exclude)  )

#confirm it was created
dbListTables(neds)
dbListFields(neds, "ecode_exclude")
dbGetQuery(neds, "SELECT * FROM ecode_exclude LIMIT 20")
rm(exclude)



# filters -------------------------------------------------------


# ~ filter on injury dx ----
dbSendQuery(neds, 
						"DELETE FROM dx
										WHERE dx1 NOT IN (SELECT f.injury_dx FROM injury_dx f)")




#confirm filter (should be zero)
dbGetQuery(neds, 
					 'SELECT COUNT(dx1)
					 FROM dx
					 WHERE dx1 LIKE "2%"')










# ~ filter on age ----
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











# ~ filter on ecodes ----
dbSendQuery(neds, 
						"DELETE FROM ecodes
										WHERE SUBSTR(ecode1, 1, 4) IN (SELECT f.exclude FROM ecode_exclude f)")

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
						"DELETE FROM ecodes
										WHERE SUBSTR(ecode2, 1, 4) IN (SELECT f.exclude FROM ecode_exclude f)")

#confirm filter (should be zero)
dbGetQuery(neds, 
					 'SELECT COUNT(ecode2)
					 FROM ecodes
					 WHERE SUBSTR(ecode2, 1, 4) = "E890"')

dbGetQuery(neds, 
					 'SELECT COUNT(ecode2)
					 FROM ecodes
					 WHERE SUBSTR(ecode2, 1, 4) = "E850"')







dbSendQuery(neds, 
						"DELETE FROM ecodes
										WHERE SUBSTR(ecode3, 1, 4) IN (SELECT f.exclude FROM ecode_exclude f)")

#confirm filter (should be zero)
dbGetQuery(neds, 
					 'SELECT COUNT(ecode3)
					 FROM ecodes
					 WHERE SUBSTR(ecode3, 1, 4) = "E890"')

dbGetQuery(neds, 
					 'SELECT COUNT(ecode3)
					 FROM ecodes
					 WHERE SUBSTR(ecode3, 1, 4) = "E850"')





dbSendQuery(neds, 
						"DELETE FROM ecodes
										WHERE SUBSTR(ecode4, 1, 4) IN (SELECT f.exclude FROM ecode_exclude f)")

#confirm filter (should be zero)
dbGetQuery(neds, 
					 'SELECT COUNT(ecode4)
					 FROM ecodes
					 WHERE SUBSTR(ecode4, 1, 4) = "E890"')

dbGetQuery(neds, 
					 'SELECT COUNT(ecode4)
					 FROM ecodes
					 WHERE SUBSTR(ecode4, 1, 4) = "E850"')







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
						age integer,
						discwt double,
						female double,
						hosp_ed double,
						key_ed character,
						year character,
						
						
						dx1 character,
						dx2 character,
						dx3 character,
						dx4 character,
						dx5 character,
						dx6 character,
						dx7 character,
						dx8 character,
						dx9 character,
						dx10 character,
						dx11 character,
						dx12 character,
						dx13 character,
						dx14 character,
						dx15 character,
						
						
						ecode1 character,
						ecode2 character,
						ecode3 character,
						ecode4 character,
						
						
						hospwt double,
						hosp_control double,
						hosp_region double,
						hosp_trauma double,
						neds_stratum double,
						
						
						hcupfile character,
						disp_ip double,
						died_visit double,
						disp_ed double,
						edevent double
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



dbSendQuery(neds, 
						"INSERT INTO join_res
						 SELECT demos.age,  demos.discwt,  demos.female,  demos.hosp_ed,  demos.key_ed,  demos.year,
						 dx.dx1,  dx.dx2,  dx.dx3,  dx.dx4,  dx.dx5,  dx.dx6,  dx.dx7,  dx.dx8,  dx.dx9,  dx.dx10, 
						 dx.dx11,  dx.dx12,  dx.dx13,  dx.dx14,  dx.dx15,
						 ecodes.ecode1,  ecodes.ecode2,  ecodes.ecode3,  ecodes.ecode4, 
						 hosp.hospwt,  hosp.hosp_control,  hosp.hosp_region,  hosp.hosp_trauma, hosp.neds_stratum,
						 ip.hcupfile, ip.disp_ip, 
						 outcomes.died_visit, outcomes.disp_ed,  outcomes.edevent
						 
						 FROM demos
						 INNER JOIN dx
						 ON demos.key_ed = dx.key_ed
						 
						 INNER JOIN ecodes
						 ON dx.key_ed = ecodes.key_ed
						 
						 INNER JOIN ip
						 ON ecodes.key_ed = ip.key_ed
						 
						 INNER JOIN outcomes
						 ON ip.key_ed = outcomes.key_ed
						 
						 LEFT JOIN hosp
						 ON demos.hosp_ed = hosp.hosp_ed")









# disconnect from DB ------------------------------------------------------



dbDisconnect(neds)



