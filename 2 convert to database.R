library(tidyverse)
library(dplyr)
library(dbplyr)
library(RSQLite)
library(DBI)
library(data.table)




#create database ----
neds = dbConnect(RSQLite::SQLite(), "Data/NEDS_DB.sqlite")






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
dbGetQuery(neds, "SELECT dx1 FROM dx LIMIT 20")


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







#update other dxs
dbSendQuery(neds, 
						"UPDATE dx
						 SET dx2 = (dx2 || '00')
						 WHERE LENGTH(dx2) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx2 = (dx2 || '0')
						 WHERE LENGTH(dx2) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx3 = (dx3 || '00')
						 WHERE LENGTH(dx3) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx3 = (dx3 || '0')
						 WHERE LENGTH(dx3) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx4 = (dx4 || '00')
						 WHERE LENGTH(dx4) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx4 = (dx4 || '0')
						 WHERE LENGTH(dx4) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx5 = (dx5 || '00')
						 WHERE LENGTH(dx5) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx5 = (dx5 || '0')
						 WHERE LENGTH(dx5) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx6 = (dx6 || '00')
						 WHERE LENGTH(dx6) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx6 = (dx6 || '0')
						 WHERE LENGTH(dx6) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx7 = (dx7 || '00')
						 WHERE LENGTH(dx7) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx7 = (dx7 || '0')
						 WHERE LENGTH(dx7) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx8 = (dx8 || '00')
						 WHERE LENGTH(dx8) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx8 = (dx8 || '0')
						 WHERE LENGTH(dx8) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx9 = (dx9 || '00')
						 WHERE LENGTH(dx9) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx9 = (dx9 || '0')
						 WHERE LENGTH(dx9) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx10 = (dx10 || '00')
						 WHERE LENGTH(dx10) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx10 = (dx10 || '0')
						 WHERE LENGTH(dx10) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx11 = (dx11 || '00')
						 WHERE LENGTH(dx11) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx11 = (dx11 || '0')
						 WHERE LENGTH(dx11) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx12 = (dx12 || '00')
						 WHERE LENGTH(dx12) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx12 = (dx12 || '0')
						 WHERE LENGTH(dx12) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx13 = (dx13 || '00')
						 WHERE LENGTH(dx13) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx13 = (dx13 || '0')
						 WHERE LENGTH(dx13) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx14 = (dx14 || '00')
						 WHERE LENGTH(dx14) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx14 = (dx14 || '0')
						 WHERE LENGTH(dx14) = 4")


dbSendQuery(neds, 
						"UPDATE dx
						 SET dx15 = (dx15 || '00')
						 WHERE LENGTH(dx15) = 3")

dbSendQuery(neds, 
						"UPDATE dx
						 SET dx15 = (dx15 || '0')
						 WHERE LENGTH(dx15) = 4")










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
										WHERE dx1 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")


# ~~ NULL out non-injury dxs to reduce file size -----

#skip dx1 since already filtered above

dbSendQuery(neds, 
						"UPDATE dx SET dx2 = NULL WHERE dx2 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx3 = NULL WHERE dx3 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx4 = NULL WHERE dx4 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx5 = NULL WHERE dx5 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx6 = NULL WHERE dx6 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx7 = NULL WHERE dx7 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx8 = NULL WHERE dx8 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx9 = NULL WHERE dx9 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx10 = NULL WHERE dx10 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx11 = NULL WHERE dx11 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx12 = NULL WHERE dx12 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx13 = NULL WHERE dx13 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx14 = NULL WHERE dx14 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")

dbSendQuery(neds, 
						"UPDATE dx SET dx15 = NULL WHERE dx15 NOT IN (SELECT f.injury_dx FROM injury_dx AS f)")




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
										WHERE SUBSTR(ecode1, 1, 4) IN (SELECT f.exclude FROM ecode_exclude AS f)")

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
										WHERE SUBSTR(ecode2, 1, 4) IN (SELECT f.exclude FROM ecode_exclude AS f)")

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
										WHERE SUBSTR(ecode3, 1, 4) IN (SELECT f.exclude FROM ecode_exclude AS f)")

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
										WHERE SUBSTR(ecode4, 1, 4) IN (SELECT f.exclude FROM ecode_exclude AS f)")

#confirm filter (should be zero)
dbGetQuery(neds, 
					 'SELECT COUNT(ecode4)
					 FROM ecodes
					 WHERE SUBSTR(ecode4, 1, 4) = "E890"')

dbGetQuery(neds, 
					 'SELECT COUNT(ecode4)
					 FROM ecodes
					 WHERE SUBSTR(ecode4, 1, 4) = "E850"')







# create indexes ----------------------------------------------------------
#indexes take a while to create but they speed up the joins on the key_ed column by ~4-5x

dbExecute(neds, "CREATE INDEX index_key_ed_demos ON demos (key_ed)")
dbExecute(neds, "CREATE INDEX index_key_ed_dx ON dx (key_ed)")
dbExecute(neds, "CREATE INDEX index_key_ed_ecodes ON ecodes (key_ed)")
dbExecute(neds, "CREATE INDEX index_key_ed_ip ON ip (key_ed)")
dbExecute(neds, "CREATE INDEX index_key_ed_outcomes ON outcomes (key_ed)")





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
						
						hcupfile CHARACTER,
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



#takes about 45 mins with the key_ed indexes and about 6 hours without the indexes
#vars in SELECT command have to be in same order as in the creation of join_res
dbSendQuery(neds, 
						"INSERT INTO join_res
						 SELECT demos.key_ed, demos.hosp_ed, demos.year, demos.discwt, demos.age, demos.female,  
						 outcomes.died_visit, outcomes.disp_ed,  outcomes.edevent,
						 dx.dx1,  dx.dx2,  dx.dx3,  dx.dx4,  dx.dx5,  dx.dx6,  dx.dx7,  dx.dx8,  dx.dx9,  dx.dx10, 
						 dx.dx11,  dx.dx12,  dx.dx13,  dx.dx14,  dx.dx15,
						 ecodes.ecode1,  ecodes.ecode2,  ecodes.ecode3,  ecodes.ecode4, 
						 ip.hcupfile, ip.disp_ip,
						 hosp.neds_stratum
						 
						 
						 FROM demos
						 LEFT JOIN outcomes
						 ON demos.key_ed = outcomes.key_ed
						 
						 INNER JOIN dx
						 ON demos.key_ed = dx.key_ed
						 
						 INNER JOIN ecodes
						 ON demos.key_ed = ecodes.key_ed
						 
						 LEFT JOIN ip
						 ON demos.key_ed = ip.key_ed
						 
						 LEFT JOIN hosp
						 ON demos.hosp_ed = hosp.hosp_ed")


# ROWS Fetched: 0 [complete]
# Changed: 35060586




# ~ confirm proper join -----------------------------------------------------

dbListFields(neds, "join_res")


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
					 GROUP BY dx2')


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



dbGetQuery(neds, 
					 'SELECT COUNT(*), ecode2
					 FROM join_res
					 WHERE SUBSTR(ecode2, 1, 4) IN (SELECT f.exclude FROM ecode_exclude AS f)
					 GROUP BY ecode1')



dbGetQuery(neds, 
					 'SELECT COUNT(*), ecode3
					 FROM join_res
					 WHERE SUBSTR(ecode3, 1, 4) IN (SELECT f.exclude FROM ecode_exclude AS f)
					 GROUP BY ecode1')



dbGetQuery(neds, 
					 'SELECT COUNT(*), ecode4
					 FROM join_res
					 WHERE SUBSTR(ecode4, 1, 4) IN (SELECT f.exclude FROM ecode_exclude AS f)
					 GROUP BY ecode1')





gc()




# export dxs to calculate TMPM -------------------------------------------

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


gc()
dx = dbReadTable(neds, "dx_final")
dx = as.data.table(dx)
gc()


#save dx file just in case
fwrite(dx, "Data/dx_final.csv")

dx = fread("Data/dx_final.csv")


#get ICD9 dx codes in proper format: numeric, ###.##
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
	
	
	marcs = apply(Pdat, 1, get_marcs) %>%
		t() %>% #transposing takes a little bit...
		as_tibble()
	
	
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
	

	
	
	Model_Calc = {betas * marcs} - 2.217565
	
	
	pnorm(Model_Calc)
	
	
	
	
}





ns = c(1000, 2500, 5000, 10000, 20000, 50000)
times = vector("character", length(ns))
times2 = vector("character", length(ns))
times3 = vector("character", length(ns))
times4 = vector("character", length(ns))
gc()
set.seed(11)
for(i in 1:length(ns)){
	
	start = Sys.time(); tmpm(dx[1:ns[i], ], ILex = 9); stop = Sys.time()
	times[i] = format(stop-start, units = "secs")
	
	start = Sys.time(); tmpm2(dx[1:ns[i], ]); stop = Sys.time()
	times2[i] = format(stop-start, units = "secs")
	
	start = Sys.time(); tmpm3(dx[1:ns[i], ]); stop = Sys.time()
	times3[i] = format(stop-start, units = "secs")
	
	start = Sys.time(); tmpm4(dx[1:ns[i], ]); stop = Sys.time()
	times4[i] = format(stop-start, units = "secs")
}

times = times %>% gsub(" secs", "", .) %>% as.numeric()
times2 = times2 %>% gsub(" secs", "", .) %>% as.numeric()
times3 = times3 %>% gsub(" secs", "", .) %>% as.numeric()
times4 = times4 %>% gsub(" secs", "", .) %>% as.numeric()


plot(ns, times, ylim = c(0, 60))
points(ns, times2, col = "red")
points(ns, times3, col = "blue")
points(ns, times4, col = "green")

median(times2/times)  #about twice as fast :)
median(times3/times)  #faster
median(times4/times)  #faster still :)


summary(lag(times, 1)/times)
summary(lag(times2, 1)/times2)
summary(lag(times3, 1)/times3)
summary(lag(times4, 1)/times4)


all.equal(tmpm(dx[1:500, ], ILex = 9)$pDeath, tmpm2(dx[1:500, ])$pDeath )  #with the same result
all.equal(tmpm(dx[1:500, ], ILex = 9)$pDeath, tmpm3(dx[1:500, ])$pDeath )  #with the same result
all.equal(tmpm(dx[1:500, ], ILex = 9)$pDeath, tmpm4(dx[1:500, ]) )  #with the same result


rm(i, ns, times, times2, times3, start, stop, tmpm2, tmpm3)



#convert everything to a factor to reduce object size
dx = dx%>% mutate_all(., factor)



#do in chunks to reduce memory pressure
dx$pDeath = NA_real_

chunk.size = 50000
nchunks = ceiling( nrow(dx)/chunk.size)

starts = seq(1, nrow(dx), chunk.size)
ends = c(seq(chunk.size, nrow(dx), chunk.size), nrow(dx))

warning("Looks like this will take at least ", (times4[6]*length(starts))/60/60, " hours")

Sys.time()
for(i in 1:nchunks){
	dx$pDeath[ starts[i]:ends[i] ] = tmpm4(dx[ starts[i]:ends[i], ]) 
}
Sys.time()
gc()


dx[, paste0("dx", 1:15) ] = NULL

saveRDS(dx, "Data/tmpm.RDS")

#don't need backup of all dxs anymore
file.remove("Data/dx_final.csv")

rm(list = ls())
gc()




# export ecodes for recoding and consolidation  --------------------------------------




neds = dbConnect(RSQLite::SQLite(), "Data/NEDS_DB.sqlite")



dbSendQuery(neds, 
						"DELETE FROM ecodes
										WHERE key_ed NOT IN (SELECT f.key_ed FROM join_res AS f)")

# ROWS Fetched: 0 [complete]
# Changed: 184913890


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


saveRDS(ecodes, "Data/ecodes_final.RDS")

rm(ecodes, icd_map, dx_recode)


# disconnect from DB ------------------------------------------------------



dbDisconnect(neds)



