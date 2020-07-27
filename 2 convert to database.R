library(tidyverse)
library(dplyr)
library(dbplyr)
# library(RSQLite) # starts throwing "Error: database or disk is full" after updating to R v4, use postgresql instead
library(RPostgreSQL)
library(DBI)
library(data.table)
library(bit64)  #required for 64 bit integers used in key_ed
library(fst)
library(comorbidity)


#create database ----


#configure server
conf_file = "/usr/local/var/postgres/postgresql.conf"

cat("\nshared_buffers = 400MB", file = conf_file, append = T)
cat("\ntemp_buffers = 300MB", file = conf_file, append = T)
cat("\nwork_mem = 6GB", file = conf_file, append = T)
cat("\nmaintenance_work_mem = 1GB", file = conf_file, append = T)
cat("\nautovacuum_work_mem = -1", file = conf_file, append = T)
cat("\nautovacuum = off", file = conf_file, append = T)
cat("\ntrack_counts = off", file = conf_file, append = T)
cat("\nfsync = off", file = conf_file, append = T)
cat("\nsynchronous_commit = off", file = conf_file, append = T)
cat("\nwal_level = minimal", file = conf_file, append = T)
cat("\nmax_wal_senders = 0", file = conf_file, append = T)
cat("\nmax_wal_size = 2GB", file = conf_file, append = T)
cat("\nfull_page_writes = off", file = conf_file, append = T)
rm(conf_file)






system("pg_ctl -D /usr/local/var/postgres start") #start PostgreSQL server in daemon (-D) mode

system("createdb --username 'jr-f' --no-password  --host 'localhost'  --port 5432  neds")


neds = dbConnect(dbDriver("PostgreSQL"), 
								 user="jr-f", 
								 password="",
								 host="localhost",
								 port=5432,
								 dbname="neds")




#function to make sure all years of data get imported
check_for_all_years = function(files){

	years = as.character(2010:2016)
	
	all_years = TRUE
	for(i in seq_along(years)){
		if(any(str_detect(files, years[i])) == FALSE){
			all_years = FALSE
			break()
		}
	}
	
	if(all_years == FALSE){
		warning(paste("Missing data file for year", years[i]))
		rm(files, envir = pos.to.env(1))
		dbDisconnect(neds)
	}
	
	
}


# ~ dx ----

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
dbExecute(neds, "DROP TABLE IF EXISTS injury_dx")
dbWriteTable(neds, "injury_dx",  data.frame(injury_dx = injury_codes), row.names=FALSE  )

#confirm it was created
dbListTables(neds)
dbListFields(neds, "injury_dx")
dbGetQuery(neds, "SELECT * FROM injury_dx LIMIT 20")





# ~~ import dx data ----

files = list.files("Data/", pattern = "dx", full.names = T)

check_for_all_years(files)

dbExecute(neds, "DROP TABLE IF EXISTS dx")



for(i in 1:length(files)){
	file = files[i]
	cat("\n", file)
	temp = read_fst(file)
	
	if(str_detect(file, "2010")){temp$year = "2010"}
	if(str_detect(file, "2011")){temp$year = "2011"}
	if(str_detect(file, "2012")){temp$year = "2012"}
	if(str_detect(file, "2013")){temp$year = "2013"}
	if(str_detect(file, "2014")){temp$year = "2014"}
	if(str_detect(file, "2015Q1Q3")){temp$year = "2015Q1Q3"}
	if(str_detect(file, "2015Q4")){temp$year = "2015Q4"}
	if(str_detect(file, "2016")){temp$year = "2016"}


	#standardize the length of the strings
	temp = temp %>% mutate_at(vars(starts_with("dx")), dx_recode)


	dbWriteTable(neds, "dx", temp, append = TRUE, row.names=FALSE)
	if(i==1){
		dbExecute(neds, "ALTER TABLE dx SET UNLOGGED") 
		dbExecute(neds, "ALTER TABLE dx DISABLE TRIGGER ALL") 
		}
	rm(temp)
	cat("\t\tcomplete")
}

beepr::beep()

rm(injury_codes)


query = dbSendQuery(neds, "SELECT * FROM dx LIMIT 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)


dbGetQuery(neds, "SELECT year, COUNT(*) FROM dx GROUP BY year")
# 			year    count
# 1     2010 28584301
# 2     2011 28788399
# 3     2012 31091020
# 4     2013 29581718
# 5     2014 31026417
# 6 2015Q1Q3 19787395
# 7   2015Q4  6534198
# 8     2016 32680232




file.remove("nobs log.txt")
n = dbGetQuery(neds, "SELECT COUNT(key_ed) FROM dx")
n = as.integer(n)
cat("Initial N_obs = ", format(n, big.mark=","), "\n", file = "nobs log.txt")




gc()




# ~ demographics ----
files = list.files("Data/", pattern = "demos", full.names = T)
check_for_all_years(files)
dbExecute(neds, "DROP TABLE IF EXISTS demos")
for(i in 1:length(files)){
	file = files[i]
	cat("\n", file)
	temp = read_fst(file)
	
	dbWriteTable(neds, "demos", temp, append = TRUE, row.names=FALSE)
	
	if(i==1){
		dbExecute(neds, "ALTER TABLE demos SET UNLOGGED") 
		dbExecute(neds, "ALTER TABLE demos DISABLE TRIGGER ALL") 
	}
	rm(temp)
	cat("\t\tcomplete")
}


query = dbSendQuery(neds, "SELECT * FROM demos LIMIT 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)
gc()






# ~ ecodes ----
files = list.files("Data/", pattern = "ecode", full.names = T)
check_for_all_years(files)
dbExecute(neds, "DROP TABLE IF EXISTS ecodes")
for(i in 1:length(files)){
	file = files[i]
	cat("\n", file)
	temp = read_fst(file)
	dbWriteTable(neds, "ecodes", temp, append = TRUE, row.names=FALSE)
	if(i==1){
		dbExecute(neds, "ALTER TABLE ecodes SET UNLOGGED") 
		dbExecute(neds, "ALTER TABLE ecodes DISABLE TRIGGER ALL") 
	}
	rm(temp)
	cat("\t\tcomplete")
}



query = dbSendQuery(neds, "SELECT * FROM ecodes LIMIT 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)
gc()





# ~ outcomes ----
files = list.files("Data/", pattern = "outcome", full.names = T)
check_for_all_years(files)
dbExecute(neds, "DROP TABLE IF EXISTS outcomes")
for(i in 1:length(files)){
	file = files[i]
	cat("\n", file)
	temp = read_fst(file)
	dbWriteTable(neds, "outcomes", temp, append = TRUE, row.names=FALSE)
	if(i==1){
		dbExecute(neds, "ALTER TABLE outcomes SET UNLOGGED") 
		dbExecute(neds, "ALTER TABLE outcomes DISABLE TRIGGER ALL") 
	}
	rm(temp)
	cat("\t\tcomplete")
}



query = dbSendQuery(neds, "SELECT * FROM outcomes LIMIT 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)
gc()







# ~ IP ----
files = list.files("Data/", pattern = "IP.fst", full.names = T)
check_for_all_years(files)
dbExecute(neds, "DROP TABLE IF EXISTS ip")
for(i in 1:length(files)){
	file = files[i]
	cat("\n", file)
	temp = read_fst(file)
	dbWriteTable(neds, "ip", temp, append = TRUE, row.names=FALSE)
	if(i==1){
		dbExecute(neds, "ALTER TABLE ip SET UNLOGGED") 
		dbExecute(neds, "ALTER TABLE ip DISABLE TRIGGER ALL") 
	}
	rm(temp)
	cat("\t\tcomplete")
}




query = dbSendQuery(neds, "SELECT * FROM ip LIMIT 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)
gc()



# ~ hospital ----
files = list.files("Data/", pattern = "Hospital.fst", full.names = T)
check_for_all_years(files)
dbExecute(neds, "DROP TABLE IF EXISTS hosp")
for(i in 1:length(files)){
	file = files[i]
	cat("\n", file)
	temp = read_fst(file)
	temp$year_hosp = as.character(temp$year)
	temp = temp[, c("hosp_ed", "neds_stratum", "year_hosp")]
	dbWriteTable(neds, "hosp", temp, append = TRUE, row.names=FALSE)
	if(i==1){
		dbExecute(neds, "ALTER TABLE hosp SET UNLOGGED") 
		dbExecute(neds, "ALTER TABLE hosp DISABLE TRIGGER ALL") 
	}
	rm(temp)
	cat("\t\tcomplete")
}


query = dbSendQuery(neds, "SELECT * FROM hosp LIMIT 20")
dbFetch(query)
dbColumnInfo(query)
rm(query)
gc()




















# create indexes ----------------------------------------------------------

dbGetQuery(neds, "SELECT pg_size_pretty( pg_database_size('neds') )")


dbExecute(neds, "ALTER TABLE demos ALTER COLUMN key_ed TYPE bigint USING key_ed::bigint")
dbExecute(neds, "ALTER TABLE dx ALTER COLUMN key_ed TYPE bigint USING key_ed::bigint")
dbExecute(neds, "ALTER TABLE ecodes ALTER COLUMN key_ed TYPE bigint USING key_ed::bigint")
dbExecute(neds, "ALTER TABLE ip ALTER COLUMN key_ed TYPE bigint USING key_ed::bigint")
dbExecute(neds, "ALTER TABLE outcomes ALTER COLUMN key_ed TYPE bigint USING key_ed::bigint")

dbGetQuery(neds, "SELECT pg_size_pretty( pg_database_size('neds') )")




dbExecute(neds, "CREATE INDEX index_key_ed_demos ON demos (key_ed)")
dbExecute(neds, "CREATE INDEX index_key_ed_dx ON dx (key_ed)")
dbExecute(neds, "CREATE INDEX index_key_ed_ecodes ON ecodes (key_ed)")
dbExecute(neds, "CREATE INDEX index_key_ed_ip ON ip (key_ed)")
dbExecute(neds, "CREATE INDEX index_key_ed_outcomes ON outcomes (key_ed)")


dbGetQuery(neds, "SELECT pg_size_pretty( pg_database_size('neds') )")











# filters -------------------------------------------------------

# ~ injury dx filter ---------------------------------------
dbSendQuery(neds, 
						"DELETE FROM dx
										WHERE dx1 IS NULL OR dx1 NOT IN (SELECT injury_dx FROM injury_dx)")

dbExecute(neds, "VACUUM ANALYZE dx")


dbSendQuery(neds, 
						"DELETE FROM demos
										WHERE key_ed NOT IN (SELECT key_ed FROM dx)")

dbExecute(neds, "VACUUM ANALYZE demos")

dbSendQuery(neds, 
						"DELETE FROM ecodes
										WHERE key_ed NOT IN (SELECT key_ed FROM dx)")

dbExecute(neds, "VACUUM ANALYZE ecodes")

dbSendQuery(neds, 
						"DELETE FROM ip
										WHERE key_ed NOT IN (SELECT key_ed FROM dx)")

dbExecute(neds, "VACUUM ANALYZE ip")

dbSendQuery(neds, 
						"DELETE FROM outcomes
										WHERE key_ed NOT IN (SELECT key_ed FROM dx)")

dbExecute(neds, "VACUUM ANALYZE outcomes")





dbExecute(neds, "REINDEX INDEX index_key_ed_demos")
dbExecute(neds, "REINDEX INDEX index_key_ed_dx")
dbExecute(neds, "REINDEX INDEX index_key_ed_ecodes")
dbExecute(neds, "REINDEX INDEX index_key_ed_ip")
dbExecute(neds, "REINDEX INDEX index_key_ed_outcomes")



dbGetQuery(neds, "SELECT year, COUNT(*) FROM dx GROUP BY year")
# 			year   count
# 1     2010 6045184
# 2     2011 5938588
# 3     2012 6244736
# 4     2013 5853463
# 5     2014 5963440
# 6 2015Q1Q3 4165400
# 7   2015Q4 1278639
# 8     2016 6248782







n = dbGetQuery(neds, "SELECT COUNT(key_ed) FROM dx")
n = as.integer(n)
cat("N_obs after filtering on injury dx = ", format(n, big.mark=","), "\n", file="nobs log.txt", append=T)









# ~ age filter ----
dbSendQuery(neds, 
						"DELETE FROM demos
										WHERE age < 50 OR age IS NULL")

dbExecute(neds, "VACUUM ANALYZE demos")

dbSendQuery(neds, 
						"DELETE FROM dx
										WHERE key_ed NOT IN (SELECT key_ed FROM demos)")

dbExecute(neds, "VACUUM ANALYZE dx")

dbSendQuery(neds, 
						"DELETE FROM ecodes
										WHERE key_ed NOT IN (SELECT key_ed FROM demos)")

dbExecute(neds, "VACUUM ANALYZE ecodes")

dbSendQuery(neds, 
						"DELETE FROM ip
										WHERE key_ed NOT IN (SELECT key_ed FROM demos)")

dbExecute(neds, "VACUUM ANALYZE ip")

dbSendQuery(neds, 
						"DELETE FROM outcomes
										WHERE key_ed NOT IN (SELECT key_ed FROM demos)")

dbExecute(neds, "VACUUM ANALYZE outcomes")






dbExecute(neds, "REINDEX INDEX index_key_ed_demos")
dbExecute(neds, "REINDEX INDEX index_key_ed_dx")
dbExecute(neds, "REINDEX INDEX index_key_ed_ecodes")
dbExecute(neds, "REINDEX INDEX index_key_ed_ip")
dbExecute(neds, "REINDEX INDEX index_key_ed_outcomes")





dbGetQuery(neds, "SELECT year, COUNT(*) FROM dx GROUP BY year")
# 			year   count
# 1     2010 1691726
# 2     2011 1704286
# 3     2012 1811691
# 4     2013 1791377
# 5     2014 1889341
# 6 2015Q1Q3 1213637
# 7   2015Q4  388882
# 8     2016 2186191





n = dbGetQuery(neds, "SELECT COUNT(key_ed) FROM dx")
n = as.integer(n)
cat("N_obs after filtering on age = ", format(n, big.mark=","), "\n", file="nobs log.txt", append=T)






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
dbWriteTable(neds, "ecode_exclude",  data.frame(exclude = exclude) , row.names=FALSE )

#confirm it was created
dbListTables(neds)
dbListFields(neds, "ecode_exclude")
dbGetQuery(neds, "SELECT * FROM ecode_exclude LIMIT 20")
rm(exclude)



# ~~ filter on ecodes ----


#Per Alan, only filter on ecode1
dbSendQuery(neds,
						"DELETE FROM ecodes
										WHERE SUBSTR(ecode1, 1, 4) IN (SELECT exclude FROM ecode_exclude)")
dbSendQuery(neds,
						"DELETE FROM ecodes
										WHERE ecode1 IN (SELECT exclude FROM ecode_exclude)")

dbSendQuery(neds,
						"DELETE FROM ecodes
										WHERE ecode1 IS NULL")


dbExecute(neds, "VACUUM ANALYZE ecodes")



dbSendQuery(neds, 
						"DELETE FROM dx
										WHERE key_ed NOT IN (SELECT key_ed FROM ecodes)")

dbExecute(neds, "VACUUM ANALYZE dx")

dbSendQuery(neds, 
						"DELETE FROM demos
										WHERE key_ed NOT IN (SELECT key_ed FROM ecodes)")

dbExecute(neds, "VACUUM ANALYZE demos")

dbSendQuery(neds, 
						"DELETE FROM ip
										WHERE key_ed NOT IN (SELECT key_ed FROM ecodes)")

dbExecute(neds, "VACUUM ANALYZE ip")

dbSendQuery(neds, 
						"DELETE FROM outcomes
										WHERE key_ed NOT IN (SELECT key_ed FROM ecodes)")

dbExecute(neds, "VACUUM ANALYZE outcomes")





dbExecute(neds, "REINDEX INDEX index_key_ed_demos")
dbExecute(neds, "REINDEX INDEX index_key_ed_dx")
dbExecute(neds, "REINDEX INDEX index_key_ed_ecodes")
dbExecute(neds, "REINDEX INDEX index_key_ed_ip")
dbExecute(neds, "REINDEX INDEX index_key_ed_outcomes")






dbGetQuery(neds, "SELECT year, COUNT(*) FROM dx GROUP BY year")
#       year   count
# 1     2010 1303163
# 2     2011 1320362
# 3     2012 1447254
# 4     2013 1424419
# 5     2014 1506363
# 6 2015Q1Q3  972570
# 7   2015Q4  328945
# 8     2016 2041286




n = dbGetQuery(neds, "SELECT COUNT(key_ed) FROM dx")
n = as.integer(n)
cat("N_obs after filtering on ecodes = ", format(n, big.mark=","), "\n", file="nobs log.txt", append=T)










# join statement -------------------------------------------------------


dbExecute(neds, "SET work_mem = '2GB'")


#get all var types
tables = dbListTables(neds)
for(i in 1:length(tables)){
	q = paste0("SELECT * FROM ", tables[i], " LIMIT 1")
	q = dbSendQuery(neds, q)
	r = dbColumnInfo(q)
	dbClearResult(q)
	cat(paste0(r$name, " ", r$type, sep = ", \n"),"\n\n")
}
rm(tables, q, i, r)




#create table for join results to go into
dbSendQuery(neds, 
						"CREATE UNLOGGED TABLE join_res (
						age INTEGER, 
            discwt FLOAT8, 
            female INTEGER, 
          --  hosp_ed FLOAT8, 
            key_ed BIGINT, 
            pay1 INTEGER, 
            pay2 INTEGER, 
            totchg_ed FLOAT8, 
            zipinc_qrtl INTEGER, 
 
						
						
						died_visit INTEGER,
						disp_ed INTEGER,
						edevent INTEGER,
						
						dx1 TEXT,
						dx2 TEXT,
						dx3 TEXT,
						dx4 TEXT,
						dx5 TEXT,
						dx6 TEXT,
						dx7 TEXT,
						dx8 TEXT,
						dx9 TEXT,
						dx10 TEXT,
						dx11 TEXT,
						dx12 TEXT,
						dx13 TEXT,
						dx14 TEXT,
						dx15 TEXT,
						year TEXT,
						
						
						ecode1 TEXT,
						ecode2 TEXT,
						ecode3 TEXT,
						ecode4 TEXT,
						
						
						disp_ip INTEGER, 
            los_ip INTEGER, 
            npr_ip INTEGER, 
            totchg_ip INTEGER, 
 
						
						hosp_ed FLOAT8, 
            neds_stratum FLOAT8, 
            year_hosp TEXT
						)")




#get all the var names
tables = dbListTables(neds)
for(i in 1:length(tables)){
	print(tables[i])
	q = paste0("SELECT * FROM ", tables[i], " LIMIT 1")
	q = dbSendQuery(neds, q)
	r = dbColumnInfo(q)
	dbClearResult(q)
	cat(paste0(tables[i], ".", r$name, sep = ", "),"\n\n")
}
rm(tables, q, i, r)





#vars in INSERT command have to be in same order as in the creation of join_res
dbSendQuery(neds,
						"INSERT INTO join_res
						 SELECT demos.age,  demos.discwt,  demos.female,  
						 -- demos.hosp_ed,  
						 demos.key_ed,  demos.pay1,  demos.pay2,  
						 demos.totchg_ed,  demos.zipinc_qrtl,
						 
						 outcomes.died_visit,  outcomes.disp_ed,  outcomes.edevent,
						 
						 dx.dx1,  dx.dx2,  dx.dx3,  dx.dx4,  dx.dx5, 
						 dx.dx6,  dx.dx7,  dx.dx8,  dx.dx9,  dx.dx10, 
						 dx.dx11,  dx.dx12,  dx.dx13,  dx.dx14,  dx.dx15, 
						 dx.year,
						 
						 ecodes.ecode1,  ecodes.ecode2,  ecodes.ecode3,  ecodes.ecode4,
						 
						 
						 ip.disp_ip,  ip.los_ip,  ip.npr_ip,  ip.totchg_ip,
						 
						 hosp.hosp_ed,  hosp.neds_stratum,  hosp.year_hosp


						 FROM demos
						 LEFT JOIN outcomes
						 ON demos.key_ed = outcomes.key_ed

						 LEFT JOIN dx
						 ON demos.key_ed = dx.key_ed

						 LEFT JOIN ecodes
						 ON demos.key_ed = ecodes.key_ed

						 LEFT JOIN ip
						 ON demos.key_ed = ip.key_ed

						 LEFT JOIN hosp
						 ON demos.hosp_ed = hosp.hosp_ed")






dbGetQuery(neds, "SELECT year, COUNT(*) FROM join_res GROUP BY year")
# 			year   count
# 1     2010 4124638
# 2     2011 4347725
# 3     2012 4775188
# 4     2013 4634410
# 5     2014 4794694
# 6 2015Q1Q3 2866723
# 7   2015Q4  977469
# 8     2016 5890822


dbExecute(neds, "DROP INDEX IF EXISTS index_key_ed_demos")
dbExecute(neds, "DROP INDEX IF EXISTS index_key_ed_dx")
dbExecute(neds, "DROP INDEX IF EXISTS index_key_ed_ecodes")
dbExecute(neds, "DROP INDEX IF EXISTS index_key_ed_ip")
dbExecute(neds, "DROP INDEX IF EXISTS index_key_ed_outcomes")




dbExecute(neds, "VACUUM ANALYZE dx")
dbExecute(neds, "VACUUM ANALYZE demos")
dbExecute(neds, "VACUUM ANALYZE ecodes")
dbExecute(neds, "VACUUM ANALYZE ip")
dbExecute(neds, "VACUUM ANALYZE outcomes")
dbExecute(neds, "VACUUM ANALYZE join_res")













# ~ delete duplicates that were added in the join -----------------------------------------------------

dbListFields(neds, "join_res")

dbExecute(neds, "CREATE INDEX index_key_ed_join_res ON join_res (key_ed)")


dbGetQuery(neds,
					 'SELECT COUNT(*), key_ed FROM join_res
					 GROUP BY key_ed
					 ORDER BY COUNT(*) DESC LIMIT 10')



dbExecute(neds, "SET work_mem = '5GB'")



dbExecute(neds, "ALTER TABLE join_res ADD COLUMN row BIGSERIAL")



dbSendQuery(neds,
					"DELETE FROM join_res
					 WHERE row NOT IN (SELECT MIN(row) FROM join_res GROUP BY key_ed)")




dbGetQuery(neds, "SELECT year, COUNT(*) FROM join_res GROUP BY year")
# 			year   count
# 1     2010 1303163
# 2     2011 1320362
# 3     2012 1447254
# 4     2013 1424419
# 5     2014 1506363
# 6 2015Q1Q3  972570
# 7   2015Q4  328945
# 8     2016 2041286


dbGetQuery(neds,
					 'SELECT COUNT(*), key_ed FROM join_res
					 GROUP BY key_ed
					 ORDER BY COUNT(*) DESC LIMIT 10')


dbSendQuery(neds, "ALTER TABLE join_res DROP COLUMN row")










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
					 WHERE dx1 NOT IN (SELECT injury_dx FROM injury_dx)
					 GROUP BY dx1')



dbGetQuery(neds, 
					 'SELECT COUNT(*), ecode1
					 FROM join_res
					 WHERE SUBSTR(ecode1, 1, 4) IN (SELECT exclude FROM ecode_exclude)
					 GROUP BY ecode1')








n = dbGetQuery(neds, "SELECT COUNT(key_ed) FROM join_res")
n = as.integer(n)
cat("N_obs after all filters = ", format(n, big.mark=","), "\n", file="nobs log.txt", append = T)















# export dxs & calculate TMPM -------------------------------------------

dbSendQuery(neds, 
						"CREATE UNLOGGED TABLE dx_final (
						key_ed FLOAT8,

						dx1 TEXT,
						dx2 TEXT,
						dx3 TEXT,
						dx4 TEXT,
						dx5 TEXT,
						dx6 TEXT,
						dx7 TEXT,
						dx8 TEXT,
						dx9 TEXT,
						dx10 TEXT,
						dx11 TEXT,
						dx12 TEXT,
						dx13 TEXT,
						dx14 TEXT,
						dx15 TEXT
						)")


dbSendQuery(neds, 
						"INSERT INTO dx_final
						 SELECT join_res.key_ed, 
						 join_res.dx1,  join_res.dx2,  join_res.dx3,  join_res.dx4,  join_res.dx5,  
						 join_res.dx6,  join_res.dx7,  join_res.dx8,  join_res.dx9,  join_res.dx10, 
						 join_res.dx11,  join_res.dx12,  join_res.dx13,  join_res.dx14,  join_res.dx15
						 FROM join_res")



dx = dbReadTable(neds, "dx_final")
dx = as.data.table(dx)
gc()


#save dx file just in case
write_fst(dx, "Data/final/dx_final.fst")

dx = read_fst("Data/final/dx_final.fst")


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

tmpm5_old = tmpm5 = function(Pdat){
	
	
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




# mb =  microbenchmark::microbenchmark(
# 	#{tmpm(dx[1:1000, ], ILex = 9)},
# 	{tmpm2(dx[1:1000, ])},
# 	{tmpm3(dx[1:1000, ])},
# 	{tmpm4(dx[1:1000, ])},
# 	{tmpm4p(dx[1:1000, ])},
# 	{tmpm4p(dx[1:1000, ], max_combine = 1000)},
# 	{tmpm4p(dx[1:1000, ], max_combine = 500)},
# 	{tmpm5(dx[1:1000, ])},
# 	{tmpm5p(dx[1:1000, ])},
# 	{tmpm5p(dx[1:1000, ], max_combine = 1000)},
# 	{tmpm5p(dx[1:1000, ], max_combine = 500)},
# 	{tmpm5_old(dx[1:1000, ])},
# 	times = 100,
# 	control = list(warmup = 5))
# gc()
# 
# 
# mb
# ggplot2::autoplot(mb)
# 
# mb2
# ggplot2::autoplot(mb2)
# 








temp = tmpm(dx[1:1000, ], ILex = 9)$pDeath
all.equal(temp, tmpm2(dx[1:1000, ])$pDeath )  #same results
all.equal(temp, tmpm3(dx[1:1000, ])$pDeath )  #same results
all.equal(temp, tmpm4(dx[1:1000, ]) )  #same results
all.equal(temp, tmpm4p(dx[1:1000, ]) )  #same results
all.equal(temp, tmpm5(dx[1:1000, ]) )  #same results
all.equal(temp, tmpm5p(dx[1:1000, ]) )  #same results
all.equal(temp, tmpm5_old(dx[1:1000, ]) )  #same results



rm(temp, mb, tmpm2, tmpm3, tmpm4p, tmpm5, tmpm5p)









#convert everything to a factor to reduce object size
dx = dx%>% mutate_all(., factor)



#do in chunks to reduce memory pressure
#takes about 25 mins with 7 cores and chunk.size = 1000
dx$pDeath = NA_real_

chunk.size = 1000
nchunks = ceiling( nrow(dx)/chunk.size)

starts = seq(1, nrow(dx), chunk.size)
ends = c(seq(chunk.size, nrow(dx), chunk.size), nrow(dx))


# max_cores = parallel::detectCores()
# choices = c("No, don't use parallel processing",
# 						paste("Yes, use parallel processing with", 2:max_cores, "cores") )
# 
# res = menu(choices, 
# 					 title = "Use parallel processing (multiple processors) to calculate TMPM?")

res = 7

writeLines(c(""), "Data/tmpm_log.txt") #make file to monitor progress

if(res>1){
	library(foreach)
	library(doParallel)
	
	doParallel::registerDoParallel(cores = res)
	print(Sys.time())
	dx$pDeath = foreach::foreach(i = 1:nchunks, .combine=c, .multicombine = T, .maxcombine = 1000) %dopar% {
		try(cat(paste(Sys.time(), ":\t\t chunk", i, "of", nchunks, "\t\t", round( (i/nchunks)*100,2), "%\n"),
						file="Data/tmpm_log.txt", append=TRUE))
		tmpm5_old(dx[ starts[i]:ends[i], ])
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


write_fst(dx, "Data/final/tmpm.fst")

rm(dx, ICs)

gc()







# calculate comorbidity scores ----------------------------------------------


library(comorbidity)


dx = read_fst("Data/final/dx_final.fst", columns = "key_ed")


#do in chunks to reduce memory pressure
chunk.size = 100000
nchunks = ceiling( nrow(dx)/chunk.size)

starts = seq(1, nrow(dx), chunk.size)
ends = c(seq(chunk.size, nrow(dx), chunk.size), nrow(dx))

rm(dx)


if(file.exists("Data/final/comorbids.csv")){file.remove("Data/final/comorbids.csv")}

for(i in 1:nchunks){
	
	cat("\nchunk", i, "of", nchunks, sep = "\t")
	
	#import a subset
	dx = read_fst("Data/final/dx_final.fst", from = starts[i], to = ends[i])
	
	#reformat from wide to long
	dx = dx %>% pivot_longer(cols = -key_ed)
	
	
	#calculate scores
	charl = comorbidity(dx, id = "key_ed", code = "value",
											score = "charlson", icd = "icd9", 
											assign0 = TRUE) %>%
		select(key_ed, score)
	
	colnames(charl) = c("key_ed", "charlson")
	
	
	
	elix = comorbidity(dx, id = "key_ed", code = "value",
											score = "elixhauser", icd = "icd9", 
											assign0 = TRUE) %>%
		select(key_ed, score)
	
	colnames(elix) = c("key_ed", "elixhauser")
	
	
	
	
	rm(dx)
	
	
	#join the two scores
	res = left_join(charl, elix, by = "key_ed")
	
	
	#save the results by appended existing ones
	fwrite(res, "Data/final/comorbids.csv", append = TRUE)
	
	rm(res, charl, elix)
	
	cat("\tcomplete")
	
	
}



#read CSV file back in, convert to FST, and delete the CSV file
res = fread("Data/final/comorbids.csv")
write_fst(res, "Data/final/comorbids.fst")
file.remove("Data/final/comorbids.csv")
rm(res)








# export ecodes, recode, and consolidate  --------------------------------------




dbSendQuery(neds, 
						"DELETE FROM ecodes
										WHERE key_ed NOT IN (SELECT key_ed FROM join_res)")



ecodes = dbReadTable(neds, "ecodes")
ecodes = as.data.table(ecodes)








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


#remove duplicates
ecodes = ecodes[!duplicated(ecodes$key_ed), ]

ecodes = ecodes[!is.na(ecodes$key_ed), ]





#join mapping file to ecode2
colnames(icd_map) = c("ecode2", "mech2", "intent2")
ecodes = plyr::join(ecodes, icd_map,
										by = "ecode2",
										type = "left", 
										match = "first")

#remove duplicates
ecodes = ecodes[!duplicated(ecodes$key_ed), ]

ecodes = ecodes[!is.na(ecodes$key_ed), ]






#join mapping file to ecode3
colnames(icd_map) = c("ecode3", "mech3", "intent3")
ecodes = plyr::join(ecodes, icd_map,
										by = "ecode3",
										type = "left", 
										match = "first")

#remove duplicates
ecodes = ecodes[!duplicated(ecodes$key_ed), ]

ecodes = ecodes[!is.na(ecodes$key_ed), ]







#join mapping file to ecode4
colnames(icd_map) = c("ecode4", "mech4", "intent4")
ecodes = plyr::join(ecodes, icd_map,
										by = "ecode4",
										type = "left", 
										match = "first")

#remove duplicates
ecodes = ecodes[!duplicated(ecodes$key_ed), ]

ecodes = ecodes[!is.na(ecodes$key_ed), ]





write_fst(ecodes, "Data/final/ecodes_final.fst")

rm(ecodes, icd_map, dx_recode)

# disconnect from DB ------------------------------------------------------



dbDisconnect(neds)
rm(neds)
system("pg_ctl -D /usr/local/var/postgres stop")

