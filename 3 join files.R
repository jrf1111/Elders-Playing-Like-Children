library(tidyverse)
library(dplyr)
library(dbplyr)
# library(RSQLite) # starts throwing "Error: database or disk is full" after updating to R v4, use postgresql instead
library(RPostgreSQL)
library(DBI)
library(data.table)
library(fst)

#need ecodes_final, tmpm, comorbids, NEDS_DB::join_res (without dxs or ecodes)



#connect to database ----
system("pg_ctl -D /usr/local/var/postgres start")

neds = dbConnect(dbDriver("PostgreSQL"), 
								 user="jr-f", 
								 password="",
								 host="localhost",
								 port=5432,
								 dbname="neds")


dbExecute(neds, "SET work_mem = '4GB'")


dbSendQuery(neds, 
						"CREATE TABLE join_res_small AS 
						SELECT key_ed, hosp_ed, year, discwt, age, female,
						died_visit, disp_ed, edevent, disp_ip, neds_stratum
						FROM join_res")


#export from DB into R ----
mdata = dbReadTable(neds, "join_res_small")




#save a backup ----
write_fst(mdata, "Data/final/join_res_small.fst")



#disconnect from DB ----
dbDisconnect(neds)
rm(neds)
system("pg_ctl -D /usr/local/var/postgres stop")



#read in data ----
tmpm = read_fst("Data/final/tmpm.fst")
ecodes = read_fst("Data/final/ecodes_final.fst")
mdata = read_fst("Data/final/join_res_small.fst")


tmpm$key_ed = as.character(tmpm$key_ed)
ecodes$key_ed = as.character(ecodes$key_ed)
mdata$key_ed = as.character(mdata$key_ed)


#make sure there are no duplicated cases (should all return 0)
sum(duplicated(mdata$key_ed))
sum(duplicated(ecodes$key_ed))
sum(duplicated(tmpm$key_ed))



#check that all the cases are there (should all return 1)
#sum(...)/nrow(...) is much faster than mean(...)
sum(mdata$key_ed %in% tmpm$key_ed)/nrow(mdata)
sum(mdata$key_ed %in% ecodes$key_ed)/nrow(mdata)

sum(tmpm$key_ed %in% mdata$key_ed)/nrow(tmpm)
sum(tmpm$key_ed %in% ecodes$key_ed)/nrow(tmpm)

sum(ecodes$key_ed %in% tmpm$key_ed)/nrow(ecodes)
sum(ecodes$key_ed %in% mdata$key_ed)/nrow(ecodes)





#join data -----

mdata = data.table::data.table(mdata, key = "key_ed")
ecodes = data.table::data.table(ecodes, key = "key_ed")
tmpm = data.table::data.table(tmpm, key = "key_ed")


final = data.table::merge.data.table(mdata, ecodes, by = "key_ed")


final = data.table::merge.data.table(final, tmpm, by = "key_ed")



rm(mdata, ecodes, tmpm)





# a little bit of cleaning --------------------------------------

final$key_ed = as.factor(final$key_ed)

final$female = as.integer(final$female)




final$died_visit = case_when(
	final$died_visit == 0 ~ "Survived to discharge",
	final$died_visit == 1 ~ "Died in ED",
	final$died_visit == 2 ~ "Died in the hospital",
	TRUE ~ NA_character_
) %>% as.factor()



final$disp_ed = case_when(
	final$disp_ed == 1 ~ "Routine",
	final$disp_ed == 2 ~ "Transfer to short-term hospital",
	final$disp_ed == 5 ~ "Transfer other",
	final$disp_ed == 6 ~ "Home Health Care",
	final$disp_ed == 7 ~ "Against medical advice",
	final$disp_ed == 9 ~ "Admitted as inpatient",
	final$disp_ed == 20 ~ "Died in ED",
	final$disp_ed == 21 ~ "Discharged to law enforcement",
	final$disp_ed == 98 ~ "Not admitted to this hospital, destination unknown",
	final$disp_ed == 99 ~ "Not admitted to this hospital, discharged alive, destination unknown",
	TRUE ~ NA_character_
) %>% as.factor()




final$disp_ip = case_when(
	final$disp_ip == 1 ~ "Routine",
	final$disp_ip == 2 ~ "Transfer to short-term hospital",
	final$disp_ip == 5 ~ "Transfer other",
	final$disp_ip == 6 ~ "Home Health Care",
	final$disp_ip == 7 ~ "Against medical advice",
	final$disp_ip == 20 ~ "Died in hospital",
	final$disp_ip == 99 ~ "Discharged alive, destination unknown",
	TRUE ~ NA_character_
) %>% as.factor()





final$edevent = case_when(
	final$edevent == 1 ~ "Treated and released",
	final$edevent == 2 ~ "Admitted as inpatient",
	final$edevent == 3 ~ "Transferred to a short-term hospital",
	final$edevent == 9 ~ "Died in the ED",
	final$edevent == 98 ~ "Not admitted to this hospital, destination unknown",
	final$edevent == 99 ~ "Not admitted to this hospital, discharged alive, destination unknown",
	TRUE ~ NA_character_
) %>% as.factor()





final$mortality = (final$died_visit %in% "Died in ED") | (final$died_visit %in% "Died in the hospital")

final$mortality = as.integer(final$mortality)






final$mech1[final$mech1=="" | is.na(final$mech1)] = "UNSPECIFIED"







final$age_group = case_when(
	final$age %in% 50:65 ~ "50-65",
	final$age %in% 66:80 ~ "66-80",
	final$age >80 ~ "81+",
	TRUE ~ NA_character_
) %>% as.factor()







# save final dataset ----

#convert some vars to factors (to reduce object size)
final = final %>% mutate_if(is.character, as.factor)


write_fst(final, "Data/final/final combined dataset.fst")
fwrite(final, "Data/final/final combined dataset.csv")


#remove everything----
rm(list = ls())
