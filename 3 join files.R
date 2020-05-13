library(tidyverse)
library(dplyr)
library(dbplyr)
library(RSQLite)
library(DBI)
library(data.table)


#need ecodes_final.RDS, tmpm.RDS, NEDS_DB::join_res (without dxs or ecodes)



#connect to database ----
neds = dbConnect(dbDriver("PostgreSQL"), 
								 user="jr-f", 
								 password="",
								 host="localhost",
								 port=5432,
								 dbname="neds")



dbSendQuery(neds, 
						"CREATE TABLE join_res_small AS 
						SELECT key_ed, hosp_ed, year, discwt, age, female,
						died_visit, disp_ed, edevent, disp_ip, neds_stratum
						FROM join_res")


#export from DB into R ----
mdata = dbReadTable(neds, "join_res_small")

#convert some vars to factors (to reduce object size)
mdata = mdata %>% mutate_at(vars(key_ed, hosp_ed, year,
																 female, died_visit, disp_ed, edevent,
																 disp_ip, neds_stratum), factor)



#save a backup ----
saveRDS(mdata, "Data/final/join_res_small.RDS")



#disconnect from DB ----
dbDisconnect(neds)
rm(neds)



#read in data ----
tmpm = readRDS("Data/final/tmpm.RDS")
ecodes = readRDS("Data/final/ecodes_final.RDS")
mdata = readRDS("Data/final/join_res_small.RDS")


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

mdata = as.data.table(mdata)
ecodes = as.data.table(ecodes)
tmpm = as.data.table(tmpm)


final = merge(mdata, ecodes, 
							by = "key_ed")


final = merge(final, tmpm, 
							by = "key_ed")


#hosp_ed gets duplicated in the merge.  Remove it here.
final$hosp_ed.y = NULL
colnames(final) = str_replace_all(colnames(final), fixed(".x"), "")



# save final dataset ----

#convert some vars to factors (to reduce object size)
final = final %>% mutate_if(is.character, as.factor)


saveRDS(final, "Data/final/final combined dataset.RDS")



#remove everything----
rm(list = ls())
