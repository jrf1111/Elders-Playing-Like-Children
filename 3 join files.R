library(tidyverse)
library(dplyr)
library(dbplyr)
library(RSQLite)
library(DBI)
library(data.table)


#need ecodes_final.RDS, tmpm.RDS, NEDS_DB::join_res (without dxs or ecodes)



#connect to database ----
neds = dbConnect(RSQLite::SQLite(), "Data/NEDS_DB.sqlite")

dbSendQuery(neds, 
						"CREATE TABLE join_res_small AS 
						SELECT key_ed, hosp_ed, year, discwt, age, female,
						died_visit, disp_ed, edevent, hcupfile, disp_ip, neds_stratum
						FROM join_res")


#don't need full join_res table anymore
dbSendQuery(neds, "DROP TABLE join_res")


#export from DB into R
mdata = dbReadTable(neds, "join_res_small")

#convert some vars to factors (to reduce object size)
mdata = mdata %>% mutate_at(vars(key_ed, hosp_ed, year,
												 female, died_visit, disp_ed, edevent,
												 disp_ip, hcupfile, neds_stratum), factor)


#save a backup
saveRDS(mdata, "Data/join_res_small.RDS")


#disconnect from DB and optionally delete DB file (around 45-50GBs)
dbDisconnect(neds)
rm(neds)


res = menu(c("YES, I want to IMMEDIATELY DELETE the database", 
						 "NO, do NOT delete the database"), 
					 title = "Delete `Data/NEDS_DB.sqlite` **immediately**? It will **NOT** go to the trash can.")
if(res == 1){file.remove("Data/NEDS_DB.sqlite")}; rm(res)






tmpm = read_rds("Data/tmpm.RDS")
ecodes = read_rds("Data/ecodes_final.RDS")




mdata$dup = duplicated( paste0( mdata$year, mdata$hosp_ed, mdata$key_ed))
ecodes$dup = duplicated( paste0( ecodes$year, ecodes$hosp_ed, ecodes$key_ed))
tmpm$dup = duplicated( paste0( tmpm$year, tmpm$hosp_ed, tmpm$key_ed))


mdata %>% group_by(year) %>%
	summarise(dup_perc = mean(dup))

ecodes %>% summarise(dup_perc = mean(dup)) 

tmpm %>% summarise(dup_perc = mean(dup)) 




#check that all the cases are there (should all return 1)
mean(mdata$key_ed %in% tmpm$key_ed)
mean(mdata$key_ed %in% ecodes$key_ed)

mean(tmpm$key_ed %in% mdata$key_ed)
mean(tmpm$key_ed %in% ecodes$key_ed)

mean(ecodes$key_ed %in% tmpm$key_ed)
mean(ecodes$key_ed %in% mdata$key_ed)









