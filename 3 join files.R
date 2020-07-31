library(tidyverse)
library(dplyr)
library(dbplyr)
# library(RSQLite) # starts throwing "Error: database or disk is full" after updating to R v4, use postgresql instead
library(RPostgreSQL)
library(DBI)
library(data.table)
library(bit64)  #required for 64 bit integers used in key_ed
library(fst)

#need ecodes_final, tmpm, comorbids, dx_final (for number of dxs), NEDS_DB::join_res (without dxs or ecodes)



#connect to database ----
system("pg_ctl -D /usr/local/var/postgres start")

neds = dbConnect(dbDriver("PostgreSQL"), 
								 user="jr-f", 
								 password="",
								 host="localhost",
								 port=5432,
								 dbname="neds")


dbExecute(neds, "SET work_mem = '4GB'")



#export from DB into R ----
mdata = dbGetQuery(neds, 
									 "SELECT key_ed, hosp_ed, year, year_hosp, discwt, neds_stratum,
										age, female, zipinc_qrtl, pay1, pay2, 
										died_visit, disp_ed, edevent, disp_ip, los_ip,
									  npr_ip, totchg_ed, totchg_ip
										FROM join_res")




#save a backup ----
write_fst(mdata, "Data/final/join_res_small.fst")



#disconnect from DB ----
dbDisconnect(neds)
rm(neds)
system("pg_ctl -D /usr/local/var/postgres stop")



#read in data ----
mdata = read_fst("Data/final/join_res_small.fst")
tmpm = read_fst("Data/final/tmpm.fst")
ecodes = read_fst("Data/final/ecodes_final.fst")
comorbids = read_fst("Data/final/comorbids.fst")
dx = read_fst("Data/final/dx_final.fst")

#only need number of dxs
dx$ndx = rowSums( !is.na(dx[, paste0("dx", 1:15)]) )
dx = select(dx, key_ed, ndx)
dx$ndx = as.integer(dx$ndx)




#make sure key_eds are all in the same format
#and that there are no duplicated values (should all return TRUE)
tmpm$key_ed = as.integer64(tmpm$key_ed)
n_distinct(tmpm$key_ed) == nrow(tmpm)

ecodes$key_ed = as.integer64(ecodes$key_ed)
n_distinct(ecodes$key_ed) == nrow(ecodes)

comorbids$key_ed = as.integer64(comorbids$key_ed)
n_distinct(comorbids$key_ed) == nrow(comorbids)

dx$key_ed = as.integer64(dx$key_ed)
n_distinct(dx$key_ed) == nrow(dx)

mdata$key_ed = as.integer64(mdata$key_ed)
n_distinct(mdata$key_ed) == nrow(mdata)






#make sure there are no duplicated cases (should all return 0)
sum(duplicated(mdata$key_ed))
sum(duplicated(ecodes$key_ed))
sum(duplicated(tmpm$key_ed))
sum(duplicated(dx$key_ed))
sum(duplicated(comorbids$key_ed))



#check that all the cases are there (should all return 1)
#sum(...)/nrow(...) is much faster than mean(...)
sum(mdata$key_ed %in% tmpm$key_ed)/nrow(mdata)
sum(mdata$key_ed %in% ecodes$key_ed)/nrow(mdata)
sum(mdata$key_ed %in% dx$key_ed)/nrow(mdata)
sum(mdata$key_ed %in% comorbids$key_ed)/nrow(mdata)


sum(tmpm$key_ed %in% mdata$key_ed)/nrow(tmpm)
sum(tmpm$key_ed %in% ecodes$key_ed)/nrow(tmpm)
sum(tmpm$key_ed %in% dx$key_ed)/nrow(tmpm)
sum(tmpm$key_ed %in% comorbids$key_ed)/nrow(tmpm)


sum(ecodes$key_ed %in% tmpm$key_ed)/nrow(ecodes)
sum(ecodes$key_ed %in% mdata$key_ed)/nrow(ecodes)
sum(ecodes$key_ed %in% dx$key_ed)/nrow(ecodes)
sum(ecodes$key_ed %in% comorbids$key_ed)/nrow(ecodes)


sum(dx$key_ed %in% tmpm$key_ed)/nrow(dx)
sum(dx$key_ed %in% mdata$key_ed)/nrow(dx)
sum(dx$key_ed %in% ecodes$key_ed)/nrow(dx)
sum(dx$key_ed %in% comorbids$key_ed)/nrow(dx)


sum(comorbids$key_ed %in% tmpm$key_ed)/nrow(comorbids)
sum(comorbids$key_ed %in% mdata$key_ed)/nrow(comorbids)
sum(comorbids$key_ed %in% ecodes$key_ed)/nrow(comorbids)
sum(comorbids$key_ed %in% dx$key_ed)/nrow(comorbids)






#join data -----

mdata = data.table::data.table(mdata, key = "key_ed")
ecodes = data.table::data.table(ecodes, key = "key_ed")
tmpm = data.table::data.table(tmpm, key = "key_ed")
comorbids = data.table::data.table(comorbids, key = "key_ed")
dx = data.table::data.table(dx, key = "key_ed")


final = data.table::merge.data.table(mdata, ecodes, by = "key_ed")
rm(mdata, ecodes)

final = data.table::merge.data.table(final, tmpm, by = "key_ed")
rm(tmpm)

final = data.table::merge.data.table(final, comorbids, by = "key_ed")
rm(comorbids)

final = data.table::merge.data.table(final, dx, by = "key_ed")
rm(dx)

gc()




# a little bit of cleaning --------------------------------------


final$female[which(final$female<0)] = NA


final$totchg_ed = as.integer(final$totchg_ed)
final$totchg_ed[which(final$totchg_ed<0)] = NA

final$totchg_ip = as.integer(final$totchg_ip)
final$totchg_ip[which(final$totchg_ip<0)] = NA


#use L after zero to use integer form of zero instead of double
final$totchg = case_when(
	!is.na(final$totchg_ed) & !is.na(final$totchg_ip) ~ final$totchg_ed + final$totchg_ip,
	!is.na(final$totchg_ed) & is.na(final$totchg_ip) ~ final$totchg_ed + 0L,
	is.na(final$totchg_ed) & !is.na(final$totchg_ip) ~ 0L + final$totchg_ip,
	TRUE ~ NA_integer_
)



final$los_ip = as.integer(final$los_ip)
final$los_ip[which(final$los_ip<0)] = NA




final$npr_ip = as.integer(final$npr_ip)



final %>% select_if(is.numeric) %>% summary()





final$zipinc_qrtl = factor(final$zipinc_qrtl,
													 levels = 1:4, 
													 ordered = T)




final$pay1 = case_when(
	final$pay1 == 1 ~ "Medicare",
	final$pay1 == 2 ~ "Medicaid",
	final$pay1 == 3 ~ "Private insurance",
	final$pay1 == 4 ~ "Self-pay",
	final$pay1 == 5 ~ "No charge",
	final$pay1 == 6 ~ "Other",
	TRUE ~ "Missing/Unknown"
) %>% as.factor()



final$pay2 = case_when(
	final$pay2 == 1 ~ "Medicare",
	final$pay2 == 2 ~ "Medicaid",
	final$pay2 == 3 ~ "Private insurance",
	final$pay2 == 4 ~ "Self-pay",
	final$pay2 == 5 ~ "No charge",
	final$pay2 == 6 ~ "Other",
	TRUE ~ "Missing/Unknown"
) %>% as.factor()





final$died_visit = case_when(
	final$died_visit == 0 ~ "Survived to discharge",
	final$died_visit == 1 ~ "Died in ED",
	final$died_visit == 2 ~ "Died in the hospital",
	TRUE ~ "Missing/Unknown"
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
	TRUE ~ "Missing/Unknown"
) %>% as.factor()




final$disp_ip = case_when(
	final$disp_ip == 1 ~ "Routine",
	final$disp_ip == 2 ~ "Transfer to short-term hospital",
	final$disp_ip == 5 ~ "Transfer other",
	final$disp_ip == 6 ~ "Home Health Care",
	final$disp_ip == 7 ~ "Against medical advice",
	final$disp_ip == 20 ~ "Died in hospital",
	final$disp_ip == 99 ~ "Discharged alive, destination unknown",
	TRUE ~ "Missing/Unknown"
) %>% as.factor()





final$edevent = case_when(
	final$edevent == 1 ~ "Treated and released",
	final$edevent == 2 ~ "Admitted as inpatient",
	final$edevent == 3 ~ "Transferred to a short-term hospital",
	final$edevent == 9 ~ "Died in the ED",
	final$edevent == 98 ~ "Not admitted to this hospital, destination unknown",
	final$edevent == 99 ~ "Not admitted to this hospital, discharged alive, destination unknown",
	TRUE ~ "Missing/Unknown"
) %>% as.factor()





final$mortality = (final$died_visit == "Died in ED") | (final$died_visit == "Died in the hospital")

final$mortality = as.integer(final$mortality)

final$mortality[final$died_visit == "Missing/Unknown"] = NA

count(final, died_visit, mortality)




final$mech1[final$mech1=="" | is.na(final$mech1)] = "UNSPECIFIED"







final$age_group = case_when(
	final$age %in% 50:65 ~ "50-65",
	final$age %in% 66:80 ~ "66-80",
	final$age >80 ~ "81+",
	TRUE ~ "Missing/Unknown"
) %>% as.factor()





final = final %>% mutate_if(is.character, as.factor)

final %>% select_if(is.factor) %>% summary()







# save final dataset ----
write_fst(final, "Data/final/final combined dataset.fst")



#remove everything----
rm(list = ls())
