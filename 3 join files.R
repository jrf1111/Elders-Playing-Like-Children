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



#Per NEDS documentation at https://www.hcup-us.ahrq.gov/db/vars/totchg_ip/nedsnote.jsp
#   "TOTCHG_IP contains the edited total charge for the inpatient stay, including the emergency department charges"
#recode totchg_ip to only be in-patient charges


final$totchg_ed = as.integer(final$totchg_ed)
final$totchg_ed[which(final$totchg_ed < 0)] = NA


final$totchg = as.integer(final$totchg_ip)
final$totchg[which(final$totchg < 0)] = NA


final$totchg_ip = as.integer(final$totchg - final$totchg_ed)
final$totchg_ip[which(final$totchg_ip<0)] = NA




final$los_ip = as.integer(final$los_ip)
final$los_ip[which(final$los_ip<0)] = NA




final$npr_ip = as.integer(final$npr_ip)



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







#add definition for high risk vs. not -----
high_risk_codes = readxl::read_excel("high-risk_ecodes.xlsx",
																		 sheet = "ICD-9"
)

#remove periods
high_risk_codes$ecode = str_remove_all(high_risk_codes$ecode, 
																			 fixed("."))


final$high_risk = final$ecode1 %in% high_risk_codes$ecode

final$high_risk[final$high_risk == TRUE] = "High-risk"
final$high_risk[final$high_risk == "FALSE"] = "Non-high-risk"

rm(high_risk_codes)


#add ecode descriptions and groups -----


#to standardize the length of the strings
dx_recode = function(dx){
	stringr::str_pad(dx, width=5, side = "right", pad = "0")
}




ecodes = read_csv("https://raw.githubusercontent.com/jrf1111/ICD9/master/icd9_ecodes_with_groups.csv")



#reformat to match ecodes file
ecodes$ecode = ecodes$ecode %>% 
	as.character() %>%  #convert to character
	gsub(".", "", ., fixed = T) %>% #remove periods
	dx_recode() #standardize the length of the strings





#join mapping file to ecode1
colnames(ecodes) = c("ecode1", "ecode1_desc", "ecode1_group2_code", "ecode1_group2_name",
										 "ecode1_group1_code", "ecode1_group1_name")

final = plyr::join(final, ecodes,
									 by = "ecode1",
									 type = "left", 
									 match = "first")

rm(ecodes)









final$trauma_type = case_when(
	final$mech1 == "CUT_PIERCE" ~ "Penetrating",
	final$mech1 == "FIREARM" ~ "Penetrating",
	final$mech1 == "MACHINERY" ~ "Blunt",
	str_detect(final$mech1, "MOTOR_VEHICLE_") ~ "Blunt",
	final$mech1 == "PEDAL_CYCLIST_OTHER" ~ "Blunt",
	final$mech1 == "PEDESTRIAN_OTHER" ~ "Blunt",
	final$mech1 == "TRANSPORT_OTHER" ~ "Blunt",
	final$mech1 == "STRUCK_BY_AGAINST" ~ "Blunt",
	final$mech1 == "FALL" ~ "Blunt",
	TRUE ~ "Other"
)



#pull out trauma center level -----
#from https://www.hcup-us.ahrq.gov/db/vars/neds_stratum/nedsnote.jsp
#NEDS Stratum: 2nd digit – Trauma: 
#(0) Not a trauma center,
#(1) Trauma center level I, 
#(2) Trauma center level II,
#(3) Trauma center level III.
#Collapsed categories used for strata with small sample sizes: 
#(4) Nontrauma center or trauma center level III, 
#(8) Trauma center level I or II. 


final$hosp_trauma_level = case_when(
	substring(final$neds_stratum, 2, 2) == 1 ~ "Trauma center level I",
	substring(final$neds_stratum, 2, 2) == 2 ~ "Trauma center level II",
	substring(final$neds_stratum, 2, 2) == 3 ~ "Trauma center level III",
	TRUE ~ "All Other Hospitals") %>% 
	factor(., 
				 levels = c("Trauma center level I", 
				 					 "Trauma center level II",
				 					 "Trauma center level III",
				 					 "All Other Hospitals"),
				 ordered = TRUE)







#add injury dx flags ----

dx = read_fst("Data/final/dx_final.fst")
source("icd9_to_Barell.R")

if(all.equal(as.integer64(final$key_ed), as.integer64(dx$key_ed))){
	
	
	# Brain Injury
	final$injury_brain = FALSE
	# Skull Fracture
	final$injury_skull_fx = FALSE
	# Cervical Spine Fracture
	final$injury_cspine = FALSE
	# Rib or Sternum Fracture
	final$injury_rib_fx = FALSE
	# Cardiac or Pulmonary Injury:
	final$injury_cardio_pulm = FALSE
	# Thoracic Spine Fracture
	final$injury_tspine = FALSE
	# Lumbar Spine Fracture
	final$injury_lspine = FALSE
	# Solid Abdominal Organ Injury
	final$injury_solid_abd = FALSE
	# Hollow Viscera Injury
	final$injury_hollow_abd = FALSE
	# Upper Extremity Fracture
	final$injury_ue_fx = FALSE
	# Pelvis Fracture
	final$injury_pelvic_fx = FALSE
	# Lower Extremity Fracture
	final$injury_le_fx = FALSE
	
	
	
	
	
	dx_nums = paste0("dx", 1:15)
	
	for(i in 1:length(dx_nums)){
		
		dx_num = dx_nums[i]
		
		temp = dx[, dx_num]
		# DX13: 3 digit code for diagnosis
		DX13 = substr(temp, 1, 3) %>% as.numeric()
		
		# DX14: 4 digit code for diagnosis
		DX14 = substr(temp, 1, 4) %>% as.numeric()
		
		# DX15: 5 digit code for diagnosis
		DX15 = substr(temp, 1, 5) %>% as.numeric()
		
		# D5:		5th digit of diagnosis code
		D5 = substr(temp, 5, 5) %>% as.numeric()
		
		#any fracture
		fx = dplyr::between(DX13, 800, 829)
		
		
		# Brain Injury: 850-854
		final$injury_brain = final$injury_brain | temp %in% as.character(85000:85499)
		
		# Skull Fracture: 800-804
		final$injury_skull_fx = final$injury_skull_fx | temp %in% as.character(80000:80499)
		
		
		# Cervical Spine Fracture
		final$injury_cspine = final$injury_cspine | 
			#CERVICAL SCI
			dplyr::between(DX14, 8060, 8061) | (DX14 == 9520) | 
			#CERVICAL VCI
			dplyr::between(DX14, 8050, 8051) | dplyr::between(DX14, 8390, 8391) | DX14 == 8470
		
		
		# Rib or Sternum Fracture
		final$injury_rib_fx = final$injury_rib_fx | temp %in% as.character(80700:80740)
		
		# Cardiac or Pulmonary Injury: 860-861
		final$injury_cardio_pulm = final$injury_cardio_pulm | temp %in% as.character(86000:86199)
		
		# Thoracic Spine Fracture
		final$injury_tspine = final$injury_tspine | 
			#THORACIC/DORSAL VCI
			dplyr::between(DX14, 8052, 8053) | (83921 == DX15 | 83931 == DX15) | DX14 == 8471 |
			#THORACIC/DORSAL SCI
			dplyr::between(DX14, 8062, 8063) | (DX14 == 9521)
		
		
		
		
		# Lumbar Spine Fracture
		final$injury_lspine = final$injury_lspine | 
			#LUMBAR VCI 
			dplyr::between(DX14, 8054, 8055) | (83920 == DX15 | 83930 == DX15) | DX14 == 8472 |
			#LUMBAR SCI
			dplyr::between(DX14, 8064, 8065) | (DX14 == 9522)
		
		
		
		# Solid Abdominal Organ Injury
		final$injury_solid_abd = final$injury_solid_abd | 
			temp %in% as.character(c( 86400:86699, #liver, spleen, kidneys
																86381:86384, #pancreas
																86391:86394  #pancreas
			))
		
		# Hollow Viscera Injury
		final$injury_hollow_abd = final$injury_hollow_abd | 
			temp %in% as.character(c( 86300:86380, #stomach, intestines
																86389, 86390, 86399, #other GI
																86700:86799 #bladder, ureter, urethra
			))
		
		
		
		
		# Upper Extremity Fracture
		final$injury_ue_fx = final$injury_ue_fx | 
			fx & 
			(	dplyr::between(DX13, 810, 812) | DX13 == 831 | DX13 == 840 | 
					DX13 == 880 | dplyr::between(DX14, 8872, 8873) | (DX13 == 943 & dplyr::between(D5, 3, 6)) |
					DX13 == 912 | DX14 == 9230 | DX14 == 9270 | DX14 == 9592  |
					
					DX13 == 813 | DX13 == 832 | DX13 == 841 | (DX13 == 881 & dplyr::between(D5, 0, 1) ) | 
					dplyr::between(DX14, 8870, 8871) | DX14 == 9231 | DX14 == 9271 | 
					(DX13 == 943 & dplyr::between(D5, 1, 2)) |
					
					dplyr::between(DX13, 814, 817) | dplyr::between(DX13, 833, 834) | 
					DX13 == 842 | (DX13 == 881 & D5 == 2) | dplyr::between(DX13, 882, 883) | 
					dplyr::between(DX13, 885, 886) | dplyr::between(DX13, 914, 915) | 
					dplyr::between(DX14, 9232, 9233) | dplyr::between(DX14, 9272, 9273) | 
					DX13 == 944 | dplyr::between(DX14, 9594, 9595) |
					
					DX13 == 818 | DX13 == 884 | dplyr::between(DX14, 8874, 8877) | DX13 == 903 |
					DX13 == 913 | DX14 == 9593 | dplyr::between(DX14, 9238, 9239) | 
					dplyr::between(DX14, 9278, 9279) | DX14 == 9534 | DX13 == 955 |
					(DX13 == 943 & (D5 == 0 | D5 == 9)) 
			)		
		
		
		
		
		
		# Pelvis Fracture: 808
		final$injury_pelvic_fx = final$injury_pelvic_fx | temp %in% as.character(80800:80899)
		
		# Lower Extremity Fracture
		final$injury_le_fx = final$injury_le_fx | 
			fx & (
				DX13 == 820 | DX13 == 835 | DX13 == 843 | DX15 == 92401 | DX15 == 92801  |
					
					DX13 == 821 | dplyr::between(DX14, 8972, 8973) | DX15 == 92400 | 
					DX15 == 92800 | (DX13 == 945 & D5 == 6) |
					
					DX13 == 822 | DX13 == 836 | dplyr::between(DX14, 8440, 8443) | 
					DX15 == 92411 | DX15 == 92811 | (DX13 == 945 & D5 == 5)  |
					
					dplyr::between(DX13, 823, 824) | dplyr::between(DX14, 8970, 8971) | 
					DX13 == 837 | DX14 == 8450 | DX15 == 92410 | DX15 == 92421 | 
					DX15 == 92810 | DX15 == 92821 | (DX13 == 945 & dplyr::between(D5, 3, 4))  |
					
					dplyr::between(DX13, 825, 826) | DX13 == 838 | DX14 == 8451 | 
					dplyr::between(DX13, 892, 893) | dplyr::between(DX13, 895, 896) | 
					DX13 == 917 | DX15 == 92420 | DX14 == 9243 | DX15 == 92820 | 
					DX14 == 9283 | (DX13 == 945 & dplyr::between(D5, 1, 2) ) |
					
					DX13 == 827 | dplyr::between(DX14, 8448, 8449) | dplyr::between(DX13, 890, 891) |
					DX13 == 894 | dplyr::between(DX14, 8974, 8977) | dplyr::between(DX14, 9040, 9048) | 
					DX13 == 916 | dplyr::between(DX14, 9244, 9245) | DX14 == 9288 | DX14 == 9289 | 
					dplyr::between(DX14, 9596, 9597) | (DX13 == 945 & (D5 == 0 | D5 == 9)) 
			)
		
		gc()
		
	}
	
	rm(dx, temp, fx, DX13, DX14, DX15, D5)
}



# a little more recoding ----
final$year_whole = final$year %>% substr(., 1, 4) %>% as.integer()

final$pay1_recode = case_when(
	final$pay1 == "Medicare" | final$pay1 == "Medicaid" ~ "Medicare/Medicaid",
	TRUE ~ as.character(final$pay1)
) %>% as.factor()






final$disp_ip_recode = as.character(final$disp_ip)

final$disp_ip_recode = case_when(
	final$disp_ip == "Home Health Care"  ~  "Home with/without services", 
	final$disp_ip == "Routine"  ~  "Home with/without services", 
	TRUE ~ as.character(final$disp_ip_recode)
)








final %>% select_if(is.numeric) %>% summary()


final = final %>% mutate_if(is.character, as.factor)

final %>% select_if(is.factor) %>% summary()







# remove cases with missing data -----
final = filter(final, !is.na(mortality))
final = filter(final, !is.na(totchg))


n = nrow(final)
n = as.integer(n)
cat("N_obs removing missing mortality, total charges = ", 
		format(n, big.mark=","), "\n", file = "nobs log.txt", 
		append = TRUE)








# save final dataset ----
write_fst(final, "Data/final/final combined dataset.fst")



#remove everything----
rm(list = ls())
