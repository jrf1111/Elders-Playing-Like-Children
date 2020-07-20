library(data.table)
setDTthreads(3)
library(tidyverse)
library(haven)
library(bit64)  #required for 64 bit integers used in key_ed



#convert files to more memory efficient RDS files
files = list.files("Data/", pattern = "_Hospital.dta", full.names = T)
for(i in files){
	
	print(i) #print file name
	gc() #garbage clean up
	temp = read_dta(i) #read in file
	
	name = gsub("dta", "RDS", i) #change file format for resaving
	saveRDS(temp, file = name) #save in more memory effecient file
	
	rm(temp) #remove data
	
}


#2016 file in CSV form
temp = fread("Data/NEDS_2016/NEDS_2016_HOSPITAL.csv", 
						 header = FALSE) %>%
	setnames(old = colnames(.), 
					 new = c("discwt", "hospwt", "hosp_control", "hosp_ed",
					 				"hosp_region", "hosp_trauma", "hosp_urcat4",
					 				"hosp_ur_teach", "neds_stratum", "n_disc_u",
					 				"n_hosp_u", "s_disc_u", "s_hosp_u", 
					 				"total_edvisits", "year"))

saveRDS(temp, "Data/NEDS_2016_Hospital.RDS")
rm(temp)







files = list.files("Data/", pattern = "_IP.dta", full.names = T)
for(i in files){
	
	print(i) #print file name
	gc() #garbage clean up
	temp = read_dta(i) #read in file
	
	name = gsub("dta", "RDS", i) #change file format for resaving
	saveRDS(temp, file = name) #save in more memory effecient file
	
	rm(temp) #remove data
	
}


#2016 file in CSV form
temp = fread("Data/NEDS_2016/NEDS_2016_IP.csv", 
						 header = FALSE) %>%
	setnames(old = colnames(.), 
					 new = c("hosp_ed", "key_ed", "disp_ip", 
					 				"drg", "drgver", "drg_nopoa", 
					 				"i10_npr_ip", "i10_pr_ip1", "i10_pr_ip2", 
					 				"i10_pr_ip3", "i10_pr_ip4", "i10_pr_ip5", 
					 				"i10_pr_ip6", "i10_pr_ip7", "i10_pr_ip8", 
					 				"i10_pr_ip9", "los_ip", 
					 				"mdc", "mdc_nopoa", "prver", "totchg_ip")
	)

saveRDS(temp, "Data/NEDS_2016_IP.RDS")
rm(temp)







#core data files are *much* larger :|
# don't read in all vars at once

blank_to_na = function(x){ 
	
	if(is.numeric(x)){return(x)}
	if(is.logical(x)){return(x)}
	
	if(is.factor(x)){
		x = as.character(x)
		x = trimws(x)
		x[x==""] = NA
		as.factor(x)
	}
	
	if(is.character(x)){
		x = trimws(x)
		x[x==""] = NA
		x
	}
	
	
	x = trimws(x)
	x[x==""] = NA
	x
	
}




#core 2010 -------------------------------------------------------------------------------------------------


temp = read_dta("Data/NEDS_2010_Core.dta", 
								col_select = c(
									"key_ed", 
									paste0("dx", 1:15)
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2010_Core_dx.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2010_Core.dta", 
								col_select = c(
									"key_ed", 
									"age",  "female", 
									"pay1", "pay2", "totchg_ed", 
									"zipinc_qrtl"
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)
temp$female = as.integer(temp$female)
temp$pay1 = as.integer(temp$pay1)
temp$pay2 = as.integer(temp$pay2)
temp$zipinc_qrtl = as.integer(temp$zipinc_qrtl)


saveRDS(temp, "Data/NEDS_2010_Core_demos.RDS")
rm(temp)
gc()




temp = read_dta("Data/NEDS_2010_Core.dta", 
								col_select = c(
									"key_ed", 
									"died_visit", "disp_ed", "edevent"
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$died_visit = as.integer(temp$died_visit)
temp$disp_ed = as.integer(temp$disp_ed)
temp$edevent = as.integer(temp$edevent)


saveRDS(temp, "Data/NEDS_2010_Core_outcomes.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2010_Core.dta", 
								col_select = c(
									"key_ed", 
									starts_with("ecode")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)


saveRDS(temp, "Data/NEDS_2010_Core_ecode.RDS")
rm(temp)
gc()

beepr::beep()









#core 2011 -----------------------------------------------------------------------------------------------------

temp = read_dta("Data/NEDS_2011_Core.dta", 
								col_select = c(
									"key_ed", 
									paste0("dx", 1:15)
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2011_Core_dx.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2011_Core.dta", 
								col_select = c(
									"key_ed", 
									"age",  "female", 
									"pay1", "pay2", "totchg_ed", 
									"zipinc_qrtl"
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)
temp$female = as.integer(temp$female)
temp$pay1 = as.integer(temp$pay1)
temp$pay2 = as.integer(temp$pay2)
temp$zipinc_qrtl = as.integer(temp$zipinc_qrtl)


saveRDS(temp, "Data/NEDS_2011_Core_demos.RDS")
rm(temp)
gc()




temp = read_dta("Data/NEDS_2011_Core.dta", 
								col_select = c(
									"key_ed", 
									"died_visit", "disp_ed", "edevent"
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$died_visit = as.integer(temp$died_visit)
temp$disp_ed = as.integer(temp$disp_ed)
temp$edevent = as.integer(temp$edevent)

saveRDS(temp, "Data/NEDS_2011_Core_outcomes.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2011_Core.dta", 
								col_select = c(
									"key_ed", 
									starts_with("ecode")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)


saveRDS(temp, "Data/NEDS_2011_Core_ecode.RDS")
rm(temp)
gc()


beepr::beep()















#core 2012 -----------------------------------------------------------------------------------------------------

temp = read_dta("Data/NEDS_2012_Core.dta", 
								col_select = c(
									"key_ed", 
									paste0("dx", 1:15)
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2012_Core_dx.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2012_Core.dta", 
								col_select = c(
									"key_ed", 
									"age",  "female", 
									"pay1", "pay2", "totchg_ed", 
									"zipinc_qrtl"
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)
temp$female = as.integer(temp$female)
temp$pay1 = as.integer(temp$pay1)
temp$pay2 = as.integer(temp$pay2)
temp$zipinc_qrtl = as.integer(temp$zipinc_qrtl)


saveRDS(temp, "Data/NEDS_2012_Core_demos.RDS")
rm(temp)
gc()




temp = read_dta("Data/NEDS_2012_Core.dta", 
								col_select = c(
									"key_ed", 
									"died_visit", "disp_ed", "edevent"
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$died_visit = as.integer(temp$died_visit)
temp$disp_ed = as.integer(temp$disp_ed)
temp$edevent = as.integer(temp$edevent)

saveRDS(temp, "Data/NEDS_2012_Core_outcomes.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2012_Core.dta", 
								col_select = c(
									"key_ed", 
									starts_with("ecode")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)


saveRDS(temp, "Data/NEDS_2012_Core_ecode.RDS")
rm(temp)
gc()




beepr::beep()









#core 2013 -----------------------------------------------------------------------------------------------------

temp = read_dta("Data/NEDS_2013_Core.dta", 
								col_select = c(
									"key_ed", 
									paste0("dx", 1:15)
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2013_Core_dx.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2013_Core.dta", 
								col_select = c(
									"key_ed", 
									"age",  "female", 
									"pay1", "pay2", "totchg_ed", 
									"zipinc_qrtl"
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)
temp$female = as.integer(temp$female)
temp$pay1 = as.integer(temp$pay1)
temp$pay2 = as.integer(temp$pay2)
temp$zipinc_qrtl = as.integer(temp$zipinc_qrtl)


saveRDS(temp, "Data/NEDS_2013_Core_demos.RDS")
rm(temp)
gc()




temp = read_dta("Data/NEDS_2013_Core.dta", 
								col_select = c(
									"key_ed", 
									"died_visit", "disp_ed", "edevent"
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$died_visit = as.integer(temp$died_visit)
temp$disp_ed = as.integer(temp$disp_ed)
temp$edevent = as.integer(temp$edevent)

saveRDS(temp, "Data/NEDS_2013_Core_outcomes.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2013_Core.dta", 
								col_select = c(
									"key_ed", 
									starts_with("ecode")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)


saveRDS(temp, "Data/NEDS_2013_Core_ecode.RDS")
rm(temp)
gc()



beepr::beep()















#core 2014 -----------------------------------------------------------------------------------------------------

#in 2014, there are up to 30dxs.  Only import first 15 to avoid measurement bias.

temp = read_dta("Data/NEDS_2014_Core.dta", 
								col_select = c(
									"key_ed", 
									paste0("dx", 1:15)  
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2014_Core_dx.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2014_Core.dta", 
								col_select = c(
									"key_ed", 
									"age",  "female", 
									"pay1", "pay2", "totchg_ed", 
									"zipinc_qrtl"
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)
temp$female = as.integer(temp$female)
temp$pay1 = as.integer(temp$pay1)
temp$pay2 = as.integer(temp$pay2)
temp$zipinc_qrtl = as.integer(temp$zipinc_qrtl)


saveRDS(temp, "Data/NEDS_2014_Core_demos.RDS")
rm(temp)
gc()




temp = read_dta("Data/NEDS_2014_Core.dta", 
								col_select = c(
									"key_ed", 
									"died_visit", "disp_ed", "edevent"
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$died_visit = as.integer(temp$died_visit)
temp$disp_ed = as.integer(temp$disp_ed)
temp$edevent = as.integer(temp$edevent)

saveRDS(temp, "Data/NEDS_2014_Core_outcomes.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2014_Core.dta", 
								col_select = c(
									"key_ed", 
									starts_with("ecode")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2014_Core_ecode.RDS")
rm(temp)
gc()




beepr::beep()




#2015 -----------------------------------------------------------------------------------------------------

#things get weird in 2015: 
# - dxs are in ED files not in core
# - Q1-Q3 are ICD9, Q4 is ICD10


# ~ 2015 Q1-Q3 dxs ----------------------------------------------------------------------------------------

temp2 = read_dta("Data/NEDS_2015Q1Q3_ED.dta",
								 col_select = c(
								 	c("key_ed"),
								 	paste0("dx", 1:5)
								 )
								 
)
gc()


#take care of bad/corrupt/multibyte strings


temp = temp %>% mutate_at(vars(starts_with("dx")), 
													function(x){ 
														x[which(!grepl("^(V?)(\\d{2,})$", x))] = NA
														x
													}
)



temp = temp %>% mutate_at(vars(starts_with("dx")), blank_to_na)



saveRDS(temp, "Data/NEDS_2015Q1Q3_dx.RDS")
rm(temp)
gc()




# ~ 2015 Q4 dxs ---------------------------------------------------------------------------------------------
temp = read_dta("Data/NEDS_2015Q4_ED.dta",
								col_select = c("key_ed", 
															 num_range("i10_dx", range = 1:15)
								)
)


temp = temp %>% mutate_at(vars(starts_with("i10")), blank_to_na)



#set inconsistent and invalid values to missing
temp = temp %>% mutate_at(vars(starts_with("i10")), function(x){ x[x %in% c("incn", "invl")] = NA; x } )


#convert ICD10 to ICD9
icd_map = read_csv("https://raw.githubusercontent.com/jrf1111/ICD10-to-ICD9/master/icd10toicd9gem.csv")


icd10_to_icd9 = function(x){
	new = icd_map$icd9cm[match(x, icd_map$icd10cm)] #convert to icd9
	new[which(is.na(new) & !is.na(x))] = x[which(is.na(new) & !is.na(x))] #use icd10 if there isnt a mapping
	new
}

temp = temp %>% mutate_at(vars(starts_with("i10_dx")), icd10_to_icd9)


colnames(temp) = gsub("i10_", "", colnames(temp))

saveRDS(temp, "Data/NEDS_2015Q4_dx.RDS")
rm(temp, icd_map)
gc()





# ~ 2015 Q1-Q3 ecodes ---------------------------------------------------------

temp = read_dta("Data/NEDS_2015Q1Q3_ED.dta", 
								col_select = c(
									"key_ed", 
									starts_with("ecode")
								)
)

temp = temp %>% mutate_at(vars(starts_with("ecode")), blank_to_na)


#lots of invalid bad/corrupt/multibyte strings
temp$ecode1[11633187] = NA #invalid multibyte string: "N/0\x82\001\036\004"
temp$ecode1[11633194] = NA #invalid multibyte string: "uA\xb6x\"\017w"
temp$ecode1[14034782] = NA #invalid multibyte string: "\177\xe5\177\xe5\177\xe5\177"
temp$ecode1[14034783] = NA #invalid multibyte string: "\177\xe5\177\xe5\177\xe5\177"

#take care of the rest here
temp = temp %>% mutate_at(vars(starts_with("ecode")), 
													function(x){ 
														x[which(!grepl("^(E)(\\d{2,})$", x))] = NA
														x
													})




#convert any invalid values (ones that don't start with E) to missing
temp = temp %>% mutate_at(vars(starts_with("ecode")),
													function(x){ x[which(substr(x,1,1)!="E")] = NA; x  })




#lots of blank key_ed values for some reason, plus some invalid multibyte strings


sum(temp$key_ed == "")
View(temp[temp$key_ed == "", ])
temp = temp[temp$key_ed != "", ]

length(temp$key_ed[str_detect(temp$key_ed, "\\D")])
View(temp[temp$key_ed  %in% temp$key_ed[str_detect(temp$key_ed, "\\D")], ])
temp = temp[!grepl("\\D", temp$key_ed), ]




saveRDS(temp, "Data/NEDS_2015Q1Q3_ecode.RDS")
rm(temp)
gc()





# ~ 2015 Q4 ecodes -------------------------------------------------------------



temp = read_dta("Data/NEDS_2015Q4_ED.dta", 
								col_select = c(
									"key_ed", 
									starts_with("i10_ecause")
								)
)

temp = temp %>% mutate_at(vars(starts_with("i10_ecause")), blank_to_na)


temp = temp %>% mutate_at(vars(starts_with("i10_ecause")), function(x){ x[x=="invl"]=NA; x  })




#convert to icd9
icd_map = read_csv("https://raw.githubusercontent.com/jrf1111/ICD10-to-ICD9/master/ICD10%20ecodes%20with%20ICD9%20links.csv")

#get rid of period in ecode
icd_map$icd10_ecode = gsub(".", "", icd_map$icd10_ecode, fixed = T)
icd_map$icd9_ecode = gsub(".", "", icd_map$icd9_ecode, fixed = T)


icd10_to_icd9 = function(x){
	new = icd_map$icd9_ecode[match(x, icd_map$icd10_ecode)] #convert to icd9
	new[which(is.na(new) & !is.na(x))] = x[which(is.na(new) & !is.na(x))] #use icd10 if there isnt a mapping
	new
}


temp = temp %>% mutate_at(vars(starts_with("i10_ecause")), icd10_to_icd9)

colnames(temp) = gsub("i10_ecause", "ecode", colnames(temp))




saveRDS(temp, "Data/NEDS_2015Q4_ecode.RDS")
rm(temp, icd_map)
gc()
























# ~ core 2015 ------------------------------------------------------------------
temp = read_dta("Data/NEDS_2015_Core.dta", 
								col_select = c(
									"key_ed", 
									"age",  "female", 
									"pay1", "pay2", "totchg_ed", 
									"zipinc_qrtl"
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)
temp$female = as.integer(temp$female)
temp$pay1 = as.integer(temp$pay1)
temp$pay2 = as.integer(temp$pay2)
temp$zipinc_qrtl = as.integer(temp$zipinc_qrtl)

saveRDS(temp, "Data/NEDS_2015_Core_demos.RDS")
rm(temp)
gc()




temp = read_dta("Data/NEDS_2015_Core.dta", 
								col_select = c(
									"key_ed", 
									"died_visit", "disp_ed", "edevent"
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$died_visit = as.integer(temp$died_visit)
temp$disp_ed = as.integer(temp$disp_ed)
temp$edevent = as.integer(temp$edevent)

saveRDS(temp, "Data/NEDS_2015_Core_outcomes.RDS")
rm(temp)
gc()


beepr::beep()





















# 2016 files -----------------------------------------------------------------------------------------------------

#2016 files are all CSV

# ~ core 2016 ------------------------------------------------------------


temp = fread("Data/NEDS_2016/NEDS_2016_CORE.csv",
						 select = c(
						 	"key_ed", 
						 	"age",  "female", 
						 	"pay1", "pay2", "totchg_ed", 
						 	"zipinc_qrtl"
						 ),
						 na.strings = c(getOption("datatable.na.strings","NA"), "-9", "-8")
)




temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)
temp$female = as.integer(temp$female)
temp$pay1 = as.integer(temp$pay1)
temp$pay2 = as.integer(temp$pay2)
temp$zipinc_qrtl = as.integer(temp$zipinc_qrtl)

saveRDS(temp, "Data/NEDS_2016_Core_demos.RDS")
rm(temp)
gc()







temp = fread("Data/NEDS_2016/NEDS_2016_CORE.csv",
						 select = c(
						 	"key_ed", 
						 	"died_visit", "disp_ed", "edevent"
						 ),
						 na.strings = c(getOption("datatable.na.strings","NA"), "-9", "-8")
)


temp = temp %>% mutate_if(is.character, blank_to_na)
temp$died_visit = as.integer(temp$died_visit)
temp$disp_ed = as.integer(temp$disp_ed)
temp$edevent = as.integer(temp$edevent)

saveRDS(temp, "Data/NEDS_2016_Core_outcomes.RDS")
rm(temp)
gc()








# ~~ ecodes ----

temp = fread("Data/NEDS_2016/NEDS_2016_CORE.csv",
						 select = c(
						 	"key_ed", 
						 	"i10_ecause1", "i10_ecause2", "i10_ecause3", "i10_ecause4"),
						 na.strings = c(getOption("datatable.na.strings","NA"), "", " ")
)

temp = temp %>% mutate_at(vars(starts_with("i10_ecause")), blank_to_na)


#convert to icd9
icd_map = read_csv("https://raw.githubusercontent.com/jrf1111/ICD10-to-ICD9/master/ICD10%20ecodes%20with%20ICD9%20links.csv")

#get rid of period in ecode
icd_map$icd10_ecode = gsub(".", "", icd_map$icd10_ecode, fixed = T)
icd_map$icd9_ecode = gsub(".", "", icd_map$icd9_ecode, fixed = T)


icd10_to_icd9 = function(x){
	new = icd_map$icd9_ecode[match(x, icd_map$icd10_ecode)] #convert to icd9
	new[which(is.na(new) & !is.na(x))] = x[which(is.na(new) & !is.na(x))] #use icd10 if there isnt a mapping
	new
}


temp = temp %>% mutate_at(vars(starts_with("i10_ecause")), icd10_to_icd9)

colnames(temp) = gsub("i10_ecause", "ecode", colnames(temp))


saveRDS(temp, "Data/NEDS_2016_ecode.RDS")

temp = temp %>% select(-starts_with("ecode"))


rm(icd_map, icd10_to_icd9, temp)





############################### STOPPED HERE ################################

# ~~ dxs ----

temp = fread("Data/NEDS_2016/NEDS_2016_CORE.csv",
						 select = c(
						 	"key_ed", 
						 	paste0("i10_dx", 1:15)),
						 na.strings = c(getOption("datatable.na.strings","NA"), "")
)

temp = temp %>% mutate_at(vars(starts_with("i10_dx")), blank_to_na)



#convert ICD10 to ICD9
icd_map = read_csv("https://raw.githubusercontent.com/jrf1111/ICD10-to-ICD9/master/icd10toicd9gem.csv")


icd10_to_icd9 = function(x){
	new = icd_map$icd9cm[match(x, icd_map$icd10cm)] #convert to icd9
	new[which(is.na(new) & !is.na(x))] = x[which(is.na(new) & !is.na(x))] #use icd10 if there isnt a mapping
	new
}

temp = temp %>% mutate_at(vars(starts_with("i10_dx")), icd10_to_icd9)


colnames(temp) = gsub("i10_", "", colnames(temp))



saveRDS(temp, "Data/NEDS_2016_dx.RDS")




# ~~ demos and outcomes ----

temp %>% select(c("key_ed",  "age", "female")) %>% 
	saveRDS(., "Data/NEDS_2016_demos.RDS")


temp %>% select(c("key_ed",  "died_visit", "disp_ed", "edevent")) %>% 
	saveRDS(., "Data/NEDS_2016_outcomes.RDS")



rm(temp)
gc()

beepr::beep()


# ~ ED 2016 ------------------------------------------------------------

temp = fread("Data/NEDS_2016_ED.csv", 
						 logical01 = F, keepLeadingZeros = T, stringsAsFactors = T,
						 na.strings=c(getOption("datatable.na.strings","NA"), "",
						 						 "invl", "incn2",
						 						 -1:-9, -99, -999, -100000000)
)

colnames(temp) = c("HOSP_ED", "KEY_ED", "CPT1", "CPT2", 
									 "CPT3", "CPT4", "CPT5", "CPT6", "CPT7", 
									 "CPT8", "CPT9", "CPT10", "CPT11", "CPT12",
									 "CPT13", "CPT14", "CPT15", "CPTCCS1", 
									 "CPTCCS2", "CPTCCS3", "CPTCCS4", 
									 "CPTCCS5", "CPTCCS6", "CPTCCS7", 
									 "CPTCCS8", "CPTCCS9", "CPTCCS10",
									 "CPTCCS11", "CPTCCS12", "CPTCCS13", 
									 "CPTCCS14", "CPTCCS15", "NCPT")

colnames(temp) = tolower(colnames(temp))


saveRDS(temp, "Data/NEDS_2016_ED.RDS")
rm(temp)
gc()
