library(data.table)
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



#################################### STOPPED HERE ####################################


#2015 -----------------------------------------------------------------------------------------------------

#things get weird in 2015: 
# - dxs are in ED files not in core
# - Q1-Q3 are ICD9, Q4 is ICD10


# ~ 2015 Q1-Q3 dxs ----------------------------------------------------------------------------------------
temp = read_dta("Data/NEDS_2015_Core.dta", 
								col_select = c("key_ed")
								)

gc()
temp$year = NULL


temp2 = read_dta("Data/NEDS_2015Q1Q3_ED.dta",
								 col_select = c(
								 	c("key_ed"),
								 	paste0("dx", 1:5)
								 )
)
gc()


#make sure ID columns are the same class in both files
temp$key_ed = as.character(temp$key_ed)
temp2$key_ed = as.character(temp2$key_ed)

temp$hosp_ed = as.character(temp$hosp_ed)
temp2$hosp_ed = as.character(temp2$hosp_ed)


#make keyed columns
temp = data.table(temp, key = c("key_ed"))
temp2 = data.table(temp2, key = c("key_ed"))


#add dxs to temp from temp2, then remove from temp2
#a merge would be easier/cleaner, but this is more memory effecient
temp[temp2, dx1:=dx1]; temp2$dx1=NULL
temp[temp2, dx2:=dx2]; temp2$dx2=NULL
temp[temp2, dx3:=dx3]; temp2$dx3=NULL
temp[temp2, dx4:=dx4]; temp2$dx4=NULL
temp[temp2, dx5:=dx5]; temp2$dx5=NULL


rm(temp2)
gc()






temp2 = read_dta("Data/NEDS_2015Q1Q3_ED.dta",
								 col_select = c(
								 	c("key_ed"),
								 	paste0("dx", 6:10)
								 )
)
gc()


#make sure ID columns are the same class in both files
temp2$key_ed = as.character(temp2$key_ed)
temp2$hosp_ed = as.character(temp2$hosp_ed)


#make keyed columns
temp = data.table(temp, key = c("key_ed"))
temp2 = data.table(temp2, key = c("key_ed"))


#add dxs to temp from temp2, then remove from temp2
temp[temp2, dx6:=dx6]; temp2$dx6=NULL
temp[temp2, dx7:=dx7]; temp2$dx7=NULL
temp[temp2, dx8:=dx8]; temp2$dx8=NULL
temp[temp2, dx9:=dx9]; temp2$dx9=NULL
temp[temp2, dx10:=dx10]; temp2$dx10=NULL


rm(temp2)
gc()





temp2 = read_dta("Data/NEDS_2015Q1Q3_ED.dta",
								 col_select = c(
								 	c("key_ed"),
								 	paste0("dx", 11:15)
								 )
)
gc()


#make sure ID columns are the same class in both files
temp2$key_ed = as.character(temp2$key_ed)
temp2$hosp_ed = as.character(temp2$hosp_ed)


#make keyed columns
temp = data.table(temp, key = c("key_ed"))
temp2 = data.table(temp2, key = c("key_ed"))


#add dxs to temp from temp2, then remove from temp2
temp[temp2, dx11:=dx11]; temp2$dx11=NULL
temp[temp2, dx12:=dx12]; temp2$dx12=NULL
temp[temp2, dx13:=dx13]; temp2$dx13=NULL
temp[temp2, dx14:=dx14]; temp2$dx14=NULL
temp[temp2, dx15:=dx15]; temp2$dx15=NULL


rm(temp2)
gc()



temp = temp %>% mutate_at(vars(starts_with("dx")), blank_to_na)
temp = temp %>% mutate_at(vars(starts_with("dx")), as.factor)


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


#lots of invalid multibyte strings
temp$ecode1[11633187] = NA #invalid multibyte string: "N/0\x82\001\036\004"
temp$ecode1[11633194] = NA #invalid multibyte string: "uA\xb6x\"\017w"
temp$ecode1[14034782] = NA #invalid multibyte string: "\177\xe5\177\xe5\177\xe5\177"
temp$ecode1[14034783] = NA #invalid multibyte string: "\177\xe5\177\xe5\177\xe5\177"

#take care of the rest here
temp$ecode1[grepl("\\W", temp$ecode1)] = NA
temp$ecode2[grepl("\\W", temp$ecode2)] = NA
temp$ecode3[grepl("\\W", temp$ecode3)] = NA
temp$ecode4[grepl("\\W", temp$ecode4)] = NA



#convert any invalid values (ones that don't start with E) to missing
temp = temp %>% mutate_at(vars(starts_with("ecode")),
													function(x){ x[which(substr(x,1,1)!="E")] = NA; x  })



#also lots of issues with hosp_ed
temp$hosp_ed[grepl("\\W", temp$hosp_ed)] = NA
temp$hosp_ed = as.numeric(temp$hosp_ed)
temp$hosp_ed[which(nchar(temp$hosp_ed) != 5)] = NA
temp = temp %>% fill(hosp_ed)
temp$hosp_ed = as.character(temp$hosp_ed)


#lots of blank key_ed values for some reason, plus some invalid multibyte strings
temp = temp[temp$key_ed != "", ]
temp = temp[!grepl("\\W", temp$key_ed), ]
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



temp = fread("Data/NEDS_2016//NEDS_2016_CORE 49 55 1 thru 10.csv",
						 stringsAsFactors = T) %>% data.table(., key = "key_ed")

temp = temp %>% select(-c(aweekend, dqtr, dxver, amonth))



temp2 = fread("Data/NEDS_2016//NEDS_2016_CORE 49 55 11 thru 20.csv",
						 stringsAsFactors = T) %>% data.table(., key = "key_ed")

temp2 = temp2 %>% select(key_ed, hosp_ed, num_range("i10_dx", 1:8))

temp = data.table::merge.data.table(temp, temp2)
rm(temp2)



temp2 = fread("Data/NEDS_2016//NEDS_2016_CORE 49 55 21 thru 30.csv",
							stringsAsFactors = T) %>% data.table(., key = "key_ed")

temp2 = temp2 %>% select(key_ed, num_range("i10_dx", 9:15))

temp = data.table::merge.data.table(temp, temp2)
rm(temp2)
gc()




#"NEDS_2016_CORE 49 55 31 thru 40.csv" has dxs 19-28, which aren't needed



temp2 = fread("Data/NEDS_2016//NEDS_2016_CORE 49 55 41 thru 50.csv",
							stringsAsFactors = T) %>% data.table(., key = "key_ed")

temp2 = temp2 %>% select(key_ed, i10_ecause1)

temp = data.table::merge.data.table(temp, temp2)
rm(temp2)
gc()




saveRDS(temp, "Data/NEDS_2016_CORE.RDS")
gc()





# ~~ ecodes ----
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

saveRDS(temp[, c("key_ed",  "ecode1")], "Data/NEDS_2016_ecode.RDS")

temp = temp %>% select(-starts_with("ecode"))



rm(icd_map, icd10_to_icd9)





# ~~ dxs ----
#convert ICD10 to ICD9
icd_map = read_csv("https://raw.githubusercontent.com/jrf1111/ICD10-to-ICD9/master/icd10toicd9gem.csv")


icd10_to_icd9 = function(x){
	new = icd_map$icd9cm[match(x, icd_map$icd10cm)] #convert to icd9
	new[which(is.na(new) & !is.na(x))] = x[which(is.na(new) & !is.na(x))] #use icd10 if there isnt a mapping
	new
}

temp = temp %>% mutate_at(vars(starts_with("i10_dx")), icd10_to_icd9)


colnames(temp) = gsub("i10_", "", colnames(temp))


saveRDS(temp[, c("key_ed",  paste0("dx", 1:15))], "Data/NEDS_2016_dx.RDS")

temp = temp %>% select(-starts_with("dx"))


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
