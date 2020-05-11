library(data.table)
library(tidyverse)
library(haven)




#convert files to more memory effecient RDS files
files = list.files("Data/", pattern = "_Hospital.dta", full.names = T)
for(i in files){
	
	print(i) #print file name
	gc() #garbage clean up
	temp = read_dta(i) #read in file
	
	name = gsub("dta", "RDS", i) #change file format for resaving
	saveRDS(temp, file = name) #save in more memory effecient file
	
	rm(temp) #remove data
	
}




files = list.files("Data/", pattern = "_IP.dta", full.names = T)
for(i in files){
	
	print(i) #print file name
	gc() #garbage clean up
	temp = read_dta(i) #read in file
	
	name = gsub("dta", "RDS", i) #change file format for resaving
	saveRDS(temp, file = name) #save in more memory effecient file
	
	rm(temp) #remove data
	
}









#core data files are *much* larger :|
# don't read in all vars

blank_to_na = function(x){ 
	x[x==""] = NA
	x
}

ids = c("key_ed", "hosp_ed", "discwt", "year")


#core 2010 -------------------------------------------------------------------------------------------------


temp = read_dta("Data/NEDS_2010_Core.dta", 
								col_select = c(
									ids,  
									paste0("dx", 1:15)
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2010_Core_dx.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2010_Core.dta", 
								col_select = c(
									ids,  
									c("age",  "female")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)

saveRDS(temp, "Data/NEDS_2010_Core_demos.RDS")
rm(temp)
gc()




temp = read_dta("Data/NEDS_2010_Core.dta", 
								col_select = c(
									ids,  
									c("died_visit", "disp_ed", "edevent")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2010_Core_outcomes.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2010_Core.dta", 
								col_select = c(
									ids,  
									starts_with("ecode")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

temp = temp %>% mutate_at(vars(starts_with("ecode")), 
													function(x){ x[which(substr(x, 1, 1)!="E")]=NA; x }  )


saveRDS(temp, "Data/NEDS_2010_Core_ecode.RDS")
rm(temp)
gc()

beepr::beep()









#core 2011 -----------------------------------------------------------------------------------------------------

temp = read_dta("Data/NEDS_2011_Core.dta", 
								col_select = c(
									ids,  
									paste0("dx", 1:15)
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2011_Core_dx.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2011_Core.dta", 
								col_select = c(
									ids,  
									c("age",  "female")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)

saveRDS(temp, "Data/NEDS_2011_Core_demos.RDS")
rm(temp)
gc()




temp = read_dta("Data/NEDS_2011_Core.dta", 
								col_select = c(
									ids,  
									c("died_visit", "disp_ed", "edevent")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2011_Core_outcomes.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2011_Core.dta", 
								col_select = c(
									ids,  
									starts_with("ecode")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

temp = temp %>% mutate_at(vars(starts_with("ecode")), 
													function(x){ x[which(substr(x, 1, 1)!="E")]=NA; x }  )


saveRDS(temp, "Data/NEDS_2011_Core_ecode.RDS")
rm(temp)
gc()


beepr::beep()















#core 2012 -----------------------------------------------------------------------------------------------------

temp = read_dta("Data/NEDS_2012_Core.dta", 
								col_select = c(
									ids,  
									paste0("dx", 1:15)
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2012_Core_dx.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2012_Core.dta", 
								col_select = c(
									ids,  
									c("age",  "female")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)

saveRDS(temp, "Data/NEDS_2012_Core_demos.RDS")
rm(temp)
gc()




temp = read_dta("Data/NEDS_2012_Core.dta", 
								col_select = c(
									ids,  
									c("died_visit", "disp_ed", "edevent")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2012_Core_outcomes.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2012_Core.dta", 
								col_select = c(
									ids,  
									starts_with("ecode")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

temp = temp %>% mutate_at(vars(starts_with("ecode")), 
													function(x){ x[which(substr(x, 1, 1)!="E")]=NA; x }  )


saveRDS(temp, "Data/NEDS_2012_Core_ecode.RDS")
rm(temp)
gc()




beepr::beep()









#core 2013 -----------------------------------------------------------------------------------------------------

temp = read_dta("Data/NEDS_2013_Core.dta", 
								col_select = c(
									ids,  
									paste0("dx", 1:15)
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2013_Core_dx.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2013_Core.dta", 
								col_select = c(
									ids,  
									c("age",  "female")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)

saveRDS(temp, "Data/NEDS_2013_Core_demos.RDS")
rm(temp)
gc()




temp = read_dta("Data/NEDS_2013_Core.dta", 
								col_select = c(
									ids,  
									c("died_visit", "disp_ed", "edevent")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2013_Core_outcomes.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2013_Core.dta", 
								col_select = c(
									ids,  
									starts_with("ecode")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

temp = temp %>% mutate_at(vars(starts_with("ecode")), 
													function(x){ x[which(substr(x, 1, 1)!="E")]=NA; x }  )


saveRDS(temp, "Data/NEDS_2013_Core_ecode.RDS")
rm(temp)
gc()



beepr::beep()















#core 2014 -----------------------------------------------------------------------------------------------------

#in 2014, there are up to 30dxs.  Only import first 15 to avoid measurement bias.

temp = read_dta("Data/NEDS_2014_Core.dta", 
								col_select = c(
									ids,  
									paste0("dx", 1:15)  
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2014_Core_dx.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2014_Core.dta", 
								col_select = c(
									ids,  
									c("age",  "female")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)

saveRDS(temp, "Data/NEDS_2014_Core_demos.RDS")
rm(temp)
gc()




temp = read_dta("Data/NEDS_2014_Core.dta", 
								col_select = c(
									ids,  
									c("died_visit", "disp_ed", "edevent")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2014_Core_outcomes.RDS")
rm(temp)
gc()





temp = read_dta("Data/NEDS_2014_Core.dta", 
								col_select = c(
									ids,  
									starts_with("ecode")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

temp = temp %>% mutate_at(vars(starts_with("ecode")), 
													function(x){ x[which(substr(x, 1, 1)!="E")]=NA; x }  )


saveRDS(temp, "Data/NEDS_2014_Core_ecode.RDS")
rm(temp)
gc()




beepr::beep()





#2015 -----------------------------------------------------------------------------------------------------

#things get weird in 2015: 
# - dxs are in ED files not in core
# - Q1-Q3 are ICD9, Q4 is ICD10


# ~ 2015 Q1-Q3 dxs ----------------------------------------------------------------------------------------
temp = read_dta("Data/NEDS_2015_Core.dta", 
								col_select = ids
								)

gc()
temp$year = NULL


temp2 = read_dta("Data/NEDS_2015Q1Q3_ED.dta",
								 col_select = c(
								 	c("key_ed", "hosp_ed"),
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
temp = data.table(temp, key = c("key_ed", "hosp_ed"))
temp2 = data.table(temp2, key = c("key_ed", "hosp_ed"))


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
								 	c("key_ed", "hosp_ed"),
								 	paste0("dx", 6:10)
								 )
)
gc()


#make sure ID columns are the same class in both files
temp2$key_ed = as.character(temp2$key_ed)
temp2$hosp_ed = as.character(temp2$hosp_ed)


#make keyed columns
temp = data.table(temp, key = c("key_ed", "hosp_ed"))
temp2 = data.table(temp2, key = c("key_ed", "hosp_ed"))


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
								 	c("key_ed", "hosp_ed"),
								 	paste0("dx", 11:15)
								 )
)
gc()


#make sure ID columns are the same class in both files
temp2$key_ed = as.character(temp2$key_ed)
temp2$hosp_ed = as.character(temp2$hosp_ed)


#make keyed columns
temp = data.table(temp, key = c("key_ed", "hosp_ed"))
temp2 = data.table(temp2, key = c("key_ed", "hosp_ed"))


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
								col_select = c(ids[1:2],
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
									ids[1:2],  
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
									ids[1:2],  
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
									ids,  
									c("age",  "female")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)
temp$age = as.integer(temp$age)

saveRDS(temp, "Data/NEDS_2015_Core_demos.RDS")
rm(temp)
gc()




temp = read_dta("Data/NEDS_2015_Core.dta", 
								col_select = c(
									ids,  
									c("died_visit", "disp_ed", "edevent")
								)
)

temp = temp %>% mutate_if(is.character, blank_to_na)

saveRDS(temp, "Data/NEDS_2015_Core_outcomes.RDS")
rm(temp)
gc()


beepr::beep()





















# 2016 files -----------------------------------------------------------------------------------------------------

#2016 files are all CSV

# ~ core 2016 ------------------------------------------------------------

temp = fread("Data/NEDS_2016_CORE.csv", 
						 logical01 = F, keepLeadingZeros = T, stringsAsFactors = T,
						 na.strings=c(getOption("datatable.na.strings","NA"), -1:-9, -66, -99, -100000000, "")
						 )

colnames(temp) = c("AGE", "AMONTH", "AWEEKEND", "DIED_VISIT", 
									 "DISCWT", "DISP_ED", "DQTR", "DXVER", 
									 "EDEVENT", "FEMALE", "HCUPFILE", 
									 "HOSP_ED", "I10_DX1", "I10_DX2", 
									 "I10_DX3", "I10_DX4", "I10_DX5", "I10_DX6",
									 "I10_DX7", "I10_DX8", "I10_DX9", "I10_DX10",
									 "I10_DX11", "I10_DX12", "I10_DX13", "I10_DX14",
									 "I10_DX15", "I10_DX16", "I10_DX17", "I10_DX18", 
									 "I10_DX19", "I10_DX20", "I10_DX21", "I10_DX22", 
									 "I10_DX23", "I10_DX24", "I10_DX25", "I10_DX26", 
									 "I10_DX27", "I10_DX28", "I10_DX29", "I10_DX30", 
									 "I10_ECAUSE1", "I10_ECAUSE2", "I10_ECAUSE3", 
									 "I10_ECAUSE4", "I10_NDX", "I10_NECAUSE", 
									 "KEY_ED", "NEDS_STRATUM", "PAY1", "PAY2", 
									 "PL_NCHS", "TOTCHG_ED", "YEAR", "ZIPINC_QRTL")


colnames(temp) = tolower(colnames(temp))



saveRDS(temp, "Data/NEDS_2016_CORE.RDS")
gc()


#drop vars I don't need
temp = temp %>% select(-num_range("i10_dx", range = 16:30))
temp = temp %>% select(-c(i10_ndx, i10_necause, pay1, pay2, pl_nchs,
													totchg_ed, amonth, aweekend, dxver, 
													neds_stratum, zipinc_qrtl, dqtr))






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

saveRDS(temp[, c(ids, "ecode1", "ecode2", "ecode3", "ecode4")], "Data/NEDS_2016_ecode.RDS")

temp = temp %>% select(-starts_with("ecode"))










#convert ICD10 to ICD9
icd_map = read_csv("https://raw.githubusercontent.com/jrf1111/ICD10-to-ICD9/master/icd10toicd9gem.csv")


icd10_to_icd9 = function(x){
	new = icd_map$icd9cm[match(x, icd_map$icd10cm)] #convert to icd9
	new[which(is.na(new) & !is.na(x))] = x[which(is.na(new) & !is.na(x))] #use icd10 if there isnt a mapping
	new
}

temp = temp %>% mutate_at(vars(starts_with("i10_dx")), icd10_to_icd9)


colnames(temp) = gsub("i10_", "", colnames(temp))


saveRDS(temp[, c(ids, paste0("dx", 1:15))], "Data/NEDS_2016_dx.RDS")

temp = temp %>% select(-starts_with("dx"))



temp %>% select(c(ids, "age", "female")) %>% 
	saveRDS(., "Data/NEDS_2016_demos.RDS")


temp %>% select(c(ids, "died_visit", "disp_ed", "edevent")) %>% 
	saveRDS(., "Data/NEDS_2016_outcomes.RDS")



rm(temp)
gc()

beepr::beep()


# ~ IP 2016 ------------------------------------------------------------

temp = fread("Data/NEDS_2016_IP.csv", 
						 logical01 = F, keepLeadingZeros = T, stringsAsFactors = T,
						 na.strings=c(getOption("datatable.na.strings","NA"), -1:-9, -99, -100000000, "")
)

colnames(temp) = c("HOSP_ED", "KEY_ED", "DISP_IP", "DRG", "DRGVER", "DRG_NoPOA",
									 "I10_NPR_IP", "I10_PR_IP1", "I10_PR_IP2", "I10_PR_IP3", 
									 "I10_PR_IP4", "I10_PR_IP5", "I10_PR_IP6", "I10_PR_IP7", 
									 "I10_PR_IP8", "I10_PR_IP9", "LOS_IP", "MDC", "MDC_NoPOA",
									 "PRVER", "TOTCHG_IP")

colnames(temp) = tolower(colnames(temp))


temp = temp %>% mutate_if(is.character, blank_to_na)

temp = temp %>% mutate_if(is.character, factor)


#set missings
temp$los_ip[temp$los_ip<0] = NA
temp$totchg_ip[temp$totchg_ip<0] = NA



saveRDS(temp, "Data/NEDS_2016_IP.RDS")

rm(temp)
gc()






# ~ hospital 2016 ------------------------------------------------------------

temp = fread("Data/NEDS_2016_HOSPITAL.csv", 
						 logical01 = F, keepLeadingZeros = T, stringsAsFactors = T,
						 na.strings=c(getOption("datatable.na.strings","NA"), -1:-9, -99, -100000000)
)

colnames(temp) = c("DISCWT", "HOSPWT", "HOSP_CONTROL", 
									 "HOSP_ED", "HOSP_REGION", "HOSP_TRAUMA",
									 "HOSP_URCAT4", "HOSP_UR_TEACH", "NEDS_STRATUM", 
									 "N_DISC_U", "N_HOSP_U", "S_DISC_U", "S_HOSP_U",
									 "TOTAL_EDVisits", "YEAR")

colnames(temp) = tolower(colnames(temp))



temp = temp %>% mutate_if(is.character, blank_to_na)

temp = temp %>% mutate_if(is.character, factor)



saveRDS(temp, "Data/NEDS_2016_HOSPITAL.RDS")
rm(temp)
gc()




# ~ ED 2016 ------------------------------------------------------------

temp = fread("Data/NEDS_2016_ED.csv", 
						 logical01 = F, keepLeadingZeros = T, stringsAsFactors = T,
						 na.strings=c(getOption("datatable.na.strings","NA"), -1:-9, -99, -100000000)
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

temp = temp %>% mutate_if(is.character, blank_to_na)

temp = temp %>% mutate_if(is.character, factor)

saveRDS(temp, "Data/NEDS_2016_ED.RDS")
rm(temp)
gc()
