
#Selection criteria
#Include:
#2010 through 2016
#50 years and older
#presented to ED for treatment of injuries
#Exclude:
#if injuries include burns, bites/stings, overexertion, 
#poisoning, or misadventures of medical/surgical care


library(tidyverse)
library(data.table)


dir.create("Data/filtered/")





# age filter ----
gc()
files = list.files("Data/", pattern = "demos", recursive = F, full.names = T)
cols = c("key_ed", "hosp_ed", "year", "discwt", "age", "female") #to reorder cols


n_before = 0
n_after = 0


#for later filtering
keep = vector("character")

for(i in 1:length(files)){
	
	file = files[i]
	print(substr(file, 7, 50))
	temp = readRDS(file)               #read in file
	
	n_before = n_before + nrow(temp)   #to track number of pts dropped
	temp = temp %>% filter(age >=50)   #apply age filter
	n_after = n_after + nrow(temp)     #to track number of pts dropped
	temp = temp %>% select(cols)       #reorder cols
	
	
	temp$key_ed = as.character(temp$key_ed) #convert to character
	keep = c(keep, temp$key_ed)        #update IDs to keep
	
	
	#new file name
	new_file = gsub("(.*?)(NEDS_.*?)(\\.RDS)", "\\2", file)
	new_file = paste0("Data/filtered/", new_file, ".csv")
	
	#save new file as csv
	fwrite(temp, file = new_file)
	
	rm(temp)
	gc()
	
}


cat("Age filter: from ", format(n_before, big.mark = ","), " to ", 
		format(n_after, big.mark = ","), 
		" (", format(n_before-n_after, big.mark = ","), " cases dropped)\n\n", 
		sep = "", 
		file = "case count.txt")




#save list of IDs to keep
fwrite( tibble(keep = keep) , "Data/filtered/keep IDs.csv")












# injury filter (based on first/principal dx) ----
gc()
files = list.files("Data/", pattern = "dx", recursive = F, full.names = T)

cols = c("key_ed", "hosp_ed", "year",  paste0("dx", 1:15)) #to reorder cols


n_before = 0
n_after = 0


dx_recode = function(dx){
	case_when( 
		nchar(dx) == 3 ~ paste0(dx, "00"),
		nchar(dx) == 4 ~ paste0(dx, "0"),
		TRUE ~ dx
	)
}



injury_codes = seq(800, 900, 0.01)
injury_codes = as.character(injury_codes)
injury_codes = gsub(".", "", injury_codes, fixed = T)
injury_codes = dx_recode(injury_codes)





for(i in 1:length(files)){
	
	file = files[i]
	print(substr(file, 7, 50))
	temp = readRDS(file)               #read in file
	temp$key_ed = as.character(temp$key_ed)
	temp = temp %>% filter(key_ed %in% keep)   #reapply age filter
	
	n_before = n_before + nrow(temp)   #to track number of pts dropped
	
	#recode dx1 for filtering
	if(!is.character(temp$dx1)) temp$dx1=as.character(temp$dx1)
	temp$dx1 = dx_recode(temp$dx1)
	
	temp = temp %>% filter(!is.na(dx1))             #apply filter
	temp = temp %>% filter(dx1 %in% injury_codes)   #apply filter
	
	
	n_after = n_after + nrow(temp)     #to track number of pts dropped
	
	#recode other dxs
	temp[, paste0("dx", 2:15)] = sapply(temp[, paste0("dx", 2:15)], 
																			function(x){if(!is.character(x)){as.character(x)} else{x}}
																			)
	
	temp[, paste0("dx", 2:15)] = sapply(temp[, paste0("dx", 2:15)], dx_recode)
	
	
	
	
	#add year variable if missing
	if(file == "Data//NEDS_2015Q1Q3_dx.RDS"){
		temp$year = "2015Q1Q3"
	}
	
	if(file == "Data//NEDS_2015Q4_dx.RDS"){
		temp$year = "2015Q4"
	}
	
	
	temp = temp %>% select(cols)       #reorder cols
	
	
	keep = c(keep, temp$key_ed)        #update IDs to keep
	
	
	#new file name
	new_file = gsub("(.*?)(NEDS_.*?)(\\.RDS)", "\\2", file)
	new_file = paste0("Data/filtered/", new_file, ".csv")
	
	#save new file as csv
	fwrite(temp, file = new_file)
	
	rm(temp)
	gc()
	
}


cat("Injury filter: from ", format(n_before, big.mark = ","), " to ", 
		format(n_after, big.mark = ","), 
		" (", format(n_before-n_after, big.mark = ","), " cases dropped)\n\n", 
		sep = "", 
		file = "case count.txt", append = T)




#merge 2015 dx files
files = list.files("Data/filtered/", pattern = "2015Q", recursive = F, full.names = T)
temp = fread(files[1])
temp = plyr::rbind.fill(temp, 
												fread(files[2])
												)

fwrite(temp, "Data/filtered/NEDS_2015_dx.csv")
file.remove(files)
rm(temp, injury_codes)
gc()





#save list of IDs to keep
fwrite( tibble(keep = keep) , "Data/filtered/keep IDs.csv")












# ecode filter ------------------------------------------------------------

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


gc()
files = list.files("Data/", pattern = "ecode", recursive = F, full.names = T)

cols = c("key_ed", "hosp_ed", "year",  paste0("ecode", 1:4)) #to reorder cols


n_before = 0
n_after = 0





for(i in 1:length(files)){
	
	file = files[i]
	
	print(substr(file, 7, 50))
	
	temp = readRDS(file)               #read in file
	temp$key_ed = as.character(temp$key_ed)
	temp = temp %>% filter(key_ed %in% keep)   #reapply prior filter(s)
	
	n_before = n_before + nrow(temp)   #to track number of pts dropped
	
	#apply filter (only use first four chars of ecode since that's the length of the values in `exclude`)
	temp = temp %>% filter(! substr(ecode1, 1, 4)  %in% exclude)
	temp = temp %>% filter(! substr(ecode2, 1, 4)  %in% exclude)
	temp = temp %>% filter(! substr(ecode3, 1, 4)  %in% exclude)
	temp = temp %>% filter(! substr(ecode4, 1, 4)  %in% exclude)
	
	
	n_after = n_after + nrow(temp)     #to track number of pts dropped
	

	
	
	#add year variable if missing
	if(file == "Data//NEDS_2015Q1Q3_ecode.RDS"){
		temp$year = "2015Q1Q3"
	}
	
	if(file == "Data//NEDS_2015Q4_ecode.RDS"){
		temp$year = "2015Q4"
	}
	
	
	temp = temp %>% select(cols)       #reorder cols
	
	
	keep = c(keep, temp$key_ed)        #update IDs to keep
	
	
	#new file name
	new_file = gsub("(.*?)(NEDS_.*?)(\\.RDS)", "\\2", file)
	new_file = paste0("Data/filtered/", new_file, ".csv")
	
	#save new file as csv
	fwrite(temp, file = new_file)
	
	rm(temp)
	gc()
	
}


cat("Ecode filter: from ", format(n_before, big.mark = ","), " to ", 
		format(n_after, big.mark = ","), 
		" (", format(n_before-n_after, big.mark = ","), " cases dropped)\n\n", 
		sep = "", 
		file = "case count.txt", append = T)





#merge 2015 ecode files
files = list.files("Data/filtered/", pattern = "2015Q.*?_ecode", recursive = F, full.names = T)
temp = fread(files[1])
temp = plyr::rbind.fill(temp, 
												fread(files[2])
)

fwrite(temp, "Data/filtered/NEDS_2015_ecode.csv")
file.remove(files)
rm(temp, injury_codes)
gc()





#save list of IDs to keep
fwrite( tibble(keep = keep) , "Data/filtered/keep IDs.csv")

