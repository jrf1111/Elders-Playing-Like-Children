#Based on SAS syntax taken from https://www.cdc.gov/nchs/injury/ice/barell_sas.htm
# on Aug. 15, 2020.





library(tidyverse)


# SAS Input Statements
# The Barell Injury Diagnosis Matrix: A Framework for Classifying Injuries by Body Region and Nature of the Injury
# These SAS codes are all valid for data years 1998 and forward. Prior to 1998, some of the codes may have not had 5th or 4th digit specificity and hence could show up as missing, particularly for the site specific data. CODE CORRECTED 9/05/2002.




icd9_to_barrel = function(dx){
	
	if(!is.character(dx)){
		dx = as.character(dx)
	}	
	
	
	#remove period
	dx = gsub(".", "", dx, fixed = TRUE)
	
	
	# DX13: 3 digit code for first-listed ICD-9 CM diagnosis
	DX13 = substr(dx, 1, 3) %>% as.numeric()
	
	# DX14: 4 digit code for first-listed diagnosis
	DX14 = substr(dx, 1, 4) %>% as.numeric()
	
	# DX15: 5 digit code for first-listed diagnosis
	DX15 = substr(dx, 1, 5) %>% as.numeric()
	
	# D5:		5th digit of code
	D5 = substr(dx, 5, 5) %>% as.numeric()
	
	
	
	
	ISRCODE = case_when(
		dplyr::between(DX13, 800, 829)  ~ 1,
		dplyr::between(DX13, 830, 839)  ~  2,
		dplyr::between(DX13, 840, 848)  ~  3,
		dplyr::between(DX13, 860, 869) | dplyr::between(DX13, 850, 854) | 
			DX13 == 952 | DX15 == 99555  ~  4,
		dplyr::between(DX13, 870, 884) | dplyr::between(DX13, 890, 894)  ~  5,
		dplyr::between(DX13, 885, 887) | dplyr::between(DX13, 895, 897)  ~  6,
		dplyr::between(DX13, 900, 904)  ~  7,
		dplyr::between(DX13, 910, 924)  ~  8,
		dplyr::between(DX13, 925, 929)  ~  9,
		dplyr::between(DX13, 940, 949)  ~  10,
		(dplyr::between(DX13, 950, 951)) | dplyr::between(DX13, 953, 957)  ~  11,
		DX13 == 959  ~  12,
		dplyr::between(DX13, 930, 939) | dplyr::between(DX13, 960, 994) | 
			dplyr::between(DX13, 905, 908) | dplyr::between(DX14, 9090, 9092) | 
			DX13 == 958 | dplyr::between(DX15, 99550, 99554) | DX15 == 99559 | 
			DX14 == 9094 | DX14 == 9099 | dplyr::between(DX15, 99580, 99585)  ~  13,
		TRUE ~ NA_real_
	)
	
	
	
	
	
	
	
	
	ISRSITE = case_when(
		
		dplyr::between(DX14, 8001, 8004) | dplyr::between(DX14, 8006, 8009) | 
			dplyr::between(DX15, 80003, 80005) | dplyr::between(DX15, 80053, 80055) |
			dplyr::between(DX14, 8011, 8014) | dplyr::between(DX14, 8016, 8019) | 
			dplyr::between(DX15, 80103, 80105) | dplyr::between(DX15, 80153, 80155) | 
			dplyr::between(DX14, 8031, 8034) | dplyr::between(DX14, 8036, 8039) | 
			dplyr::between(DX15, 80303, 80305) | dplyr::between(DX15, 80353, 80355) |
			dplyr::between(DX14, 8041, 8044) | dplyr::between(DX14, 8046, 8049) | 
			dplyr::between(DX15, 80403, 80405) | dplyr::between(DX15, 80453, 80455) | 
			dplyr::between(DX14, 8502, 8504) | dplyr::between(DX13, 851, 854) | 
			dplyr::between(DX14, 9501, 9503) | DX15 == 99555  ~  1,
		
		DX15 == 80000 | DX15 == 80002 | DX15 == 80006 | DX15 == 80009 |
			DX15 == 80100 | DX15 == 80102 | DX15 == 80106 | DX15 == 80109 | 
			DX15 == 80300 | DX15 == 80302 | DX15 == 80306 | DX15 == 80309 | 
			DX15 == 80400 | DX15 == 80402 | DX15 == 80406 | DX15 == 80409 |
			DX15 == 80050 | DX15 == 80052 | DX15 == 80056 | DX15 == 80059 |
			DX15 == 80150 | DX15 == 80152 | DX15 == 80156 | DX15 == 80159 | 
			DX15 == 80350 | DX15 == 80352 | DX15 == 80356 | DX15 == 80359 | 
			DX15 == 80450 | DX15 == 80452 | DX15 == 80456 | DX15 == 80459 | 
			DX14 == 8500 | DX14 == 8501 | DX14 == 8505 | DX14 == 8509  ~  2,
		
		
		DX15 == 80001 | DX15 == 80051 | DX15 == 80101 | DX15 == 80151 | 
			DX15 == 80301 | DX15 == 80351 | DX15 == 80401 | DX15 == 80451  ~  3,
		
		
		(DX13 == 951) | 
			(DX14 == 8730 | DX14 == 8731 | DX14 == 8738 | DX14 == 8739) | 
			(DX13 == 941 & D5 == 6) | DX15 == 95901  ~  4,
		
		
		DX13 == 802 | DX13 == 830 | DX14 == 8480 | DX14 == 8481 | 
			DX13 == 872 | dplyr::between(DX14, 8732, 8737) | 
			(DX13 == 941 & D5 == 1) | (DX13 == 941 &  dplyr::between(D5, 3, 5) ) | 
			(DX13 == 941 & D5 == 7)  ~  5,
		
		
		DX14 == 9500 | DX14 == 9509 | dplyr::between(DX13, 870, 871) |
			DX13 == 921 | DX13 == 918 | DX13 == 940 | (DX13 == 941 & D5 == 2) ~ 6,
		
		
		dplyr::between(DX14, 8075, 8076) | DX14 == 8482 | DX14 == 9252 | 
			DX14 == 9530 | DX14 == 9540 | DX13 == 874 | 
			(DX13 == 941 & D5 == 8)  ~ 7,
		
		
		DX14 == 9251 | DX13 == 900 | DX14 == 9570 | DX13 == 910 | 
			DX13 == 920 | DX14 == 9470 | DX15 == 95909 | 
			(DX13 == 941 & (D5 == 0 | D5 == 9))  ~  8,
		
		dplyr::between(DX14, 8060, 8061) | (DX14 == 9520)  ~  9,
		dplyr::between(DX14, 8062, 8063) | (DX14 == 9521)  ~  10,
		dplyr::between(DX14, 8064, 8065) | (DX14 == 9522)  ~  11,
		dplyr::between(DX14, 8066, 8067) | dplyr::between(DX14, 9523, 9524)  ~  12,
		dplyr::between(DX14, 8068, 8069) | dplyr::between(DX14, 9528, 9529)  ~  13,
		dplyr::between(DX14, 8050, 8051) | dplyr::between(DX14, 8390, 8391) | DX14 == 8470  ~  14,
		dplyr::between(DX14, 8052, 8053) | (83921 == DX15 | 83931 == DX15) | DX14 == 8471  ~  15,
		dplyr::between(DX14, 8054, 8055) | (83920 == DX15 | 83930 == DX15) | DX14 == 8472  ~  16,
		
		dplyr::between(DX14, 8056, 8057) | (83941==DX15 | 83942==DX15)| 
			dplyr::between(DX15, 83951, 83952) | dplyr::between(DX14, 8473, 8474)  ~  17,
		
		
		dplyr::between(DX14, 8058, 8059) | (83940==DX15 | 83949==DX15) | 
			(83950 == DX15 | DX15 == 83959)  ~  18,
		
		dplyr::between(DX14, 8070, 8074) | DX15 == 83961 | DX15 == 83971 | 
			dplyr::between(DX14, 8483, 8484) | DX15 == 92619 | 
			dplyr::between(DX13, 860, 862) | DX13 == 901 | DX14 == 9531 | 
			DX13 == 875 | DX14 == 8790 | DX14 == 8791 | DX14 == 9220 |
			DX14 == 9221 | DX15 == 92233 | (DX13 == 942 & (D5 == 1 | D5 == 2))  ~  19,
		
		
		dplyr::between(DX13, 863, 866) | DX13 == 868 |
			dplyr::between(DX14, 9020, 9024) | DX14 == 9532 | DX14 == 9535 |
			dplyr::between(DX14, 8792, 8795) | DX14 == 9222 | 
			(DX13 == 942 & D5 == 3) | DX14 == 9473  ~  20,
		
		
		DX13 == 808 | DX15 == 83969 | DX15 == 83979 | DX13 == 846 | 
			DX14 == 8485 | DX14 == 9260 | DX15 == 92612 | DX13 == 867 | 
			DX14 == 9025 | dplyr::between(DX15, 90281, 90282) | 
			DX14 == 9533 | dplyr::between(DX13, 877, 878) | DX14 == 9224 | 
			(DX13 == 942 & D5 == 5) | DX14 == 9474  ~  21,
		
		
		DX13 == 809 | dplyr::between(DX14, 9268, 9269) | DX14 == 9541 | 
			dplyr::between(DX14, 9548, 9549) | dplyr::between(DX14, 8796, 8797) |
			dplyr::between(DX14, 9228, 9229) | DX13 == 911 | (DX13 == 942 & D5 == 0) | 
			(DX13 == 942 & D5 == 9) | DX14 == 9591  ~  22,
		
		
		DX14 == 8479 | DX15 == 92611 | DX13 == 876 | DX15 == 92232 | 
			DX15 == 92231 | (DX13 == 942 & D5 == 4)  ~  23,
		
		
		dplyr::between(DX13, 810, 812) | DX13 == 831 | DX13 == 840 | 
			DX13 == 880 | dplyr::between(DX14, 8872, 8873) | (DX13 == 943 & dplyr::between(D5, 3, 6)) |
			DX13 == 912 | DX14 == 9230 | DX14 == 9270 | DX14 == 9592  ~  24,
		
		
		DX13 == 813 | DX13 == 832 | DX13 == 841 | (DX13 == 881 & dplyr::between(D5, 0, 1) ) | 
			dplyr::between(DX14, 8870, 8871) | DX14 == 9231 | DX14 == 9271 | 
			(DX13 == 943 & dplyr::between(D5, 1, 2))  ~  25,
		
		
		dplyr::between(DX13, 814, 817) | dplyr::between(DX13, 833, 834) | 
			DX13 == 842 | (DX13 == 881 & D5 == 2) | dplyr::between(DX13, 882, 883) | 
			dplyr::between(DX13, 885, 886) | dplyr::between(DX13, 914, 915) | 
			dplyr::between(DX14, 9232, 9233) | dplyr::between(DX14, 9272, 9273) | 
			DX13 == 944 | dplyr::between(DX14, 9594, 9595)  ~  26,
		
		
		DX13 == 818 | DX13 == 884 | dplyr::between(DX14, 8874, 8877) | DX13 == 903 |
			DX13 == 913 | DX14 == 9593 | dplyr::between(DX14, 9238, 9239) | 
			dplyr::between(DX14, 9278, 9279) | DX14 == 9534 | DX13 == 955 |
			(DX13 == 943 & (D5 == 0 | D5 == 9))  ~  27,
		
		
		DX13 == 820 | DX13 == 835 | DX13 == 843 | DX15 == 92401 | DX15 == 92801  ~  28,
		
		DX13 == 821 | dplyr::between(DX14, 8972, 8973) | DX15 == 92400 | 
			DX15 == 92800 | (DX13 == 945 & D5 == 6)  ~  29,
		
		DX13 == 822 | DX13 == 836 | dplyr::between(DX14, 8440, 8443) | 
			DX15 == 92411 | DX15 == 92811 | (DX13 == 945 & D5 == 5)  ~  30,
		
		
		dplyr::between(DX13, 823, 824) | dplyr::between(DX14, 8970, 8971) | 
			DX13 == 837 | DX14 == 8450 | DX15 == 92410 | DX15 == 92421 | 
			DX15 == 92810 | DX15 == 92821 | (DX13 == 945 & dplyr::between(D5, 3, 4))  ~  31,
		
		
		dplyr::between(DX13, 825, 826) | DX13 == 838 | DX14 == 8451 | 
			dplyr::between(DX13, 892, 893) | dplyr::between(DX13, 895, 896) | 
			DX13 == 917 | DX15 == 92420 | DX14 == 9243 | DX15 == 92820 | 
			DX14 == 9283 | (DX13 == 945 & dplyr::between(D5, 1, 2) ) ~  32,
		
		
		DX13 == 827 | dplyr::between(DX14, 8448, 8449) | dplyr::between(DX13, 890, 891) |
			DX13 == 894 | dplyr::between(DX14, 8974, 8977) | dplyr::between(DX14, 9040, 9048) | 
			DX13 == 916 | dplyr::between(DX14, 9244, 9245) | DX14 == 9288 | DX14 == 9289 | 
			dplyr::between(DX14, 9596, 9597) | (DX13 == 945 & (D5 == 0 | D5 == 9))  ~  33,
		
		
		DX13 == 828 | DX13 == 819 | DX15 == 90287 | DX15 == 90289 | DX14 == 9538 |
			dplyr::between(DX14, 9471, 9472) | DX13 == 956 ~ 34,
		
		
		DX13 == 829 | dplyr::between(DX14, 8398, 8399) | dplyr::between(DX14, 8488, 8489) | 
			DX13 == 869 | dplyr::between(DX14, 8798, 8799) | DX14 == 9029 | DX14 == 9049 | 
			DX13 == 919 | dplyr::between(DX14, 9248, 9249) | DX13 == 929 | DX13 == 946 | 
			dplyr::between(DX14, 9478, 9479) | dplyr::between(DX13, 948, 949) | DX14 == 9539 |
			DX14 == 9571 | dplyr::between(DX14, 9578, 9579) | dplyr::between(DX14, 9598, 9599)  ~  35,
		
		dplyr::between(DX13, 930, 939) | dplyr::between(DX13, 960, 994) | 
			dplyr::between(DX13, 905, 908) | dplyr::between(DX14, 9090, 9092) | 
			DX13 == 958 | dplyr::between(DX15, 99550, 99554) | DX15 == 99559 | 
			DX14 == 9094 | DX14 == 9099 | dplyr::between(DX15, 99580, 99585)  ~ 36,				
		
		TRUE ~ NA_real_
	)
	
	
	
	
	ISRSITE2 = case_when(
		dplyr::between(ISRSITE, 1, 3)  ~  1,
		dplyr::between(ISRSITE, 4, 8)  ~  2,
		dplyr::between(ISRSITE, 9, 13)  ~  3,
		dplyr::between(ISRSITE, 14, 18)  ~  4,
		dplyr::between(ISRSITE, 19, 23)  ~  5,
		dplyr::between(ISRSITE, 24, 27)  ~  6,
		dplyr::between(ISRSITE, 28, 33)  ~  7,
		dplyr::between(ISRSITE, 34, 35)  ~  8,
		ISRSITE == 36  ~  9,
		TRUE ~ NA_real_
	)
	
	
	
	ISRSITE3 = case_when(
		dplyr::between(ISRSITE, 1, 8)  ~  1,
		dplyr::between(ISRSITE, 9, 18)  ~  2,
		dplyr::between(ISRSITE, 19, 23)  ~  3,
		dplyr::between(ISRSITE, 24, 33)  ~  4,
		dplyr::between(ISRSITE, 34, 36)  ~  5,
		TRUE ~ NA_real_
	)
	
	
	
	
	ISRSITE = factor(ISRSITE, 
									 levels = 1:36, 
									 labels = c("TYPE 1 TBI", "TYPE 2 TBI", "TYPE 3 TBI", 
									 					 "OTHER HEAD", "FACE", "EYE", "NECK",
									 					 "HEAD, FACE, NECK UNSPEC", "CERVICAL SCI", 
									 					 "THORACIC/DORSAL SCI", "LUMBAR SCI", 
									 					 "SACRUM COCCYX SCI", "SPINE & BACK UNSPEC SCI", 
									 					 "CERVICAL VCI", "THORACIC/DORSAL VCI", "LUMBAR VCI",
									 					 "SACRUM COCCYX VCI", "SPINE, BACK UNSPEC VCI", "CHEST",
									 					 "ABDOMEN", "PELVIS & UROGENITAL", "TRUNK", "BACK & BUTTOCK",
									 					 "SHOULDER & UPPER ARM", "FOREARM & ELBOW", "HAND & WRIST & FINGERS", 
									 					 "OTHER & UNSPEC UPPER EXTREM", "HIP", "UPPER LEG & THIGH", "KNEE", 
									 					 "LOWER LEG & ANKLE", "FOOT & TOES", "OTHER & UNSPEC LOWER EXTREM", 
									 					 "OTHER, MULTIPLE, NEC", "UNSPECIFIED", "SYSTEM WIDE & LATE EFFECTS")
	)
	
	
	
	ISRSITE2 = factor(ISRSITE2,
										levels = 1:9, 
										labels = 	c("TRAUMATIC BRAIN INJURY (TBI)", 
																"OTHER HEAD, FACE, NECK", "SPINAL CORD (SCI)", 
																"VERTEBRAL COLUMN (VCI)", "TORSO", 
																"UPPER EXTREMITY", "LOWER EXTREMITY", 
																"OTHER & UNSPECIFIED", "SYSTEM WIDE & LATE EFFECTS"))
	
	
	
	
	ISRSITE3 = factor(ISRSITE3, 
										levels=1:5,
										labels = c("HEAD & NECK","SPINE & BACK", "TORSO", 
															 "EXTREMITIES", "UNCLASSIFIABLE BY SITE")
	)
	
	
	
	
	
	
	ISRCODE = factor(ISRCODE, 
									 levels=1:13, 
									 labels = c("FRACTURES", "DISLOCATION", "SPRAINS & STRAINS", 
									 					 "INTERNAL ORGAN", "OPEN WOUNDS", "AMPUTATIONS", 
									 					 "BLOOD VESSELS", "SUPERFIC/CONT", "CRUSHING",
									 					 "BURNS", "NERVES", "UNSPECIFIED", "SYSTEM WIDE & LATE EFFECTS")
	)
	
	
	
	res = data.frame(dx_type = ISRCODE,
									 region_1 = ISRSITE3,
									 region_2 = ISRSITE2,
									 region_3 = ISRSITE
	)
	
	return(res)
	
	
	
	
	
}




