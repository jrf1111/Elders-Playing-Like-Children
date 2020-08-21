
library(tidyverse)

syn = read_lines("icd9_to_barrel.txt", skip_empty_rows = TRUE)
syn = str_replace_all(syn, "‘", "'")
syn = str_replace_all(syn, "’", "'")
syn = str_replace_all(syn, "′", "'")



syn = 
	syn %>% 
	str_replace_all("\\(?'(\\d{1,})'\\s?<=\\s?(DX\\d{1,})\\s?<=\\s?'(\\d{1,})'\\)?",
									"dplyr::between\\(\\2, \\1, \\3\\)") %>%
	str_replace_all("\\(?'(\\d{1,})'\\s?<=\\s?(D5)\\s?<=\\s?'(\\d{1,})'\\)?",
									"dplyr::between\\(\\2, \\1, \\3\\)") %>%
	str_replace_all("\\bGE\\b", ">=") %>% 
	str_replace_all("\\bLE\\b", "<=") %>% 
	str_replace_all("\\bAND\\b", "&") %>% 
	str_replace_all("\\bOR\\b", "|") %>% 
	str_replace_all("'(\\d{1,})'", "\\1") %>% 
	str_replace_all("(DX\\d{1,2})\\s?=\\s?(\\d{1,})", "\\1 == \\2") %>% 
	str_replace_all("(D5)\\s?=\\s?(\\d{1,})", "\\1 == \\2") %>% 
	str_replace_all("\\bTHEN\\b", " ~ ") %>% 
	#str_replace_all("(~.*?=\\s?\\d{1,});", "\\1,") %>% 
	str_replace_all("^(IF\\s?)", "") %>% 
	str_replace_all("^\\s{1,}IF\\s?", "")
	
write_lines(syn, "partial barell syntax for R.txt")

