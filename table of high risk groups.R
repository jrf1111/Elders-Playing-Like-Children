library(tidylog)

high_risk_codes = readxl::read_excel("high-risk ecode groups.xlsx",
																		 sheet = "ICD-9"
)


high_risk_codes = high_risk_codes %>% select(ecode_with_dot, group)

high_risk_codes = high_risk_codes %>% rename("hr_group" = "group")


hr_groups = high_risk_codes %>% 
	group_by(hr_group) %>% 
	nest() %>% ungroup() %>% 
	mutate(
		data = sapply(data, as.character),
		data = str_remove_all(data, 'c\\(\\"'),
		data = str_remove_all(data, '\\"'),
		data = str_remove_all(data, '\\)'),
		data = str_remove_all(data, '\n')
		)

write_csv(hr_groups, "Table of ecodes for high-risk groups.csv")

rm(hr_groups, high_risk_codes)
