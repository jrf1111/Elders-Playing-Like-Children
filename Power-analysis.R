library(tidyverse)
library(broom)

library(foreach)
library(doParallel)


#' The alpha level for statistical significance was calculated for the primary outcome: 
#' pairwise comparisons of two high-risk groups from the testing sample in the risk-adjusted
#' mortality model. 
#' These calculations assumed highly imbalanced groups (80% of cases in one group);
#' the groups were weakly to moderately correlated with other variables in the model 
#'      (correlation with other variables, r = 0.32, r2 = 0.1); 
#' a 1% mortality rate in one group and a 3% mortality rate in the second group; 
#' and a total sample size of at least 5,000 (weighted) cases between the two groups. 



n_total = 5000
n_big = round(n_total*0.8)
n_small = n_total - n_big

sims = 10000



doParallel::registerDoParallel(cores = 4)

res = foreach::foreach(i = 1:sims, .combine = bind_rows) %dopar% {

	#outcome
	y = c(
		rbinom(n = n_big, size = 1, prob = 0.01),
		rbinom(n = n_small, size = 1, prob = 0.03)
	)
	
	#groups
	g = c(
		rep(0, times = n_big),
		rep(1, times = n_small)
	)
	
	#covariates
	x = c(
		rbinom(n = n_big, size = 5, prob = 0.1),
		rbinom(n = n_small, size = 5, prob = 0.25)
	)
	
	
	
	mod = glm(y ~ g + x, family = binomial())
	p = tidy(mod) %>% filter(term == "g") %>% pull(p.value)
	
	
	r2_gx = cor(x, g)^2
	delta_prob = mean(y[g == 1]) - mean(y[g == 0])
	
	list(pval = p, r2_gx = r2_gx, delta_prob = delta_prob)
	
}

doParallel::stopImplicitCluster()



summary(res)

hist(res$pval, breaks = 50)
abline(v = 0.05, col = "red")
abline(v = 0.01, col = "red")
abline(v = 0.001, col = "red")


mean(res$pval < 0.01)
mean(res$pval < 0.005)
mean(res$pval < 0.001)





# for 80% of cases in one group:
# > mean(res$pval < 0.01)
# [1] 0.9192
# > mean(res$pval < 0.005) ****** winner
# [1] 0.8828
# > mean(res$pval < 0.001)
# [1] 0.7818




# for 90% of cases in one group:
# > mean(res$pval < 0.01)
# [1] 0.7355
# > mean(res$pval < 0.005)
# [1] 0.6748
# > mean(res$pval < 0.001)
# [1] 0.5211
