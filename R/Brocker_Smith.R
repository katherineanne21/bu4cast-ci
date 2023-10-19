#remotes::install_github("FK83/scoringRules")

forecast <- c(1,2,3)
obs <- 10

n <- length(forecast)

#Parameters from Brocker and Smith Equations 20 and 21
a <- 1
r1 <-  0
r2 <- 0
s1 <- 1
s2 <- 0


#ScoringRules is a special case
#a = 1, r2 = 0, r1 = 0
#s1 = 1, s2 = 0

#Brocker and Smith Equation 16.5 (Silverman's factor)
h_s <- 0.5 * (4 / (3 * n)) ^ (1/5)

#Equation 21
z <- a*forecast + r2 * mean(forecast) + r1
#Equation 22
sigma_2 <- h_s^2 * (s1 + s2 * var(z))
#Converting to bandwidth
bw <- sqrt(sigma_2)

#Examples of different calculations
#1. completely by hand
f <- (obs - z) / bw
p_forecast <-  1/(bw * n) * (sum((1 / sqrt(2 * pi)) * exp(-0.5 * f^2)))
-log(p_forecast)

#Using dnorm
-log(sum((1/(n)*dnorm(obs, z, bw))))

#Using scoringRules
bw
scoringRules::logs_sample(y = obs, dat = z, bw = bw)
stats::bw.nrd0(z)
scoringRules::logs_sample(y = obs, dat = z, bw = stats::bw.nrd0(z))
stats::bw.nrd(z)
scoringRules::logs_sample(y = obs, dat = z, bw = stats::bw.nrd(z))
scoringRules::logs_sample(y = obs, dat = z)

#The stats::bw.nrd(z) calculation
r <- quantile(z, c(0.25, 0.75))
h <- (r[2] - r[1])/1.34
1.06 * min(sqrt(var(z)), h) * length(z)^(-1/5)

#The stats::bw.nrd0(z)
hi <- sd(z)
if (!(lo <- min(hi, IQR(z)/1.34))) (lo <- hi) || (lo <- abs(z[1])) || (lo <- 1)
0.9 * lo * length(z)^(-0.2)


## Optimizing parameters to set of forecasts


cost_func <- function(dat, y, par){

  a <- par[1]
  r1 <- par[2]
  r2 <- 0
  s1 <- 1
  s2 <- 0
  n <- ncol(dat)

  #ScoringRules is a special case
  #a = 1, r2 = 0, r1 = 0
  #s1 = 1, s2 = 0

  h_s <- 0.5 * (4 / (3 * n)) ^ (1/5)

  ens_func <- function(i){
    z <- a*dat[i, ] + r2 * mean(dat[i, ] ) + r1
    sigma_2 <- h_s^2 * (s1 + s2 * var(z))
    bw <- sqrt(sigma_2)

    -log(sum((1/(n)*dnorm(y[i], z, bw))))
  }

  sum(sapply(seq_along(y), ens_func))

}

set.seed(100)
forecast_day1 <- rnorm(100, 2, 1)
forecast_day2 <- rnorm(100, 10, 1)

forecast <- t(matrix(data = c(forecast_day1,forecast_day2), ncol = 2))
obs <- c(2,8)

n <- ncol(forecast)
h_s <- 0.5 * (4 / (3 * n)) ^ (1/5)

fit <- optim(par = c(1,1), cost_func, dat = forecast, y = obs)

hist(forecast[2,])
hist(forecast[2,]*fit$par[1] + fit$par[2])

#sigma_2 <- h_s^2 * (s1 + s2 * apply(forecast, 1, var))
#bw <- sqrt(sigma_2)

scoringRules::logs_sample(y = obs,
                          dat = forecast)

#sigma_2 <- h_s^2 * (s1 + s2 * apply(forecast * fit$par[1] + fit$par[2], 1, var))
#bw <- sqrt(sigma_2)

scoringRules::logs_sample(y = obs, dat = forecast * fit$par[1] + fit$par[2])

