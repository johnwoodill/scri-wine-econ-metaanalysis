library(tidyverse)
library(viridis)

tdat <- as.data.frame(read_csv("~/Projects/scri-wine-econ-metaanalysis/data/full_prism_degree_days.csv"))

ggplot(filter(tdat, date == 20120101), aes(lon, lat, fill=ppt)) + geom_tile()

library(lubridate)

ggplot(tdat, aes(date, tavg)) + geom_smooth() + facet_wrap(~gridNumber)


tdat$year <- substring(tdat$date, 1, 4)
tdat$month <- substring(tdat$date, 5, 6)
tdat$day <- substring(tdat$date, 7, 8)

tdat$vpdmean <- (tdat$vpdmax + tdat$vpdmin) / 2

tdat2 <- tdat %>% group_by(year, month) %>% summarise(tavg = mean(tavg),
                                                      ppt = mean(ppt),
                                                      dday0C = mean(dday0C),
                                                      vpdmean = mean(vpdmean)) %>% 
  gather(key = vars, value=value, -year, -month)

ggplot(tdat2, aes(month, year, fill=value)) + 
  geom_tile() +
  scale_fill_viridis_c(option = "C") +
  labs(x="Month of Year", y="Year") +
  theme_bw() +
  facet_wrap(~vars) 

ggsave("~/Projects/scri-wine-econ-metaanalysis/figs/EDA-weather-vars.png")
