# Title     : TODO
# Objective : TODO
# Created by: misha
# Created on: 3/4/21

library(fst)

d <- "/Users/misha/harvard/projects/data_server/nsaph/data"
name <- "NOAAreanalysis_annual_USAODGrid_2007"
input <- file.path(d, paste0(name, ".fst"))
print(input)
y <- read_fst(input)
output <- file.path(d, paste0(name, ".csv.gz"))
out <- gzfile(output, "w")
write.csv(y, out)
