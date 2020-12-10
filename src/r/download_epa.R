library(arepa)
library(optparse)

download_annual <- function (out_file, year, parameter) {
  get_AQS_data_annual(year = year)
  x <- load_annual_average(year = year)

  x <- x[x$Parameter.Code == parameter]
  write.csv(x, file = out_file)

  system("rm -r Data_AQS")
}

download_daily <- function (out_file, year, parameter) {
  get_AQS_data_daily(year = year, parameter = parameter)
  x <- load_daily_data(year = year, parameter = parameter)
  write.csv(x, file = out_file, row.names = F)

  system("rm -r Data_AQS")
}

option_list <- list(
  make_option(c("-y", "--year"), type="character", default="1990:2020",
              help="Year", metavar="character"),
  make_option(c("-p", "--parameter"), type="integer", default=88101,
              help="EPA Parameter Code", metavar="character"),
  make_option(c("-a", "--aggregation"), type="character", default="annual",
              help="annual/daily", metavar="character"),
  make_option(c("-o", "--out"), type="character", default = "pm25_monitor_annual_average.csv",
              help="output file [default= %default]", metavar="character")
);

opt_parser <- OptionParser(option_list=option_list);
opt <- parse_args(opt_parser);
out_file <- opt$out;

syear <- unlist(strsplit(opt$year, ':'));
if (length(syear) < 2) {
  y <- strtoi(syear[1])
  year <- y:y
} else {
  year <- strtoi(syear[1]) : strtoi(syear[2])
}

parameter <- opt$parameter
aggr <- opt$aggregation

if (aggr == "annual") {
  download_annual(out_file = out_file, year = year, parameter = parameter)
} else if (aggr == "daily") {
  download_daily (out_file = out_file, year = year, parameter = parameter)
} else {
  stop(paste("Aggregation should be annual or daily, but is: ", aggr))
}

# Codes: https://www.epa.gov/aqs/aqs-memos-technical-note-reporting-pm25-continuous-monitoring-and-speciation-data-air-quality
"
Parameter Name                          Parameter Code

PM2.5 LOCAL CONDITIONS                      88101

PM2.5 TOTAL ATMOSPHERIC                     88500

PM2.5 RAW DATA                              88501

ACCEPTABLE PM2.5 AQI & SPECIATION MASS1     88502

PM2.5 VOLATILE CHANNEL1                     88503

"