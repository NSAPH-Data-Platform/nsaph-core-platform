library(arepa)
library(optparse)

download_annual <- function (out_file, year, parameter = 88101) {
  get_AQS_data_annual(year = 1990:2020)
  x <- load_annual_average(year = year)

  x <- x[x$Parameter.Code == parameter]
  write.csv(x, file = out_file)

  system("rm -r Data_AQS")
}


option_list <- list(
  make_option(c("-y", "--year"), type="character", default=1990:2020,
              help="Year", metavar="character", ),
  make_option(c("-p", "--parameter"), type="character", default=88101,
              help="EPA Parameter Code", metavar="character", ),
  make_option(c("-o", "--out"), type="character", default = "pm25_monitor_annual_average.csv",
              help="output file [default= %default]", metavar="character"),
);

opt_parser <- OptionParser(option_list=option_list);
opt <- parse_args(opt_parser);
out_file <- opt$out;

year <- opt$year;
parameter <- opt$parameter

download_annual(out_file = out_file, year = year, parameter = parameter)

# Codes: https://www.epa.gov/aqs/aqs-memos-technical-note-reporting-pm25-continuous-monitoring-and-speciation-data-air-quality
"
Parameter Name                          Parameter Code

PM2.5 LOCAL CONDITIONS                      88101

PM2.5 TOTAL ATMOSPHERIC                     88500

PM2.5 RAW DATA                              88501

ACCEPTABLE PM2.5 AQI & SPECIATION MASS1     88502

PM2.5 VOLATILE CHANNEL1                     88503

"