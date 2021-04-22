# Title     : TODO
# Objective : TODO
# Created by: misha
# Created on: 3/4/21

library(fst)
library(optparse)

option_list <- list(
  make_option(c("-i", "--input"), type="character",
              help="input file", metavar="character"),
  make_option(c("-o", "--out"), type="character", default = "",
              help="output file [default= input.csv.gz]", metavar="character")
);

opt_parser <- OptionParser(option_list=option_list);
opt <- parse_args(opt_parser);
in_file <- opt$input;
out_file <- opt$out;

if (out_file == "") {
  n <- nchar(in_file)
  ext <- substr(in_file, n-2, n)
  name <- substr(in_file, 0, n-4)
  out_file <- paste0(name, ".csv.gz")
}

input <- file.path(in_file)
print(input)
y <- read_fst(input)
output <- file.path(out_file)
out <- gzfile(output, "w")
print(output)
ret <- write.csv(y, out)
print(ret)
