
#' read in cms patient summary fixed with files
#' 
#' @param year year of data being read in
#'
#' @importFrom readr fwf_positions read_fwf
#' @export
read_cms_medicare_summary_fwf <- function(file_name, year) {
  if (year %in% 2011:2014) {
    col_positions  <- fwf_positions(start =  c(1,16,20,21,22,30,32,34,37,46,49,57,58,
                                               66,74,75,76,77,78,79,80,82,83,84,87,90,
                                               93,96,97,98,99,100,101,102,103,104,105,
                                               106,107,108,109,110,111,112,113,114,115,
                                               116,117,118,119),
                                    end = c(15,19,20,21,29,31,33,36,45,48,56,57,65,73,
                                            74,75,76,77,78,79,81,82,83,86,89,92,95,96,
                                            97,98,99,100,101,102,103,104,105,106,107,
                                            108,109,110,111,112,113,114,115,116,117,
                                            118,119),
                                col_names =  c("BENE_ID","RFRNC_YR","FIVEPCT","EFIVEPCT",
                                   "COVSTART","CRNT_BIC","STATE_CD","CNTY_CD",
                                   "BENE_ZIP","AGE","BENE_DOB","V_DOD_SW",
                                   "DEATH_DT","NDI_DEATH_DT","SEX","RACE",
                                   "RTI_RACE_CD","OREC","CREC","ESRD_IND",
                                   "MS_CD","A_TRM_CD","B_TRM_CD","A_MO_CNT",
                                   "B_MO_CNT","BUYIN_MO","HMO_MO","BUYIN01",
                                   "BUYIN02","BUYIN03","BUYIN04","BUYIN05",
                                   "BUYIN06","BUYIN07","BUYIN08","BUYIN09",
                                   "BUYIN10","BUYIN11","BUYIN12","HMOIND01",
                                   "HMOIND02","HMOIND03","HMOIND04","HMOIND05",
                                   "HMOIND06","HMOIND07","HMOIND08","HMOIND09",
                                   "HMOIND10","HMOIND11","HMOIND12"))
  } else if (year %in%  2015:2016) {
    col_positions <- fwf_positions(start = c(1,16,20,23,25,26,28,30,33,38,43,48,53,
                                            58,63,68,73,78,83,88,93,98,101,109,110,
                                            118,119,120,121,129,130,131,132,134,136,
                                            138,140,142,144,146,148,150,152,154,156,
                                            157,158,161,164,167,170,173,176,179,180,
                                            181,182,183,184,185,186,187,188,189,190,
                                            191,192,193,194,195,196,197,198,199,200,
                                            201,202,203,208,213,218,223,228,233,238,
                                            243,248,253,258,263,266,269,272,275,278,
                                            281,284,287,290,293,296,299,302,305,308,
                                            311,314,317,320,323,326,329,332,335,340,
                                            345,350,355,360,365,370,375,380,385,390,
                                            395,398,401,404,407,410,413,416,419,422,
                                            425,428,431,434,437,440,443,446,449,452,
                                            455,458,461,464,467,468,469,470,471,472,
                                            473,474,475,476,477,478,479,481,483,485,
                                            487,489,491,493,495,497,499,501,503,505,
                                            507,509,511,513,515,517,519,521,523,525),
                                  end = c(15,19,22,24,25,27,29,32,37,42,47,52,57,62,
                                          67,72,77,82,87,92,97,100,108,109,117,118,
                                          119,120,128,129,130,131,133,135,137,139,
                                          141,143,145,147,149,151,153,155,156,157,
                                          160,163,166,169,172,175,178,179,180,181,
                                          182,183,184,185,186,187,188,189,190,191,
                                          192,193,194,195,196,197,198,199,200,201,
                                          202,207,212,217,222,227,232,237,242,247,
                                          252,257,262,265,268,271,274,277,280,283,
                                          286,289,292,295,298,301,304,307,310,313,
                                          316,319,322,325,328,331,334,339,344,349,
                                          354,359,364,369,374,379,384,389,394,397,
                                          400,403,406,409,412,415,418,421,424,427,
                                          430,433,436,439,442,445,448,451,454,457,
                                          460,463,466,467,468,469,470,471,472,473,
                                          474,475,476,477,478,480,482,484,486,488,
                                          490,492,494,496,498,500,502,504,506,508,
                                          510,512,514,516,518,520,522,524,526),
                                  col_names = c("BENE_ID","RFRNC_YR","ENRL_SRC","SAMPLE_GROUP",
                                                "EFIVEPCT","CRNT_BIC","STATE_CD","CNTY_CD","ZIP_CD",
                                                "STATE_CNTY_FIPS_CD_01","STATE_CNTY_FIPS_CD_02",
                                                "STATE_CNTY_FIPS_CD_03","STATE_CNTY_FIPS_CD_04",
                                                "STATE_CNTY_FIPS_CD_05","STATE_CNTY_FIPS_CD_06",
                                                "STATE_CNTY_FIPS_CD_07","STATE_CNTY_FIPS_CD_08",
                                                "STATE_CNTY_FIPS_CD_09","STATE_CNTY_FIPS_CD_10",
                                                "STATE_CNTY_FIPS_CD_11","STATE_CNTY_FIPS_CD_12",
                                                "AGE","BENE_DOB","V_DOD_SW","DEATH_DT",
                                                "SEX","RACE","RTI_RACE_CD","COVSTART",
                                                "OREC","CREC","ESRD_IND","MDCR_STUS_CD_01",
                                                "MDCR_STUS_CD_02","MDCR_STUS_CD_03",
                                                "MDCR_STUS_CD_04","MDCR_STUS_CD_05",
                                                "MDCR_STUS_CD_06","MDCR_STUS_CD_07",
                                                "MDCR_STUS_CD_08","MDCR_STUS_CD_09",
                                                "MDCR_STUS_CD_10","MDCR_STUS_CD_11",
                                                "MDCR_STUS_CD_12","A_TRM_CD","B_TRM_CD",
                                                "A_MO_CNT","B_MO_CNT","BUYIN_MO","HMO_MO",
                                                "PTD_MO","RDS_MO","DUAL_MO","BUYIN01",
                                                "BUYIN02","BUYIN03","BUYIN04","BUYIN05",
                                                "BUYIN06","BUYIN07","BUYIN08","BUYIN09",
                                                "BUYIN10","BUYIN11","BUYIN12","HMOIND01",
                                                "HMOIND02","HMOIND03","HMOIND04",
                                                "HMOIND05","HMOIND06","HMOIND07",
                                                "HMOIND08","HMOIND09","HMOIND10",
                                                "HMOIND11","HMOIND12","PTC_CNTRCT_ID_01",
                                                "PTC_CNTRCT_ID_02","PTC_CNTRCT_ID_03",
                                                "PTC_CNTRCT_ID_04","PTC_CNTRCT_ID_05",
                                                "PTC_CNTRCT_ID_06","PTC_CNTRCT_ID_07",
                                                "PTC_CNTRCT_ID_08","PTC_CNTRCT_ID_09",
                                                "PTC_CNTRCT_ID_10","PTC_CNTRCT_ID_11",
                                                "PTC_CNTRCT_ID_12","PTC_PBP_ID_01",
                                                "PTC_PBP_ID_02","PTC_PBP_ID_03",
                                                "PTC_PBP_ID_04","PTC_PBP_ID_05",
                                                "PTC_PBP_ID_06","PTC_PBP_ID_07",
                                                "PTC_PBP_ID_08","PTC_PBP_ID_09",
                                                "PTC_PBP_ID_10","PTC_PBP_ID_11",
                                                "PTC_PBP_ID_12","PTC_PLAN_TYPE_CD_01",
                                                "PTC_PLAN_TYPE_CD_02","PTC_PLAN_TYPE_CD_03",
                                                "PTC_PLAN_TYPE_CD_04","PTC_PLAN_TYPE_CD_05",
                                                "PTC_PLAN_TYPE_CD_06","PTC_PLAN_TYPE_CD_07",
                                                "PTC_PLAN_TYPE_CD_08","PTC_PLAN_TYPE_CD_09",
                                                "PTC_PLAN_TYPE_CD_10","PTC_PLAN_TYPE_CD_11",
                                                "PTC_PLAN_TYPE_CD_12","PTDCNTRCT01",
                                                "PTDCNTRCT02","PTDCNTRCT03","PTDCNTRCT04",
                                                "PTDCNTRCT05","PTDCNTRCT06","PTDCNTRCT07",
                                                "PTDCNTRCT08","PTDCNTRCT09","PTDCNTRCT10",
                                                "PTDCNTRCT11","PTDCNTRCT12","PTDPBPID01",
                                                "PTDPBPID02","PTDPBPID03","PTDPBPID04",
                                                "PTDPBPID05","PTDPBPID06","PTDPBPID07",
                                                "PTDPBPID08","PTDPBPID09","PTDPBPID10",
                                                "PTDPBPID11","PTDPBPID12","SGMTID01",
                                                "SGMTID02","SGMTID03","SGMTID04",
                                                "SGMTID05","SGMTID06","SGMTID07",
                                                "SGMTID08","SGMTID09","SGMTID10",
                                                "SGMTID11","SGMTID12","RDSIND01",
                                                "RDSIND02","RDSIND03","RDSIND04",
                                                "RDSIND05","RDSIND06","RDSIND07",
                                                "RDSIND08","RDSIND09","RDSIND10",
                                                "RDSIND11","RDSIND12","DUAL_01",
                                                "DUAL_02","DUAL_03","DUAL_04",
                                                "DUAL_05","DUAL_06","DUAL_07",
                                                "DUAL_08","DUAL_09","DUAL_10",
                                                "DUAL_11","DUAL_12","CSTSHR01",
                                                "CSTSHR02","CSTSHR03","CSTSHR04",
                                                "CSTSHR05","CSTSHR06","CSTSHR07",
                                                "CSTSHR08","CSTSHR09","CSTSHR10",
                                                "CSTSHR11","CSTSHR12"))
    
  } else {
    stop(paste0("There is no file template for ", year))
  }
  return(read_fwf(file_name, col_positions = col_positions))
}


#' read in cms patient summary fixed with files
#'
#'
#' 
#' @export
read_cms_medicare_admissions_fwf <- function(file_name, year) {
  if (year %in% 2011:2016) {
  col_positions <- fwf_positions(start = c(1,31,45,46,49,51,54,59,95,106,
                                           149,157,175,183,275,762,765,766,767,
                                           768,769,770,771,772,773,774,775,
                                           776,777,778,779,780,781,782,783,
                                           784,785,786,787,788,789,790,791,
                                           798,805,812,819,826,833,840,847,
                                           854,861,868,875,882,889,896,903,
                                           910,917,924,931,938,945,952,959),
                                 end = c(15,34,45,46,50,53,58,59,104,106,
                                         156,164,182,183,277,764,765,766,767,
                                         768,769,770,771,772,773,774,775,
                                         776,777,778,779,780,781,782,783,
                                         784,785,786,787,788,789,790,797,
                                         804,811,818,825,832,839,846,853,
                                         860,867,874,881,888,895,902,909,
                                         916,923,930,937,944,951,958,965),
                                 col_names = c("BENE_ID","MEDPAR_YR_NUM",
                                               "SEX","RACE","STATE_CD",
                                               "CNTY_CD","BENE_ZIP","DSCHRGCD",
                                               "PRVDRNUM","SSLSSNF","ADMSN_DT",
                                               "DSCHRG_DT","BENE_DEATH_DT",
                                               "BENE_DEATH_DT_VRFY_CD","DRG_CD","DGNS_CD_CNT",
                                               "DGNS_VRSN_CD","DGNS_VRSN_CD_1",
                                               "DGNS_VRSN_CD_2","DGNS_VRSN_CD_3",
                                               "DGNS_VRSN_CD_4","DGNS_VRSN_CD_5",
                                               "DGNS_VRSN_CD_6","DGNS_VRSN_CD_7",
                                               "DGNS_VRSN_CD_8","DGNS_VRSN_CD_9",
                                               "DGNS_VRSN_CD_10","DGNS_VRSN_CD_11",
                                               "DGNS_VRSN_CD_12","DGNS_VRSN_CD_13",
                                               "DGNS_VRSN_CD_14","DGNS_VRSN_CD_15",
                                               "DGNS_VRSN_CD_16","DGNS_VRSN_CD_17",
                                               "DGNS_VRSN_CD_18","DGNS_VRSN_CD_19",
                                               "DGNS_VRSN_CD_20","DGNS_VRSN_CD_21",
                                               "DGNS_VRSN_CD_22","DGNS_VRSN_CD_23",
                                               "DGNS_VRSN_CD_24","DGNS_VRSN_CD_25",
                                               "DGNS_1_CD","DGNS_2_CD","DGNS_3_CD",
                                               "DGNS_4_CD","DGNS_5_CD","DGNS_6_CD",
                                               "DGNS_7_CD","DGNS_8_CD","DGNS_9_CD",
                                               "DGNS_10_CD","DGNS_11_CD",
                                               "DGNS_12_CD","DGNS_13_CD",
                                               "DGNS_14_CD","DGNS_15_CD",
                                               "DGNS_16_CD","DGNS_17_CD",
                                               "DGNS_18_CD","DGNS_19_CD",
                                               "DGNS_20_CD","DGNS_21_CD",
                                               "DGNS_22_CD","DGNS_23_CD",
                                               "DGNS_24_CD","DGNS_25_CD"))
  } else {
    stop(paste0("There is no file template for ", year))
  }
  return(read_fwf(file_name, col_positions = col_positions))
}

#' read in csv versions of pre 2011 medicare admission files
#' @param file_name path to file
#' @param year year of the data being read
#'
#'
#' @export 
read_cms_medicare_admissions_csv <- function(file_name, year) {
  if (year %in% 1999:2002) {
    ## read.csv used here due to NULL values fread doesn't like
    admissions <- read.csv(file_name, stringsAsFactors = F)
    admissions <- as.data.table(admissions)
    select_vars <- c("Intbid","MEDPAR_BENE_SEX_CD","MEDPAR_BENE_AGE_CNT",
                     "MEDPAR_BENE_RACE_CD","MEDPAR_BENE_RSDNC_SSA_STATE_CD",
                     "MEDPAR_BENE_RSDNC_SSA_CNTY_CD",
                     "MEDPAR_BENE_MLG_CNTCT_ZIP_CD","MEDPAR_BENE_DSCHRG_STUS_CD",
                     "MEDPAR_PRVDR_NUM_3RD_CD","MEDPAR_SS_LS_SNF_IND_CD",
                     "MEDPAR_ADMSN_DT","MEDPAR_DSCHRG_DT",
                     "MEDPAR_BENE_DEATH_DT","MEDPAR_BENE_DEATH_DT_VRFY_CD",
                     "MEDPAR_DGNS_CD_CNT","MEDPAR_DGNS_CD_1","MEDPAR_DGNS_CD_2",
                     "MEDPAR_DGNS_CD_3","MEDPAR_DGNS_CD_4","MEDPAR_DGNS_CD_5",
                     "MEDPAR_DGNS_CD_6","MEDPAR_DGNS_CD_7","MEDPAR_DGNS_CD_8",
                     "MEDPAR_DGNS_CD_9","MEDPAR_DGNS_CD_10","MEDPAR_DRG_CD")
    admissions <- admissions[,..select_vars]
    
  } else if (year %in% 2003:2005) {
    ## read.csv used here due to NULL values fread doesn't like
    admissions <- read.csv(file_name, stringsAsFactors = F)
    admissions <- as.data.table(admissions)
    select_vars <- c("BID_5333_1","SEX","AGE_CNT","RACE","STATE_CD","CNTY_CD","BENE_ZIP",
                     "DSCHRGCD","PRVNUM3","SSLSSNF","ADMSNDT","DSCHRGDT",
                     "DEATHDT","DEATHCD","DGNSCNT","DGNS_CD01","DGNS_CD02","DGNS_CD03",
                     "DGNS_CD04","DGNS_CD05","DGNS_CD06","DGNS_CD07","DGNS_CD08",
                     "DGNS_CD09","DGNS_CD10","DRG_CD")
    admissions <- admissions[,..select_vars]
    
  } else if (year == 2006) {
    ## read.csv used here to account for NULLs. 2006 requires encoding specification as well
    admissions <- read.csv(file_name, stringsAsFactors=FALSE, fileEncoding="latin1")
    admissions <- as.data.table(admissions)
    select_vars <- c("BID_5333_5","SEX","AGE_CNT","RACE","STATE_CD","CNTY_CD","BENE_ZIP",
                     "DSCHRGCD","PRVNUM3","SSLSSNF","ADMSNDT","DSCHRGDT",
                     "DEATHDT","DEATHCD","DGNSCNT","DGNS_CD01","DGNS_CD02","DGNS_CD03",
                     "DGNS_CD04","DGNS_CD05","DGNS_CD06","DGNS_CD07","DGNS_CD08",
                     "DGNS_CD09","DGNS_CD10","DRG_CD")
    admissions <- admissions[,..select_vars]
    
  } else if (year %in% 2007:2010) {
    admissions <- fread(file_name,  select = c("BENE_ID",
                                 "BENE_SEX_CD","BENE_AGE_CNT","BENE_RACE_CD","BENE_RSDNC_SSA_STATE_CD",
                                 "BENE_RSDNC_SSA_CNTY_CD","BENE_MLG_CNTCT_ZIP_CD","BENE_DSCHRG_STUS_CD",
                                 "PRVDR_NUM","SS_LS_SNF_IND_CD","ADMSN_DT",
                                 "DSCHRG_DT","BENE_DEATH_DT",
                                 "BENE_DEATH_DT_VRFY_CD","DGNS_CD_CNT",
                                 "DGNS_1_CD","DGNS_2_CD","DGNS_3_CD",
                                 "DGNS_4_CD","DGNS_5_CD","DGNS_6_CD",
                                 "DGNS_7_CD","DGNS_8_CD","DGNS_9_CD",
                                 "DGNS_10_CD","DRG_CD"))
  }
  
  return(admissions)
}

#' Split up lines with corrupted line endings
#' 
#' @param infile path to the input fixed width file
#' @param year year the data represents, must be either 2015 or 2016
#' @param data_type Is the data "inpatient" or "summary". Must be specified.
#' @param outfile path to the output file, cannot overwrite the input file to 
#'                prevent accidental deletion of original data
#' @param logfile the path to the file that records that this was done, and lists
#'        any potentially malformed rows in the output. Default name is 
#'        <data_type>_<year>.log
#'
#' @details The CMS Medicare data files are prepared and processed on a windows
#'          system, and despite their documentation, are not designed to work
#'          on a UNIX like system (such as the RCE or Odyssey). This manifests
#'          in some of the data files having the majority of the data locked in 
#'          to a couple rows, unable to be read by programs designed to parse fixed
#'          width files. This function works to fix those files. the files that 
#'          need fixing are as follows:
#'          \itemize{
#'            \item{2015 Medicare}
#'            \item{2015 Medicare Inpatient Data}
#'            \item{2016 Medicare Denominator Data}
#'            \item{2016 Medicare Inpatient Data}
#'            }
#' @export
fix_medicare_line_length <- function(infile, year, data_type, outfile, logfile = NULL) {
  
  # check inputs
  valid_years <- c(2015, 2016)
  valid_types <- c("summary", "inpatient")
  stopifnot(infile != outfile, year %in% valid_years, data_type %in% valid_types)
  
  # set up log
  if (is.null(logfile)) {
    logfile <- paste0(data_type, "_", year, ".log")
  }
  cat("Medicare Fixed Width File Cleaning Log", file = logfile, sep = "\n")
  cat(paste("infle:", infile), file = logfile, sep = "\n", append = T)
  cat(paste("outfile:", outfile), file = logfile, sep = "\n", append = T)
  cat(paste("year:", year), file = logfile, sep = "\n", append = T)
  cat(paste("data_type:", data_type), file = logfile, sep = "\n", append = T)
  cat(paste("Code run at", Sys.time()), file = logfile, sep = "\n", append = T)
  cat("\nRows with issues:\n", file = logfile, sep = "\n", append = T)
  
  
  if (year == 2015) {
    if (data_type == "summary") {
      line_length = 526
    } else if (data_type == "inpatient") {
      line_length <- 2023
    }
  } else if (year == 2016) {
    if (data_type == "summary") {
      line_length <- 526
    } else if (data_type == "inpatient") {
      line_length <- 2053
    }
  }
  
  in_con <- file(infile, "r")
  file.remove(outfile)
  in_count <- 0
  out_count <- 0
  
  while(TRUE) {
    line <- readLines(con = in_con, 1)
    if (length(line) == 0) break
    in_count <- in_count + 1
    
    if (nchar(line) == line_length) {
      cat(line, file = outfile, sep = "\n", append = T)
      out_count <- out_count + 1
    } else {
      cat(paste("In line", in_count, "has", nchar(line), "characters, not", line_length, "Attempting Cleaning."),
          file = logfile, sep = "\n", append = T)
      start_clean <- out_count + 1
      while(TRUE) {
        out <- substr(line, 1, line_length)
        if (nchar(out) == 0) break
        cat(out, file = outfile, sep = "\n", append = T)
        out_count <- out_count + 1
        if (nchar(out) != line_length) {
          cat(paste("Out line:", out_count, "has", nchar(line), "characters, not", line_length),
              file = logfile, sep = "\n", append = T)
        }
        line <- substr(line, line_length+1, max(line_length + 1, nchar(line)))
      }
      cat(paste("Out lines", start_clean, "to", out_count, "created from in line", in_count),
          file = logfile, sep = "\n", append = T)
    }
  }
  
  cat("\n\n", file = logfile, sep = "\n", append = T)
  cat(paste(in_count, "lines read from", infile), file = logfile, sep = "\n", append = T)
  cat(paste(out_count, "lines written to", outfile), file = logfile, sep = "\n", append = T)
}

