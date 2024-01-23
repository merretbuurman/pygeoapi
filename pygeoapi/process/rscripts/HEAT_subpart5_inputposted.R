
# Import packages
library(data.table)
library(readxl)
library(sf)


# Call:
# Rscript --vanilla HEAD_subpart5.R "2011-2016/Configuration2011-2016.xlsx" "/tmp/tmpblabla/AssessmentIndicators.csv" "/tmp/tmpblabla"
# TODO Pass name of result file!

# User params
args <- commandArgs(trailingOnly = TRUE)
print(paste0('R Command line args: ', args))
configurationFilePath = args[1]
inputIndicatorsPath = args[2] # Full path to: AssessmentIndicators.csv
outputPathComplete = args[3]


# Create directory for outputs (in this case, one CSV file: Assessment.csv)
dir.create(dirname(outputPathComplete), showWarnings = FALSE, recursive = TRUE)
#print(paste('Created output path:', outputPath))


# Load R input data: AssessmentIndicators.csv
# This was HTTP-POSTed by the user, then stored to disk by pygeoapi, and now read by R:
wk5 = fread(file=inputIndicatorsPath)

# Load static input data:
print(paste('Reading indicators from', configurationFilePath))
indicators <- as.data.table(read_excel(configurationFilePath, sheet = "Indicators", col_types = c("numeric", "numeric", "text", "text", "text", "text", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "text", "numeric", "numeric", "text", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"))) %>% setkey(IndicatorID)
print('Reading indicators 1/2 done')

indicatorUnits <- as.data.table(read_excel(configurationFilePath, sheet = "IndicatorUnits", col_types = "numeric")) %>% setkey(IndicatorID, UnitID)
print('Reading indicators 2/2 done')

#####################
### Computing wk4 ###
#####################


# Criteria ---------------------------------------------------------------------

# Check indicator weights
indicators[indicatorUnits][!is.na(CriteriaID), .(IWs = sum(IW, na.rm = TRUE)), .(CriteriaID, UnitID)]

# Criteria result as a simple average of the indicators in each category per unit - CategoryID, UnitID, N, ER, EQR, EQRS, C
wk6 <- wk5[!is.na(CriteriaID) & !is.na(EQRS), .(.N, ER = mean(ER), EQR = mean(EQR), EQRS = mean(EQRS), C = mean(C)), .(CriteriaID, UnitID)]

# Criteria result as a weighted average of the indicators in each category per unit - CategoryID, UnitID, N, ER, EQR, EQRS, C
#wk6 <- wk5[!is.na(CriteriaID) & !is.na(EQR), .(.N, ER = weighted.mean(ER, IW, na.rm = TRUE), EQR = weighted.mean(EQR, IW, na.rm = TRUE), EQRS = weighted.mean(EQRS, IW, na.rm = TRUE), C = weighted.mean(C, IW, na.rm = TRUE)), .(CriteriaID, UnitID)]

wk7 <- dcast(wk6, UnitID ~ CriteriaID, value.var = c("N","ER","EQR","EQRS","C"))

# Assessment -------------------------------------------------------------------

# Assessment result - UnitID, N, ER, EQR, EQRS, C
wk8 <- wk6[, .(.N, ER = max(ER), EQR = min(EQR), EQRS = min(EQRS), C = mean(C)), (UnitID)] %>% setkey(UnitID)

wk9 <- wk7[wk8, on = .(UnitID = UnitID), nomatch=0]

# Assign Status and Confidence Classes
wk9[, EQRS_Class := ifelse(EQRS >= 0.8, "High",
                           ifelse(EQRS >= 0.6, "Good",
                                  ifelse(EQRS >= 0.4, "Moderate",
                                         ifelse(EQRS >= 0.2, "Poor","Bad"))))]
wk9[, EQRS_1_Class := ifelse(EQRS_1 >= 0.8, "High",
                           ifelse(EQRS_1 >= 0.6, "Good",
                                  ifelse(EQRS_1 >= 0.4, "Moderate",
                                         ifelse(EQRS_1 >= 0.2, "Poor","Bad"))))]
wk9[, EQRS_2_Class := ifelse(EQRS_2 >= 0.8, "High",
                           ifelse(EQRS_2 >= 0.6, "Good",
                                  ifelse(EQRS_2 >= 0.4, "Moderate",
                                         ifelse(EQRS_2 >= 0.2, "Poor","Bad"))))]
wk9[, EQRS_3_Class := ifelse(EQRS_3 >= 0.8, "High",
                           ifelse(EQRS_3 >= 0.6, "Good",
                                  ifelse(EQRS_3 >= 0.4, "Moderate",
                                         ifelse(EQRS_3 >= 0.2, "Poor","Bad"))))]

wk9[, C_Class := ifelse(C >= 75, "High",
                        ifelse(C >= 50, "Moderate", "Low"))]
wk9[, C_1_Class := ifelse(C_1 >= 75, "High",
                        ifelse(C_1 >= 50, "Moderate", "Low"))]
wk9[, C_2_Class := ifelse(C_2 >= 75, "High",
                        ifelse(C_2 >= 50, "Moderate", "Low"))]
wk9[, C_3_Class := ifelse(C_3 >= 75, "High",
                        ifelse(C_3 >= 50, "Moderate", "Low"))]


print('R script finished running.')

#intermediateFileName = paste0(intermediatePath,'/my_wk9.rds')
#print(paste('Now writing intermediate files to:', intermediateFileName))
#saveRDS(wk9, file = intermediateFileName)

print(paste('Now writing outputs to', outputPathComplete))
fwrite(wk9, file = outputPathComplete)
print(paste('R script wrote outputs to', outputPathComplete))
