# Import packages
library(data.table)
library(readxl)
library(sf)


# Call:
# Rscript --vanilla HEAD_subpart5.R "2011-2016/Configuration2011-2016.xlsx" "/tmp/tmpblabla"

# User params
args <- commandArgs(trailingOnly = TRUE)
print(paste0('R Command line args: ', args))
configurationFileName = args[1]
outputPath = args[2]


# Define paths for input data
#inputPath <- file.path("Input", assessmentPeriod)
#inputPath <- paste0("/home/ubuntu/Input/", assessmentPeriod) # TODO Fix workding dirs!
inputPath <- "/home/ubuntu/Input" # TODO Fix workding dirs!
#outputPath <- file.path(paste0("Output", format(Sys.time(), "%Y%m%d_%H%M%S")), assessmentPeriod)
#outputPath <- file.path(paste0("Output", format(Sys.time(), "%Y%m%d")), assessmentPeriod)
dir.create(outputPath, showWarnings = FALSE, recursive = TRUE)
#print(paste('Created output path:', outputPath))


# Load R input data:
print('Loading inputs: my_wk5.rds')
wk5 = readRDS(file = "/home/ubuntu/intermediate_files/my_wk5.rds")
#wk5 = fread(file=inputIndicatorsPath)



# Define input files
configurationFile <- file.path(inputPath, configurationFileName)
print(paste('Reading indicators from', configurationFile))
indicators <- as.data.table(read_excel(configurationFile, sheet = "Indicators", col_types = c("numeric", "numeric", "text", "text", "text", "text", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "text", "numeric", "numeric", "text", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"))) %>% setkey(IndicatorID)
print('Reading indicators 1/2 done')

indicatorUnits <- as.data.table(read_excel(configurationFile, sheet = "IndicatorUnits", col_types = "numeric")) %>% setkey(IndicatorID, UnitID)
print('Reading indicators 2/2 done')

#####################
### Computing wk4 ###
#####################


# Criteria ---------------------------------------------------------------------

# Check indicator weights
indicators[indicatorUnits][!is.na(CriteriaID), .(IWs = sum(IW, na.rm = TRUE)), .(CriteriaID, UnitID)]

# Criteria result as a simple average of the indicators in each category per unit - CategoryID, UnitID, N, ER, EQR, EQRS, C
# This one fails!
#Error in `[.data.table`(wk5, !is.na(CriteriaID) & !is.na(EQRS), .(.N,  : 
#  fastmean was passed type closure, not numeric or logical
#Calls: [ -> [.data.table -> .External
#Execution halted

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

#print('Now writing intermediate files to /home/ubuntu/intermediate_files/')
#intermediateFileName = '/home/ubuntu/intermediate_files/my_wk9.rds'
#saveRDS(wk9, file = intermediateFileName)

outputPathComplete = file.path(outputPath, "Assessment.csv")
print(paste('Now writing outputs to', outputPathComplete))
fwrite(wk9, file = outputPathComplete)
print(paste('R script wrote outputs to', outputPathComplete))

