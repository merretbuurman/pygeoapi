
# Import packages
library(data.table)
library(readxl)
library(sf)

# Call:
# Rscript --vanilla 


# User params
args <- commandArgs(trailingOnly = TRUE)
print(paste0('R Command line args: ', args))
configurationFilePath = args[1]
intermediatePath = args[2]
outputPathComplete = args[3]


# Create directory for outputs (in this case, one CSV file: Assessment_Indicator.csv)
dir.create(dirname(outputPathComplete), showWarnings = FALSE, recursive = TRUE)
#print(paste('Created output path:', outputPath))

# Load R input data:
# Which has the same content as: AnnualIndicators.csv, but was stored on disk by previous process
intermediateFileName = paste0(intermediatePath,"/my_wk3.rds")
print(paste('Now reading intermediate files from:', intermediateFileName))
wk3 = readRDS(file = intermediateFileName)

print('Loading inputs: my_units.rds, my_stationSamples.rds')

# Load static input data:
print(paste('Reading indicators from', configurationFilePath))
indicators <- as.data.table(read_excel(configurationFilePath, sheet = "Indicators", col_types = c("numeric", "numeric", "text", "text", "text", "text", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "text", "numeric", "numeric", "text", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"))) %>% setkey(IndicatorID)
indicatorUnits <- as.data.table(read_excel(configurationFilePath, sheet = "IndicatorUnits", col_types = "numeric")) %>% setkey(IndicatorID, UnitID)


#####################
### Computing wk4 ###
#####################

# Calculate assessment means --> UnitID, Period, ES, SD, N, N_OBS, EQR, EQRS GTC, STC, SSC
wk4 <- wk3[, .(Period = ifelse(min(Period) > 9999, min(Period), min(Period) * 10000 + max(Period)), ES = mean(ES), SD = sd(ES), ER = mean(ER), EQR = mean(EQR), EQRS = mean(EQRS), N = .N, N_OBS = sum(N), GTC = mean(GTC), STC = mean(STC), SSC = mean(SSC)), .(IndicatorID, UnitID)]


wk4[, EQRS_Class := ifelse(EQRS >= 0.8, "High",
                           ifelse(EQRS >= 0.6, "Good",
                                  ifelse(EQRS >= 0.4, "Moderate",
                                         ifelse(EQRS >= 0.2, "Poor","Bad"))))]


# Add Year Count where STC = 100 --> NSTC100
wk4 <- wk3[STC == 100, .(NSTC100 = .N), .(IndicatorID, UnitID)][wk4, on = .(IndicatorID, UnitID)]

# Adjust Specific Spatial Confidence if number of years where STC = 100 is at least half of the number of years with meassurements
wk4[, STC := ifelse(!is.na(NSTC100) & NSTC100 >= N/2, 100, STC)]

# Combine with indicator and indicator unit configuration tables
wk5 <- indicators[indicatorUnits[wk4]]

# Confidence Assessment---------------------------------------------------------

# Calculate Temporal Confidence averaging General and Specific Temporal Confidence 
wk5 <- wk5[, TC := (GTC + STC) / 2]

wk5[, TC_Class := ifelse(TC >= 75, "High", ifelse(TC >= 50, "Moderate", "Low"))]

# Calculate Spatial Confidence as the Specific Spatial Confidence 
wk5 <- wk5[, SC := SSC]

wk5[, SC_Class := ifelse(SC >= 75, "High", ifelse(SC >= 50, "Moderate", "Low"))]

# Standard Error - using number of years in the assessment period and the associated standard deviation
#wk5[, SE := SD / sqrt(N)]

# Accuracy Confidence for Non-Problem Area
#wk5[, AC_NPA := ifelse(Response == 1, pnorm(ET, ES, SD), pnorm(ES, ET, SD))]

# Standard Error - using number of observations behind the annual mean - to be used in Accuracy Confidence Calculation!!!
wk5[, AC_SE := SD / sqrt(N_OBS)]

# Accuracy Confidence for Non-Problem Area
wk5[, AC_NPA := ifelse(Response == 1, pnorm(ET, ES, AC_SE), pnorm(ES, ET, AC_SE))]

# Accuracy Confidence for Problem Area
wk5[, AC_PA := 1 - AC_NPA]

# Accuracy Confidence Area Class - Not sure what this should be used for?
#wk5[, ACAC := ifelse(AC_NPA > 0.5, "NPA", ifelse(AC_NPA < 0.5, "PA", "PPA"))]

# Accuracy Confidence
wk5[, AC := ifelse(AC_NPA > AC_PA, AC_NPA, AC_PA)]

# Accuracy Confidence Class
wk5[, ACC := ifelse(AC > 0.9, 100, ifelse(AC < 0.7, 0, 50))]

wk5[, ACC_Class := ifelse(ACC >= 75, "High", ifelse(ACC >= 50, "Moderate", "Low"))]

# Calculate Overall Confidence
wk5 <- wk5[, C := (TC + SC + ACC) / 3]

wk5[, C_Class := ifelse(C >= 75, "High", ifelse(C >= 50, "Moderate", "Low"))]



print('R script finished running.')

intermediateFileName = paste0(intermediatePath,'/my_wk5.rds')
print(paste('Now writing intermediate files to:', intermediateFileName))
saveRDS(wk5, file = intermediateFileName)
# TODO Maybe instead I can reread Assessment_Indicator.csv?

print(paste('Now writing outputs to', outputPathComplete))
fwrite(wk5, file = outputPathComplete)
print(paste('R script wrote outputs to', outputPathComplete))

