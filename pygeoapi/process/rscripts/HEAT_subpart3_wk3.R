
# Import packages
library('readxl')
library('sf') # to get "%>%"
library('data.table') # to get "setkey"


# TODO: Can we have named ones?
args <- commandArgs(trailingOnly = TRUE)
print(paste0('R Command line args: ', args))
configurationFilePath = args[1]
combined_Chlorophylla_IsWeighted = args[2]
outputPathComplete = args[3]
intermediatePath = args[4]


# User params
#assessmentPeriod <- "2016-2021" # HOLAS III
#assessmentPeriod <- "1877-9999"
#assessmentPeriod <- "2011-2016"
# Set flag to determined if the combined chlorophyll a in-situ/satellite indicator is a simple mean or a weighted mean based on confidence measures
#combined_Chlorophylla_IsWeighted <- TRUE
if (combined_Chlorophylla_IsWeighted == 'true') {
  combined_Chlorophylla_IsWeighted <- TRUE
} else {
  combined_Chlorophylla_IsWeighted <- FALSE
}

# Create directory for outputs (in this case, one CSV file: Annual_Indicator.csv)
dir.create(dirname(outputPathComplete), showWarnings = FALSE, recursive = TRUE)
#print(paste('Created output path:', outputPath))


# Load required intermediate file:
intermediateFileName1 = paste0(intermediatePath,"/my_units.rds")
intermediateFileName2 = paste0(intermediatePath,"/my_stationSamples.rds")
print(paste('Now reading intermediate files from:', intermediateFileName1, 'and', intermediateFileName2))
units = readRDS(file = intermediateFileName1)
stationSamples = readRDS(file = intermediateFileName2)


# Define input files
#if (assessmentPeriod == "1877-9999") {
#  configurationFileName = "Configuration1877-9999.xlsx"
#} else if (assessmentPeriod == "2011-2016") {
#  configurationFileName = "Configuration2011-2016.xlsx"
#} else {
#  configurationFileName = "Configuration2016-2021.xlsx"
#}

# Read indicator configs -------------------------------------------------------
# This needs "readxl"
print(paste('Reading indicators from', configurationFilePath))
indicators <- as.data.table(read_excel(configurationFilePath, sheet = "Indicators", col_types = c("numeric", "numeric", "text", "text", "text", "text", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "text", "numeric", "numeric", "text", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric"))) %>% setkey(IndicatorID)
indicatorUnits <- as.data.table(read_excel(configurationFilePath, sheet = "IndicatorUnits", col_types = "numeric")) %>% setkey(IndicatorID, UnitID)
indicatorUnitResults <- as.data.table(read_excel(configurationFilePath, sheet = "IndicatorUnitResults", col_types = "numeric")) %>% setkey(IndicatorID, UnitID, Period)




#####################
### Computing wk2 ###
#####################

wk2list = list()

print('Looping indicators...')
# Loop indicators --------------------------------------------------------------
for(i in 1:nrow(indicators[IndicatorID < 1000,])){
  indicatorID <- indicators[i, IndicatorID]
  criteriaID <- indicators[i, CriteriaID]
  name <- indicators[i, Name]
  print(paste('Loop', i, name))
  year.min <- indicators[i, YearMin]
  year.max <- indicators[i, YearMax]
  month.min <- indicators[i, MonthMin]
  month.max <- indicators[i, MonthMax]
  depth.min <- indicators[i, DepthMin]
  depth.max <- indicators[i, DepthMax]
  metric <- indicators[i, Metric]
  response <- indicators[i, Response]

  # Copy data
  #stationSamples = readRDS(file = "my_stationSamples.rds")
  if (name == 'Chlorophyll a (FB)') {
    wk <- as.data.table(stationSamples[Type == 'P'])     
  } else {
    wk <- as.data.table(stationSamples[Type != 'P'])     
  }
  
  # Create Period
  wk[, Period := ifelse(month.min > month.max & Month >= month.min, Year + 1, Year)]
  
  # Create Indicator
  if (name == 'Dissolved Inorganic Nitrogen') {
    wk$ES <- apply(wk[, list(Nitrate.Nitrogen..NO3.N...umol.l., Nitrite.Nitrogen..NO2.N...umol.l., Ammonium.Nitrogen..NH4.N...umol.l.)], 1, function(x) {
      if (all(is.na(x)) | is.na(x[1])) {
        NA
      }
      else {
        sum(x, na.rm = TRUE)
      }
    })
    wk$ESQ <- apply(wk[, .(QV.ODV.Nitrate.Nitrogen..NO3.N...umol.l., QV.ODV.Nitrite.Nitrogen..NO2.N...umol.l., QV.ODV.Ammonium.Nitrogen..NH4.N...umol.l.)], 1, function(x){
      max(x, na.rm = TRUE)
    })
  } else if (name == 'Dissolved Inorganic Phosphorus') {
    wk[, ES := Phosphate.Phosphorus..PO4.P...umol.l.]
    wk[, ESQ := QV.ODV.Phosphate.Phosphorus..PO4.P...umol.l.]
  } else if (name == 'Chlorophyll a (In-Situ)' | name == 'Chlorophyll a (FB)') {
    wk[, ES := Chlorophyll.a..ug.l.]
    wk[, ESQ := QV.ODV.Chlorophyll.a..ug.l.]
  } else if (name == "Total Nitrogen") {
    wk[, ES := Total.Nitrogen..N...umol.l.]
    wk[, ESQ := QV.ODV.Total.Nitrogen..N...umol.l.]
  } else if (name == "Total Phosphorus") {
    wk[, ES := Total.Phosphorus..P...umol.l.]
    wk[, ESQ := QV.ODV.Total.Phosphorus..P...umol.l.]
  } else if (name == 'Secchi Depth') {
    wk[, ES := Secchi.Depth..m..METAVAR.FLOAT]
    wk[, ESQ := QV.ODV.Secchi.Depth..m.]
  } else {
    next # like "continue" in Python
  }

  # Filter stations rows and columns --> UnitID, GridID, GridArea, Period, Month, StationID, Depth, Temperature, Salinity, ES
  if (month.min > month.max) {
    wk0 <- wk[
      (Period >= year.min & Period <= year.max) &
        (Month >= month.min | Month <= month.max) &
        (Depth..m. >= depth.min & Depth..m. <= depth.max) &
        !is.na(ES) &
        ESQ <= 1 &
        !is.na(UnitID),
      .(IndicatorID = indicatorID, UnitID, GridSize, GridID, GridArea, Period, Month, StationID, Depth = Depth..m., Temperature = Temperature..degC., Salinity = Practical.Salinity..dmnless., ES)]
  } else {
    wk0 <- wk[
      (Period >= year.min & Period <= year.max) &
        (Month >= month.min & Month <= month.max) &
        (Depth..m. >= depth.min & Depth..m. <= depth.max) &
        !is.na(ES) &
        ESQ <= 1 &
        !is.na(UnitID),
      .(IndicatorID = indicatorID, UnitID, GridSize, GridID, GridArea, Period, Month, StationID, Depth = Depth..m., Temperature = Temperature..degC., Salinity = Practical.Salinity..dmnless., ES)]
  }

  # Calculate station depth mean
  wk0 <- wk0[, .(ES = mean(ES), SD = sd(ES), N = .N), keyby = .(IndicatorID, UnitID, GridID, GridArea, Period, Month, StationID, Depth)]
  
  # Calculate station mean --> UnitID, GridID, GridArea, Period, Month, ES, SD, N
  wk1 <- wk0[, .(ES = mean(ES), SD = sd(ES), N = .N), keyby = .(IndicatorID, UnitID, GridID, GridArea, Period, Month, StationID)]
  
  # Calculate annual mean --> UnitID, Period, ES, SD, N, NM
  wk2 <- wk1[, .(ES = mean(ES), SD = sd(ES), N = .N, NM = uniqueN(Month)), keyby = .(IndicatorID, UnitID, Period)]
  
  # Calculate grid area --> UnitID, Period, ES, SD, N, NM, GridArea
  a <- wk1[, .N, keyby = .(IndicatorID, UnitID, Period, GridID, GridArea)] # UnitGrids
  b <- a[, .(GridArea = sum(as.numeric(GridArea))), keyby = .(IndicatorID, UnitID, Period)] #GridAreas
  wk2 <- merge(wk2, b, by = c("IndicatorID", "UnitID", "Period"), all.x = TRUE)
  rm(a,b)

  wk2list[[i]] <- wk2
}
# End of Loop indicators --------------------------------------------------------------
print('Looping indicators... done.')
print('TODO / FIXME: Here there were 50 or more warnings, right?')

# HERE:
# There were 50 or more warnings (use warnings() to see the first 50)
# FIXME

# Combine annual indicator results
wk2 <- rbindlist(wk2list)

# Combine with indicator results reported
wk2 <- rbindlist(list(wk2, indicatorUnitResults), fill = TRUE)

print('Start wk3')

# Combine with indicator and indicator unit configuration tables
wk3 <- indicators[indicatorUnits[wk2]]

# Calculate General Temporal Confidence (GTC) - Confidence in number of annual observations
wk3[is.na(GTC), GTC := ifelse(N > GTC_HM, 100, ifelse(N < GTC_ML, 0, 50))]

print('wk3 1')

# Calculate Number of Months Potential
wk3[, NMP := ifelse(MonthMin > MonthMax, 12 - MonthMin + 1 + MonthMax, MonthMax - MonthMin + 1)]

# Calculate Specific Temporal Confidence (STC) - Confidence in number of annual missing months
wk3[is.na(STC), STC := ifelse(NMP - NM <= STC_HM, 100, ifelse(NMP - NM >= STC_ML, 0, 50))]

print('wk3 2')

# Calculate General Spatial Confidence (GSC) - Confidence in number of annual observations per number of grids
#wk3 <- wk3[as.data.table(gridunits)[, .(NG = as.numeric(sum(GridArea) / mean(GridSize^2))), .(UnitID)], on = .(UnitID = UnitID), nomatch=0]
#wk3[, GSC := ifelse(N / NG > GSC_HM, 100, ifelse(N / NG < GSC_ML, 0, 50))]

# Calculate Specific Spatial Confidence (SSC) - Confidence in area of sampled grid units as a percentage to the total unit area
# HERE WE NEED UNITS FILE
#units = readRDS(file = "my_units.rds")
wk3 <- merge(wk3, as.data.table(units)[, .(UnitArea = as.numeric(UnitArea)), keyby = .(UnitID)], by = c("UnitID"), all.x = TRUE)

print('wk3 3')

wk3[is.na(SSC), SSC := ifelse(GridArea / UnitArea * 100 > SSC_HM, 100, ifelse(GridArea / UnitArea * 100 < SSC_ML, 0, 50))]

print('wk3 4')

# Calculate Standard Error
wk3[, SE := SD / sqrt(N)]

# Calculate 95 % Confidence Interval
wk3[, CI := qnorm(0.975) * SE]

print('wk3 5')

# Calculate Eutrophication Ratio (ER)
wk3[, ER := ifelse(Response == 1, ES / ET, ET / ES)]

# Calculate (BEST)
wk3[, BEST := ifelse(Response == 1, ET / (1 + ACDEV / 100), ET / (1 - ACDEV / 100))]

print('wk3 6')

# Calculate Ecological Quality Ratio (EQR)
wk3[is.na(EQR), EQR := ifelse(Response == 1, ifelse(BEST > ES, 1, BEST / ES), ifelse(ES > BEST, 1, ES / BEST))]

print('wk3 7')

# Calculate Ecological Quality Ratio Boundaries (ERQ_HG/GM/MP/PB)
wk3[is.na(EQR_GM), EQR_GM := ifelse(Response == 1, 1 / (1 + ACDEV / 100), 1 - ACDEV / 100)]
wk3[is.na(EQR_HG), EQR_HG := 0.5 * 0.95 + 0.5 * EQR_GM]
wk3[is.na(EQR_PB), EQR_PB := 2 * EQR_GM - 0.95]
wk3[is.na(EQR_MP), EQR_MP := 0.5 * EQR_GM + 0.5 * EQR_PB]

print('wk3 8')

# Calculate Ecological Quality Ratio Scaled (EQRS)
wk3[is.na(EQRS), EQRS := ifelse(EQR <= EQR_PB, (EQR - 0) * (0.2 - 0) / (EQR_PB - 0) + 0,
                                ifelse(EQR <= EQR_MP, (EQR - EQR_PB) * (0.4 - 0.2) / (EQR_MP - EQR_PB) + 0.2,
                                       ifelse(EQR <= EQR_GM, (EQR - EQR_MP) * (0.6 - 0.4) / (EQR_GM - EQR_MP) + 0.4,
                                              ifelse(EQR <= EQR_HG, (EQR - EQR_GM) * (0.8 - 0.6) / (EQR_HG - EQR_GM) + 0.6,
                                                     (EQR - EQR_HG) * (1 - 0.8) / (1 - EQR_HG) + 0.8))))]

print(paste('Chlorophyll: combined_Chlorophylla_IsWeighted =', combined_Chlorophylla_IsWeighted))

# Calculate and add combined annual Chlorophyll a (In-Situ/EO/FB) indicator
if(combined_Chlorophylla_IsWeighted) {
  # Calculate combined chlorophyll a indicator as a weighted average
  wk3[IndicatorID == 501, W := ifelse(UnitID %in% c(12), 0.70, ifelse(UnitID %in% c(13, 14), 0.40, 0.55))]
  wk3[IndicatorID == 502, W := ifelse(UnitID %in% c(12, 13, 14), 0.30, 0.45)]
  wk3[IndicatorID == 503, W := ifelse(UnitID %in% c(13, 14), 0.30, 0.00)]
  wk3_CPHL <- wk3[IndicatorID %in% c(501, 502, 503), .(IndicatorID = 5, ES = weighted.mean(ES, W, na.rm = TRUE), SD = NA, N = sum(N, na.rm = TRUE), NM = max(NM, na.rm = TRUE), ER = weighted.mean(ER, W, na.rm = TRUE), EQR = weighted.mean(EQR, W, na.rm = TRUE), EQRS = weighted.mean(EQRS, W, na.rm = TRUE), GTC = weighted.mean(GTC, W, na.rm = TRUE), NMP = max(NMP, na.rm = TRUE), STC = weighted.mean(STC, W, na.rm = TRUE), SSC = weighted.mean(SSC, W, na.rm = TRUE)), by = .(UnitID, Period)]
  wk3 <- rbindlist(list(wk3, wk3_CPHL), fill = TRUE)
} else {
  # Calculate combined chlorophyll a indicator as a simple average
  wk3_CPHL <- wk3[IndicatorID %in% c(501, 502, 503), .(IndicatorID = 5, ES = mean(ES, na.rm = TRUE), SD = NA, N = sum(N, na.rm = TRUE), NM = max(NM, na.rm = TRUE), ER = mean(ER, na.rm = TRUE), EQR = mean(EQR, na.rm = TRUE), EQRS = mean(EQRS, na.rm = TRUE), GTC = mean(GTC, na.rm = TRUE), NMP = max(NMP, na.rm = TRUE), STC = mean(STC, na.rm = TRUE), SSC = mean(SSC, na.rm = TRUE)), by = .(UnitID, Period)]
  wk3 <- rbindlist(list(wk3, wk3_CPHL), fill = TRUE)
}
print('Chlorophyll done')


# Calculate and add combined annual Cyanobacteria Bloom Index (BM/CSA) indicator
wk3_CBI <- wk3[IndicatorID %in% c(601, 602), .(IndicatorID = 6, ES = mean(ES, na.rm = TRUE), SD = NA, N = sum(N, na.rm = TRUE), NM = max(NM, na.rm = TRUE), ER = mean(ER, na.rm = TRUE), EQR = mean(EQR, na.rm = TRUE), EQRS = mean(EQRS, na.rm = TRUE), GTC = mean(GTC, na.rm = TRUE), NMP = max(NMP, na.rm = TRUE), STC = mean(STC, na.rm = TRUE), SSC = mean(SSC, na.rm = TRUE)), by = .(UnitID, Period)]
wk3 <- rbindlist(list(wk3, wk3_CBI), fill = TRUE)

setkey(wk3, IndicatorID, UnitID, Period)

# Classify Ecological Quality Ratio Scaled (EQRS_Class)
wk3[, EQRS_Class := ifelse(EQRS >= 0.8, "High",
                           ifelse(EQRS >= 0.6, "Good",
                                  ifelse(EQRS >= 0.4, "Moderate",
                                         ifelse(EQRS >= 0.2, "Poor","Bad"))))]


print('R script finished running.')

intermediateFileName = paste0(intermediatePath,'/my_wk3.rds')
print(paste('Now writing intermediate files to:', intermediateFileName))
saveRDS(wk3, file = intermediateFileName)

print(paste('Now writing outputs to', outputPathComplete))
fwrite(wk3, file = outputPathComplete)
print(paste('R script wrote outputs to', outputPathComplete))

