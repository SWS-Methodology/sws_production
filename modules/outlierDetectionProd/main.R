##' # detects outliers
##'
##' **Author: Carlo Del Bello**
##'
##' **Description:**
##'
##' This module is designed to identify outliers in the production module
##' 
##' 
##' **Inputs:**
##'
##' * aprod data


##'
##' **Flag assignment:**
##'
##' None



## load the library
library(faosws)
library(data.table)
library(faoswsUtil)
library(sendmailR)
library(dplyr)
library(faoswsUtil)
library(faoswsStandardization)
library(faoswsFlag)


# ## set up for the test environment and parameters
# R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
  message("Not on server, so setting up environment...")
  
  library(faoswsModules)
  SETT <- ReadSettings("modules/outlierDetectionProd/sws.yml")
  
  R_SWS_SHARE_PATH <- SETT[["share"]]  
  ## Get SWS Parameters
  SetClientFiles(dir = SETT[["certdir"]])
  GetTestEnvironment(
    baseUrl = SETT[["server"]],
    token = SETT[["token"]]
  )
}


THRESHOLD_NONIMPUTED <- 0.5
THRESHOLD_YIELD <- 0.3
THRESHOLD_IMPUTED <- 0.3

message("PRODOUT: parameters")

# The first year is the first VALIDATION year, e.g., 2016
startYear <- as.numeric(swsContext.computationParams$startYear)

# The last year is the last VALIDATION year, e.g., 2018
endYear <- as.numeric(swsContext.computationParams$endYear)

## Number of years we will use in average of `window` years (e.g., 3)
#window <- as.numeric(swsContext.computationParams$window)
window <- 3

only_protected <- as.logical(swsContext.computationParams$only_protected)

# Years used in the average
interval <- (startYear-window):(startYear-1)

geoM49 <- swsContext.computationParams$country_selection

stopifnot(startYear <= endYear)

# All years required (validation years plus `window` validated years)
yearVals <- (startYear-window):endYear

##' Get data configuration and session
sessionKey <- swsContext.datasets[[1]]

sessionCountries <-
  getQueryKey("geographicAreaM49", sessionKey)

geoKeys <- GetCodeList(domain = "agriculture", dataset = "aproduction",
                      dimension = "geographicAreaM49")[type == "country", code]




##Select the countries based on the user input parameter
selectedGEOCode <-
  switch(geoM49,
         "session" = sessionCountries,
         "all" = geoKeys)



itemKeys <- GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredItemCPC")
itemKeys <- itemKeys[, code]
 
ItemImputationSelection <- swsContext.computationParams$items

sessionItems <- getQueryKey("measuredItemCPC", sessionKey)

sessionItems <-
    switch(ItemImputationSelection,
           "session" = sessionItems,
           "all" = itemkeys)

sessionElements <- getQueryKey("measuredElement", sessionKey)
 

#########################################
##### Pull from SUA unbalanced data #####
#########################################

message("PRODOUT: pulling aprod data")

#take geo keys
geoDim <- Dimension(name = "geographicAreaM49", keys = selectedGEOCode)

#Define element dimension. These elements are needed to calculate net supply (production + net trade)

eleKeys <- GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredElement")

eleKeys <- eleKeys[, code]

eleDim <- Dimension(name = "measuredElement", keys = sessionElements)

#Define item dimension


itemDim <- Dimension(name = "measuredItemCPC", keys = sessionItems)


# Define time dimension

timeDim <- Dimension(name = "timePointYears", keys = as.character(yearVals))

#Define the key to pull SUA data
key <-
  DatasetKey(
    domain = "agriculture",
    dataset = "aproduction",
    dimensions = list(
      geographicAreaM49 = geoDim,
      measuredElement   = eleDim,
      measuredItemCPC   = itemDim,
      timePointYears    = timeDim
    )
  )


data <- GetData(key, omitna = FALSE)

data_flags <- data[, .(geographicAreaM49, measuredElement,  measuredItemCPC, timePointYears, flagObservationStatus, flagMethod)]

data <- merge(data, flagValidTable, by = c("flagObservationStatus", "flagMethod"), all.x = TRUE)

data[, c("flagObservationStatus", "flagMethod", "Valid") := NULL]

d <- dcast.data.table(data, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value', 'Protected'))

d[, computed := NA_character_]

# Both production and yield are protected
d[
  Protected_5510 == TRUE & Protected_5312 == TRUE & Value_5312 > 0,
  `:=`(
    Value_5421 = Value_5510 / Value_5312,
    computed = "yield"
  )
]

# Only production is protected
d[
  Protected_5510 == TRUE & Protected_5312 == FALSE & Value_5421 > 0,
  `:=`(
    Value_5312 = Value_5510 / Value_5421,
    computed = "area"
  )
]


# Only area is protected
d[
  Protected_5510 == FALSE & Protected_5312 == TRUE & Value_5421 > 0,
  `:=`(
    Value_5510 = Value_5421 * Value_5312,
    computed = "production"
  )
]



d_flags <- merge(data_flags, d[!is.na(computed), .(geographicAreaM49, measuredItemCPC, timePointYears, computed)], by = c("geographicAreaM49", "measuredItemCPC", "timePointYears"), all.x = TRUE)


d_flags <- d_flags[!is.na(flagObservationStatus)]

d_flags[computed == "yield", computed := "5421"]
d_flags[computed == "production", computed := "5510"]
d_flags[computed == "area", computed := "5312"]

d_flags[measuredElement == computed, flagMethod := "i"]


d_flags[,
  flag1 :=
    ifelse(
      any(!is.na(computed)),
      faoswsFlag::aggregateObservationFlag(flagObservationStatus[measuredElement != computed]),
      NA_character_
    ),
  .(geographicAreaM49, measuredItemCPC, timePointYears)
]

d_flags[measuredElement == computed, flagObservationStatus := flag1]

d_modified <- d_flags[measuredElement == computed]

d_flags[, flag1 := NULL]
d_flags[, computed := NULL]


#data <- GetData(key, omitna = FALSE, normalized = FALSE)
#
#data <- normalise(data, areaVar = "geographicAreaM49",
#               itemVar = "measuredItemCPC", elementVar = "measuredElement",
#               yearVar = "timePointYears", flagObsVar = "flagObservationStatus",
#               flagMethodVar = "flagMethod", valueVar = "Value",
#               removeNonExistingRecords = FALSE)



###########################################################
##### calculate historical means                     #####
###########################################################

message("PRODOUT: historical means")

data[,
  Meanold := mean(Value[timePointYears %in% interval], na.rm = TRUE),
  by = c('geographicAreaM49', 'measuredItemCPC', 'measuredElement')
]

data[is.na(data$Value), Value := 0]

data[, ratio := Value / Meanold]

data[,
  `:=`(
     outlierImput = abs(ratio-1) > THRESHOLD_IMPUTED & flagObservationStatus == "I",
     outlierOff   = abs(ratio-1) > THRESHOLD_NONIMPUTED & flagObservationStatus != "I"
   )
]

if (only_protected == TRUE) {
  what_out <- "NON-IMPUTED"
  data[, outlier := outlierOff]
} else {
  what_out <- "IMPUTED AND NON-IMPUTED"
  data[, outlier := (outlierImput == TRUE | outlierOff == TRUE)]
}

data[, c("outlierImput", "outlierOff") := NULL]

outlierList <- data[outlier == TRUE & timePointYears >= startYear]

outlierList <- nameData("agriculture", "aproduction", outlierList, except = "timePointYears")

################################################################
###########  send Email with outlier list, if any.   ###########
################################################################

if (nrow(outlierList) > 0) {
  bodyOutliers <-
    paste("The Email contains a list of", what_out, "production outliers")

  sendMailAttachment(outlierList, "outlierList", bodyOutliers)

  print("An email with outliers was just sent to you.")
} else {
  print("Great, no outliers were found.")
}

