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

startYear = as.numeric(swsContext.computationParams$startYear)
#startYear = as.numeric(2013)

endYear = as.numeric(swsContext.computationParams$endYear)
window = as.numeric(swsContext.computationParams$window)

#endYear = as.numeric(2017)

geoM49 = swsContext.computationParams$country_selection
stopifnot(startYear <= endYear)
yearVals = (startYear-window):endYear

##' Get data configuration and session
sessionKey = swsContext.datasets[[1]]

sessionCountries =
  getQueryKey("geographicAreaM49", sessionKey)

geoKeys = GetCodeList(domain = "agriculture", dataset = "aproduction",
                      dimension = "geographicAreaM49")[type == "country", code]




##Select the countries based on the user input parameter
 selectedGEOCode =
   switch(geoM49,
          "session" = sessionCountries,
          "all" = selectedCountries)


 
 itemKeys = GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredItemCPC")
 itemKeys = itemKeys[, code]
  
 ItemImputationSelection = swsContext.computationParams$items
 sessionItems=getQueryKey("measuredItemCPC", sessionKey)
 sessionItems =
     switch(ItemImputationSelection,
            "session" = sessionItems,
            "all" = itemkeys)
 
 sessionElements=getQueryKey("measuredElement", sessionKey)
 

#########################################
##### Pull from SUA unbalanced data #####
#########################################

message("Pulling aprod Data")

#take geo keys
geoDim = Dimension(name = "geographicAreaM49", keys = selectedGEOCode)

#Define element dimension. These elements are needed to calculate net supply (production + net trade)

eleKeys <- GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredElement")
eleKeys = eleKeys[, code]

eleDim <- Dimension(name = "measuredElement", keys = sessionElements)

#Define item dimension


itemDim <- Dimension(name = "measuredItemCPC", keys = sessionItems)


# Define time dimension

timeDim <- Dimension(name = "timePointYears", keys = as.character(yearVals))

#Define the key to pull SUA data
key = DatasetKey(domain = "agriculture", dataset = "aproduction", dimensions = list(
  geographicAreaM49 = geoDim,
  measuredElement = eleDim,
  measuredItemCPC = itemDim,
  timePointYears = timeDim
))




data = GetData(key, omitna = FALSE, normalized = FALSE)
data=normalise(data, areaVar = "geographicAreaM49",
               itemVar = "measuredItemCPC", elementVar = "measuredElement",
               yearVar = "timePointYears", flagObsVar = "flagObservationStatus",
               flagMethodVar = "flagMethod", valueVar = "Value",
               removeNonExistingRecords = FALSE)





###########################################################
##### calculate historical means                     #####
###########################################################
interval<-(startYear-1):(startYear-window)

data[,
     Meanold := mean(Value[timePointYears%in% interval & timePointYears>2010], na.rm = TRUE),
     by = c('geographicAreaM49', 'measuredItemCPC', 'measuredElement')
    ]

data[is.na(data$Value), Value := 0]

data[, ratio := Value / Meanold]

data[,
     `:=`(
        outlierImput = abs(ratio-1) > .3 & flagObservationStatus == "I",
        outlierOff   = abs(ratio-1) > .5 & flagObservationStatus != "I"
      )]

data[,
     `:=`(
        outlier      = (outlierImput == TRUE | outlierOff == TRUE),
        outlierImput = NULL,
        outlierOff   = NULL
      )]

outlierList <- data[outlier == TRUE & timePointYears >= startYear]

outlierList <- nameData("agriculture", "aproduction", outlierList, except = "timePointYears")

################################################################
###########  send Email with outlier list, if any.   ###########
################################################################

if (nrow(outlierList) > 0) {
  bodyOutliers <- "The Email contains a list of production outliers"

  sendMailAttachment(outlierList, "outlierList", bodyOutliers)

  print("An with outliers was just sent to you.")
} else {
  print("Great, no outliers were found.")
}
