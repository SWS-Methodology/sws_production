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

#########################################
##### Pull from SUA unbalanced data #####
#########################################

message("Pulling aprod Data")

#take geo keys
geoDim = Dimension(name = "geographicAreaM49", keys = selectedGEOCode)

#Define element dimension. These elements are needed to calculate net supply (production + net trade)

eleKeys <- GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredElement")
eleKeys = eleKeys[, code]

eleDim <- Dimension(name = "measuredElement", keys = c("5510","5312","5421"))

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




data = GetData(key,omitna = F, normalized=F)
data=normalise(data, areaVar = "geographicAreaM49",
               itemVar = "measuredItemCPC", elementVar = "measuredElement",
               yearVar = "timePointYears", flagObsVar = "flagObservationStatus",
               flagMethodVar = "flagMethod", valueVar = "Value",
               removeNonExistingRecords = F)





###########################################################
##### calculate historical means                     #####
###########################################################
data <-data %>% group_by(geographicAreaM49,measuredItemCPC,measuredElement) %>% mutate(Meanold=mean(Value[timePointYears<2017 & timePointYears>2010],na.rm=T))
data$Value[is.na(data$Value)] <- 0


data <-data %>% mutate(ratio=Value/Meanold)


data <-data %>% mutate(outlierImput=abs(ratio-1)>.3 & flagObservationStatus=="I")
data <-data %>% mutate(outlierOff=abs(ratio-1)>.5 & flagObservationStatus!="I")
#data <-data %>% mutate(outlier=(outlierImput==T | outlierOff==T | is.na(flagObservationStatus)) )
data <-data %>% mutate(outlier=(outlierImput==T | outlierOff==T) )


setDT(data)
data[,c("outlierImput","outlierOff"):=NULL]

outlierList=subset(data,outlier==T & timePointYears==2017)

outlierList <-nameData("agriculture","aproduction",outlierList)
outlierList[,timePointYears_description := NULL]

bodyOutliers= paste("The Email contains a list of production outliers",
                    sep='\n')

sendMailAttachment(outlierList,"outlierList",bodyOutliers)



################################################################
#####  send Email with outlier list   #####
################################################################


