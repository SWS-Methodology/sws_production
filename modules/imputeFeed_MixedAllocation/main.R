# UPLOAD TEMPORARY FUNCTIONS
## The following code is only needed if R function are uploaded in the SWS without changing the faoswsfeed module
## this is highly NOT recommended

# adjustement by CSI to have the plug in working with run docker slow
system("wget 'https://production-sws-rcranrepo.s3-eu-west-1.amazonaws.com/src/contrib/faosws_0.9.8.tar.gz' -O faosws_0.9.8.tar.gz")
install.packages('faosws_0.9.8.tar.gz' , lib='/opt/R/3.1.2/lib/R/library/' , type ='source')
paste(packageVersion('faosws'), "")

system("wget 'https://production-sws-rcranrepo.s3.eu-west-1.amazonaws.com/src/contrib/faoswsFeed_0.4.7.tar' -O faoswsFeed_0.4.7.tar.gz")
install.packages('faoswsFeed_0.4.7.tar.gz' , lib='/opt/R/3.1.2/lib/R/library/' , type ='source')



sapply(dir("R", full.names = TRUE), source)

# UPLOAD REQUIRED PACKAGES
library(faosws)
suppressPackageStartupMessages(library(data.table))
library(faoswsFeed)
library(tidyr)
library(faoswsUtil)
library(lpSolve)
library(dplyr)
library(plm)
library(faoswsFlag)

# SET ENVIRONMENT

if (CheckDebug()) {
  
  library(faoswsModules)
  SETTINGS <- ReadSettings("modules/impute_feed/sws.yml")
  
  SetClientFiles(SETTINGS[["certdir"]])
  GetTestEnvironment(SETTINGS[["server"]], SETTINGS[["token"]])
  
  R_SWS_SHARE_PATH = "//hqlprsws1.hq.un.fao.org/sws_r_share"
}

if (!CheckDebug()) {
  
  R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
  
}

# USER <- regmatches(
#   swsContext.username,
#   regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
# )

USER <- if(grepl( '@', swsContext.username, fixed = TRUE) == FALSE){
  
  regmatches(swsContext.username,regexpr("(?<=/).+$", swsContext.username, perl = TRUE))}else{
    
    
    
    regmatches(swsContext.username,regexpr("^([^.])+(?=\\.)", swsContext.username, perl = TRUE))
    
  }

TMP_DIR <- file.path(tempdir(), USER)
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)

# START FEED ROUTINE

## DEFINE YEARS
# giulia changed years definition

# if (!CheckDebug()) {
#   startYear = as.numeric(swsContext.computationParams$startYear)
#   endYear = as.numeric(swsContext.computationParams$endYear)
# }else{
#   startYear = as.numeric(2014)
#   endYear = as.numeric(2018)
# }

startYear = as.numeric(swsContext.computationParams$startYear)
endYear = as.numeric(swsContext.computationParams$endYear)
stopifnot(startYear <= endYear)
yearVals = startYear:endYear

## LOAD LABOR PRODUCTIVITY DATA (WDI) AND FILL IN MISSING YEAR VALUES BY A CARRY FORWARD/BACKWARD PROCEDURE
wdi_data = generate_wdi(startYear,endYear,countries=getQueryKey("geographicAreaM49"))

## UPLOAD FEED NUTRIENTS FROM SWS
### On 2022-10-22 feedNutrients have been moved to a DataTable
### On 2020-02-27 some items were revised.
myfeedNutrients<-ReadDatatable("feed_nutrients")
setnames(myfeedNutrients,colnames(myfeedNutrients),c("measuredItemCPC", "energyContent","proteinContent","feedClassification"))
myfeedNutrients[,energyContent:=as.numeric(energyContent)]
myfeedNutrients[,proteinContent:=as.numeric(proteinContent)]

## CALCULATE FEED DEMAND
feedDemand = calculateFeedDemand()

## DEFINE POTENTIAL FEEDS: All feed items excluding Oil meals, meals and brans)
potentialFeeds = myfeedNutrients[feedClassification == "Potential Feed", measuredItemCPC]

## DEFINE FEED ONLY: Protein meals and Items that have only feed purpose
feedOnlyFeeds = myfeedNutrients[feedClassification == "FeedOnly", measuredItemCPC]

## GET OFFICIAL FEED DATA
### define measuredElement for Feed
feedItem = "5520"
officialKey = DatasetKey(domain = "suafbs", dataset = "sua_unbalanced",
                         dimensions = list(
                           Dimension(name = "geographicAreaM49", keys = getQueryKey("geographicAreaM49")),
                           Dimension(name = "measuredItemFbsSua", keys = c(potentialFeeds, feedOnlyFeeds)),
                           Dimension(name = "measuredElementSuaFbs", keys = feedItem),
                           Dimension(name = "timePointYears", keys = as.character(startYear:endYear))
                         ),
                         sessionId =  slot(swsContext.datasets[[1]], "sessionId")
)


officialFeed = GetData(officialKey)

officialFeed[, measuredElementSuaFbs := NULL]

setnames(officialFeed, "Value", "feed")

## UPLOAD FLAGS
### Read flag table from sws
FlagTable <- ReadDatatable("valid_flags")

FlagTable[is.na(flagObservationStatus),flagObservationStatus:=""]

### Set (e,e) flags as not protected
FlagTable[flagObservationStatus=="E" & flagMethod=="e",Protected:=FALSE]
FlagTable[flagObservationStatus=="E" & flagMethod=="e",Valid:=FALSE]

## EXTRACT NOT PROTECTED FIGURES, DELETE VALUES AND SAVE ALLT HE ZEROES BACK 

NonProtectedFeed <-
  left_join(
    officialFeed,
    FlagTable,
    by = c("flagObservationStatus", "flagMethod")
  ) %>%
  dplyr::filter(Protected == FALSE) %>%
  dplyr::select(-Protected, -Valid)

if (nrow(NonProtectedFeed) > 0) {
  NonProtectedFeed = dplyr::rename(NonProtectedFeed,Value = feed)
  NonProtectedFeed$Value = NA_real_
  NonProtectedFeed$flagObservationStatus = NA_character_
  NonProtectedFeed$flagMethod = NA_character_
  NonProtectedFeed$measuredElementSuaFbs = "5520"
  NonProtectedFeed = as.data.table(NonProtectedFeed)
  
  results1 <- SaveData("suafbs", "sua_unbalanced", NonProtectedFeed)
}

## CREATE OBJECT WITH OFFICIAL FIGURES (=PROTECTED) 
### NOTE: below, "official" was changed to "Protected"
### Official feed is only that with official flags
officialFeed <-
  left_join(
    officialFeed,
    FlagTable,
    by = c("flagObservationStatus", "flagMethod")
  ) %>%
  dplyr::filter(Protected == TRUE) %>%
  dplyr::select(-Protected, -Valid) %>%
  setDT()

# extract official and semi official feed in the time-series to compute feed estimates based on official ratios

key_suaunBal <- DatasetKey(
  domain = "suafbs",
  dataset = "sua_unbalanced",
  dimensions = list(
    Dimension(name = "geographicAreaM49", keys = getQueryKey("geographicAreaM49")),
    Dimension(name = "measuredElementSuaFbs", keys = c("5510","5610","5910","5520")),
    Dimension(name = "measuredItemFbsSua", keys = c(potentialFeeds, feedOnlyFeeds)),
    Dimension(name = "timePointYears", keys = as.character(2010:endYear))
  )
)

suaunBalData <- GetData(key_suaunBal)

suaunBalData <- dcast.data.table(suaunBalData, geographicAreaM49+measuredItemFbsSua+timePointYears~ measuredElementSuaFbs, 
                               value.var = c("Value","flagObservationStatus","flagMethod"))
#compute net trade
suaunBalData [, netTrade := sum(Value_5610, -Value_5910, na.rm = TRUE), by= c("geographicAreaM49",
                                                                            "measuredItemFbsSua","timePointYears")]
#compute supply

suaunBalData[, supply := sum(Value_5510,netTrade,na.rm = TRUE),by= c("geographicAreaM49",
                                                                   "measuredItemFbsSua","timePointYears")]
suaunBalData[supply<0, supply:=0]

# past data for ratio

official_series<- suaunBalData[ timePointYears %in% (2010: startYear-1)]

#subsetting official/semiofficial feed
official_series <- subset(suaunBalData, flagObservationStatus_5520 %in% c("","T"))

#compute feed ratio over supply

official_series[,feedRatio := Value_5520/supply, by= c("geographicAreaM49",
                                                  "measuredItemFbsSua","timePointYears")]
#delete ratios higher than 1 or lower than 0

official_series <- official_series[!(feedRatio < 0 | feedRatio > 1)]

#last official ratio
official_series <- official_series[order(measuredItemFbsSua,timePointYears)]

last_feed_official_ratio<- official_series[, .SD[.N], by= c("geographicAreaM49",
                               "measuredItemFbsSua")]  [,c("geographicAreaM49","measuredItemFbsSua","feedRatio"),with = FALSE]  

# availability of current years

AvailabilityforRatio <- suaunBalData[timePointYears %in% (startYear:endYear)]

AvailabilityforRatio<- AvailabilityforRatio[ , c(1,2,3,17), with=FALSE]


##merge feedratio 

AvailabilityforRatio <- merge(AvailabilityforRatio, last_feed_official_ratio, by=c("geographicAreaM49","measuredItemFbsSua"), all.x = TRUE )

AvailabilityforRatio  <- AvailabilityforRatio[!is.na(feedRatio)]

AvailabilityforRatio<- AvailabilityforRatio[supply>0 | !is.na(supply)]

AvailabilityforRatio[, feed := supply * feedRatio, by=c("geographicAreaM49","measuredItemFbsSua")]

AvailabilityforRatio[, c("supply","feedRatio") := NULL]

AvailabilityforRatio[, `:=` (flagObservationStatus = "I", flagMethod = "c")]

# rbind to officialFeed

officialFeedItems <- unique(officialFeed[,c("geographicAreaM49","measuredItemFbsSua","timePointYears"), with = F]) 

AvailabilityforRatio <- AvailabilityforRatio[ !officialFeedItems]

officialFeed <- rbind(officialFeed,AvailabilityforRatio)



## RETRIEVE AVAILABILITY (SUPPLY) OF FEED ONLY ITEMS 
### Only those with no official data
feedOnlyAvailability = feedAvail_from_sua_unb(c("production", "imports", "exports"), 
                                              measuredItem = feedOnlyFeeds, officialData = officialFeed, negate = TRUE)

setnames(feedOnlyAvailability, "measuredItemFbsSua", "measuredItemCPC")

feedOnlyNutrients = merge(feedOnlyAvailability, myfeedNutrients, all.x = T, by = "measuredItemCPC")

## CALCULATE NUTRIENT AVAILABILITY OF FEED ONLY ITEMS 
### Calculate feed aìvailability
feedOnlyNutrients[, feedOnlyEnergyAvailability := feedAvailability * energyContent]
feedOnlyNutrients[, feedOnlyProteinAvailability := feedAvailability * proteinContent]

### aggregate energy and protein availability from all feed only items 
feedOnlyNutrientSupply = feedOnlyNutrients[, lapply(.SD, sum, na.rm = TRUE), by = .(geographicAreaM49, timePointYears),
                                           .SDcols = c("feedOnlyEnergyAvailability", "feedOnlyProteinAvailability")]


### merge with demand data
minusfeedOnlyDemand = merge(feedDemand, feedOnlyNutrientSupply, all.x = T)

## SUBTRACT AVAILABILITY OF FEED ONLY ITEMS FROM DEMAND 
minusfeedOnlyDemand[, minusfeedOnlyEnergyDemand := energyDemand - feedOnlyEnergyAvailability]
minusfeedOnlyDemand[, minusfeedOnlyProteinDemand := proteinDemand - feedOnlyProteinAvailability]

## SEND EMAIL IF AVAILABILITY FROM FEED ONLY ITEMS IS BIGGER THAN DEMAND
### In this case Figures of feed only item should be revised 
### in order not to allocate more feed that required
### An email is sent with the data to revise
### the module continue working but no other allocation is expected, as the demand is already been covered
if(nrow(minusfeedOnlyDemand[minusfeedOnlyEnergyDemand < 0]) > 0| 
   nrow(minusfeedOnlyDemand[minusfeedOnlyProteinDemand < 0]) > 0){
  
  if(CheckDebug()){
    message("Nutrient supply from feed only items (oil cakes and cereal brans) is already larger than nutrient demand of livestock. Please check the attached 
            crop supply table and commodities with highest percentage contribution to meet nutrient demand (EnergyPercent, ProteinPercent).")
  }else{
    Output = merge(feedOnlyNutrients, feedDemand, by = c("timePointYears","geographicAreaM49"))
    Output[, ProteinPercent := (feedOnlyProteinAvailability / proteinDemand) *100]
    Output[, EnergyPercent := (feedOnlyEnergyAvailability / energyDemand) *100]
    # Output = Output[order(-EnergyPercent),.(measuredItemCPC, geographicAreaM49, timePointYears, feedAvailability, ProteinPercent, EnergyPercent) ]
    Output = Output[order(-EnergyPercent), ]
    
    setnames(Output, "measuredItemCPC", "measuredItemFbsSua")
    Output = nameData(domain = "suafbs", "sua_unbalanced", Output)
    TMP_DIR <- file.path(tempdir(), USER)
    if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)
    
    for (g in Output[feedOnlyEnergyAvailability>energyDemand,unique(geographicAreaM49)]){
      text = paste("Feed only supply larger than feed demand for the following country: "
                   ,g
                   ,"Nutrient supply from feed only items (oil cakes and cereal brans) is already larger than nutrient demand of livestock. Please check the attached 
                   crop supply table and commodities with highest percentage contribution to meet nutrient demand (EnergyPercent, ProteinPercent)."
                   ,sep='\n')
      tmp_file_feedOnly <- file.path(TMP_DIR, paste0("FeedOnlySupply_", g, ".csv"))
      write.csv(Output[geographicAreaM49==g], tmp_file_feedOnly, row.names = FALSE,na="")
      send_mail(
        from = "sws@fao.org",
        to = swsContext.userEmail,
        subject = "Feed only supply larger than feed demand",
        body = c(text,tmp_file_feedOnly)
      )
    }
    unlink(TMP_DIR, recursive = TRUE)
  }
}

### old comment kept
# For validation: Here, we need a scatterplot with log(feedonlyProteinAvailability) on the x - axis 
# and log(energydemand)  on y axis

## SET TO ZERO AL NEGATIVE RESIDUAL DEMAND AFTER SUBTRACTION OF FEED ONLY AVAILABILITIES 
minusfeedOnlyDemand[minusfeedOnlyEnergyDemand < 0, minusfeedOnlyEnergyDemand := 0]
minusfeedOnlyDemand[minusfeedOnlyProteinDemand < 0, minusfeedOnlyProteinDemand := 0]


## SUBTRACT OFFICIAL FEED

### pull all official feed figures, so all feed elements with flag " "
setnames(officialFeed, "measuredItemFbsSua", "measuredItemCPC")

officialFeedNutrients = merge(officialFeed, myfeedNutrients, all.x = T, by = "measuredItemCPC")

### Calculate nutrient availability
officialFeedNutrients[, officialFeedEnergyAvailability := feed * energyContent]
officialFeedNutrients[, officialFeedProteinAvailability := feed * proteinContent]

### aggregate energy and protein availability from different items 
officialFeedNutrientSupply = officialFeedNutrients[, lapply(.SD, sum, na.rm = TRUE), 
                                                   by = .(geographicAreaM49, timePointYears),
                                                   .SDcols = c("officialFeedEnergyAvailability", 
                                                               "officialFeedProteinAvailability")]
### merge with residual demand
residualFeedDemand = merge(minusfeedOnlyDemand, officialFeedNutrientSupply, all.x = TRUE)

### Remove all NAs introduced by the merge
residualFeedDemand[is.na(residualFeedDemand)] <- 0

### subtract official feed nutrients
residualFeedDemand[, residualEnergyDemand := minusfeedOnlyEnergyDemand - officialFeedEnergyAvailability]
residualFeedDemand[, residualProteinDemand := minusfeedOnlyProteinDemand - officialFeedProteinAvailability]

### All that are less than 0 become 0
residualFeedDemand[residualEnergyDemand < 0, residualEnergyDemand := 0]
residualFeedDemand[residualProteinDemand < 0, residualProteinDemand := 0]


## CALCULATE AVAILABILITY OF POTENTIAL FEED ITEMS
setnames(officialFeed, "measuredItemCPC", "measuredItemFbsSua")

### Retrieve Potential feed items data
feedAvailability = feedAvail_from_sua_unb(vars = c("production", "imports", "exports", "food", "processed"), 
                                          measuredItem = potentialFeeds, officialData = officialFeed, negate = TRUE)

setnames(feedAvailability, "measuredItemFbsSua", "measuredItemCPC")

### Apply nutritive factors and calculate nutrient availability
feedAvailabilityData <- merge(feedAvailability, myfeedNutrients, all.x = T,by="measuredItemCPC")

feedAvailabilityData[is.na(energyContent), energyContent := 0]
feedAvailabilityData[is.na(proteinContent), proteinContent := 0]

feedAvailabilityData[, energyAvailability := feedAvailability * energyContent]
feedAvailabilityData[, proteinAvailability := feedAvailability * proteinContent]

### Sum nutrient Availabilities for each country and year (for construction of shares)
nutrientAvailability = feedAvailabilityData[, lapply(.SD, sum, na.rm = TRUE), 
                                            by = .(geographicAreaM49, timePointYears),
                                            .SDcols = c("energyAvailability", 
                                                        "proteinAvailability")]
### change names for merging later
setnames(nutrientAvailability,  c("energyAvailability", "proteinAvailability"),
         c("sumEnergyAvailability", "sumProteinAvailability"))

### merge back with availability data 
availabilityData = merge(feedAvailabilityData, nutrientAvailability, 
                         by = c("geographicAreaM49", "timePointYears"), all.x = T)


## DEFINE SHARES OF FEED AVAILABILITY 
availabilityData[, energyShare := energyAvailability / sumEnergyAvailability]
availabilityData[, proteinShare := proteinAvailability / sumProteinAvailability]

### Remove NaNs introduced by 0 / 0
availabilityData[is.na(energyShare), energyShare := 0]
availabilityData[is.na(proteinShare), proteinShare := 0]

## MERGE DEMAND AND AVAILBILITY DATA
availabilityDemand = merge(residualFeedDemand, availabilityData, 
                           by = c("geographicAreaM49", "timePointYears"))

## ALLOCATE FEED
### Allocate feed with mixed methodology 

#### apply availability shares to demand and convert back to quantities

feedAllocated = allocate_mixed(availabilityDemand=availabilityDemand)

#### allocated Feed
allocatedFeed = feedAllocated[ ,.(geographicAreaM49, measuredItemFbsSua = measuredItemCPC, timePointYears, FINALallocatedFeed)]
setnames(allocatedFeed, "FINALallocatedFeed", "feed")

## SET FLAGS

### Flags for allocated Feeds (I,e)
allocatedFeed[, `:=`(flagObservationStatus = "I",
                     flagMethod = "e")]

### lags for Feed-only Feed (I,b)
feedOnlyFeed = feedOnlyAvailability[, .(geographicAreaM49, measuredItemFbsSua = measuredItemCPC, timePointYears, feedAvailability)]
setnames(feedOnlyFeed, "feedAvailability", "feed")    

feedOnlyFeed[, `:=`(flagObservationStatus = "I",
                    flagMethod = "b")]

## RBIND ALL DATASETS 
feedData <- rbind(allocatedFeed, feedOnlyFeed)
feedData[,measuredElementSuaFbs := feedItem]
setnames(feedData, "feed", "Value")

## CHECK TOO HIGH VALUES AND SEND EMAIL
options(scipen = 100000000)
if(nrow(feedData[Value >= 10e20])==0){
  if(CheckDebug()){
    text = paste("No too high values detected. "
                 ,"The limit for values removal = "
                 ,10e20
                 ,". The maximum value allocated is "
                 ,feedData[,max(Value)]
                 
                 ," for the following country - year - commodity: "
                 ,paste(feedData[Value ==max(Value),.(geographicAreaM49,timePointYears,measuredItemFbsSua)],collapse = " - ")
                 ,sep='\n')
    
    message(strsplit(text,"\n")[[1]])
  }else{
    text = paste("No too high values detected. "
                 ,"The limit for values removal is"
                 ,10e20
                 ,"The maximum value allocated is "
                 ,feedData[,max(Value)]
                 ,"for the following country - year - commodity: "
                 ,paste(feedData[Value ==max(Value),.(geographicAreaM49,timePointYears,measuredItemFbsSua)],collapse = " - ")
    )
    
    send_mail(
      from = "sws@fao.org",
      to = swsContext.userEmail,
      subject = "NO too high values",
      body = c(text)
    )
  }
}else{
  
  highValues = feedData[Value >= 10e20]
  
  if(CheckDebug()){
    text = paste("Too high values detected. "
                 ,"Limit for values removal = "
                 ,10e20
                 ,". The maximum value allocated is"
                 ,feedData[,max(Value)]
                 ," for the following country - year - commodity: "
                 ,paste(feedData[Value ==max(Value),.(geographicAreaM49,timePointYears,measuredItemFbsSua)],collapse = " - ")
                 ,". The total number of row with high values is "
                 ,nrow(highValues)
                 ,". These values have been changed to the limit value."
                 ,sep='\n')
    
    message(strsplit(text,"\n")[[1]])
  }else{
    text = paste("Too high values detected."
                 ,"The limit for values removal is "
                 ,10e20
                 ,"The maximum value allocated is "
                 ,feedData[,max(Value)]
                 ,"for the following country - year - commodity: "
                 ,paste(feedData[Value ==max(Value),.(geographicAreaM49,timePointYears,measuredItemFbsSua)],collapse = " - ")
                 ,"The total number of row with high values is"
                 ,nrow(highValues)
                 ,"These values have been changed to the limit value."
                 ,"The table of too hig values is attached for revision.")
    
    TMP_DIR <- file.path(tempdir(), USER)
    if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)
    tmp_file_high <- file.path(TMP_DIR, paste0("labor_wdi_data_", i, ".csv"))
    write.csv(highValues, tmp_file_high, row.names = FALSE,na="")
    send_mail(
      from = "sws@fao.org",
      to = swsContext.userEmail,
      subject = "Too high values allocated",
      body = c(text,tmp_file_high)
    )
    unlink(TMP_DIR, recursive = TRUE)
    
  }
  # feedData <- feedData[Value < 10e20,]
  feedData[Value >= 10e20,Value:=10e20]
  
}
# 
setcolorder(feedData, c("geographicAreaM49", "measuredElementSuaFbs", "measuredItemFbsSua", "timePointYears", "Value", "flagObservationStatus", "flagMethod"))
setkey(feedData, geographicAreaM49, measuredElementSuaFbs, measuredItemFbsSua, timePointYears)

## REMOVE ZERO AND NEAR-ZERO VALUES
feedData[dplyr::near(Value, 0), Value := NA]
feedData <- feedData[!is.na(Value)]

## MERGE DATA WITH FLAGS
feedData <- merge(feedData, FlagTable, by = c("flagObservationStatus", "flagMethod"), all.x = TRUE)

feedData[is.na(Protected), Protected := FALSE]

feedData <- feedData[Protected != TRUE][, c("Protected", "Valid") := NULL]

# add feed imputed with official and semi official ratios
AvailabilityforRatio<- AvailabilityforRatio[ , measuredElementSuaFbs:= "5520"]
AvailabilityforRatio <- setnames(AvailabilityforRatio, "feed", "Value")
setcolorder(AvailabilityforRatio, c("geographicAreaM49", "measuredElementSuaFbs", "measuredItemFbsSua", "timePointYears", "Value", "flagObservationStatus", "flagMethod"))

feedData<- rbind(feedData, AvailabilityforRatio)

# SAVE DATA BACK TO THE SWS
results <- SaveData("suafbs", "sua_unbalanced", feedData)

## Assigning to new object to remove warnings for output but still have them around in case of debugging
results_out <- results
results_out[["warnings"]] <- NULL
paste0(paste(names(results_out), unlist(results_out), collapse = "\n", sep = ": "), "\n\nModule completed successfully")

