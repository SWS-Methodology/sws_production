# Create Indigenous Meat Production
##'
##' **Author: Giulia Piva**
##'
##' **Description:**
##'
##' This module is designed to harvest the data from Trade and Production domain and create Indigenous Meat Production as: (SLaughtered - Imported)*carcass weight
##'


## load the library
library(faosws)
library(data.table)
library(faoswsUtil)
library(sendmailR)
library(tidyr)
library(faoswsFlag)
library(faoswsProcessing)
library(faoswsEnsure)
library(magrittr)
library(dplyr)
library(sendmailR)


## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
  message("Not on server, so setting up environment...")
  
  library(faoswsModules)
  SETT <- ReadSettings("sws.yml")
  
  #R_SWS_SHARE_PATH <- SETT[["share"]]  
  ## Get SWS Parameters
  SetClientFiles(dir = SETT[["certdir"]])
  GetTestEnvironment(
    baseUrl = SETT[["server"]],
    token = SETT[["token"]]
  )
}

startYear = as.numeric(swsContext.computationParams$startYear)
endYear = as.numeric(swsContext.computationParams$endYear)
geoM49 = swsContext.computationParams$geom49
stopifnot(startYear <= endYear)
yearVals = startYear:endYear

##' Get data configuration and session
sessionKey = swsContext.datasets[[1]]

sessionCountries =
  getQueryKey("geographicAreaM49", sessionKey)

geoKeys = GetCodeList(domain = "agriculture", dataset = "aproduction",
                      dimension = "geographicAreaM49")[type == "country", code]

##' Select the countries based on the user input parameter
selectedGEOCode =
  switch(geoM49,
         "session" = sessionCountries,
         "all" = geoKeys)

################################################
##### Harvest from Agricultural Production #####
################################################

message("Pulling data from Agricultural Production")


geoDim = Dimension(name = "geographicAreaM49", keys = selectedGEOCode)

eleDim <- Dimension(name = "measuredElement", keys = c("5315", "5316","5417", "5424","54170","54240", "55100","5510"))


itemKeys = GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredItemCPC")
itemKeys = itemKeys[, code]

itemDim <- Dimension(name = "measuredItemCPC", keys = itemKeys)


timeDim = Dimension(name = "timePointYears", keys = as.character(yearVals))

agKey = DatasetKey(domain = "agriculture", dataset = "aproduction",
                   dimensions = list(
                     geographicAreaM49 = geoDim,
                     measuredElement = eleDim,
                     measuredItemCPC = itemDim,
                     timePointYears = timeDim)
)
agData = GetData(agKey)




################################################
##### Harvest from Trade #####
################################################

message("Pulling data from Trade")


geoDim = Dimension(name = "geographicAreaM49", keys = selectedGEOCode)


Trade_eleDim <- Dimension(name = "measuredElementTrade", keys = c("5608", "5609", "5908", "5909"))

Tradekey = DatasetKey(domain = "trade", dataset = "total_trade_cpc_m49", dimensions = list(
  geographicAreaM49 = geoDim,
  measuredElementTrade = Trade_eleDim,
  measuredItemCPC = itemDim,
  timePointYears = timeDim
))


TradeData = GetData(Tradekey)

# Separate Meat data and Live Animals data
ProductionData<-agData[measuredElement %in% c("5315", "5316")]
MeatData <- agData[measuredElement %in% c("5417", "5424","54170","54240", "55100","5510")]


# give labels
ProductionData <-nameData("agriculture", "aproduction", ProductionData)
TradeData<- nameData("trade", "total_trade_cpc_m49", TradeData)


#Merge trade and Live animals data

#prepare trade table for merging
colnames(TradeData)<-c( "geographicAreaM49","geographicAreaM49_description", "measuredElement","measuredElement_description","measuredItemCPC",
                        "measuredItemCPC_description","timePointYears", "timePointYears_description","Value","flagObservationStatus","flagMethod")



#merge
IndigenTable<-rbind(ProductionData,TradeData)


#create timeseries for indigenous slaughtered animals

IndigenTable<-rbind(IndigenTable, data.table(measuredElement="53200"),fill=T)


timeSeriesData<- as.data.table(expand.grid(measuredItemCPC = unique(IndigenTable$measuredItemCPC), 
                                           geographicAreaM49 = unique(IndigenTable$geographicAreaM49),
                                           measuredElement = unique(IndigenTable$measuredElement),
                                           timePointYears = c(startYear:endYear)))

timeSeriesData[, names(timeSeriesData) := lapply(.SD, as.character), .SDcols = names(timeSeriesData)]

timeSeriesData<- na.omit(timeSeriesData, cols=c("geographicAreaM49", "measuredItemCPC"))


#fill timeseries with values and na
IndigenTable2 <- merge(timeSeriesData, IndigenTable, by=c("measuredElement","measuredItemCPC","timePointYears","geographicAreaM49"), all.x= TRUE)


##Attribute code for (1000 head) to small animal

IndigenTable2[ , measuredElement := ifelse(measuredItemCPC %in% c("02151", "02154", "02153", "02194", "02192.01","02191","02152")
                                           & measuredElement=="53200", "53210", measuredElement)]

####create Indigenous animal number 


IndigenTable2[ , Value := ifelse(measuredElement =="53200", sum(Value[measuredElement=="5315"], -Value[measuredElement=="5608"],Value[measuredElement=="5908"], na.rm=TRUE),
                                 ifelse(measuredElement =="53210", sum(Value[measuredElement=="5316"], -Value[measuredElement=="5609"],Value[measuredElement=="5909"],na.rm=T),
                                        Value)),
               by = list(geographicAreaM49, measuredItemCPC, timePointYears)]



#correct too low (negative) values

IndigenTable2[ , Value := ifelse((measuredElement =="53200" |measuredElement=="53210") & Value <=0 , 1, Value)]



##correct too high values (where indigenous is higher than total slaughtered)


IndigenTable2[ , Diff:= ifelse(Value[get("measuredElement") %in% c("53200")] > Value[get("measuredElement") %in% c("5315")], TRUE,FALSE),
               by = list(`geographicAreaM49`, `measuredItemCPC`, `timePointYears`)] 

IndigenTable2[ , Diff2:= ifelse(Value[get("measuredElement") %in% c("53210")] > Value[get("measuredElement") %in% c("5316")], TRUE,FALSE),
               by = list(`geographicAreaM49`, `measuredItemCPC`, `timePointYears`)] 

IndigenTable2[ , Value:= ifelse(measuredElement=="53200" & Diff== TRUE, Value[measuredElement=="5315"],Value),
               by = list(`geographicAreaM49`, `measuredItemCPC`, `timePointYears`)]

IndigenTable2[ , Value:= ifelse(measuredElement=="53210" & Diff2== TRUE, Value[measuredElement=="5316"],Value),
               by = list(`geographicAreaM49`, `measuredItemCPC`, `timePointYears`)]


##Remove data where Slaughtered is NA or 0 
IndigenTable2[ , Delete:= ifelse(measuredItemCPC %in% c("02132",
                                                        "02112",
                                                        "02121.01",
                                                        "02111",
                                                        "02123",
                                                        "02131",
                                                        "02133",
                                                        "02121.02",
                                                        "02192",
                                                        "02122",
                                                        "02140") & is.na(Value[measuredElement=="5315"]), TRUE, FALSE),
               by= list(geographicAreaM49, measuredItemCPC, timePointYears)]

IndigenTable2[ , Delete2:= ifelse(measuredItemCPC %in% c("02151", "02154", "02153", "02194", "02192.01","02191","02152") &
                                    is.na(Value[measuredElement=="5316"]) , TRUE, FALSE),
               by= list(geographicAreaM49, measuredItemCPC, timePointYears)]

###>Subset Indigenous slughtered 


Animal_toupload<-IndigenTable2[measuredElement %in% c("53200", "53210"), c(1,2,3,4,9,10,11,14,15), with = FALSE]

Animal_toupload_notNA<-Animal_toupload[Delete==FALSE&Delete2==FALSE]
Animals<-Animal_toupload_notNA[ , c(1,2,3,4,5,6,7), with=FALSE]

Animals[ , flagObservationStatus:= "I"]
Animals[ , flagMethod:= "i"]

############################

#Create Indigenous MEAT Production


###create two separate tables for big and small animals


tableBIG<-Animals[measuredItemCPC %in% c("02132",
                                         "02112",
                                         "02121.01",
                                         "02111",
                                         "02123",
                                         "02131",
                                         "02133",
                                         "02121.02",
                                         "02192",
                                         "02122",
                                         "02140")]

tableSMALL<- Animals[measuredItemCPC %in% c("02151", "02154", "02153", "02194", "02192.01","02191","02152")]

###Calculations for BIG animals

#match code for live animals with code for meat 

tableBIG[ , measuredItemCPC:= ifelse(measuredItemCPC=="02111", "21111.01", ifelse(measuredItemCPC=="02153", "21123",
                                                                                  ifelse(measuredItemCPC=="02112", "21112",
                                                                                         ifelse(measuredItemCPC=="02154", "21122",
                                                                                                ifelse(measuredItemCPC=="02140", "21113.01",
                                                                                                       ifelse(measuredItemCPC=="02152", "21124",
                                                                                                              ifelse(measuredItemCPC=="02122", "21115",
                                                                                                                     ifelse(measuredItemCPC=="02121.01", "21117.01",
                                                                                                                            ifelse(measuredItemCPC=="02123", "21116",
                                                                                                                                   ifelse(measuredItemCPC=="02191", "21114",
                                                                                                                                          ifelse(measuredItemCPC=="02131", "21118.01",
                                                                                                                                                 ifelse(measuredItemCPC=="02132", "21118.02",
                                                                                                                                                        ifelse(measuredItemCPC=="02194", "21170.01",
                                                                                                                                                               ifelse(measuredItemCPC=="02121.02", "21117.02",
                                                                                                                                                                      ifelse(measuredItemCPC=="02151", "21121",
                                                                                                                                                                             ifelse(measuredItemCPC=="02133", "21118.03",NA))))))))))))))))]


##Merge useful data

MeatDataBIG<-MeatData[measuredItemCPC %in% c(unique(tableBIG$measuredItemCPC))]

tableBIG<- rbind(tableBIG, MeatDataBIG)

#Clean dataset

towipe <-
  tableBIG[measuredElement=="55100" | measuredElement=="54240" | measuredElement=="54170"]

towipe[, `:=`(Value = NA_real_, flagObservationStatus = NA_character_, flagMethod = NA_character_)]

tokeep <-
  tableBIG[!towipe, on = c('geographicAreaM49', 'measuredElement', 'measuredItemCPC', 'timePointYears')]

if (nrow(towipe) > 0) {
  tableBIG <- rbind(tokeep,towipe)
}


#create timeseries for indigenous meat for big animals

timeSeriesData<- as.data.table(expand.grid(measuredItemCPC = unique(tableBIG$measuredItemCPC), 
                                           geographicAreaM49 = unique(tableBIG$geographicAreaM49),
                                           measuredElement = unique(tableBIG$measuredElement),
                                           timePointYears = c(startYear:endYear)))

timeSeriesData[, names(timeSeriesData) := lapply(.SD, as.character), .SDcols = names(timeSeriesData)]

timeSeriesData<- na.omit(timeSeriesData, cols=c("geographicAreaM49", "measuredItemCPC"))


#fill timeseries with values and na
tableBIG <- merge(timeSeriesData, tableBIG, by=c("measuredElement","measuredItemCPC","timePointYears","geographicAreaM49"), all.x= TRUE)



##assign carcass weight value to indigenous carcass weight

tableBIG[ , Value:= ifelse( measuredElement=="54170" & !is.na(Value[measuredElement=="5417"]), Value[measuredElement=="5417"], Value),  by= list(geographicAreaM49, measuredItemCPC, timePointYears)]



#calculate prod ind in tonnes for big animals (head)

tableBIG[ , Value:= ifelse (measuredElement=="55100",(Value[get("measuredElement") %in% c("53200")])*(Value[get("measuredElement") %in% c("54170")])/1000, Value),
          by = list(geographicAreaM49, measuredItemCPC, timePointYears)]



#####Calculations for SMALL animals
#match code for live animals with code for meat 

tableSMALL[ , measuredItemCPC:= ifelse(measuredItemCPC=="02111", "21111.01", ifelse(measuredItemCPC=="02153", "21123",
                                                                                    ifelse(measuredItemCPC=="02112", "21112",
                                                                                           ifelse(measuredItemCPC=="02154", "21122",
                                                                                                  ifelse(measuredItemCPC=="02140", "21113.01",
                                                                                                         ifelse(measuredItemCPC=="02152", "21124",
                                                                                                                ifelse(measuredItemCPC=="02122", "21115",
                                                                                                                       ifelse(measuredItemCPC=="02121.01", "21117.01",
                                                                                                                              ifelse(measuredItemCPC=="02123", "21116",
                                                                                                                                     ifelse(measuredItemCPC=="02191", "21114",
                                                                                                                                            ifelse(measuredItemCPC=="02131", "21118.01",
                                                                                                                                                   ifelse(measuredItemCPC=="02132", "21118.02",
                                                                                                                                                          ifelse(measuredItemCPC=="02194", "21170.01",
                                                                                                                                                                 ifelse(measuredItemCPC=="02121.02", "21117.02",
                                                                                                                                                                        ifelse(measuredItemCPC=="02151", "21121",
                                                                                                                                                                               ifelse(measuredItemCPC=="02133", "21118.03",NA))))))))))))))))]


##Merge useful data

MeatDataSMALL<-MeatData[measuredItemCPC %in% c(unique(tableSMALL$measuredItemCPC))]

tableSMALL<- rbind(tableSMALL, MeatDataSMALL)

#Clean dataset

towipe <-
  tableSMALL[measuredElement=="55100" | measuredElement=="54240" | measuredElement=="54170"]

towipe[, `:=`(Value = NA_real_, flagObservationStatus = NA_character_, flagMethod = NA_character_)]

tokeep <-
  tableSMALL[!towipe, on = c('geographicAreaM49', 'measuredElement', 'measuredItemCPC', 'timePointYears')]

if (nrow(towipe) > 0) {
  tableSMALL <- rbind(tokeep,towipe)
}


#create timeseries for indigenous meat for small animals



timeSeriesData<- as.data.table(expand.grid(measuredItemCPC = unique(tableSMALL$measuredItemCPC), 
                                           geographicAreaM49 = unique(tableSMALL$geographicAreaM49),
                                           measuredElement = unique(tableSMALL$measuredElement),
                                           timePointYears = c(startYear:endYear)))

timeSeriesData[, names(timeSeriesData) := lapply(.SD, as.character), .SDcols = names(timeSeriesData)]

timeSeriesData<- na.omit(timeSeriesData, cols=c("geographicAreaM49", "measuredItemCPC"))


#fill timeseries with values and na
tableSMALL <- merge(timeSeriesData, tableSMALL, by=c("measuredElement","measuredItemCPC","timePointYears","geographicAreaM49"), all.x= TRUE)


#assign carcass weight (yield) to Indigenous carcass weight
tableSMALL[ , Value:= ifelse( measuredElement=="54240"& !is.na(Value[measuredElement=="5424"]), Value[measuredElement=="5424"], Value),  by= list(geographicAreaM49, measuredItemCPC, timePointYears)]


#calculate prod ind in tonnes for small animals (1000 head)

tableSMALL[ , Value:= ifelse (measuredElement=="55100",(Value[get("measuredElement") %in% c("53210")])*(Value[get("measuredElement") %in% c("54240")])/1000, Value),
            by = list(geographicAreaM49, measuredItemCPC, timePointYears)]



#####Merge data for small and big animals

meat_total<- rbind(tableBIG,tableSMALL)

meat_toupload<- meat_total

meat_toupload[ , flagObservationStatus:=ifelse(measuredElement=="55100"& !is.na(Value), "I", flagObservationStatus)]

meat_toupload[ , flagMethod:=ifelse(measuredElement=="55100" & !is.na(Value), "i", flagMethod)]

meat_toupload[ , flagObservationStatus:=ifelse(measuredElement=="54170" & !is.na(Value) , flagObservationStatus[measuredElement=="5417"], flagObservationStatus), 
               by = list(geographicAreaM49, measuredItemCPC, timePointYears)]

meat_toupload[ , flagMethod:=ifelse(measuredElement=="54170" & !is.na(Value), "c", flagMethod) ]

meat_toupload[ , flagObservationStatus:=ifelse(measuredElement=="54240" & !is.na(Value), flagObservationStatus[measuredElement=="5424"], flagObservationStatus), 
               by = list(geographicAreaM49, measuredItemCPC, timePointYears)]

meat_toupload[ , flagMethod:=ifelse(measuredElement=="54240" & !is.na(Value), "c", flagMethod) ]




##transform indigenous meat from 0 to blank in 2014-2016 (if they are base year, value cannot be 0) (NOT EXISTING CASES)

# # meat_toupload_FINAL<- meat_toupload[ , Value:= ifelse(measuredElement=="55100" & timePointYears>=2014 & timePointYears<=2016 & Value[measuredElement=="55100"]==0, NA, Value),
#                                            by = list(geographicAreaM49, measuredItemCPC, timePointYears) ]


meat_toupload_FINAL<-meat_toupload[ measuredElement=="53200" |measuredElement=="53210" |measuredElement=="54170" |measuredElement=="54240" |measuredElement=="55100"]



###SAVE in session

##check to perform before saving. all should be character but value
setDT(meat_toupload_FINAL)[, ("timePointYears") := lapply(.SD, as.character), .SDcols = "timePointYears"]

setDT(meat_toupload_FINAL)[, ("measuredElement") := lapply(.SD, as.character), .SDcols = "measuredElement"]

setDT(meat_toupload_FINAL)[, ("measuredItemCPC") := lapply(.SD, as.character), .SDcols = "measuredItemCPC"]

setDT(meat_toupload_FINAL)[, ("geographicAreaM49") := lapply(.SD, as.character), .SDcols = "geographicAreaM49"]

setDT(meat_toupload_FINAL)[, ("Value") := lapply(.SD, as.numeric), .SDcols = "Value"]




SaveData(domain = "agriculture", dataset = "aproduction", data= meat_toupload_FINAL)

print("Plug in completed")