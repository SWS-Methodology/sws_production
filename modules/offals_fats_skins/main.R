## load the libraries
library(faosws)
library(data.table)
library(faoswsUtil)
library(sendmailR)
library(dplyr)
library(faoswsFlag)



## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
  message("Not on server, so setting up environment...")
  
  library(faoswsModules)
  SETT <- ReadSettings("sws.yml")
  
  R_SWS_SHARE_PATH <- SETT[["share"]]  
  ## Get SWS Parameters
  SetClientFiles(dir = SETT[["certdir"]])
  GetTestEnvironment(
    baseUrl = SETT[["server"]],
    token = SETT[["token"]]
  )
}


sessionKey = swsContext.datasets[[1]]

geoKeys = GetCodeList(domain = "agriculture", dataset = "aproduction",
                      dimension = "geographicAreaM49")[type == "country", code]


sessionCountries =
  getQueryKey("geographicAreaM49", sessionKey)

geoM49 = swsContext.computationParams$geographicAreaM49

COUNTRY <-   switch(geoM49,
                    "session" = sessionCountries,
                    "all" = geoKeys)

START_YEAR <- swsContext.computationParams$start_year

END_YEAR <- swsContext.computationParams$end_year

############################################################################################################

#conversion factors

#cattle
conversion_cattles <- ReadDatatable("conversion_factors_cattles")[,-c("geographic_area"),with = FALSE]
conversion_cattles[,live_animal := "02111"]

#pigs
conversion_pigs <- ReadDatatable("conversion_factors_pigs")[,-c("geographic_area"),with = FALSE]
conversion_pigs[,live_animal := "02140"]

#buffalos

conversion_buffalos <- ReadDatatable("conversion_factors_buffalos")[,-c("geographic_area"),with = FALSE]
conversion_buffalos[,live_animal := "02112"]


#goats

conversion_goats <- ReadDatatable("conversion_factors_goats")[,-c("geographic_area"),with = FALSE]
conversion_goats[,live_animal := "02123"]


#horses

conversion_horses <- ReadDatatable("conversion_factors_horses")[,-c("geographic_area"),with = FALSE]
conversion_horses[,live_animal := "02131"]


#camels

conversion_camels <- ReadDatatable("conversion_factors_camels")[,-c("geographic_area"),with = FALSE]
conversion_camels[,live_animal := "02121.01"]


#sheep

conversion_sheep <- ReadDatatable("conversion_factors_sheep")[,-c("geographic_area"),with = FALSE]
conversion_sheep[,live_animal := "02122"]

#rabbits 

conversion_rabbits <- ReadDatatable("conversion_factors_rabbits")[,-c("geographic_area"),with = FALSE]
conversion_rabbits[,live_animal := "02191"]



#bind all conversion factors


conversion_factors <- rbind(conversion_cattles,conversion_pigs,conversion_buffalos,conversion_sheep,conversion_goats,conversion_horses,conversion_camels,conversion_rabbits
                            ,fill= T)

conversion_factors[, geographic_area_m49 := as.character(geographic_area_m49)]

conversion_factors <- conversion_factors[!is.na(geographic_area_m49)]



#meat table


meat_table <- ReadDatatable("meat")[,-c("live_animal", "meat"),with = F]


#offals and fats

offals_fats <- ReadDatatable("offals_fats")[,-c("meat"),with =FALSE]
offals_fats <-offals_fats[cpc_meat != ""]
setnames(offals_fats, "cpc","cpc_offals_fats")


#items to pull

items <- unique(c(meat_table$cpc_meat,offals_fats$cpc_offals_fats,offals_fats$cpc_meat))



#pull production data 

prodKey <- DatasetKey(domain = "agriculture", dataset = "aproduction",
                      dimensions = list(
                        geographicAreaM49 = Dimension(name = "geographicAreaM49", keys = COUNTRY),
                        measuredElement = Dimension(name = "measuredElement", keys = c("5510","5320")),
                        measuredItemCPC =  Dimension(name = "measuredItemCPC", keys = items),
                        timePointYears = Dimension(name = "timePointYears", keys = as.character(c(START_YEAR:END_YEAR))))
)


prodData <- GetData(prodKey)

prodFlags <- copy(prodData)


##########################Filling up missing conversion factors


conversion_factors[, world_average_carcass := round(mean(carcass_weight_of_live_weight_perc, na.rm = TRUE),1), by = c("live_animal")]
conversion_factors[, world_average_offals := round(mean(edible_offals_content_of_live_weight_perc, na.rm = TRUE),1), by = c("live_animal")]
conversion_factors[, world_average_fats := round(mean(slaughter_fats_content_of_live_weight_perc, na.rm = TRUE),1), by = c("live_animal")]
conversion_factors[, world_average_skin := round(mean(hides_and_skins_content_of_live_weight_perc, na.rm = TRUE),1), by = c("live_animal")]

conversion_factors <- subset(conversion_factors, geographic_area_m49 %in% COUNTRY)


conversion_factors[world_average_carcass == "NaN", world_average_carcass := NA] 
conversion_factors[world_average_offals == "NaN",world_average_offals := NA] 
conversion_factors[world_average_fats == "NaN",world_average_fats := NA] 
conversion_factors[world_average_skin == "NaN",world_average_skin := NA] 

production_data <- copy(prodData)


production_data <- subset(production_data, measuredElement == "5510")

#remove offals, fats and skin cpcs 

production_data <- subset(production_data, !(measuredItemCPC %in% unique(offals_fats$cpc_offals_fats)))

production_data[, c("flagObservationStatus","flagMethod"):= NULL]

production_data[,average_production := mean(Value, na.rm = TRUE), by = c("geographicAreaM49","measuredItemCPC")]

#remove NA average (this means for those CPC there have not been production in the past)

production_data <- production_data[!is.na(average_production)]

#meat cpc where production existed in the past 

meatCPC <- data.table(meatCPC = unique(production_data$measuredItemCPC))

meatCPC <- merge(meatCPC, meat_table, by.x = c("meatCPC"),by.y = c("cpc_meat"),all.x = TRUE)


#live animals where their meat exists in the country
liveAnimals <- unique(meatCPC$cpc_live_animal)

#create an indicator variable for production existance

conversion_factors[, prodcution_indicator := ifelse(live_animal %in% liveAnimals,1,0)]


conversion_factors[, carcass_weight_of_live_weight_perc := ifelse(is.na(carcass_weight_of_live_weight_perc) & prodcution_indicator == 1,
                                                                  world_average_carcass, carcass_weight_of_live_weight_perc)]

conversion_factors[, edible_offals_content_of_live_weight_perc := ifelse(is.na(edible_offals_content_of_live_weight_perc) & prodcution_indicator == 1,
                                                                         world_average_offals, edible_offals_content_of_live_weight_perc)]

conversion_factors[, slaughter_fats_content_of_live_weight_perc := ifelse(is.na(slaughter_fats_content_of_live_weight_perc) & prodcution_indicator == 1,
                                                                          world_average_fats, slaughter_fats_content_of_live_weight_perc)]

conversion_factors[, hides_and_skins_content_of_live_weight_perc := ifelse(is.na(hides_and_skins_content_of_live_weight_perc) & prodcution_indicator == 1,
                                                                           world_average_skin, hides_and_skins_content_of_live_weight_perc)]



conversion_factors <- conversion_factors[,-c ("world_average_carcass","world_average_offals","world_average_fats","world_average_skin","prodcution_indicator"),with = FALSE]


#####################################################




timeseriesProd <-as.data.table(expand.grid(geographicAreaM49 = unique(prodData$geographicAreaM49),
                                           measuredElement = unique(prodData$measuredElement), measuredItemCPC = unique(prodData$measuredItemCPC),
                                           timePointYears = unique(prodData$timePointYears)))




timeseriesProd <- merge(timeseriesProd, prodData, by =c("geographicAreaM49","measuredElement", "measuredItemCPC","timePointYears"), all.x = TRUE)

prodData <- copy(timeseriesProd)


conv_factor <- copy(conversion_factors)
conv_factor[, offal_factor:= `edible_offals_content_of_live_weight_perc`/`carcass_weight_of_live_weight_perc`, by =c("geographic_area_m49","live_animal")]
conv_factor[, fat_factor:= `slaughter_fats_content_of_live_weight_perc`/`carcass_weight_of_live_weight_perc`,by =c("geographic_area_m49","live_animal")]
conv_factor[, skin_factor:= `hides_and_skins_content_of_live_weight_perc`/`carcass_weight_of_live_weight_perc`,by =c("geographic_area_m49","live_animal")]


#########################################################################################################################################

#offals imputation


offals_fat_factor <- conv_factor[,c("geographic_area_m49", "live_animal", "offal_factor", "fat_factor"),with=FALSE]
skin_factor <- conv_factor[,c("geographic_area_m49", "live_animal", "skin_factor"),with=FALSE]

#offals and meet 

offals_fat_meat <- copy(offals_fats)
offals_fat_meat <- subset(offals_fat_meat, offals_fats %in% c("offals","fat"))


skin_meat <- copy(offals_fats)
skin_meat <- subset(skin_meat, !(offals_fats %in% c("offals","fat")))



#offals, meat and live 

offals_fat_meat <- merge(offals_fat_meat, meat_table, by = c("cpc_meat"), all.x = TRUE)
skin_meat<- merge(skin_meat, meat_table, by = c("cpc_meat"), all.x = TRUE)

#merge offals_meat table with offals_factor table

xx<- copy(offals_fat_factor)


yy_offals <- merge(xx, offals_fat_meat[offals_fats == "offals"], by.x = "live_animal",by.y = "cpc_live_animal", all.x = TRUE)
yy_offals <- yy_offals[!is.na(offals_fats)]

yy_fat <-  merge(xx, offals_fat_meat[offals_fats == "fat"], by.x = "live_animal",by.y = "cpc_live_animal", all.x = TRUE)
yy_fat <- yy_fat[!is.na(offals_fats)]


offals_fat_factor <- rbind(yy_offals, yy_fat)


# offals_fat_factor <- merge(offals_fat_factor, offals_fat_meat, by.x = "live_animal",by.y = "cpc_live_animal", all.x = TRUE)
skin_factor <- merge(skin_factor, skin_meat, by.x = "live_animal",by.y = "cpc_live_animal", all.x = TRUE)



production <- copy(prodData)
production_offals <- merge(production,offals_fat_factor[,-c("fat_factor"),with=F][offals_fats == "offals"], by.x = c("geographicAreaM49","measuredItemCPC"), 
                           by.y = c("geographic_area_m49","cpc_meat"), all.x = TRUE)

production_fats <- merge(production,offals_fat_factor[,-c("offal_factor"),with=F][offals_fats == "fat"], by.x = c("geographicAreaM49","measuredItemCPC"), 
                         by.y = c("geographic_area_m49","cpc_meat"), all.x = TRUE)


production_skin <- merge(production,skin_factor, by.x = c("geographicAreaM49","measuredItemCPC"), 
                         by.y = c("geographic_area_m49","cpc_meat"), all.x = TRUE)



production_offals <- production_offals[!is.na(live_animal)]
production_fats <- production_fats[!is.na(live_animal)]
production_skin <- production_skin[!is.na(live_animal)]


#offals###############################################################################

production_offals[, offals_production := ifelse(measuredElement == "5510",offal_factor*Value[measuredElement == "5510"],NA),
                  , by= c("geographicAreaM49","live_animal","timePointYears")]


#1 metric ton = 10^6 grams

production_offals[, offals_yield := ifelse(measuredElement == "5510",offals_production[measuredElement == "5510"]*10^6/Value[measuredElement== "5320"],NA),
                  , by= c("geographicAreaM49","live_animal","timePointYears")]

prod_5320_offals <- copy(production_offals)

prod_5320_offals <- prod_5320_offals[,c("geographicAreaM49", "measuredElement", "cpc_offals_fats",
                                        "timePointYears", "Value", "flagObservationStatus", "flagMethod"),with=FALSE]


prod_5320_offals <- subset(prod_5320_offals, measuredElement == "5320")
setnames(prod_5320_offals, "cpc_offals_fats","measuredItemCPC")


production_offals <-subset(production_offals, measuredElement == "5510")

production_offals <- production_offals[,c("geographicAreaM49", "timePointYears", "cpc_offals_fats", "offals_production","offals_yield"), with = FALSE]




######fats#################################################################################################################



production_fats[, fat_production := ifelse(measuredElement == "5510",fat_factor*Value[measuredElement == "5510"], NA 
), by= c("geographicAreaM49","live_animal","timePointYears")]


#1 metric ton = 10^6 grams

production_fats[, fat_yield := ifelse(measuredElement == "5510",fat_production[measuredElement == "5510"]*10^6/Value[measuredElement== "5320"],
                                      NA ), by= c("geographicAreaM49","live_animal","timePointYears")]

prod_5320_fats <- copy(production_fats)

prod_5320_fats <- prod_5320_fats[,c("geographicAreaM49", "measuredElement", "cpc_offals_fats",
                                    "timePointYears", "Value", "flagObservationStatus", "flagMethod"),with=FALSE]


prod_5320_fats <- subset(prod_5320_fats, measuredElement == "5320")
setnames(prod_5320_fats, "cpc_offals_fats","measuredItemCPC")


production_fats <-subset(production_fats, measuredElement == "5510")
production_fats <- production_fats[,c("geographicAreaM49", "timePointYears", "cpc_offals_fats", "fat_production","fat_yield"), with = FALSE]



###skin######################################################################################################################


production_skin[, skin_production := ifelse(measuredElement == "5510",skin_factor*Value[measuredElement == "5510"], NA 
), by= c("geographicAreaM49","live_animal","timePointYears")]


#1 metric ton = 10^6 grams

production_skin[, skin_yield := ifelse(measuredElement == "5510",skin_production[measuredElement == "5510"]*10^6/Value[measuredElement== "5320"],
                                       NA ), by= c("geographicAreaM49","live_animal","timePointYears")]

prod_5320_skin <- copy(production_skin)

prod_5320_skin <- prod_5320_skin[,c("geographicAreaM49", "measuredElement", "cpc_offals_fats",
                                    "timePointYears", "Value", "flagObservationStatus", "flagMethod"),with=FALSE]


prod_5320_skin <- subset(prod_5320_skin, measuredElement == "5320")
setnames(prod_5320_skin, "cpc_offals_fats","measuredItemCPC")


production_skin <-subset(production_skin, measuredElement == "5510")
production_skin <- production_skin[,c("geographicAreaM49", "timePointYears", "cpc_offals_fats", "skin_production","skin_yield"), with = FALSE]










############################Melting data sets of fats and offals

production_offals <- melt.data.table(production_offals, id=c("geographicAreaM49","cpc_offals_fats","timePointYears"), na.rm = TRUE)
production_fats <- melt.data.table(production_fats, id=c("geographicAreaM49","cpc_offals_fats","timePointYears"), na.rm = TRUE)
production_skin <- melt.data.table(production_skin, id=c("geographicAreaM49","cpc_offals_fats","timePointYears"), na.rm = TRUE)

##rbind estimated data

prod_fat_offals_skin <- rbind(production_offals,production_fats,production_skin)

prod_fat_offals_skin[, variable := ifelse(variable %in% c( "offals_production", "fat_production", "skin_production"), "5510", "5424")]

prod_fat_offals_skin[, `:=` (flagObservationStatus = "I", flagMethod = "i")]

setnames(prod_fat_offals_skin, c("cpc_offals_fats", "variable", "value"),c("measuredItemCPC","measuredElement","Value"))


#rbind the element 5320

prod_fat_offals_skin <- rbind(prod_fat_offals_skin, prod_5320_offals,prod_5320_fats,prod_5320_skin)

prod_fat_offals_skin <- prod_fat_offals_skin[!is.na(Value)]



###porduction data in SWS

setnames(prodFlags, c("Value","flagObservationStatus","flagMethod"),c("ValueSWS", "FlagSWS","MethodSWS"))



prod_fat_offals_skin <- merge(prod_fat_offals_skin, prodFlags, by=c("geographicAreaM49","measuredElement","measuredItemCPC", "timePointYears"), all.x = TRUE)


#replace official vlaues, flag and method. retain (M,-) combinations


prod_fat_offals_skin[, Value := ifelse(measuredElement == "5510" & FlagSWS == "M" &  !is.na(FlagSWS) & !is.na(MethodSWS) &  MethodSWS == "-", ValueSWS, Value)]
prod_fat_offals_skin[, flagObservationStatus := ifelse(measuredElement == "5510" & FlagSWS == "M" &  !is.na(FlagSWS) & !is.na(MethodSWS) & MethodSWS == "-", FlagSWS, flagObservationStatus)]
prod_fat_offals_skin[, flagMethod := ifelse(measuredElement == "5510" & FlagSWS == "M" &  !is.na(FlagSWS) & !is.na(MethodSWS) & MethodSWS == "-", MethodSWS, flagMethod)]


prod_fat_offals_skin[,c("ValueSWS","FlagSWS","MethodSWS") := NULL]

prod_fat_offals_skin <- prod_fat_offals_skin[!is.na(Value)]

#remove rows with official flag 




SaveData(domain = "agriculture",
         dataset = "aproduction",
         data =  prod_fat_offals_skin,waitTimeout = 1800)

paste0("The plugin ran successfully ! ")
