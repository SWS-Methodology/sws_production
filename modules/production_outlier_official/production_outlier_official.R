library(faosws)
library(faoswsUtil)
library(dplyr)
library(data.table)
library(tidyr)
library(openxlsx)
library(RcppRoll)
library(faoswsProduction)
library(faoswsFlag)
library(faoswsImputation)
library(faoswsProcessing)
library(faoswsEnsure)
library(magrittr)
library(sendmailR)




start_time <- Sys.time()

R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

if (CheckDebug()) {
    R_SWS_SHARE_PATH <- "//hqlprsws1.hq.un.fao.org/sws_r_share"
    
    mydir <- "modules/production_outlier_official/"
    
    SETTINGS <- faoswsModules::ReadSettings(file.path(mydir, "sws.yml"))
    
    SetClientFiles(SETTINGS[["certdir"]])
    
    GetTestEnvironment(baseUrl = SETTINGS[["server"]], token = SETTINGS[["token"]])
}



COUNTRY <- as.character(swsContext.datasets[[1]]@dimensions$geographicAreaM49@keys)

COUNTRY_NAME <-
    nameData(
        "aproduction", "aproduction",
        data.table(geographicAreaM49 = COUNTRY))$geographicAreaM49_description

USER <- regmatches(
    swsContext.username,
    regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
)

dbg_print <- function(x) {
    message(paste0("PROD (", COUNTRY, "): ", x))
}

dbg_print("start parameters")

startYear <- as.numeric(swsContext.computationParams$startYear)
#startYear = as.numeric(2014)

endYear <- as.numeric(swsContext.computationParams$endYear)
#endYear = as.numeric(2018)

stopifnot(startYear <= endYear)

window <- as.numeric(3)

liveStockItems_table <-
    getAnimalMeatMapping(R_SWS_SHARE_PATH = R_SWS_SHARE_PATH,
                         onlyMeatChildren = FALSE)

liveStockItems <- unique(liveStockItems_table$measuredItemParentCPC)
#############################################
#                                           #
#   interval e yearVal                      #
#                                           #
#############################################

YEARS <- startYear:endYear#2013-2019 sono gli anni che mi devo scaricare
interval <- (startYear-window):(startYear-1)#2011-2015 sono gli anni per la media, da cambiare con Irina


#TMP file
TMP_DIR <- file.path(tempdir(), USER)
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)

tmp_file_outliers <- file.path(TMP_DIR, paste0("production_outlier_", COUNTRY_NAME, ".xlsx"))

dbg_print("end parameters")

dbg_print("start functions")
##########################################################SEND EMAIL FUNCTION#############################################
`%!in%` <- Negate(`%in%`)

######################COMPUTE Yield######################à##


yield_function <- function(data_table) {
    
    data <- copy(data_table)
    
    datasetConfig <- GetDatasetConfig(domainCode = "agriculture",
                                      datasetCode = "aproduction")
    
    processingParameters <-
        productionProcessingParameters(datasetConfig = datasetConfig)
    
    
    sessionItems <- c(unique(data$measuredItemCPC))
    # sessionItems <-
    #     getQueryKey("measuredItemCPC", session_key)
    
    final_data <- data.table(geographicAreaM49=character(), measuredElement=character(), measuredItemCPC=character(),
                             timePointYears=character(),Value=numeric(),flagObservationStatus=character(),
                             flagMethod=character())
    
    for (iter in seq(sessionItems)) {
        
        imputationProcess <- try({
            
            set.seed(070416)
            
            currentItem <- sessionItems[iter]
            
            print(currentItem)
            
            liveStockItems <-
                getAnimalMeatMapping(R_SWS_SHARE_PATH = R_SWS_SHARE_PATH,
                                     onlyMeatChildren = FALSE)
            
            liveStockItems <- unique(liveStockItems$measuredItemParentCPC)
            #if the code is an animal code: skip the iteration (they have no yield)
            if(currentItem %in%liveStockItems) next 
            ## Obtain the formula and remove indigenous and biological meat.
            
            # suppressMessages({
            #     formulaTable <-
            #         getProductionFormula(itemCode = currentItem) %>%
            #         removeIndigenousBiologicalMeat(formula = .)
            # })
            
            suppressWarnings(formulaTable <- removeIndigenousBiologicalMeat(getProductionFormula(currentItem)))
            
            if (nrow(formulaTable) > 1) {
                stop("Imputation should only use one formula")
            }
            
            ## Create the formula parameter list
            formulaParameters <-
                with(formulaTable,
                     productionFormulaParameters(datasetConfig = datasetConfig,
                                                 productionCode = output,
                                                 areaHarvestedCode = input,
                                                 yieldCode = productivity,
                                                 unitConversion = unitConversion)
                )
            
            
            
            extractedData <-data[measuredItemCPC %in% currentItem,]
            
            extractedData <- extractedData[measuredElement %!in% formulaTable$productivity,]
            
            if (nrow(extractedData) == 0) {
                message("Item : ", currentItem, " does not contain any data")
                next
            }
            
            if (nrow(extractedData[measuredElement %in% formulaTable$productivity & flagObservationStatus %in% "" 
                                   & flagMethod %in% "q",]) > 0) {
                message("Item : ", currentItem, " has a protected productivity value")
                next
            }
            
            processedData <-
                extractedData %>%
                preProcessing(data = .)    
            
            
            processedData <-
                denormalise(
                    normalisedData = processedData,
                    denormaliseKey = "measuredElement",
                    fillEmptyRecords = TRUE
                )
            
            
            
            processedData <-
                createTriplet(
                    data = processedData,
                    formula = formulaTable
                )
            
            
            processedData[
                get(formulaParameters$yieldObservationFlag) == processingParameters$missingValueObservationFlag,
                ":="(
                    c(formulaParameters$yieldMethodFlag),
                    list(processingParameters$missingValueMethodFlag))
                ]
            
            
            if (typeof(processedData[, formulaParameters$yieldValue]) == "character"){
                
                processedData[, formulaParameters$yieldValue := NULL]
                
                processedData[, formulaParameters$yieldValue:= NA_real_]
                
            }
            
            
            computed_Data = computeYield(processedData,
                                         processingParameters = processingParameters,
                                         formulaParameters = formulaParameters)
            
            
            computed_Data <-
                normalise(
                    computed_Data,
                    removeNonExistingRecords = FALSE
                )
            
            final_data <- rbind(final_data,computed_Data[measuredElement %in% formulaTable$productivity,])
            
            #data_to_save <- data_to_save[!is.na(Value),]
            
            #data_to_save[flagObservationStatus %in% "M" & flagMethod %in% "c", flagMethod := "u"]
            
            # SaveData(domain = sessionKey@domain,
            #          dataset = sessionKey@dataset,
            #          data =  data_to_save)
        })
        
    }
    return(final_data)
}


######################### COMPUTE OFF TAKE RATES #######################

offtake_function <- function(data_table) {
    
    data <- copy(data_table)
    
    datasetConfig <- GetDatasetConfig(domainCode = "agriculture",
                                      datasetCode = "aproduction")
    
    processingParameters <-
        productionProcessingParameters(datasetConfig = datasetConfig)
    
    
    sessionItems <- c(unique(data$measuredItemCPC))
    # sessionItems <-
    #     getQueryKey("measuredItemCPC", session_key)
    
    final_data <- data.table(geographicAreaM49=character(), measuredElement=character(), measuredItemCPC=character(),
                             timePointYears=character(),Value=numeric(),flagObservationStatus=character(),
                             flagMethod=character())
    
    for (iter in seq(sessionItems)) {
        
        imputationProcess <- try({
            
            set.seed(070416)
            
            currentItem <- sessionItems[iter]
            
            print(currentItem)
            
            
            ## Obtain the formula and remove indigenous and biological meat.
            
            
            suppressWarnings(formulaTable <- removeIndigenousBiologicalMeat(getProductionFormula(currentItem)))
            
            formulaTable$productivity <- "5077"
            
            if (nrow(formulaTable) > 1) {
                stop("Imputation should only use one formula")
            }
            
            ## Create the formula parameter list
            formulaParameters <-
                with(formulaTable,
                     productionFormulaParameters(datasetConfig = datasetConfig,
                                                 productionCode = output,
                                                 areaHarvestedCode = input,
                                                 yieldCode = productivity,
                                                 unitConversion = unitConversion)
                )
            
            #New code for the off take rate. We add it as the productivity element in live animals
            
            
            extractedData <-data[measuredItemCPC %in% currentItem,]
            
            extractedData <- extractedData[measuredElement %!in% formulaTable$productivity,]
            
            if (nrow(extractedData) == 0) {
                message("Item : ", currentItem, " does not contain any data")
                next
            }
            
            if (nrow(extractedData[measuredElement %in% formulaTable$productivity & flagObservationStatus %in% "" 
                                   & flagMethod %in% "q",]) > 0) {
                message("Item : ", currentItem, " has a protected productivity value")
                next
            }
            
            processedData <-
                extractedData %>%
                preProcessing(data = .)    
            
            
            processedData <-
                denormalise(
                    normalisedData = processedData,
                    denormaliseKey = "measuredElement",
                    fillEmptyRecords = TRUE
                )
            
            
            
            processedData <-
                createTriplet(
                    data = processedData,
                    formula = formulaTable
                )
            
            
            processedData[
                get(formulaParameters$yieldObservationFlag) == processingParameters$missingValueObservationFlag,
                ":="(
                    c(formulaParameters$yieldMethodFlag),
                    list(processingParameters$missingValueMethodFlag))
                ]
            
            
            if (typeof(processedData[, formulaParameters$yieldValue]) == "character"){
                
                processedData[, formulaParameters$yieldValue := NULL]
                
                processedData[, formulaParameters$yieldValue:= NA_real_]
                
            }
            
            ## Data quality check
            suppressMessages({
                ensureProductionInputs(processedData,
                                       processingParameters = processingParameters,
                                       formulaParameters = formulaParameters,
                                       returnData = FALSE,
                                       normalised = FALSE)
            })
            
            
            missingYield =
                is.na(processedData[[formulaParameters$yieldValue]])&
                processedData[[formulaParameters$yieldMethodFlag]]!="-"
            nonMissingProduction =
                !is.na(processedData[[formulaParameters$productionValue]]) &
                processedData[[formulaParameters$productionObservationFlag]] != processingParameters$missingValueObservationFlag
            nonMissingAreaHarvested =
                !is.na(processedData[[formulaParameters$areaHarvestedValue]]) &
                processedData[[formulaParameters$areaHarvestedObservationFlag]] != processingParameters$missingValueObservationFlag
            
            feasibleFilter =
                missingYield &
                nonMissingProduction &
                nonMissingAreaHarvested
            
            nonZeroProductionFilter =
                (processedData[[formulaParameters$productionValue]] != 0)
            
            
            #computation of the off take rate
            processedData[feasibleFilter, `:=`(c(formulaParameters$yieldValue),
                                               computeRatio(get(formulaParameters$areaHarvestedValue),
                                                            get(formulaParameters$productionValue)))]
            
            
            
            processedData[feasibleFilter & nonZeroProductionFilter,
                          `:=`(c(formulaParameters$yieldObservationFlag),
                               aggregateObservationFlag(get(formulaParameters$productionObservationFlag),
                                                        get(formulaParameters$areaHarvestedObservationFlag)))]
            
            
            processedData[feasibleFilter & !nonZeroProductionFilter,
                          `:=`(c(formulaParameters$yieldObservationFlag),
                               processingParameters$missingValueObservationFlag)]
            
            processedData[feasibleFilter & !nonZeroProductionFilter,
                          `:=`(c(formulaParameters$yieldMethodFlag),
                               processingParameters$missingValueMethodFlag)]
            
            ## Assign method flag i to that ratio with areaHarvested!=0
            processedData[feasibleFilter & nonZeroProductionFilter,
                          `:=`(c(formulaParameters$yieldMethodFlag),
                               processingParameters$balanceMethodFlag)]
            
            
            ## If  Prod or Area Harvested is (M,-) also yield should be flagged as (M,-)
            
            MdashProduction =  processedData[,get(formulaParameters$productionObservationFlag)==processingParameters$missingValueObservationFlag
                                             & get(formulaParameters$productionMethodFlag)=="-"]
            
            blockFilterProd= MdashProduction & missingYield
            
            processedData[blockFilterProd ,
                          `:=`(c(formulaParameters$yieldValue,formulaParameters$yieldObservationFlag,formulaParameters$yieldMethodFlag),
                               list(NA_real_,processingParameters$missingValueObservationFlag, "-"))]
            
            
            MdashAreaHarvested= processedData[,get(formulaParameters$areaHarvestedObservationFlag)==processingParameters$missingValueObservationFlag
                                              & get(formulaParameters$areaHarvestedMethodFlag)=="-"]
            
            blockFilterAreaHarv= MdashAreaHarvested & missingYield
            
            processedData[blockFilterAreaHarv ,
                          `:=`(c(formulaParameters$yieldValue,formulaParameters$yieldObservationFlag,formulaParameters$yieldMethodFlag),
                               list(NA_real_,processingParameters$missingValueObservationFlag, "-"))]
            
            #Normalize the computed data
            
            computed_Data <-
                normalise(
                    processedData,
                    removeNonExistingRecords = FALSE
                )
            
            final_data <- rbind(final_data,computed_Data[measuredElement %in% formulaTable$productivity,])
            
            #data_to_save <- data_to_save[!is.na(Value),]
            
            #data_to_save[flagObservationStatus %in% "M" & flagMethod %in% "c", flagMethod := "u"]
            
            # SaveData(domain = sessionKey@domain,
            #          dataset = sessionKey@dataset,
            #          data =  data_to_save)
        })
        
    }
    return(final_data)
}
######################SEND EMAIL############################
#New version with unlink of the temporary folders integrated
send_mail <- function(from = NA, to = NA, subject = NA,
                      body = NA, remove = FALSE) {
    
    if (missing(from)) from <- 'no-reply@fao.org'
    
    if (missing(to)) {
        if (exists('swsContext.userEmail')) {
            to <- swsContext.userEmail
        }
    }
    
    if (is.null(to)) {
        stop('No valid email in `to` parameter.')
    }
    
    if (missing(subject)) stop('Missing `subject`.')
    
    if (missing(body)) stop('Missing `body`.')
    
    if (length(body) > 1) {
        body <-
            sapply(
                body,
                function(x) {
                    if (file.exists(x)) {
                        # https://en.wikipedia.org/wiki/Media_type 
                        file_type <-
                            switch(
                                tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
                                txt  = 'text/plain',
                                csv  = 'text/csv',
                                png  = 'image/png',
                                jpeg = 'image/jpeg',
                                jpg  = 'image/jpeg',
                                gif  = 'image/gif',
                                xls  = 'application/vnd.ms-excel',
                                xlsx = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                doc  = 'application/msword',
                                docx = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                                pdf  = 'application/pdf',
                                zip  = 'application/zip',
                                # https://stackoverflow.com/questions/24725593/mime-type-for-serialized-r-objects
                                rds  = 'application/octet-stream'
                            )
                        
                        if (is.null(file_type)) {
                            stop(paste(tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
                                       'is not a supported file type.'))
                        } else {
                            res <- sendmailR:::.file_attachment(x, basename(x), type = file_type)
                            
                            if (remove == TRUE)    {
                                unlink(x)
                            }
                            
                            return(res)
                        }
                    } else {
                        return(x)
                    }
                }
            )
    } else if (!is.character(body)) {
        stop('`body` should be either a string or a list.')
    }
    
    sendmailR::sendmail(from, to, subject, as.list(body))
}
############################################################END SEND EMAIL##########################################
dbg_print("end functions")

dbg_print("Getting data")


#Items needed:
cpc_cluster <- ReadDatatable("cpc_validation_production")
proxy_cluster <- ReadDatatable("utilization_table_2018")
proxy_cluster <- proxy_cluster[proxy_primary %in% "X",]
#ELEMENTS: let get the triplets needed:
#o tutti da chiave
#measuredElement = Dimension("measuredElement", GetCodeList("aproduction", "aproduction", "measuredElement")$code)
#Da prendere poi unique(ELEMENTS) in the POST CALL
ELEMENTS <- c("5312","5510","5421",#crops: Area Harvested, Production, Yields 
              "5111","5315","5417",#Big animals: stocks, slaughtered, Yield/Carcass
              "5112","5316","5424",#Small animals: stocks, slaughtered, Yield/Carcass
              "5320","5510","5417",#Big meat: Slaughtered, Production, Yield carcass
              "5321","5510","5424",#Small meat: Slaughtered, Production, Yield/Carcass
              "5314","5319","5422", #keep only official/EF flag. Do not compute Yield
              "5318","5510","5417",#Milk: Milk animals, Production, Yields/Carcass
              "5313","5424","5510","5513")#EGGS: Laying, Yelds/Carcass, Production t, Production 1000. Yield: 5510/5313

#Session data
#c("5421","5417","5424","5422")

session_key <- swsContext.datasets[[1]]

#session_key@dimensions$timePointYears@keys <- as.character(interval[1]:endYear)
#session_key@dimensions$measuredItemCPC@keys <- unique(cpc_cluster$cpc)
#session_key@dimensions$measuredElement@keys <- unique(ELEMENTS)
#session_key@dimensions$geographicAreaM49@keys <- COUNTRY

data <- GetData(session_key)

data_backup <- copy(data)

data <- as.data.table(data)

data <- data[measuredElement %in% unique(ELEMENTS),]
data <- data[measuredItemCPC %in% c(unique(cpc_cluster$cpc), unique(proxy_cluster$cpc_code)),]

# keep only protected data
data <- data[flagObservationStatus %in% "" | flagObservationStatus %in% "T" | flagMethod %in% "f",]

# get free of protected data that are not in the interest range of year (startyear:endyear)
# this passage is useful to discard the data point that are still in the dataset but they don t have new info from 
# questionnaires. I will attach later from the dataset backup the items left in the dataset to have the mean reference 

protected_current <- data[, .SD[timePointYears %in% as.character(startYear:endYear)],
                        by= c("geographicAreaM49","measuredElement", "measuredItemCPC")]

combination_of_interest <- unique(protected_current[, .(measuredItemCPC, measuredElement)])

data_of_interest <- data.table(geographicAreaM49=character(), measuredElement=character(), measuredItemCPC=character(),
                               timePointYears=character(),Value=numeric(),flagObservationStatus=character(),
                               flagMethod=character())

for (i in 1:length(c(combination_of_interest)$measuredItemCPC)){
    data_of_interest <- rbind(data_of_interest,
                              data_backup[measuredItemCPC %in% c(combination_of_interest)$measuredItemCPC[i]&
                                              measuredElement %in% c(combination_of_interest)$measuredElement[i],] )
    i = i+1
}



clean_data <- data_of_interest[, checkFlag := ifelse(timePointYears %in% as.character(startYear:endYear) &
                            flagObservationStatus %!in% c("","T","E"), TRUE,FALSE)]


clean_data <- clean_data[checkFlag == FALSE,]


clean_data[, checkFlag := NULL]

#I should copy the slaughtered values of animals under the meat. SO I keep the meats in the triplet dataset
copy_for_big_animals <- clean_data[measuredElement %in% "5315"]
copy_for_small_animals <- clean_data[measuredElement %in% "5316"]

copy_for_big_animals[, c("measuredElement"):=NULL]
copy_for_big_animals[, measuredElement := "5320"] 

copy_for_small_animals[, c("measuredElement"):=NULL]
copy_for_small_animals[, measuredElement := "5321"]


copy_for_animals <- rbind(copy_for_big_animals,copy_for_small_animals)

mapping_for_animals <- data.table("meat_code" = c("21111.01","21115","21116","21113.01","21121","21122","21124","21112","21123","21118.01","21118.02","21118.03","21117.01","21114","21119.01","21170.01"),
                                  "animal_code"= c("02111","02122","02123","02140","02151","02154","02152","02112","02153","02131","02132","02133","02121.01","02191","02192.01","02194"))

copy_for_animals <- merge(copy_for_animals, mapping_for_animals, by.x = "measuredItemCPC",
                          by.y = "animal_code", all.x = T)


copy_for_animals[, c("measuredItemCPC"):=NULL]

setnames(copy_for_animals, "meat_code","measuredItemCPC")

copy_for_animals[, flagMethod := "c"]


clean_data <- rbind(clean_data[! copy_for_animals , on = c("measuredElement","geographicAreaM49",
                                                           "timePointYears", "measuredItemCPC")], copy_for_animals)

# let s separate not live animals and live animals data 

not_livestock_data <- clean_data[measuredItemCPC %!in% liveStockItems,]


livestock_data <- clean_data[measuredItemCPC %in% liveStockItems,]

#filter dataset for items which have both input and output element, so that we can compute the yield

#############################################################################
#############################################################################
#     NOT LIVESTOCK DATA COMPLETE TRIPLET EXTRACTION & YIELD COMPUTATION    #
#############################################################################
#############################################################################

# COMPLETE DATA - Triplet, With input and output

not_livestock_complete_data <- not_livestock_data[, length(unique(measuredElement)) > 1, 
                            by = c("geographicAreaM49", "measuredItemCPC", "timePointYears")]

not_livestock_complete_data <- left_join(not_livestock_data, not_livestock_complete_data, 
                                         by =  c("geographicAreaM49", "measuredItemCPC", "timePointYears"))

not_livestock_complete_data <- as.data.table(not_livestock_complete_data)
#I decided to not filter only for year val since I would need the previous yield values to see outliers in prodctivty
not_livestock_complete_data <- not_livestock_complete_data[V1 == "TRUE",]

not_livestock_complete_data[, V1 := NULL]

#Inside the Yield function I remove the productivity present data to calculate them from the scratch
yield_data <- yield_function(not_livestock_complete_data)

#Let s merge the productivity data recalculated 

#Before I remove all the old productivity elements since I recomputed
not_livestock_complete_data <- not_livestock_complete_data[measuredElement %!in% c("5421","5417","5424","5422"),]


not_livestock_complete_data <- rbind(not_livestock_complete_data[! yield_data , on = c("measuredElement","geographicAreaM49",
                                                               "timePointYears", "measuredItemCPC")], yield_data)




# PARTIAL DATA - Triplet, Without input or output

#############################################################################
#############################################################################
#     NOT LIVESTOCK DATA PARTIAL TRIPLET EXTRACTION                         #
#############################################################################
#############################################################################

not_livestock_partial_data <- anti_join(not_livestock_data, not_livestock_complete_data, by=c("measuredElement","geographicAreaM49",
                                                                "timePointYears", "measuredItemCPC"))

#let add historical data for items which perhaps have partial triplet only for one year

not_livestock_partial_data <- as.data.table(not_livestock_partial_data)

not_live_partial_comb <- unique(not_livestock_partial_data[, .(measuredItemCPC, measuredElement)])

not_live_partial_historic <- data.table(geographicAreaM49=character(), measuredElement=character(), measuredItemCPC=character(),
                               timePointYears=character(),Value=numeric(),flagObservationStatus=character(),
                               flagMethod=character())

for (i in 1:length(c(not_live_partial_comb)$measuredItemCPC)){
    not_live_partial_historic <- rbind(not_live_partial_historic,
                              data_backup[measuredItemCPC %in% c(not_live_partial_comb)$measuredItemCPC[i]&
                                       measuredElement %in% c(not_live_partial_comb)$measuredElement[i],] )
    i = i+1
}

not_livestock_partial_data <- rbind(not_live_partial_historic[! not_livestock_partial_data, on = c("measuredElement","geographicAreaM49",
                                                                 "timePointYears", "measuredItemCPC")], not_livestock_partial_data)

#vedi diff tra not live partial data e historical


#############################################################################
#############################################################################
#         LIVESTOCK DATA COMPLETE TRIPLET EXTRACTION & YIELD COMPUTATION    #
#############################################################################
#############################################################################
complete_livestock_data <- livestock_data[, length(unique(measuredElement)) > 1, 
                            by = c("geographicAreaM49", "measuredItemCPC", "timePointYears")]

complete_livestock_data <- left_join(livestock_data, complete_livestock_data, by =  c("geographicAreaM49", "measuredItemCPC", "timePointYears"))

complete_livestock_data <- as.data.table(complete_livestock_data)
#I decided to not filter only for year val since I would need the previous yield values to see outliers in prodctivty
complete_livestock_data <- complete_livestock_data[V1 == "TRUE",]

complete_livestock_data[, V1 := NULL]


offtake_data <- offtake_function(complete_livestock_data)

complete_livestock_data <- complete_livestock_data[measuredElement %!in% c("5077"),]

complete_livestock_data <- rbind(complete_livestock_data[! offtake_data , on = c("measuredElement","geographicAreaM49",
                                                    "timePointYears", "measuredItemCPC")], offtake_data)


#############################################################################
#############################################################################
#         LIVESTOCK DATA PARTIAL TRIPLET EXTRACTION                         #
#############################################################################
#############################################################################

livestock_partial_data <- anti_join(livestock_data, complete_livestock_data, by=c("measuredElement","geographicAreaM49",
                                                                                              "timePointYears", "measuredItemCPC"))

#let add historical data for items which perhaps have partial triplet only for one year

livestock_partial_data <- as.data.table(livestock_partial_data)

live_partial_comb <- unique(livestock_partial_data[, .(measuredItemCPC, measuredElement)])

live_partial_historic <- data.table(geographicAreaM49=character(), measuredElement=character(), measuredItemCPC=character(),
                                        timePointYears=character(),Value=numeric(),flagObservationStatus=character(),
                                        flagMethod=character())

for (i in 1:length(c(live_partial_comb)$measuredItemCPC)){
    live_partial_historic <- rbind(live_partial_historic,
                                       data[measuredItemCPC %in% c(live_partial_comb)$measuredItemCPC[i]&
                                                measuredElement %in% c(live_partial_comb)$measuredElement[i],] )
    i = i+1
}

livestock_partial_data <- rbind(live_partial_historic[! livestock_partial_data, on = c("measuredElement","geographicAreaM49",
                                                                                                   "timePointYears", "measuredItemCPC")], livestock_partial_data)


#PERFORM OUTLIER DETECTION ON:
#1) Complete data containing triplet cleaned and recalculated with their respective historical series.
#2) Partial data contaning single series with their historical series

#Merge not livestock and livestock data

final_complete_triplet <- rbind(not_livestock_complete_data, complete_livestock_data)


final_partial_triplet <- rbind(not_livestock_partial_data,livestock_partial_data)

######################################
#                                    #
#                                    #
#          OUTLIER DETECTION         #
#      (on data with productivity)   #
#                                    #
######################################

if(nrow(final_complete_triplet) > 0){

    out1_data <- final_complete_triplet[order(geographicAreaM49, measuredElement, measuredItemCPC, timePointYears), 
                           avg := roll_meanr(Value, 3),
                           by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[order(geographicAreaM49,measuredItemCPC,measuredElement,timePointYears),prev_avg:=lag(avg),
              by= c("geographicAreaM49","measuredItemCPC","measuredElement")]
    
    out1_data <- out1_data[order(geographicAreaM49, measuredElement, measuredItemCPC),] [order( -timePointYears ),] 
    
    out1_data <- out1_data[timePointYears %in% as.character(startYear:endYear),]
    
    out1_data[,`:=`(yield_lower_th = NA_real_, yield_upper_th = NA_real_)]
    
    
    out1_data[, `:=`(
        yield_lower_th = prev_avg[measuredElement %in% c("5421","5417","5424","5417","5422","5077")] - 
            prev_avg[measuredElement %in% c("5421","5417","5424","5417","5422","5077")]*0.3,
        yield_upper_th = prev_avg[measuredElement %in% c("5421","5417","5424","5417","5422","5077")] + 
            prev_avg[measuredElement %in% c("5421","5417","5424","5417","5422","5077")]*0.3
    ),
    by = c("geographicAreaM49","measuredItemCPC","timePointYears")]
    
    
    out1_data[,yieldCheck:=ifelse(Value[measuredElement %in% c("5421","5417","5424","5417","5422","5077")] <yield_lower_th | 
                                      Value[measuredElement %in% c("5421","5417","5424","5417","5422","5077")] > yield_upper_th,TRUE,FALSE),
              by = c("geographicAreaM49","measuredItemCPC","timePointYears")]
    
    
    out1_data[,`:=`(lower_th = NA_real_, upper_th = NA_real_)]
    
    
    dbg_print("Outlier criteria")
    
    ######################################
    #                                    #
    #                                    #
    #          STRICT CRITERIA           #
    #                                    #
    #                                    #
    ######################################
    out1_data[Value < 100 & yieldCheck == TRUE | Value < 100 & is.na(yieldCheck), `:=`(
        lower_th = prev_avg - prev_avg*5,
        upper_th = prev_avg + prev_avg*5
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    
    out1_data[ Value >= 100 & Value < 1000 & yieldCheck == TRUE | Value >= 100 & Value < 1000 & is.na(yieldCheck), `:=`(
        lower_th = prev_avg - prev_avg*1,
        upper_th = prev_avg + prev_avg*1
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 1000 & Value < 10000 & yieldCheck == TRUE | Value >= 1000 & Value < 10000 & is.na(yieldCheck), `:=`(
        lower_th = prev_avg - prev_avg*0.8,
        upper_th = prev_avg + prev_avg*0.8
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 10000 & Value < 50000 & yieldCheck == TRUE | Value >= 10000 & Value < 50000 & is.na(yieldCheck), `:=`(
        lower_th = prev_avg - prev_avg*0.6,
        upper_th = prev_avg + prev_avg*0.6
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 50000 & Value < 100000 & yieldCheck == TRUE |  Value >= 50000 & Value < 100000 & is.na(yieldCheck), `:=`(
        lower_th = prev_avg - prev_avg*0.5,
        upper_th = prev_avg + prev_avg*0.5
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 100000 & Value < 500000 & yieldCheck == TRUE | Value >= 100000 & Value < 500000 & is.na(yieldCheck), `:=`(
        lower_th = prev_avg - prev_avg*0.4,
        upper_th = prev_avg + prev_avg*0.4
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 500000 & Value < 1000000 & yieldCheck == TRUE | Value >= 500000 & Value < 1000000 & is.na(yieldCheck), `:=`(
        lower_th = prev_avg - prev_avg*0.3,
        upper_th = prev_avg + prev_avg*0.3
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 1000000 & Value < 3000000 & yieldCheck == TRUE | Value >= 1000000 & Value < 3000000 & is.na(yieldCheck), `:=`(
        lower_th = prev_avg - prev_avg*0.15,
        upper_th = prev_avg + prev_avg*0.15
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 3000000 & Value < 50000000 & yieldCheck == TRUE |  Value >= 3000000 & Value < 50000000 & is.na(yieldCheck), `:=`(
        lower_th = prev_avg - prev_avg*0.1,
        upper_th = prev_avg + prev_avg*0.1
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value>=50000000 & yieldCheck == TRUE |  Value>=50000000 & is.na(yieldCheck), `:=`(
        lower_th = prev_avg - prev_avg*0.1,
        upper_th = prev_avg + prev_avg*0.1
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    dbg_print("End of strict criteria")
    #
    
    ######################################
    #                                    #
    #                                    #
    #          SOFT CRITERIA             #
    #                                    #
    #                                    #
    ######################################
    #dovrei aggiungere come filtro a ogni check: | Value >= 100 & Value < 1000 & measuredElement %in% "5111"
    
    out1_data[Value < 100 & yieldCheck == FALSE, `:=`(
        lower_th = prev_avg - prev_avg*9,
        upper_th = prev_avg + prev_avg*9
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    
    out1_data[ Value >= 100 & Value < 1000 & yieldCheck == FALSE, `:=`(
        lower_th = prev_avg - prev_avg*5,
        upper_th = prev_avg + prev_avg*5
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    
    out1_data[ Value >= 1000 & Value < 10000 & yieldCheck == FALSE, `:=`(
        lower_th = prev_avg - prev_avg*1,
        upper_th = prev_avg + prev_avg*1
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 10000 & Value < 50000 & yieldCheck == FALSE, `:=`(
        lower_th = prev_avg - prev_avg*0.6,
        upper_th = prev_avg + prev_avg*0.6
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 50000 & Value < 100000 & yieldCheck == FALSE, `:=`(
        lower_th = prev_avg - prev_avg*0.6,
        upper_th = prev_avg + prev_avg*0.6
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 100000 & Value < 500000 & yieldCheck == FALSE, `:=`(
        lower_th = prev_avg - prev_avg*0.5,
        upper_th = prev_avg + prev_avg*0.5
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 500000 & Value < 1000000 & yieldCheck == FALSE, `:=`(
        lower_th = prev_avg - prev_avg*0.4,
        upper_th = prev_avg + prev_avg*0.4
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 1000000 & Value < 3000000 & yieldCheck == FALSE, `:=`(
        lower_th = prev_avg - prev_avg*0.4,
        upper_th = prev_avg + prev_avg*0.4
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 3000000 & Value < 20000000 & yieldCheck == FALSE, `:=`(
        lower_th = prev_avg - prev_avg*0.3,
        upper_th = prev_avg + prev_avg*0.3
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value >= 20000000 & Value < 50000000 & yieldCheck == FALSE, `:=`(
        lower_th = prev_avg - prev_avg*0.15,
        upper_th = prev_avg + prev_avg*0.15
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out1_data[ Value>=50000000 & yieldCheck == FALSE, `:=`(
        lower_th = prev_avg - prev_avg*0.1,
        upper_th = prev_avg + prev_avg*0.1
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    
    out1_data[,outCheck:=ifelse(Value <lower_th | Value > upper_th ,TRUE,FALSE)]
    
    out1_data[(prev_avg/Value) > 9 ,outCheck:=TRUE]
    
    out1_data[Value < 1000 & prev_avg < 1000 & yieldCheck == FALSE,outCheck:=FALSE]

}else{
    out1_data <- data.table()
}

######################################
#                                    #
#                                    #
#          OUTLIER DETECTION         #
#          (on single series)        #
#                                    #
######################################

if(nrow(final_partial_triplet)>0){

    out2_data <- final_partial_triplet[order(geographicAreaM49, measuredElement, measuredItemCPC, timePointYears), 
                                        avg := roll_meanr(Value, 3),
                                        by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out2_data[order(geographicAreaM49,measuredItemCPC,measuredElement,timePointYears),prev_avg:=lag(avg),
              by= c("geographicAreaM49","measuredItemCPC","measuredElement")]
    
    out2_data <- out2_data[order(geographicAreaM49, measuredElement, measuredItemCPC),] [order( -timePointYears ),] 
    
    out2_data <- out2_data[timePointYears %in% as.character(startYear:endYear),]
    
    out2_data[,`:=`(lower_th = NA_real_, upper_th = NA_real_)]
    
    
    dbg_print("Outlier criteria")
    
    ######################################
    #                                    #
    #                                    #
    #          SOFT CRITERIA             #
    #                                    #
    #                                    #
    ######################################
    #dovrei aggiungere come filtro a ogni check: | Value >= 100 & Value < 1000 & measuredElement %in% "5111"
    
    out2_data[Value < 100, `:=`(
        lower_th = prev_avg - prev_avg*9,
        upper_th = prev_avg + prev_avg*9
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    
    out2_data[ Value >= 100 & Value < 1000, `:=`(
        lower_th = prev_avg - prev_avg*5,
        upper_th = prev_avg + prev_avg*5
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    
    out2_data[ Value >= 1000 & Value < 10000, `:=`(
        lower_th = prev_avg - prev_avg*1,
        upper_th = prev_avg + prev_avg*1
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out2_data[ Value >= 10000 & Value < 50000, `:=`(
        lower_th = prev_avg - prev_avg*0.6,
        upper_th = prev_avg + prev_avg*0.6
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out2_data[ Value >= 50000 & Value < 100000, `:=`(
        lower_th = prev_avg - prev_avg*0.6,
        upper_th = prev_avg + prev_avg*0.6
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out2_data[ Value >= 100000 & Value < 500000, `:=`(
        lower_th = prev_avg - prev_avg*0.5,
        upper_th = prev_avg + prev_avg*0.5
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out2_data[ Value >= 500000 & Value < 1000000, `:=`(
        lower_th = prev_avg - prev_avg*0.4,
        upper_th = prev_avg + prev_avg*0.4
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out2_data[ Value >= 1000000 & Value < 3000000, `:=`(
        lower_th = prev_avg - prev_avg*0.4,
        upper_th = prev_avg + prev_avg*0.4
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out2_data[ Value >= 3000000 & Value < 20000000, `:=`(
        lower_th = prev_avg - prev_avg*0.3,
        upper_th = prev_avg + prev_avg*0.3
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out2_data[ Value >= 20000000 & Value < 50000000, `:=`(
        lower_th = prev_avg - prev_avg*0.15,
        upper_th = prev_avg + prev_avg*0.15
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    
    out2_data[ Value>=50000000, `:=`(
        lower_th = prev_avg - prev_avg*0.1,
        upper_th = prev_avg + prev_avg*0.1
    ),
    by = c("geographicAreaM49","measuredElement","measuredItemCPC")]
    dbg_print("End of soft criteria - incomplete triplet")
    #
    
    
    
    out2_data[,outCheck:=ifelse(Value <lower_th | Value > upper_th ,TRUE,FALSE)]
    
    out2_data[(prev_avg/Value) > 9 ,outCheck:=TRUE]
    
    out2_data[Value < 1000 & prev_avg < 1000 ,outCheck:=FALSE]

}else{
    out2_data <- data.table()
}

dbg_print("Outliers final check ended")

#CLEANING DATASET SHAPE

if(nrow(out1_data)>0 & nrow(out2_data)>0){
    
    out1_data[, c("avg","yield_lower_th","yield_upper_th","lower_th","upper_th","yieldCheck"):= NULL]
    
    out2_data[, c("avg","lower_th","upper_th"):= NULL]
    
    outlier_file <- rbind(out1_data,out2_data) 
    
}else if(nrow(out1_data)>0 & nrow(out2_data)==0){
    
    out1_data[, c("avg","yield_lower_th","yield_upper_th","lower_th","upper_th","yieldCheck"):= NULL]
    
    outlier_file <- copy(out1_data)
    
}else if(nrow(out1_data)==0 & nrow(out2_data)>0){
    
    out2_data[, c("avg","lower_th","upper_th"):= NULL]
    
    outlier_file <- copy(out2_data)

}else{
    
    outlier_file <- data.table()
}


#COPYING FOR LATER
productivity_to_save <- copy(outlier_file)

if(nrow(outlier_file) > 0){

    setnames(outlier_file, "prev_avg", "moving_average_3_years")
    
    #outlier_file[, c("avg","outCheck"):= NULL]
    
    
    #outlier_file[, c("softCheck","outCheck"):= NULL]
    outlier_file <- outlier_file[outCheck==TRUE,]
    
    
    
    outlier_file <- nameData("aproduction", "aproduction", outlier_file, except = "timePointYears")
    
    outlier_file <- outlier_file[!flagMethod == "c",]
    
    
    outlier_file <- outlier_file[measuredElement %!in% c("5421","5417","5424","5417","5422","5077"),]
    
    outlier_file[, c("outCheck"):= NULL]
    
    
    
    outlier_file[,Comments := ifelse(Value >1000 & abs(moving_average_3_years/Value)> 5 |
                                         Value >1000 & abs(Value/moving_average_3_years)> 5, "YELLOW", NA_character_)]
    
    
    outlier_file[,Comments := ifelse(moving_average_3_years > 1000 & abs(moving_average_3_years/Value)> 5 |
                                    moving_average_3_years > 1000 & abs(Value/moving_average_3_years)> 5, "YELLOW", Comments)]
    
    outlier_file[,Comments := ifelse(Value > 5000000 & abs(moving_average_3_years/Value)> 2 |
                                         Value > 5000000 & abs(Value/moving_average_3_years)> 2, "YELLOW", Comments)]
    
    outlier_file[,Comments := ifelse(moving_average_3_years > 5000000 & abs(moving_average_3_years/Value)> 2 |
                                         moving_average_3_years > 5000000 & abs(Value/moving_average_3_years)> 2, "YELLOW", Comments)]
    
    
    outlier_file[,Comments := ifelse(Value >50000000 & abs((Value/moving_average_3_years)-1)> 0.2, "RED", Comments)]
    
    outlier_file[,Comments := ifelse(moving_average_3_years >50000000 & abs((Value/moving_average_3_years)-1)> 0.2, "RED", Comments)]
    
    outlier_file$Value = format(round(outlier_file$Value, 0),nsmall=0 , big.mark=",",scientific=FALSE)
    
    outlier_file$moving_average_3_years = format(round(outlier_file$moving_average_3_years, 0),nsmall=0 , big.mark=",",scientific=FALSE)
    
    #order by item
    outlier_file = outlier_file[order(measuredItemCPC),]
}else{
    outlier_file <- copy(outlier_file)
}

################################################
#                                              #
#                                              #
#               MISSING LAST YEAR              #
#                                              #
#                                              #
################################################

if(nrow(final_complete_triplet)>0 & nrow(final_partial_triplet)>0){
    
    #miss_last_year <- rbind(final_complete_triplet,final_partial_triplet)
    
    miss_last_year <- rbind(final_complete_triplet[! final_partial_triplet , on = c("measuredElement","geographicAreaM49",
                                                                                           "timePointYears", "measuredItemCPC")], final_partial_triplet)
    #miss_last_year <- unique(miss_last_year)

}else if(nrow(final_complete_triplet)>0 & nrow(final_partial_triplet)==0){
    
    miss_last_year <- copy(final_complete_triplet)
    

}else if(nrow(final_complete_triplet)==0 & nrow(final_partial_triplet)>0){
    
    miss_last_year <- copy(final_partial_triplet)

}else{
    
    miss_last_year <- data.table()
}


miss_last_year <- copy(data_backup)

if(nrow(miss_last_year)>0){

    flag_elements <- c("5510","5312","5111","5315","5112","5316","5320","5321","5314","5319","5318","5313")
    
    miss_last_year <- miss_last_year[measuredElement %in% flag_elements,]
    
    miss_last_year[,
                   `:=`(
                       mean = mean(Value[timePointYears %in% as.character((startYear-1) : (endYear-1))], na.rm = TRUE)
                   ),
                   by = c("geographicAreaM49", "measuredItemCPC", "measuredElement")
                   ]
    
    ##prendo solo quelli che hanno media passata diversa da na quinid per cui ci sono i valori
    miss_last_year = miss_last_year[!is.na(mean),]
    
    #per ogni item c e il 2019 ?
    #devi anche dire se è official flag
    miss_last_year[, exists:= ifelse(timePointYears %in% as.character(endYear),TRUE,FALSE),
                   by = c("geographicAreaM49","measuredItemCPC","measuredElement")]
    
    miss_last_year[, exists:= ifelse(sum(exists) ==1 ,TRUE,FALSE),
                   by = c("geographicAreaM49","measuredItemCPC","measuredElement")]
    
    
    miss_last_year[, official:= ifelse(flagObservationStatus %in% c("","T") & 
                                           timePointYears %in% as.character((startYear-1) : (endYear-1)),TRUE,FALSE),]
    
    miss_last_year[, official:= ifelse(sum(official) >1 ,TRUE,FALSE),
                   by = c("geographicAreaM49","measuredItemCPC","measuredElement")]
    
    
    miss_last_year = miss_last_year[exists==FALSE & official == TRUE,]
    
    miss_last_year[, c("exists", "official","avg", "prev_avg", "mean"):= NULL]
    
    # miss_comb <- unique(miss_last_year[, .(measuredItemCPC, measuredElement)])
    # 
    # missing_imputations <- data.table(geographicAreaM49=character(), measuredElement=character(), measuredItemCPC=character(),
    #                                     timePointYears=character(),Value=numeric(),flagObservationStatus=character(),
    #                                     flagMethod=character())
    # 
    # for (i in 1:length(c(miss_comb)$measuredItemCPC)){
    #     missing_imputations <- rbind( missing_imputations,
    #                                    data_backup[measuredItemCPC %in% c(miss_comb)$measuredItemCPC[i]&
    #                                             measuredElement %in% c(miss_comb)$measuredElement[i],] )
    #     i = i+1
    # }
    # 
    # livestock_partial_data <- rbind(live_partial_historic[! livestock_partial_data, on = c("measuredElement","geographicAreaM49",
    #                                                                                        "timePointYears", "measuredItemCPC")], livestock_partial_data)
    # 
    # 
    # missing_imputations <- data_backup[measuredItemCPC %in% unique(miss_last_year[, measuredItemCPC]),]
    # 
    # missing_imputations <- missing_imputations[measuredElement %in% flag_elements,]
    # 
    # miss_last_year <- rbind(miss_last_year[! missing_imputations, on = c("measuredElement","geographicAreaM49",
    #                                              "timePointYears", "measuredItemCPC")], missing_imputations)
    
    miss_last_year$Value = format(round(miss_last_year$Value, 0),nsmall=0 , big.mark=",",scientific=FALSE)
    
    miss_last_year$Value<- paste(miss_last_year$Value,miss_last_year$flagObservationStatus,
                                 miss_last_year$flagMethod)
    
    
    miss_last_year <-miss_last_year[, c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears","Value"),
                                    with= FALSE]

}else{
    miss_last_year <- data.table()
}

if(nrow(miss_last_year) != 0){
    
  
    miss_last_year_cast <- dcast(miss_last_year, geographicAreaM49 + measuredElement + measuredItemCPC  ~ timePointYears, 
                                 value.var = "Value")
    
    #setnames(miss_last_year_cast,tail(names(miss_last_year_cast), n=1), "")
    #tail(names(miss_last_year_cast), n=1)
    #last_year <- as.numeric(tail(names(miss_last_year_cast), n=1))
    if(tail(names(miss_last_year_cast), n=1) == as.character(endYear-1)){
        
        #last_year <- paste0("`",endYear-1,"`")
        #`2019`
        names(miss_last_year_cast)[length(names(miss_last_year_cast))]<-"last_year" 
        
        miss_last_year_cast <- miss_last_year_cast[!is.na(last_year),]
        #select all after first space
        #sub(".*? ", "", "478,361 p")
        #get string that contains p
        #grepl("p", "T p", fixed = TRUE)
        
        nso_data <- miss_last_year_cast[grepl("p", last_year, fixed = T) & !grepl("T", last_year, fixed = T),]
        
        tp_data <- miss_last_year_cast[grepl("p", last_year, fixed = T) & grepl("T", last_year, fixed = T),]
        
        semi_official <- miss_last_year_cast[!grepl("p", last_year, fixed = T) & grepl("T", last_year, fixed = T),]
        
        others <- miss_last_year_cast[!grepl("p", last_year, fixed = T) & !grepl("T", last_year, fixed = T) &
                                          !grepl("q", last_year, fixed = T),]
        
        quest_data <- miss_last_year_cast[grepl("q", last_year, fixed = T),]
        
        miss_last_year_cast <- rbind(nso_data, tp_data, semi_official, others, quest_data)
        
        names(miss_last_year_cast)[length(names(miss_last_year_cast))]<-as.character(endYear-1)
        
        miss_last_year_cast <- nameData("aproduction", "aproduction", miss_last_year_cast)
        
    }else if (tail(names(miss_last_year_cast), n=1) == as.character(endYear)) {
        #nel caso in cui l ultimo anno della tabella è l'attuale end year e non endyear -1 segnalare il problem
        error_message <- "ERROR IN MISSING LAST YEAR EXCEL SHEET. Please check this error"
        
    }else{
        
        miss_last_year_cast <- data.table()
    }
}else{
    
    miss_last_year_cast <- data.table()
    
}

################
#### SAVE     ##
################
#remove productivity figures related to meats
productivity_to_save <- productivity_to_save[!(measuredItemCPC %in% liveStockItems_table$measuredItemChildCPC 
                                       & measuredElement %in% c("5417","5424")),]

productivity_to_save <- productivity_to_save[measuredElement %in% c("5421","5417","5424","5417","5422","5077"),]

productivity_to_save[, c("outCheck", "prev_avg") := NULL]

productivity_to_save <- productivity_to_save[!is.na(Value),]

productivity_to_save <- as.data.table(productivity_to_save)

dbg_print("Saving Yield figures")

SaveData(
    domain = "aproduction",
    dataset = "aproduction",
    data = productivity_to_save,
    waitTimeout = 20000)

dbg_print("Yield figures saved")


######################
#### WB CREATION    ##
######################


wb <- createWorkbook(USER)

if(nrow(outlier_file) != 0){
    
    addWorksheet(wb, "Outlier_Production")
    writeDataTable(wb, "Outlier_Production",outlier_file)
    first_fill <- createStyle(fgFill = "red")
    second_fill <- createStyle(fgFill = "yellow")

    # 8 is the position of the starting columns. We add 4 later, so we are manipulating the 12th column
    for (i in c(8)) {
        addStyle(wb, "Outlier_Production", cols = i, 
                 rows = 1 + c((1:nrow(outlier_file))[outlier_file[[i+4]] == "RED"]), 
                 style = first_fill, gridExpand = TRUE)
    }
    
    for (i in c(8)) {
        addStyle(wb, "Outlier_Production", cols = i, 
                 rows = 1 + c((1:nrow(outlier_file))[outlier_file[[i+4]] == "YELLOW"]), 
                 style = second_fill, gridExpand = TRUE)
    }

    
    deleteData(wb, "Outlier_Production", cols = ncol(outlier_file), rows = 1:(nrow(outlier_file))+1, gridExpand = T)
    
}


if(nrow(miss_last_year_cast) != 0){
    
    addWorksheet(wb, "Missing_last_year")
    writeDataTable(wb, "Missing_last_year",miss_last_year_cast)
    
}
# library(devtools)
# Sys.setenv(PATH = paste("C:/Rtools/bin", Sys.getenv("PATH"), sep=";"))
# Sys.setenv(BINPREF = "C:/Rtools/mingw_$(WIN)/bin/")
# saveWorkbook(wb, file = "Arentina_outlier.xlsx", overwrite = TRUE)
#se c e l'elemento slaughtered in dataset outlier lo tolgo qualora associato a item di carne 
#(verificando che sia un doppione outlier == presente sia in animale che in carne)

#segno in rosso solo quelli tanto grandi

#I should remove slaughtered under meat if it is outlier twice!

#1) Off take rates li salvi back to the dataset

#2) slaughtered di carne copiato balnk c lo devo rimuovere prima del salvataggio

saveWorkbook(wb, tmp_file_outliers, overwrite = TRUE)


#if( exists("error_message")){paste(error_message)}
body_message = paste("Plugin completed. Production outliers official figures.", "",if( exists("error_message")){paste(error_message)},
                     "",
                     "**********************************************************************************
                     ********** All the figures showed in the excel file need to be checked. **********
                     **********************************************************************************
                     + If red figures are present they indicate quantities > 20,000,000 tons with a */- 20% jump from its past average
                     + If yellow figures are present they may indicate a value > 10,000,000 greater than 2 times its previous average or 
                     + a value > 1,000 greater than 5 times its previous average",
                     sep='\n')

send_mail(from = "no-reply@fao.org", 
          to = swsContext.userEmail,
          subject = paste0("Production outliers in ", COUNTRY_NAME), 
          body = c(body_message, tmp_file_outliers))

unlink(TMP_DIR, recursive = TRUE)

print('Plug-in Completed')

