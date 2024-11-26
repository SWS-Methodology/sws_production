##' 
##'
##' **Author: Livia Lombardi**
##' 
##' 
##' **Description:**
##'
##' This module is designed to automatically convert poultry data in USA (chciken and turkeys)
##' USA provides total stock data over the year and not the sample in Sept/Oct as per FAO guidance
##' This is due to the extremely high slaughtering rate
##'
##' The objective is to correct these figures dividing them by the slaughtering rate provided by the country 
##'
##' IMPORTANT: 
##' Documentation at: 

message("plug-in starts to run")

# Import libraries
suppressMessages({
    library(data.table)
    library(faosws)
    library(RcppRoll)
    library(faoswsFlag)
    library(faoswsUtil)
    library(faoswsImputation)
    library(faoswsProduction)
    library(faoswsProcessing)
    library(faoswsEnsure)
    library(magrittr)
    library(dplyr)
    library(sendmailR)
    library(faoswsStandardization)
    library(openxlsx)
})


R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")


if (CheckDebug()) {
    
    library(faoswsModules)
    SETTINGS <- ReadSettings("sws.yml")
    
    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH <- SETTINGS[["share"]]
    
    ## Define where your certificates are stored
    # SetClientFiles(SETTINGS[["certdir"]])
    
    ## Get session information from SWS. Token must be obtained from web interface
    
    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])
    
    # source("")
    lapply(list.files("../../R", full.names = T), source, echo = FALSE)
    # sapply("", source)
}
# Get data configuration and session
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

#Take element as it is in the production domain
#completeImputationKey = getCompleteImputationKey("production")

`%!in%` = Negate(`%in%`)
##'Da aggiungere lo start Year

# startYear = 2014
# endYear = 2021
# geomM49 = "840"
startYear = as.numeric(swsContext.computationParams$start_year)
endYear = as.numeric(swsContext.computationParams$end_year)

geoM49 = getQueryKey("geographicAreaM49", sessionKey)

if(geoM49 != "840"){
    stop("The session country is different than United States of America")
}

rate <- as.numeric(swsContext.computationParams$take_off)

if (swsContext.computationParams$animal=="chicken") {
    animal_type <- "02151"} else {animal_type <- "02152"} 


##########################
#                        #
#      FUNCTIONS         #
#                        #
##########################


processingParameters <-
    productionProcessingParameters(datasetConfig = datasetConfig)

sessionItems <- animal_type

offtake_function <- function(data_table) {
    
    data <- copy(data_table)
    
        for (iter in seq(sessionItems)) {
            
            imputationProcess <- try({
                
                set.seed(070416)
                
                currentItem <- sessionItems[iter]
                
                liveStockItems <-
                    getAnimalMeatMapping(R_SWS_SHARE_PATH = R_SWS_SHARE_PATH,
                                         onlyMeatChildren = FALSE)
                
                liveStockItems <- unique(liveStockItems$measuredItemParentCPC)
                #if the code is not an animal code: skip the iteration (they have no off take rate)
                if(currentItem %!in%liveStockItems) next 
                suppressMessages({
                    formulaTable <-
                    getProductionFormula(itemCode = currentItem) %>%
                    removeIndigenousBiologicalMeat(formula = .)
                })
                
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
                
                
                
                extractedData <-data[measuredItemCPC %in% currentItem & 
                                         timePointYears %in% as.character(c(startYear:endYear)),]
                
                #extractedData <- extractedData[measuredElement %!in% formulaTable$productivity,]
                
                if (nrow(extractedData) == 0) {
                    message("Item : ", currentItem, " does not contain any data")
                    next
                }
                
                if (nrow(extractedData[measuredElement %in% formulaTable$productivity & flagObservationStatus %in% c("", "A") 
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
                                           normalised = FALSE,
                                           flagValidTable = ReadDatatable("valid_flags_ocs2023"))
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
                                                            get(formulaParameters$areaHarvestedObservationFlag), 
                                                            flagTable = ReadDatatable("ocs2023_flagweight")))]
                
                
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
                
                
                data_to_save <- computed_Data[measuredElement %in% formulaTable$productivity,]
                
                data_to_save <- data_to_save[!is.na(Value),]
                
                return(data_to_save)
                #data_to_save[flagObservationStatus %in% "M" & flagMethod %in% "c", flagMethod := "u"]
                
                # SaveData(domain = sessionKey@domain,
                #          dataset = sessionKey@dataset,
                #          data =  data_to_save)
            })
            
        }
    return(data_to_save)
}


#################### END FUNCTIONS ##############



data =
    sessionKey %>%
    GetData(key = .)



if(any(c('5112','5316') %!in% unique(data$measuredElement))){
    
    poultry_key <- DatasetKey(domain = sessionKey@domain, dataset = sessionKey@dataset, 
               dimensions = list(measuredItemCPC = Dimension("measuredItemCPC", animal_type), 
                                 measuredElement = Dimension("measuredElement", c('5112','5316')),
                                 geographicAreaM49 = Dimension("geographicAreaM49", geoM49),
                                 timePointYears = Dimension("timePointYears", as.character(startYear:endYear))))
    
    poultry_data <- GetData(poultry_key)
    
    
}else if (any(as.character(startYear:endYear) %!in% unique(data$timePointYears))){
    
    poultry_key <- DatasetKey(domain = sessionKey@domain, dataset = sessionKey@dataset, 
                              dimensions = list(measuredItemCPC = Dimension("measuredItemCPC", animal_type), 
                                                measuredElement = Dimension("measuredElement", c('5112','5316')),
                                                geographicAreaM49 = Dimension("geographicAreaM49", geoM49),
                                                timePointYears = Dimension("timePointYears", as.character(startYear:endYear))))
    
    poultry_data <- GetData(poultry_key)
    
}else{
    poultry_data <- copy(data)
}

#filtering again in case the parametrization of the plugin was a subclass of data contained in the actual session
poultry_stock <- poultry_data[measuredItemCPC %in% animal_type & measuredElement %in% "5112" &
                                   timePointYears %in% as.character(c(startYear:endYear)) ,]

poultry_stock$Value <- round(poultry_stock$Value/rate, -3)

#converting potential questionnaire flag to p give the calculation done
#need to decide flag if original value is imputed or estimated
poultry_stock <- poultry_stock[, `:=` (flagObservationStatus = "E", flagMethod = "f")]

animal_data <- copy(poultry_data)

animal_data <- animal_data[measuredItemCPC %in% animal_type & measuredElement %!in% "5112" &
                               timePointYears %in% as.character(c(startYear:endYear)),]

animal_data <- rbind(poultry_stock, animal_data)


offTake_data <- offtake_function(animal_data)

data_to_save <- rbind(poultry_stock,offTake_data)



SaveData(domain = sessionKey@domain,
         dataset = sessionKey@dataset,
         data =  data_to_save)




if(animal_type == "02151"){
    if(any(!(between(c(offTake_data$Value), 5.75,7.25)))){
        print('Alert Off Take rates')
    } else {
        print('Plug-in Completed')
    }
} else {
    if(any(!(between(c(offTake_data$Value), 2.75,3.25)))){
        print('Alert Off Take rates')
    } else {
        print('Plug-in Completed')
    }
} 
