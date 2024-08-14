# Compute yiled if missing for session items
##'
##' **Author: Livia Lombardi**
##'
##' **Description:**
##'
##' The module is designed for the Agriculture production plugin.
##' It computes missing productivity elements of the items formula.
##' The computation is performed on the session queried and the session should have items belonging to the same group
##' (meats, crops, milk, eggs)



suppressMessages({
    library(faosws)
    library(faoswsUtil)
    library(faoswsFlag)
    library(faoswsImputation)
    library(faoswsProduction)
    library(faoswsProcessing)
    library(faoswsEnsure)
    library(magrittr)
    library(dplyr)
    library(sendmailR)
})

TARGET_DOMAIN = "agriculture"
TARGET_DATASET = "aproduction"
#TARGET_DATASET = "ocs2023_aproduction_modified"

##' Set up for the test environment and parameters
R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

if (CheckDebug()) {
    
    library(faoswsModules)
    SETTINGS <- ReadSettings("modules/Compute_yield/sws.yml")
    
    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH <- SETTINGS[["share"]]
    
    ## Get session information from SWS. Token must be obtained from web interface
    
    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])
}

sessionKey = swsContext.datasets[[1]]

sessionKey@domain = TARGET_DOMAIN
sessionKey@dataset = TARGET_DATASET

data <- GetData(sessionKey)

datasetConfig <- GetDatasetConfig(domainCode = TARGET_DOMAIN,
                                  datasetCode = TARGET_DATASET)

processingParameters <-
    productionProcessingParameters(datasetConfig = datasetConfig)

sessionItems <-
    getQueryKey("measuredItemCPC", sessionKey)


FLAG_TABLE <- ReadDatatable("ocs2023_flagweight")

for (iter in seq(sessionItems)) {
    
    imputationProcess <- try({
        
        set.seed(070416)
        
        currentItem <- sessionItems[iter]
        
        
        # suppressMessages({
        #     formulaTable <-
        #     getProductionFormula(itemCode = currentItem) %>%
        #     removeIndigenousBiologicalMeat(formula = .)
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
        
        
        sessionKey@dimensions$measuredItemCPC@keys <- currentItem
        
        sessionKey@dimensions$measuredElement@keys <-
            with(formulaParameters,
                 c(productionCode, areaHarvestedCode, yieldCode))
        
        extractedData <- GetData(sessionKey)
        
        
        if (nrow(extractedData) == 0) {
            message("Item : ", currentItem, " does not contain any data")
            next
        }
        
        if (nrow(extractedData[measuredElement %in% formulaTable$productivity & flagObservationStatus %in% c("A", "") 
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
                                     formulaParameters = formulaParameters,
                                     flagTable = FLAG_TABLE)
        
        computed_Data <-
            normalise(
                computed_Data,
                removeNonExistingRecords = FALSE
            )
        
        data_to_save <- computed_Data[measuredElement %in% formulaTable$productivity,]
        
        data_to_save <- data_to_save[!is.na(Value),]
        
        #data_to_save[flagObservationStatus %in% "M" & flagMethod %in% "c", flagMethod := "u"]
        
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data =  data_to_save)
    })
    
}


print('missing yields computed')
