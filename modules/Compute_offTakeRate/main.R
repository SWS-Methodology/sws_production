# Compute off take rate if missing for session items
##'
##' **Author: Livia Lombardi**
##'
##' **Description:**
##'
##' The module is designed for the Agriculture production plugin.
##' It computes missing off take rate elements of the items formula.
##' The computation is performed on the session queried and the session should have items belonging to the animal group
##' (meats, crops, milk, eggs)



suppressMessages({
    library(faosws)
    library(faoswsUtil)
    library(faoswsFlag)
    library(faoswsImputation)
    #library(faoswsProduction)
    library(faoswsProcessing)
    library(faoswsEnsure)
    library(magrittr)
    library(dplyr)
    library(sendmailR)
})

##' Set up for the test environment and parameters
R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

if (CheckDebug()) {
    
    # Use package root as working dir
    invisible(lapply(list.files("R", full.names = TRUE), source))
    
    library(faoswsModules)
    SETTINGS <- ReadSettings("modules/Compute_offTakeRate/sws.yml")
    
    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH <- SETTINGS[["share"]]
    
    ## Get session information from SWS. Token must be obtained from web interface
    
    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])
}

`%!in%` <- Negate(`%in%`)

sessionKey = swsContext.datasets[[1]]

data <- GetData(sessionKey)

datasetConfig <- GetDatasetConfig(domainCode = "agriculture",
                                  datasetCode = "aproduction")

processingParameters <-
    productionProcessingParameters(datasetConfig = datasetConfig)

sessionItems <-
    getQueryKey("measuredItemCPC", sessionKey)


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
        # suppressMessages({
        #     formulaTable <-
        #     getProductionFormula(itemCode = currentItem) %>%
        #     removeIndigenousBiologicalMeat(formula = .)
        # })
        
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
        
        
        
        extractedData <-data[measuredItemCPC %in% currentItem,]
        
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
        
        #data_to_save[flagObservationStatus %in% "M" & flagMethod %in% "c", flagMethod := "u"]
        
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data =  data_to_save)
    })
    
}


print('Offtake rate computed')
