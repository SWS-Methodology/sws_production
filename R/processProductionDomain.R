##' This is a wrapper for all the data manipulation steps before the preparation
##' of the imputation.
##'
##' @param data The data
##' @param processingParameters A list of the parameters for the production
##'   processing algorithms.  See defaultProductionParameters() for a starting
##'   point.
##' @param formulaParameters A list holding the names and parmater of formulas.
##'     See \code{productionFormulaParameters}.
##' @param flagValidTable Table of valid flags. Please use ReadDatatable("valid_flags_ocs2023")
##' @export
##'
##' @return Currently it returns the passed data.table after performing some
##'   checks and clean up of the data.  Eventually, it should modify the
##'   data.table in place, but this will require an update to data.table (see
##'   comment by the return statement).
##'
##' @import faoswsProcessing

processProductionDomain = function(data,
                                   processingParameters,
                                   formulaParameters,
                                   flagValidTable = ReadDatatable("valid_flags")){
    dataCopy = copy(data)

    ## Data quality check
    with(formulaParameters,
         ensureDataInput(dataCopy,
                         requiredColumn = c(areaHarvestedValue,
                                            areaHarvestedObservationFlag,
                                            areaHarvestedMethodFlag,
                                            yieldValue,
                                            yieldObservationFlag,
                                            yieldMethodFlag,
                                            productionValue,
                                            productionObservationFlag,
                                            productionMethodFlag),
                         returnData = FALSE)
         )

    ## Keep only protected data and set to (NA, M, u ) the other items


    if(processingParameters$keepOnlyProtected){
        ## Remove all non protected flag combination
        dataCopy =
            removeNonProtectedFlag( dataCopy,
                                    valueVar= "Value",
                                    observationFlagVar="flagObservationStatus",
                                    methodFlagVar="flagMethod",
                                    missingObservationFlag = "M",
                                    missingMethodFlag = "u",
                                    normalised= FALSE,
                                    denormalisedKey = "measuredElement",
                                    flagValidTable = flagValidTable)

    }    
    
 
    
    ## Remove prior imputation
    if(processingParameters$removePriorImputation){
        ## Remove current imputation (flag = I, e)
        dataCopy =
            removeImputationEstimation(data = dataCopy,
                                       value = formulaParameters$areaHarvestedValue,
                                       observationFlag =
                                           formulaParameters$areaHarvestedObservationFlag,
                                       methodFlag = formulaParameters$areaHarvestedMethodFlag,
                                       missingObservationFlag =
                                           processingParameters$missingValueObservationFlag,
                                       imputationEstimationObservationFlag =
                                           processingParameters$imputationObservationFlag,
                                       imputationEstimationMethodFlag =
                                           processingParameters$imputationMethodFlag)
        dataCopy =
            removeImputationEstimation(data = dataCopy,
                                       value = formulaParameters$yieldValue,
                                       observationFlag = formulaParameters$yieldObservationFlag,
                                       methodFlag = formulaParameters$yieldMethodFlag,
                                       missingObservationFlag =
                                           processingParameters$missingValueObservationFlag,
                                       imputationEstimationObservationFlag =
                                           processingParameters$imputationObservationFlag,
                                       imputationEstimationMethodFlag =
                                           processingParameters$imputationMethodFlag)
        dataCopy =
            removeImputationEstimation(data = dataCopy,
                                       value = formulaParameters$productionValue,
                                       observationFlag =
                                           formulaParameters$productionObservationFlag,
                                       methodFlag = formulaParameters$productionMethodFlag,
                                       missingObservationFlag =
                                           processingParameters$missingValueObservationFlag,
                                       imputationEstimationObservationFlag =
                                           processingParameters$imputationObservationFlag,
                                       imputationEstimationMethodFlag =
                                           processingParameters$imputationMethodFlag)

        ## Remove historical imputation (flag = E, e)
        ##
        ## TODO (Michael): This is however, incorrectly mapped in the database.
        ##                 The old imputation should also have the flag
        ##                 combination (I, e). When this is corrected, the
        ##                 following chunk can be removed.
        dataCopy =
            removeImputationEstimation(data = dataCopy,
                                       value = formulaParameters$areaHarvestedValue,
                                       observationFlag =
                                           formulaParameters$areaHarvestedObservationFlag,
                                       methodFlag = formulaParameters$areaHarvestedMethodFlag,
                                       missingObservationFlag =
                                           processingParameters$missingValueObservationFlag,
                                       imputationEstimationObservationFlag =
                                           processingParameters$manualEstimationObservationFlag,
                                       imputationEstimationMethodFlag =
                                           processingParameters$imputationMethodFlag)
        dataCopy =
            removeImputationEstimation(data = dataCopy,
                                       value = formulaParameters$yieldValue,
                                       observationFlag = formulaParameters$yieldObservationFlag,
                                       methodFlag = formulaParameters$yieldMethodFlag,
                                       missingObservationFlag =
                                           processingParameters$missingValueObservationFlag,
                                       imputationEstimationObservationFlag =
                                           processingParameters$manualEstimationObservationFlag,
                                       imputationEstimationMethodFlag =
                                           processingParameters$imputationMethodFlag)
        dataCopy =
            removeImputationEstimation(data = dataCopy,
                                       value = formulaParameters$productionValue,
                                       observationFlag =
                                           formulaParameters$productionObservationFlag,
                                       methodFlag = formulaParameters$productionMethodFlag,
                                       missingObservationFlag =
                                           processingParameters$missingValueObservationFlag,
                                       imputationEstimationObservationFlag =
                                           processingParameters$manualEstimationObservationFlag,
                                       imputationEstimationMethodFlag =
                                           processingParameters$imputationMethodFlag)
    }

    if(processingParameters$removeManualEstimation){
        ## Removing manual estimation (flag = E, f)
        dataCopy =
            removeImputationEstimation(data = dataCopy,
                                       value = formulaParameters$areaHarvestedValue,
                                       observationFlag =
                                           formulaParameters$areaHarvestedObservationFlag,
                                       methodFlag = formulaParameters$areaHarvestedMethodFlag,
                                       missingObservationFlag =
                                           processingParameters$missingValueObservationFlag,
                                       imputationEstimationObservationFlag =
                                           processingParameters$manualEstimationObservationFlag,
                                       imputationEstimationMethodFlag =
                                           processingParameters$manualEstimationMethodFlag)
        dataCopy =
            removeImputationEstimation(data = dataCopy,
                                       value = formulaParameters$yieldValue,
                                       observationFlag = formulaParameters$yieldObservationFlag,
                                       methodFlag = formulaParameters$yieldMethodFlag,
                                       missingObservationFlag =
                                           processingParameters$missingValueObservationFlag,
                                       imputationEstimationObservationFlag =
                                           processingParameters$manualEstimationObservationFlag,
                                       imputationEstimationMethodFlag =
                                           processingParameters$manualEstimationMethodFlag)
        dataCopy =
            removeImputationEstimation(data = dataCopy,
                                       value = formulaParameters$productionValue,
                                       observationFlag =
                                           formulaParameters$productionObservationFlag,
                                       methodFlag = formulaParameters$productionMethodFlag,
                                       missingObservationFlag =
                                           processingParameters$missingValueObservationFlag,
                                       imputationEstimationObservationFlag =
                                           processingParameters$manualEstimationObservationFlag,
                                       imputationEstimationMethodFlag =
                                           processingParameters$manualEstimationMethodFlag)
    }

    ## Remove yield that are zero
    dataCopy =
        removeZeroYield(data = dataCopy,
                        yieldValue = formulaParameters$yieldValue,
                        yieldObsFlag = formulaParameters$yieldObservationFlag,
                        yieldMethodFlag = formulaParameters$yieldMethodFlag)

    ## Remove previously calculated values
    dataCopy =
        removeCalculated(data = dataCopy,
                         valueVar = formulaParameters$productionValue,
                         observationFlagVar = formulaParameters$productionObservationFlag,
                         methodFlagVar = formulaParameters$productionMethodFlag)
    dataCopy =
        removeCalculated(data = dataCopy,
                         valueVar = formulaParameters$areaHarvestedValue,
                         observationFlagVar = formulaParameters$areaHarvestedObservationFlag,
                         methodFlagVar = formulaParameters$areaHarvestedMethodFlag)
    dataCopy =
        removeCalculated(data = dataCopy,
                         valueVar = formulaParameters$yieldValue,
                         observationFlagVar = formulaParameters$yieldObservationFlag,
                         methodFlagVar = formulaParameters$yieldMethodFlag)

    return(dataCopy)
}
