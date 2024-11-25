##' Check Production Inputs
##'
##' This function is designed to ensure that the provided dataset is valid.
##'
##' @param data A data.table containing the data.
##' @param processingParameters A list containing the parameters to be used in the processing
##'   algorithms.  See ?defaultProcessingParameters for a starting point.
##' @param formulaParameters A list holding the names and parmater of formulas. See
##'   \code{productionFormulaParameters}.
##' @param flagValidTable Datatable of valid flags. The default is a legacy version but the table
##'   "valid_flags_ocs2023" should be used instead
##' @param returnData logical, whether the data should be returned
##' @param normalised logical, whether the data is normalised
##'
##' @return The original data if all tests are passed
##'
##' @export
##'
##' @import faoswsEnsure

ensureProductionInputs = function(data,
                                  processingParameters,
                                  formulaParameters,
                                  flagValidTable = ReadDatatable("valid_flags"),
                                  returnData = TRUE,
                                  normalised = TRUE){

    dataCopy = copy(data)

    if(normalised){
        dataCopy = denormalise(dataCopy, "measuredElement")
    }


    within(formulaParameters,
           within(processingParameters,
         {
             suppressMessages({
                 ## Check data inputs
                 ensureDataInput(data = dataCopy,
                                 requiredColumn = c(productionValue,
                                                    productionObservationFlag,
                                                    productionMethodFlag,
                                                    yieldValue,
                                                    yieldObservationFlag,
                                                    yieldMethodFlag,
                                                    areaHarvestedValue,
                                                    areaHarvestedObservationFlag,
                                                    areaHarvestedMethodFlag,
                                                    itemVar,
                                                    yearVar,
                                                    areaVar),
                                 returnData = FALSE)

                 ## Ensure there is no production is zero while area harvested
                 ## is non zero, vice versa.
                 ensureNoConflictingZero(data = dataCopy,
                                         valueColumn1 = productionValue,
                                         valueColumn2 = areaHarvestedValue,
                                         returnData = FALSE,
                                         normalised = FALSE)


                 ## Ensure the range of values are correct
                 ##
                 ## NOTE (Michael): Yield can not be equal to zero
                 ensureValueRange(data = dataCopy,
                                  ensureColumn = yieldValue,
                                  min = 0,
                                  max = Inf,
                                  includeEndPoint = TRUE)
                 ensureValueRange(data = dataCopy,
                                  ensureColumn = areaHarvestedValue,
                                  min = 0,
                                  max = Inf,
                                  includeEndPoint = TRUE)
                 ensureValueRange(data = dataCopy,
                                  ensureColumn = productionValue,
                                  min = 0,
                                  max = Inf,
                                  includeEndPoint = TRUE)

                 ## Ensure missing values are correctly specified
                 ensureCorrectMissingValue(data = dataCopy,
                                           valueVar = yieldValue,
                                           flagObservationStatusVar =
                                               yieldObservationFlag,
                                           returnData = FALSE,
                                           getInvalidData = FALSE)
                 ensureCorrectMissingValue(data = dataCopy,
                                           valueVar = areaHarvestedValue,
                                           flagObservationStatusVar =
                                               areaHarvestedObservationFlag,
                                           returnData = FALSE,
                                           getInvalidData = FALSE)
                 ensureCorrectMissingValue(data = dataCopy,
                                           valueVar = productionValue,
                                           flagObservationStatusVar =
                                               productionObservationFlag,
                                           returnData = FALSE,
                                           getInvalidData = FALSE)

                 

                 ## Ensure production is balanced
                 ##
                 ## NOTE (Michael): This may be optional in input, but mandatory in
                 ##                 output
                ##ensureProductionBalanced(data = dataCopy,
                ##                         areaVar = areaHarvestedValue,
                ##                         yieldVar = yieldValue,
                ##                         prodVar = productionValue,
                ##                         conversion = unitConversion,
                ##                         returnData = FALSE,
                ##                         getInvalidData = TRUE,
                ##                         normalised = FALSE)
             })
         }))

    ## Ensure flags are valid
    dataCopy=ensureFlagValidity(data = dataCopy,
                                normalised = FALSE, flagTable = flagValidTable)
    
    if(normalised){
        dataCopy = normalise(dataCopy)
    }

    if(returnData)
        return(dataCopy)
}
