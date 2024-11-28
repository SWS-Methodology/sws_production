##' This function balances the production triplet (input, productivity, output)
##' values for production where values are missing.
##'
##' To perform this function, 0M values and previous values calculated which has
##' method flag 'i' should be removed as the function only fill in values which
##' has the value NA.
##'
##' @param data The data containing the production triplet
##' @param processingParameters A list of the parameters for the production
##'   processing algorithms.  See defaultProductionParameters() for a starting
##'   point.
##' @param formulaParameters A list holding the names and parmater of formulas.
##'     See \code{productionFormulaParameters}.
##'     
##' @param flagTable Flag weight table. Please use ReadDatatable("ocs2023_flagweight")
##'
##' @return Data where production triplet is calculated where available.
##'
##' @export

balanceProductionTriplet = function(data,
                                    processingParameters,
                                    formulaParameters,
                                    flagTable = ReadDatatable("flag_weight_table")){
    dataCopy = copy(data)
    
    ## Data quality check
    suppressMessages({
        ensureProductionInputs(dataCopy,
                               processingParameters = processingParameters,
                               formulaParameters = formulaParameters,
                               returnData = FALSE,
                               normalised = FALSE)
    })
    
    yieldComputed =
        computeYield(data = dataCopy,
                     processingParameters = processingParams,
                     formulaParameters = formulaParameters, flagTable = flagTable)
    productionBalanced =
        balanceProduction(data = yieldComputed,
                          processingParameters = processingParams,
                          formulaParameters = formulaParameters,flagTable = flagTable)
    areaHarvestedBalanced =
        balanceAreaHarvested(data = productionBalanced,
                             processingParameters = processingParams,
                             formulaParameters = formulaParameters,flagTable = flagTable)
    
    areaHarvestedBalanced
}
