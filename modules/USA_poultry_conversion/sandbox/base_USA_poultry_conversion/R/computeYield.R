##' Function to compute and update yield
##'
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the production
##'   processing algorithms.  See defaultProductionParameters() for a starting
##'   point.
##' @param flagTable see data(faoswsFlagTable) in \pkg{faoswsFlag}
##' @param formulaParameters A list holding the names and parmater of formulas.
##'     See \code{productionFormulaParameters}.
##'
##' @return The updated data.table.
##'
##' @export
##'
##' @import faoswsFlag
##'

computeYield = function(data,
                        processingParameters,
                        formulaParameters,
                        flagTable = faoswsFlagTable){
    
    dataCopy = copy(data)
    
    ## Data quality check
    suppressMessages({
        ensureProductionInputs(dataCopy,
                               processingParameters = processingParameters,
                               formulaParameters = formulaParameters,
                               returnData = FALSE,
                               normalised = FALSE)
    })
    
    ## Balance yield values only when they're missing, and both production and
    ## area harvested are not missing
    ##
    ## NOTE (Michael): When production is zero, it would result in zero yield.
    ##                 Yield can not be zero by definition. If the production is
    ##                 zero, then yield should remain a missing value as we can
    ##                 not observe the yield.
    missingYield =
        is.na(dataCopy[[formulaParameters$yieldValue]])&
        dataCopy[[formulaParameters$yieldMethodFlag]]!="-"
    nonMissingProduction =
        !is.na(dataCopy[[formulaParameters$productionValue]]) &
        dataCopy[[formulaParameters$productionObservationFlag]] != processingParameters$missingValueObservationFlag
    nonMissingAreaHarvested =
        !is.na(dataCopy[[formulaParameters$areaHarvestedValue]]) &
        dataCopy[[formulaParameters$areaHarvestedObservationFlag]] != processingParameters$missingValueObservationFlag
  ##When production is zero (numerator) we want to compute in any case the yield tha must be zero as well!
  ##This idea is not in line with the original approach of the module: the yield should have been     
  ##nonZeroProduction =
  ##    (dataCopy[[formulaParameters$productionValue]] != 0)
    
    feasibleFilter =
        missingYield &
        nonMissingProduction &
        nonMissingAreaHarvested
   ## & nonZeroProduction
    
    ## When area harvested (denominator) is zero, the calculation can be
    ## performed and returns NA. So a different flag should (see later)
    nonZeroAreaHarvestedFilter =
        (dataCopy[[formulaParameters$areaHarvestedValue]] != 0)
    
    
    
    ## Calculate the yield
    dataCopy[feasibleFilter, `:=`(c(formulaParameters$yieldValue),
                                  computeRatio(get(formulaParameters$productionValue),
                                               get(formulaParameters$areaHarvestedValue)) *
                                      formulaParameters$unitConversion)]
    
   
    
    ## Assign observation flag.
    ##
    ## NOTE (Michael): If the denominator (area harvested is non-zero) then
    ##                 perform flag aggregation, if the denominator is zero,
    ##                 then assign the missing flag as the computed yield is NA.
    dataCopy[feasibleFilter & nonZeroAreaHarvestedFilter,
             `:=`(c(formulaParameters$yieldObservationFlag),
                  aggregateObservationFlag(get(formulaParameters$productionObservationFlag),
                                           get(formulaParameters$areaHarvestedObservationFlag)))]
    
    ##Assign Observation flag M to that ratio with areaHarvested=0
    dataCopy[feasibleFilter & !nonZeroAreaHarvestedFilter,
             `:=`(c(formulaParameters$yieldObservationFlag),
                  processingParameters$missingValueObservationFlag)]
    
    dataCopy[feasibleFilter & !nonZeroAreaHarvestedFilter,
             `:=`(c(formulaParameters$yieldMethodFlag),
                  processingParameters$missingValueMethodFlag)]
    
    ## Assign method flag i to that ratio with areaHarvested!=0
    dataCopy[feasibleFilter & nonZeroAreaHarvestedFilter,
             `:=`(c(formulaParameters$yieldMethodFlag),
                  processingParameters$balanceMethodFlag)]
    ##--------------------------------------------------------------------------------------------    
    
    ## If  Prod or Area Harvested is (M,-) also yield should be flagged as (M,-)

    MdashProduction =  dataCopy[,get(formulaParameters$productionObservationFlag)==processingParameters$missingValueObservationFlag
                                & get(formulaParameters$productionMethodFlag)=="-"]
    blockFilterProd= MdashProduction & missingYield
    
    dataCopy[blockFilterProd ,
             `:=`(c(formulaParameters$yieldValue,formulaParameters$yieldObservationFlag,formulaParameters$yieldMethodFlag),
                  list(NA_real_,processingParameters$missingValueObservationFlag, "-"))]
    
    
    
    # 
    # MdashAreaHarvested= dataCopy[,get(formulaParameters$areaHarvestedObservationFlag)==processingParameters$missingValueObservationFlag
    #                              & get(formulaParameters$areaHarvestedMethodFlag)=="-"]
    # 
    # blockFilterAreaHarv= MdashAreaHarvested & missingYield
    # 
    # dataCopy[blockFilterAreaHarv ,
    #          `:=`(c(formulaParameters$yieldValue,formulaParameters$yieldObservationFlag,formulaParameters$yieldMethodFlag),
    #               list(NA_real_,processingParameters$missingValueObservationFlag, "-"))]
    
    return(dataCopy)
}