##' This function imputes the whole production domain.
##'
##' The function will impute production, area harvested and yield at the same
##' time.
##'
##' Transformation in the yield formula is not allowed and will not be taken
##' into account.
##'
##' @param data The data
##' @param processingParameters A list of the parameters for the production
##'     processing algorithms. See defaultProductionParameters() for a starting
##'     point.
##' @param formulaParameters A list holding the names and parmater of formulas.
##'     See \code{productionFormulaParameters}.
##' @param imputationParameters A list holding the imputation parameters, see
##'     \code{getImputationParameters}.
##' @export
##'
##' @import faoswsImputation
##' @import data.table
##'

imputeProductionTripletOriginal = function(data,
                                   processingParameters,
                                   formulaParameters,
                                   imputationParameters){
    originDataType = sapply(data, FUN = typeof)
    
    areaHarvestedImputationParameters = imputationParameters$areaHarvestedParams
    yieldImputationParameters = imputationParameters$yieldParams
    productionImputationParameters = imputationParameters$productionParams
    
    message("Initializing ... ")
    dataCopy = copy(data)
    
    ##filter out (m-) from the imputation process
    
    
    
    ## Data Quality Checks
    suppressMessages({
        ensureImputationInputs(data = dataCopy,
                               imputationParameters = yieldImputationParameters)
        ensureImputationInputs(data = dataCopy,
                               imputationParameters =
                                   productionImputationParameters)
        
        ensureProductionInputs(dataCopy,
                               processingParameters = processingParameters,
                               formulaParameters = formulaParameters,
                               returnData = FALSE,
                               normalised = FALSE)
    })
    
    setkeyv(x = dataCopy, cols = c(processingParameters$areaVar,
                                   processingParameters$yearValue))
    
    
    dataCopy = computeYield(dataCopy,
                            processingParameters = processingParameters,
                            formulaParameters = formulaParameters)
    ## Check whether all values are missing
    allYieldMissing = all(is.na(dataCopy[[formulaParameters$yieldValue]]))
    allProductionMissing = all(is.na(dataCopy[[formulaParameters$productionValue]]))
    allAreaMissing = all(is.na(dataCopy[[formulaParameters$areaHarvestedValue]]))
    
    
    if(!all(allYieldMissing)){
        ## Step two: Impute Yield
        message("Imputing Yield ...")
        n.missYield = sum(is.na(dataCopy[[formulaParameters$yieldValue]]))
        ## if(!missing(yieldFormula))
        ##     yieldFormula =
        ##     as.formula(gsub(yearValue, "yearValue",
        ##                     gsub(yieldValue, "yieldValue",
        ##                          deparse(yieldFormula))))
        
        dataCopy =imputeVariable(data = dataCopy,
                                 imputationParameters = yieldImputationParameters)
        ## TODO (Michael): Remove imputed zero yield as yield can not be zero by
        ##                 definition. This probably should be handled in the
        ##                 imputation parameter.
        ## Francesca: there is no reson why the zero yields have to be deleted!!
        ## It is the opposite: team B/C do not want to have yield when there is no production
        ## no areaHarvested!
        ##dataCopy =
        ##    removeZeroYield(dataCopy,
        ##                    yieldValue = formulaParameters$yieldValue,
        ##                    yieldObsFlag = formulaParameters$yieldObservationFlag,
        ##                    yieldMethodFlag = formulaParameters$yieldMethodFlag)
        n.missYield2 = length(which(is.na(
            dataCopy[[formulaParameters$yieldValue]])))
        message("Number of values imputed: ", n.missYield - n.missYield2)
        message("Number of values still missing: ", n.missYield2)
        
        ## Balance production now using imputed yield
        dataCopy =
            balanceProduction(data = dataCopy,
                              processingParameters = processingParameters,
                              formulaParameters = formulaParameters)
        
        ## NOTE (Michael): Check again whether production is available
        ##                 now after it is balanced.
        allProductionMissing = all(is.na(dataCopy[[formulaParameters$productionValue]]))
    } else {
        warning("The input dataset contains insufficient data to impute yield!")
    }
    
    if(!all(allProductionMissing)){
        ## step three: Impute production
        message("Imputing Production ...")
        n.missProduction = length(which(is.na(
            dataCopy[[formulaParameters$productionValue]])))
        
        dataCopy = imputeVariable(data = dataCopy,
                                  imputationParameters = productionImputationParameters)
        n.missProduction2 = length(which(is.na(
            dataCopy[[formulaParameters$productionValue]])))
        message("Number of values imputed: ",
                n.missProduction - n.missProduction2)
        message("Number of values still missing: ", n.missProduction2)
    } else {
        warning("The input dataset contains insufficient data to impute production!")
    }
    
    ## step four: balance area harvested
    message("Imputing Area Harvested ...")
    n.missAreaHarvested =
        length(which(is.na(
            dataCopy[[formulaParameters$areaHarvestedValue]])))
    
    dataCopy =
        balanceAreaHarvested(data = dataCopy,
                             processingParameters = processingParameters,
                             formulaParameters = formulaParameters)
    allAreaMissing = all(is.na(dataCopy[[formulaParameters$areaHarvestedValue]]))
    
    if(!all(allAreaMissing)){
        ## HACK (Michael): This is to ensure the area harvested are also
        ##                 imputed. Then we delete all computed yield and
        ##                 then balance again. This causes the yield not
        ##                 comforming to the imputation model.
        ##
        ##                 This whole function should be re-writtened so
        ##                 that an algorithm similar to the EM algorithm
        ##                 estimates and impute the triplet in a conherent
        ##                 way.
        ##
        ##                 Issue #88
        
        dataCopy = imputeVariable(data = dataCopy,
                                  imputationParameters = areaHarvestedImputationParameters)
        
        ## It was this part that caused the double "i" in methodFlag in the same triplet:
        ## beacuse I was deliting those non-protected yields even if I had used them to compute
        ## as identity the other variables.   
        ##   dataCopy[!is.na(get(formulaParameters$areaHarvestedValue)) &
        ##            !is.na(get(formulaParameters$productionValue)) &
        ##            !(combineFlag(.SD,
        ##                          formulaParameters$yieldObservationFlag,
        ##                          formulaParameters$yieldMethodFlag) %in%
        ##              getProtectedFlag()),
        ##            `:=`(c(formulaParameters$yieldValue,
        ##                   formulaParameters$yieldObservationFlag,
        ##                   formulaParameters$yieldMethodFlag),
        ##                 list(NA, "M", "u"))]
        dataCopy =
            computeYield(dataCopy,
                         processingParameters = processingParameters,
                         formulaParameters = formulaParameters)
        dataCopy = imputeVariable(data = dataCopy,
                                  imputationParameters = yieldImputationParameters)
        
        
    } ## End of HACK.
    n.missAreaHarvested2 =
        length(which(is.na(
            dataCopy[[formulaParameters$areaHarvestedValue]])))
    message("Number of values imputed: ",
            n.missAreaHarvested - n.missAreaHarvested2)
    message("Number of values still missing: ", n.missAreaHarvested2)
    
    
    ## This is to ensure the data type of the output is identical to
    ## the input data.
    dataCopy[, `:=`(colnames(dataCopy),
                    lapply(colnames(dataCopy),
                           FUN = function(x){
                               if(x %in% names(originDataType)){
                                   as(.SD[[x]], originDataType[[x]])
                               } else {
                                   .SD[[x]]
                               }
                           }))]
    dataCopy
}