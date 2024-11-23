##' Get Imputation Parameters
##'
##' This function contains the logic that we'll use to define the imputation
##' models and their parameters.
##'.
##' @param productionCode The element code of production
##' @param areaHarvestedCode The element code of area harvested
##' @param yieldCode The element code of yield.
##' @param areaVar The column name corresponding to the geographic area.
##' @param itemVar The column name corresponding to the commodity item.
##' @param elementVar The column name corresponding to the measured element.
##' @param yearVar The column name corresponding to the time dimension
##'
##' @return A list with the yield and production imputation parameters.
##' @export

getImputationParameters = function(productionCode,
                                   areaHarvestedCode,
                                   yieldCode,
                                   areaVar = "geographicAreaM49",
                                   itemVar = "measuredItemCPC",
                                   elementVar = "measuredElement",
                                   yearVar = "timePointYears"){

    ## Impute yield
    yieldParams =
        defaultImputationParameters(variable = as.numeric(yieldCode))
    ## Change the model formula to use a hierarchical mixed model.  The
    ## code to do this is a bit messy because we have to adjust the
    ## default argument of the model function (which is an element of
    ## the S4 ensembleModel object)
    formals(yieldParams$ensembleModels$defaultMixedModel@model)$modelFormula =
        paste0(yieldParams$imputationValueColumn,
               " ~ -1 + (1 + bs(", yearVar, ", df = 2, degree = 1)",
               "|", areaVar, ")")
    yieldParams$estimateNoData = TRUE
    yieldParams$byKey = c(areaVar, itemVar)
    ## Add moving average model with period of 3 years
    yieldParams$ensembleModels[[length(yieldParams$ensembleModels)+1]] =
        ensembleModel(model = defaultMovingAverage,
                      extrapolationRange = Inf, level = "local")
    names(yieldParams$ensembleModels)[[length(yieldParams$ensembleModels)]] =
        "defaultMovingAverage"
    ## Impute production
    productionParams =
        defaultImputationParameters(variable = as.numeric(productionCode))
    productionParams$estimateNoData = TRUE
    productionParams$byKey = c(areaVar, itemVar)
    ## Add moving average model with period of 3 years
    productionParams$ensembleModels[[length(productionParams$ensembleModels)+1]] =
        ensembleModel(model = defaultMovingAverage,
                      extrapolationRange = Inf, level = "local")
    names(productionParams$ensembleModels)[[length(productionParams$ensembleModels)]] =
        "defaultMovingAverage"

    ## Impute area harvested
    areaHarvestedParams =
        defaultImputationParameters(variable = as.numeric(areaHarvestedCode))
    areaHarvestedParams$estimateNoData = TRUE
    areaHarvestedParams$byKey = c(areaVar, itemVar)
    ## Add moving average model with period of 3 years
    areaHarvestedParams$ensembleModels[[length(areaHarvestedParams$ensembleModels)+1]] =
        ensembleModel(model = defaultMovingAverage,
                      extrapolationRange = Inf, level = "local")
    names(areaHarvestedParams$ensembleModels)[[length(areaHarvestedParams$ensembleModels)]] =
        "defaultMovingAverage"
    return(list(areaHarvestedParams = areaHarvestedParams,
                yieldParams = yieldParams,
                productionParams = productionParams))
}
