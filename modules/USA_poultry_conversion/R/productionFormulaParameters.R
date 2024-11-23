##' Parameter for formulas
##'
##' @param datasetConfig The dataset configuration returned by the function
##'     \code{GetDatasetConfig}
##' @param productionCode The element code corresponding to the production
##'     variable.
##' @param yieldCode The element code corresponding to the yield variable.
##' @param areaHarvestedCode The element code corresponding to the area
##'     harvested variable.
##' @param unitConversion The unit conversion between the triplet code provided.
##'
##' @return Returns a list of the formula parameters used in the data
##'   pre-processing algorithm.
##'
##' @details Below is a description of the parameters: \itemize{
##' \item productionValue: The column name of the production variable.
##' \item productionObservationFlag: The column name of the observation flag
##'   corresponding to the production variable.
##' \item productionMethodFlag: The column name of the method flag corresponding
##'   to the production variable.
##' \item yieldValue: The column name of the yield variable.
##' \item unitConversion: The unit conversion between the triplet code provided.
##' \item yieldObservationFlag: The column name of the observation flag
##'   corresponding to the yield variable.
##' \item yieldMethodFlag: The column
##'   name of the method flag corresponding to the yield variable.
##' \item areaHarvestedValue: The column name of the area harvested variable.
##' \item areaHarvestedObservationFlag: The column name of the observation flag
##'   corresponding to the area harvested variable.
##' \item areaHarvestedMethodFlag: The column name of the method flag
##'   corresponding to the area harvested variable.
##' \item unitConversion The unit conversion applied in the formula
##' }
##'
##' @export

productionFormulaParameters = function(datasetConfig,
                                       productionCode = "5510",
                                       areaHarvestedCode = "5312",
                                       yieldCode = "5416",
                                       unitConversion = 1){
    ## HACK (Michael): There is no information on how to configure this, and
    ##                 thus it is hard coded.
    areaVar = datasetConfig$dimensions[1]
    itemVar = datasetConfig$dimensions[3]
    elementVar = datasetConfig$dimensions[2]
    yearVar = datasetConfig$timeDimension
    flagObservationVar = datasetConfig$flags[1]
    flagMethodVar = datasetConfig$flags[2]
    valueVar = "Value"

    ## Return the list of parameters
    list(productionCode = productionCode,
         areaHarvestedCode = areaHarvestedCode,
         yieldCode = yieldCode,
         productionValue =
             paste0(c(valueVar, elementVar, productionCode), collapse = "_"),
         areaHarvestedValue =
             paste0(c(valueVar, elementVar, areaHarvestedCode), collapse = "_"),
         yieldValue =
             paste0(c(valueVar, elementVar, yieldCode), collapse = "_"),
         productionObservationFlag =
             paste0(c(flagObservationVar, elementVar, productionCode),
                    collapse = "_"),
         areaHarvestedObservationFlag =
             paste0(c(flagObservationVar, elementVar, areaHarvestedCode),
                    collapse = "_"),
         yieldObservationFlag =
             paste0(c(flagObservationVar, elementVar, yieldCode), collapse = "_"),
         productionMethodFlag =
             paste0(c(flagMethodVar, elementVar, productionCode), collapse = "_"),
         areaHarvestedMethodFlag =
             paste0(c(flagMethodVar, elementVar, areaHarvestedCode),
                    collapse = "_"),
         yieldMethodFlag =
             paste0(c(flagMethodVar, elementVar, yieldCode), collapse = "_"),
         unitConversion = unitConversion
         )
}

