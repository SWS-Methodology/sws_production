##' Default Processing Parameters
##'
##' This function can be used to generate the input parameters for the data
##' pre-processing code.  This is a good way to get a list of the required
##' parameters and then modify parameters to match your particular
##' configuration.
##'
##' @param datasetConfig The dataset configuration returned by the function
##'     \code{GetDatasetConfig}
##' @param dataset The dataset name in the database.
##' @param removePriorImputation logical, whether previous imputation values
##'     should be removed.
##' @param removeManualEstimation logical, whether previous manual estimation
##'     should be removed.
##'     
##' @param convertOCS2023Flags Automatically convert observation status flags 
##'     (blank) to A and T to X
##' @param imputationObservationFlag The observation flag that represents
##'     imputation.
##' @param imputationMethodFlag The observation flag that represents estimated
##'     by statistical algorithm.
##' @param balanceMethodFlag The method flag that for calculation based on
##'     identity
##' @param manualEstimationObservationFlag The observation status flag that
##'     corresponds to manual (FAO) estimates
##' @param manualEstimationMethodFlag The method flag that corresponds to manual
##'     (FAO) estimates
##' @param missingValueObservationFlag The observation flag corresponding to
##'     missing values.
##' @param missingValueMethodFlag The method flag corresponding to missing
##'     values.
##'
##' @return Returns a list of the default parameters used in the data
##'   pre-processing algorithm.
##'
##' @details Below is a description of the parameters: \itemize{
##'
##' \item areaVar: The column corresponding to the geographic area.
##' \item yearVar: The column corresponding to the time dimension.
##' \item itemVar: The column corresponding to the item/commodity.
##' \item elementVar: The column corresponding to the measured element.
##' \item valueVar: The name for value column in the normalised form.
##' \item flagObservationVar: The name for observation flag in the normalised form.
##' \item flagMethod: The name for method flag in the normalised form.
##' \item removePriorImputation: Should previous imputations be removed?
##' \item removeManualEstimation: Should previous manual estimates be removed?
##' \item imputationObservationFlag: The flag for imputation.
##' \item imputationMethodFlag: The observation flag that represents estimated by statistical algorithm.
##' \item balanceMethodFlag: The method flag that for calculation based on identity
##' \item manualEstimationObservationFlag: The observation status flag that corresponds to manual (FAO) estimates
##' \item manualEstimationMethodFlag: The method flag that corresponds to manual (FAO) estimates
##' \item missingValueObservationFlag: The observation flag corresponding to missing values.
##' \item missingValueMethodFlag: The method flag corresponding to missing values.
##' }
##'
##' @export
##'

productionProcessingParameters = function(datasetConfig,
                                          dataset = "aproduction",
                                          removePriorImputation = FALSE,
                                          removeManualEstimation = FALSE,
                                          keepOnlyProtected = TRUE,
                                          convertOCS2023Flags = TRUE,
                                          imputationObservationFlag = "I",
                                          imputationMethodFlag = "e",
                                          balanceMethodFlag = "i",
                                          manualEstimationObservationFlag = "E",
                                          manualEstimationMethodFlag = "f",
                                          missingValueObservationFlag = "M",
                                          missingValueMethodFlag = "u"){
    ## HACK (Michael): There is no information on how to configure this, and
    ##                 thus it is hard coded.
    areaVar = datasetConfig$dimensions[1]
    itemVar = datasetConfig$dimensions[3]
    elementVar = datasetConfig$dimensions[2]
    yearVar = datasetConfig$timeDimension
    flagObservationVar = datasetConfig$flags[1]
    flagMethodVar = datasetConfig$flags[2]
    valueVar = "Value"

    list(
        domain                    = datasetConfig$domain,
        dataset                   = dataset,
        areaVar                   = areaVar,
        yearVar                   = yearVar,
        itemVar                   = itemVar,
        elementVar                = elementVar,
        flagObservationVar        = flagObservationVar,
        flagMethodVar             = flagMethodVar,
        valueVar                  = valueVar,
        keepOnlyProtected         = keepOnlyProtected,
        convertOCS2023Flags       = convertOCS2023Flags,
        removePriorImputation     = removePriorImputation,
        removeManualEstimation    = removeManualEstimation,
        imputationObservationFlag = imputationObservationFlag,
        imputationMethodFlag      = imputationMethodFlag,
        ## NOTE (Michael): balanceMethod should not have an observation flag,
        ##                 it uses flag aggregation.
        balanceMethodFlag               = balanceMethodFlag,
        manualEstimationObservationFlag = manualEstimationObservationFlag,
        manualEstimationMethodFlag      = manualEstimationMethodFlag,
        missingValueObservationFlag     = missingValueObservationFlag,
        missingValueMethodFlag          = missingValueMethodFlag
    )

}
