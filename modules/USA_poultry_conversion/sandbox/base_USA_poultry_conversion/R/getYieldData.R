##' This function extracts the yield data
##'
##' @param dataContext The data contect returned by \code{GetTestEnvironment}
##'
##' @return A list containing the yield data, the yield formula and also the
##'     prefix of the column names.
##'
##' @export

getYieldData = function(dataContext){
    ## Setups

    formulaTuples =
        getYieldFormula(slot(slot(dataContext,
                                  "dimensions")$measuredItemCPC, "keys"))
    ## setting the prefix, also should be accessed by the API
    prefixTuples =
        data.table(
            valuePrefix = "Value_measuredElement_",
            flagObsPrefix = "flagObservationStatus_measuredElement_",
            flagMethodPrefix = "flagMethod_measuredElement_"
        )

    key = dataContext
    if (exists("swsContext.modifiedCells")){
        print("Checking only modified data.")
        nmodded = nrow(swsContext.modifiedCells)

        if (nmodded > 0){
            data2process = TRUE
            for(cname in colnames(swsContext.modifiedCells)){
                key@dimensions[[cname]]@keys = as.character(unique(
                    swsContext.modifiedCells[, cname, with = FALSE]))
            }
        }
    } else {
        ## running in non-interactive mode, so get all the session data
        print("Reading all yield data.")
        data2process = TRUE
    }

    ## Pivot to vectorize yield computation
    newPivot = c(
        Pivoting(code = "geographicAreaM49", ascending = TRUE),
        Pivoting(code = "measuredItemCPC", ascending = TRUE),
        Pivoting(code = "timePointYears", ascending = FALSE),
        Pivoting(code = "measuredElement", ascending = TRUE)
    )

    if (data2process == TRUE){
        ## Execute the get data call.
        query = GetData(
            key = key,
            flags = TRUE,
            normalized = FALSE,
            pivoting = newPivot
        )
    }
    ## Query the data

    list(query = query,
         formulaTuples = formulaTuples,
         prefixTuples = prefixTuples)
}
