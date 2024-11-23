##' Get Production Data
##' 
##' This function returns the production data currently in the database for a
##' particular DatasetKey.  It also performs a few clean-up operations on the
##' data before directly returning.
##' 
##' @param dataContext An object of type DatasetKey (defined in faosws).  This
##' parameter provides the configuration of the dataset (i.e. which countries,
##' commodities, etc.).
##' @param itemVar The column name of the commodity code column.
##' @param areaVar The column name of the country code column.
##' @param elementVar The column name of the element code column.
##' @param yearVar The column name of the year column.
##'   
##' @return A list of length 3:
##' \itemize{
##' \item query: A data.table containing the production data from the database.
##' \item formulaTuples: The element codes for the production, output, and yield
##' elements.  There may be multiple records in this table if multiple elements
##' are relevant.
##' \item prefixTuples: This element contains the prefixes for the flags and
##' values.
##' }
##'
##' @export

getProductionData = function(dataContext, 
                             areaVar = "geographicAreaM49",
                             itemVar = "measuredItemCPC",
                             elementVar = "measuredElement",
                             yearVar = "timePointYears"){
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
    allCountryCode = GetCodeList(domain = slot(dataContext, "domain"),
                                 dataset = slot(dataContext, "dataset"),
                                 dimension = areaVar)
    allCountryCode = unique(allCountryCode[type == "country", code])
    
    selectedYears =
        slot(slot(dataContext, "dimensions")$timePointYears,
             "keys")

    ## Set 15 years as the default required number of years for
    ## imputation
    if(length(selectedYears) < 15)
        selectedYears =
            as.character((max(as.numeric(selectedYears) - 14)):
                             max(as.numeric(selectedYears)))

    ## Create the new expanded keys
    newKey = DatasetKey(
        domain = slot(dataContext, "domain"),
        dataset = slot(dataContext, "dataset"),
        dimensions = list(
            Dimension(name = areaVar,
                      keys = allCountryCode),
            Dimension(name = elementVar,
                      keys = unique(unlist(formulaTuples[,
                          list(input, productivity, output)]))),
            Dimension(name = itemVar,
                      keys = slot(slot(dataContext,
                          "dimensions")$measuredItemCPC, "keys")),
            Dimension(name = yearVar,
                      keys = selectedYears)
            )
        )

    ## Pivot to vectorize yield computation
    newPivot = c(
        Pivoting(code = areaVar, ascending = TRUE),
        Pivoting(code = itemVar, ascending = TRUE),
        Pivoting(code = yearVar, ascending = FALSE),
        Pivoting(code = elementVar, ascending = TRUE)
        )

    ## Query the data
    query = GetData(
        key = newKey,
        flags = TRUE,
        normalized = FALSE,
        pivoting = newPivot
        )
    ## Convert time to numeric
    query[, timePointYears := as.numeric(timePointYears)]
    
    ## Assign flags of "M" where data is missing
    elements = grepl("Value_measuredElement", colnames(query))
    elements = gsub("Value_measuredElement_", "", colnames(query)[elements])
    for(element in elements)
        query[is.na(get(paste0("Value_measuredElement_", element))),
              c(paste0("flagObservationStatus_measuredElement_", element)) := "M"]

    ## Remove data where flag is missing
    for(element in elements)
        remove0M(data = query,
            value = paste0("Value_measuredElement_", element),
            flag = paste0("flagObservationStatus_measuredElement_", element))
    
    list(query = query,
         formulaTuples = formulaTuples,
         prefixTuples = prefixTuples)
}
