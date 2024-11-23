##' Function to get the complete yield key
##'
##' @param areaVar The column name corresponding to the geographic area.
##' @param itemVar The column name corresponding to commodity item.
##' @param elementVar The column name corresponding to the measured element.
##' @param yearVar The column name corresponding to the time dimension
##' @param identityElements The elements codes which are related to the
##'     production identity equation.
##'
##' @return A DatasetKey comprise of all the codes which are relevant to the
##'     production identity equation computation.
##'
##' @export


getAllYieldKey = function(areaVar = "geographicAreaM49",
                          itemVar = "measuredItemCPC",
                          elementVar = "measuredElement",
                          yearVar = "timePointYears",
                          identityElements = c("31", "41", "51")){
    areaSelection =
        GetCodeList(domain = "agriculture", dataset = "aproduction",
                    dimension = areaVar)[type == "country", code]
    elementSelection =
        GetCodeList(domain = "agriculture", dataset = "aproduction",
                    dimension = elementVar)[type %in% identityElements, code]
    itemSelection =
        GetCodeList(domain = "agriculture", dataset = "aproduction",
                    dimension = itemVar)[, code]
    yearSelection =
        GetCodeList(domain = "agriculture", dataset = "aproduction",
                    dimension = yearVar)[description != "wildcard", code]

    fullKey = DatasetKey(
        domain = "agriculture",
        dataset = "aproduction",
        dimensions = list(
            geographicAreaM49 = Dimension(name = areaVar,
                                          keys = areaSelection),
            measuredElement = Dimension(name = elementVar,
                                        keys = elementSelection),
            measuredItemCPC = Dimension(name = itemVar,
                                        keys = itemSelection),
            timePointYears = Dimension(name = yearVar,
                                       keys = yearSelection)
        )
    )
    fullKey
}
