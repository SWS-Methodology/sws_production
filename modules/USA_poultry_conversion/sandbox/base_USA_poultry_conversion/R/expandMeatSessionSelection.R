##' This function expands the current selection in the session to
##' include all necessary meat.
##'
##' @param oldKey The current key used in the session
##' @param areaVar The column name corresponding to the geographic area.
##' @param itemVar The column name corresponding to the commodity item.
##' @param elementVar The column name corresponding to the measured element.
##' @param yearVar The column name corresponding to the time dimension.
##' @param selectedMeatTable animalMeatMapping The mapping returned by the
##'     \code{getAnimalMeatMapping}
##'
##' @return A new DatasetKey object expanded to include the meat
##'     selection specified.
##'
##' @export
##'

expandMeatSessionSelection = function(oldKey,
                                      selectedMeatTable,
                                      areaVar = "geographicAreaM49",
                                      itemVar = "measuredItemCPC",
                                      elementVar = "measuredElement",
                                      yearVar = "timePointYears"){
    rowsIncluded =
        selectedMeatTable[, measuredItemParentCPC %in%
                            oldKey@dimensions$measuredItemCPC@keys |
                            measuredItemChildCPC %in%
                            oldKey@dimensions$measuredItemCPC@keys]
    requiredItemCodes =
        selectedMeatTable[rowsIncluded,
                          c(measuredItemParentCPC, measuredItemChildCPC)]
    requiredElementCodes =
        selectedMeatTable[rowsIncluded,
                          c(measuredElementParent, measuredElementChild)]
    oldKey@dimensions[["measuredItemCPC"]]@keys = unique(requiredItemCodes)
    if(length(oldKey@dimensions$measuredItemCPC@keys) == 0){
        stop("No meat/animal commodities are in the session, and thus this ",
             "module has nothing to do.")
    }

    ## Create a copy to update the key
    newKey = oldKey
    ## Update the measuredElements
    newKey@dimensions[["measuredElement"]]@keys = unique(requiredElementCodes)
    newKey
}
