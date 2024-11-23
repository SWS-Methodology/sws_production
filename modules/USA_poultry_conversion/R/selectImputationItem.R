##' This function returns the commodity codes which are selected in the session
##' and is in the imputation list
##'
##' @param selectedKey Datakey of the current session.
##' @param imputationKey The object returned by the function
##'     \code{getImputationKey}
##'
##' @return A vector containing the commodity items that are selected and is
##'     included in the imputation list.
##' @export
##'

selectImputationItem = function(selectedKey, imputationKey){
    selectedItems = selectedKey@dimensions$measuredItemCPC@keys
    imputationItems = imputationKey@dimensions$measuredItemCPC@keys
    selectedImputationItems = intersect(selectedItems, imputationItems)
    selectedImputationItems
}
