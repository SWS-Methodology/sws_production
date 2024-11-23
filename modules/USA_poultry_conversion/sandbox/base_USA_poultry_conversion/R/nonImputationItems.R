##' This function returns the commodity codes which are selected in the session
##' but not in the imputation list
##'
##' The purpose of the function to capture and give warning when items are being
##' selected in the session, but not included in the imputation list.
##'
##' @param selectedKey Datakey of the current session.
##' @param imputationKey The object returned by the function
##'     \code{getImputationKey}
##'
##' @return A vector containing the commodity items that are selected but not
##'     included in the imputation list.
##' @export
##'

nonImputationItems = function(selectedKey, imputationKey){
    selectedItems = selectedKey@dimensions$measuredItemCPC@keys
    imputationItems = imputationKey@dimensions$measuredItemCPC@keys
    nonImputationItems = setdiff(selectedItems, imputationItems)
    nonImputationItems
}
