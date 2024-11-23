##' This function tests whether a commodity is a primary item.
##'
##' HACK (Michael): This is a hack from Josh, a list should be provided.
##'
##' @param itemCode The item code to be tested
##' @param primaryPattern The regular expression pattern corresponds to primary
##'     commodities.
##'
##' @return logical, whether the item is a primary commodity
##'
##' @export
##'

isPrimary = function(itemCode, primaryPattern = "^0"){
    grepl(primaryPattern, itemCode)
}
