##' This function selects the code which are corresponds to meat
##'
##'
##' @param itemCodes Vector of item codes
##' @param meatPattern The regular expression which corresponds to meat items
##'
##' @return Item codes which corresponds to meat
##'
##' @export
##'

selectMeatCodes = function(itemCodes, meatPattern = "^211(1|2|7).*"){
    itemCodes[grepl(meatPattern, itemCodes)]
}
