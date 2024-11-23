##' Get Yield Formula
##'
##' This function provides the element codes (for production, yield, and output)
##' and conversion factor which correspond to the particular item code of
##' interest.
##'
##' @param itemCode The cpc code providing the commodity of interest.  This
##'   should be a character string.
##' @param itemVar The name of the item variable column.
##' @param warn Logical.  If FALSE, an error is thrown for missing element
##'   types.  If TRUE, it is simply a warning.
##'
##' @return A data.table object providing the relevant element codes.  In some
##'   cases, there may be multiple rows, as it is possible to have multiple
##'   elements which hold production, yield, and output data.
##'
##' @export

getProductionFormula = function(itemCode,
                                itemVar = "measuredItemCPC",
                                warn = FALSE){
  
  sessionKey = swsContext.datasets[[1]]
  
    itemData = GetCodeList(domain = sessionKey@domain, dataset = sessionKey@dataset,
                           dimension = itemVar, codes = itemCode)
    itemData = itemData[!is.na(type), ]
    if(nrow(itemData) == 0)
        stop("No valid data to process!  Maybe the item code isn't in the ",
             "database?")
    uniqueItemTypes = unique(itemData$type)
    condition =
        paste0("item_type IN (",
               paste(paste0("'", as.character(uniqueItemTypes), "'"),
                      collapse = ", "), ")")
    productionFormula = ReadDatatable(table = "item_type_yield_elements",
                                 where = condition)
    if(any(!uniqueItemTypes %in% productionFormula$item_type)){
        if(!warn){
            stop("No data in item_type_yield_elements for codes supplied",
                 " (item types are ", paste(uniqueItemTypes, collapse = ", "), ")")
        } else {
            warning("No data in item_type_yield_elements for codes supplied",
                 " (item types are ", paste(uniqueItemTypes, collapse = ", "), ")")
        }
    }
    productionFormula = merge.data.frame(itemData, productionFormula,
                                    by.x = "type", by.y = "item_type")
    productionFormula = productionFormula[, c("code", "element_31", "element_41",
                                    "element_51", "factor")]
    productionFormula = data.table(productionFormula)
    setnames(productionFormula,
             old = c("code", "element_31", "element_41",
                 "element_51", "factor"),
             new = c(itemVar, "input", "productivity",
                 "output", "unitConversion")
             )
    productionFormula
}
