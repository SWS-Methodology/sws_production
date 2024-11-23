##' This function retrieves the mapping between the parent (animal
##' such as cattle) and child (meat such as meat of cattle).
##'
##' @param R_SWS_SHARE_PATH The path to the shared drive on the SWS.
##' @param onlyMeatChildren Whether only the meat children should be returned.
##' @param meatPattern The regular expression corresponding to meat
##'
##' @return The mapping table
##'
##' @export
##'

getAnimalMeatMapping = function(R_SWS_SHARE_PATH,
                                onlyMeatChildren = FALSE,
                                meatPattern = "^211(1|2|7).*"){
    ## fread(paste0(R_SWS_SHARE_PATH,
    ##              "/browningj/production/slaughtered_synchronized.csv"),
    ##       colClasses = "character")

    ## New file which contains the animal group code
  
    mapping=ReadDatatable("animal_parent_child_mapping")
    setnames(mapping,c("measured_item_parent_cpc",
                                      "measured_element_parent",
                                      "measured_item_child_cpc",
                                      "measured_element_child"),
             c("measuredItemParentCPC",
               "measuredElementParent",
               "measuredItemChildCPC",
               "measuredElementChild"))
    
    if(onlyMeatChildren)
        mapping =
            subset(mapping,
                   measuredItemChildCPC %in%
                   selecteMeatCodes(measuredItemChildCPC,
                                    meatPattern = meatPattern))
    mapping
}
