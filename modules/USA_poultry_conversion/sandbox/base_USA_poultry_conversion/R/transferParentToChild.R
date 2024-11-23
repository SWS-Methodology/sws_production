##' Function to transfer values from parent to child.
##'
##' When values transferred between parent and child, shares are applied. To
##' transfer parent to children, we multiply by the share in order to obtain the
##' share of parents used to produce the child commodity. In the reversed
##' direction, the values are divided by the shares.
##'
##' An outer merge is applied for all the merge, this is to ensure no values are
##' omitted. Three different type of matching occur as a result and they are:
##'
##' \enumerate{
##'
##' \item{1. Value available in origin but not in target}{In this case, the
##' target is filled with the new calculated value.}
##'
##' \item{2. Value present in origin and in target}{In this case, the target
##' cell is replaced with the new calculated value.}
##'
##' \item{3. Value present in target but not in origin}{The value in the target
##' is retained.}
##'
##' }
##'
##' @param parentData The data for animal commodity.
##' @param childData The data for meat commodity.
##' @param mappingTable The mapping table between the parent and the child.
##' @param parentToChild logical, if true, slaughtered animal are transferred
##'     from animal commodity to meat, otherwise the otherway around.
##' @param transferMethodFlag The method flag to be assigned for the transfer.
##' @param imputationObservationFlag This is the flag assigned to imputation.
##'     This is a special case when share is zero, then the observation status
##'     flag should not be the same as the parent.
##'
##' @return An updated dataset depending on the direction of the transfer. The
##'     output dataset is strictly greater than the original target dataset.
##'
##' @export

transferParentToChild = function(parentData,
                                 childData,
                                 mappingTable,
                                 parentToChild = TRUE,
                                 transferMethodFlag = "c",
                                 imputationObservationFlag = "I"){

    ## Input check
    ##
    ## TODO (Michael): Need an input check for mapping table as well.
    requiredColumn = c("geographicAreaM49",
                       "measuredItemCPC",
                       "measuredElement",
                       "timePointYears",
                       "Value",
                       "flagObservationStatus",
                       "flagMethod")

    suppressMessages({
        ensureDataInput(data = childData,
                        requiredColumn = requiredColumn,
                        returnData = FALSE)
        ensureDataInput(data = parentData,
                        requiredColumn = requiredColumn,
                        returnData = FALSE)
        ensureDataInput(data = mappingTable,
                        requiredColumn = c("measuredItemParentCPC",
                                           "measuredItemChildCPC",
                                           "measuredElementParent",
                                           "measuredElementChild",
                                           "geographicAreaM49",
                                           "timePointYears",
                                           "share",
                                           "flagShare"),
                        returnData = FALSE)
    })

    ## Convert the names of the table
    childDataCopy = copy(childData)
    parentDataCopy = copy(parentData)

    setnames(x = childDataCopy,
             old = c("measuredItemCPC", "measuredElement",
                     "Value", "flagObservationStatus", "flagMethod"),
             new = c("measuredItemChildCPC", "measuredElementChild",
                     paste0(c("Value", "flagObservationStatus", "flagMethod"),
                            "_child")))
    setnames(x = parentDataCopy,
             old = c("measuredItemCPC", "measuredElement",
                     "Value", "flagObservationStatus", "flagMethod"),
             new = c("measuredItemParentCPC", "measuredElementParent",
                     paste0(c("Value", "flagObservationStatus", "flagMethod"),
                            "_parent")))

    ## Merge the three input dataset
    childMergeCol = intersect(colnames(childDataCopy),
                              colnames(mappingTable))
    childDataMapped = merge(childDataCopy, mappingTable,
                            by = childMergeCol, all = TRUE)

    parentMergeCol = intersect(colnames(parentDataCopy),
                               colnames(mappingTable))
    parentDataMapped = merge(parentDataCopy, mappingTable,
                             by = parentMergeCol, all = TRUE)

    mergeAllCol = intersect(colnames(childDataMapped),
                            colnames(parentDataMapped))
    parentChildMergedData = merge(childDataMapped, parentDataMapped,
                                  by = mergeAllCol, all = TRUE)

    ## If share is missing, it is defaulted to 1
    parentChildMergedData[is.na(share), `:=`(c("share"), 1)]

    ## Transfer the value from parent to child or the other way round.
    ##
    ## NOTE (Michael): I think everything should be copied except for protected
    ##                 data. An error should be thrown when both value are
    ##                 protected data.

    if(parentToChild){

        isMapped =
            with(parentChildMergedData,
                 !is.na(measuredItemChildCPC) &
                 !is.na(measuredElementChild))
        origCellAvailable =
            with(parentChildMergedData,
                 !is.na(Value_parent) &
                 !is.na(flagObservationStatus_parent))
        nonZeroShare =
            with(parentChildMergedData, share != 0)

        parentChildMergedData[
            nonZeroShare & isMapped & origCellAvailable,
            `:=`(c("Value_child",
                   "flagObservationStatus_child",
                   "flagMethod_child"),
                 list(Value_parent * share,
                      flagObservationStatus_parent,
                      transferMethodFlag))]
        ## NOTE (Michael): If share is zero, then the value of the child should
        ##                 be zero as well.
       ## parentChildMergedData[
       ##     !nonZeroShare,
       ##     `:=`(c("Value_child",
       ##            "flagObservationStatus_child",
       ##            "flagMethod_child"),
       ##          list(0,
       ##               imputationObservationFlag,
       ##               transferMethodFlag))]
        ## NOTE (Francesca): If the parent is (M,-) also the child must be (M,-), the series
        ##                   must be closed.
        
        parentMdash= with(parentChildMergedData, flagObservationStatus_parent=="M" & flagMethod_parent=="-")
        
        parentChildMergedData[parentMdash,
             `:=`(c("Value_child",
                    "flagObservationStatus_child",
                    "flagMethod_child"),
                 list(NA,"M","-"))]


        setnames(parentChildMergedData,
                 old = c("measuredItemChildCPC",
                         "measuredElementChild",
                         "Value_child",
                         "flagObservationStatus_child",
                         "flagMethod_child"),
                 new = c("measuredItemCPC",
                         "measuredElement",
                         "Value",
                         "flagObservationStatus",
                         "flagMethod"))
    } else {


        isMapped =
            with(parentChildMergedData,
                 !is.na(measuredItemParentCPC) &
                 !is.na(measuredElementParent))
        origCellAvailable =
            with(parentChildMergedData,
                 !is.na(Value_child) &
                 !is.na(flagObservationStatus_child))

        nonZeroShare =
            with(parentChildMergedData, share != 0)

        parentChildMergedData[
            nonZeroShare & isMapped & origCellAvailable,
            `:=`(c("Value_parent",
                   "flagObservationStatus_parent",
                   "flagMethod_parent"),
                 list(Value_child/share,
                      flagObservationStatus_child,
                      transferMethodFlag))]
        
        
        ## NOTE (Francesca): If the child is (M,-) also the parent series must be (M,-), the series
        ##                   must be closed.
        
        childMdash= with(parentChildMergedData, flagObservationStatus_child=="M" & flagMethod_child=="-")
        
        parentChildMergedData[childMdash,
                              `:=`(c("Value_parent",
                                     "flagObservationStatus_parent",
                                     "flagMethod_parent"),
                                   list(NA,"M","-"))]
        
        ## NOTE (Francesca): If the child is (M,-) also the parent must be (M,-), the series
        ##                   must be closed.
        
        childMdash= with(parentChildMergedData, flagObservationStatus_child=="M" & flagMethod_child=="-")
        
        parentChildMergedData[childMdash,
                              `:=`(c("Value_parent",
                                     "flagObservationStatus_parent",
                                     "flagMethod_parent"),
                                   list(NA,"M","-"))]
        

        setnames(parentChildMergedData,
                 old = c("measuredItemParentCPC",
                         "measuredElementParent",
                         "Value_parent",
                         "flagObservationStatus_parent",
                         "flagMethod_parent"),
                 new = c("measuredItemCPC",
                         "measuredElement",
                         "Value",
                         "flagObservationStatus",
                         "flagMethod"))
    }


    dataToBeReturned =
        subset(parentChildMergedData,
               select = requiredColumn,
               subset = !is.na(flagObservationStatus))
    dataToBeReturned
}
