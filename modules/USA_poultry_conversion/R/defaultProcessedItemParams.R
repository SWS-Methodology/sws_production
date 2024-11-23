##' Default Processed Item Parameters
##' 
##' Provides an object which contains the parameters.  This 
##' allows for easy passing into functions.  The meaning of most variables is 
##' fairly clear, but a few obscure ones are described below:
##' 
##' \itemize{
##' 
##' \item processingShare share that identify the quantity of primary availability that it is allocated in 
##' different productive processes
##' \item shareDownUp Share to transform derived commodities into primary equivalent
##' \item availability The availability can be interpreted as the amount of one good that is no
##'  consumed as it is. This quantity enters, as input, in a productive process in order to produce
##'  derived commodities.
##' 
##' }
##' 
##' @return A list with the Processed Item  parameters.
##'   
##' @export
##' 


defaultProcessedItemParams=function(){
    geoVar = "geographicAreaM49"
    yearVar = "timePointYears"
    itemVar = "measuredItemCPC"
    list(
        geoVar = geoVar,
        yearVar = yearVar,
        itemVar = itemVar,
        elementVar = "measuredElement",
        mergeKey = c(geoVar, yearVar, itemVar), # For merging with the main data
        elementPrefix = "Value_measuredElement_",
        extractVar = "extractionRate",
        productionCode = "production",
        yieldCode = "yield",
        areaHarvCode = "areaHarvested",
        importCode = "imports",
        exportCode = "exports",
        stockCode = "stock_change",
        foodCode = "food",
        foodProcCode = "foodManufacturing",
        feedCode = "feed",
        wasteCode = "loss",
        seedCode = "seed",
        industrialCode = "industrial",
        touristCode = "tourist",
        residualCode = "residual",
        itemVarSUA = "measuredItemSuaFbs",
        ##mergeKey[params$mergeKey == "measuredItemCPC"] = "measuredItemSuaFbs",
        elementVarSUA = "measuredElementSuaFbs",
        childVar = "measuredItemChildCPC",
        parentVar = "measuredItemParentCPC",
        level = "processingLevel",
        availVar = "availability",
        shareDownUp="shareDownUp",
        shareOldSystem="share",
        processingShare="processingShare",
        value="Value"
        
        
    )
}
