##' Function to compute shares (to express a derived commodities in its primary (or at least a higher) level.
##' This function has been written to be used in the DerivedCommodies_submodule which works level by level.
##' That's why this kind of shares are used to transform the derived into its direct parent.
##' that it is allocated in different productive processes.
##'
##' @param data input data.table: SUA table
##' @param tree Commodity tree, data.table
##' @param printNegativeAvailability It is TRUE if you want to produce csv file containing nagative availabilities
##' @param params computation parameters
##' @param printDirectory where intermidiate output should be saved in case printNegativeAvailability is TRUE
##' @param useAllSUAcomponents Corrently the availability is computing just a subset of components, this
##'                             parameter allows to use all the components
##'
##' @export
##'



calculateShareDownUp=function(data,tree, printNegativeAvailability=FALSE , params, printDirectory=NULL, useAllSUAcomponents=FALSE)

{
##Checks
stopifnot(c(params$parentVar,params$geoVar,params$yearVar,params$elementVarSUA,params$value)  %in% colnames(data) )    
stopifnot(c(params$parentVar,params$geoVar,params$yearVar,params$childVar,params$extractVar,  params$level) %in% colnames(tree))
    
dataMergeTree=merge(data,tree, by=c(params$parentVar, params$geoVar,params$yearVar))


if(useAllSUAcomponents){
##Simple availability that we interpret as FOOD PROCESSING: here I am using all the components
   dataMergeTree[, params$availVar := sum(ifelse(is.na(Value), 0, Value) *
                                       ifelse(measuredElementSuaFbs == params$productionCode, 1,
                                       ifelse(measuredElementSuaFbs == params$importCode, 1,
                                       ifelse(measuredElementSuaFbs == params$exportCode , -1, 
                                       ifelse(measuredElementSuaFbs == params$stockCode, -1,
                                       ifelse(measuredElementSuaFbs == params$foodCode, -1,
                                       ifelse(measuredElementSuaFbs == params$feedCode , -1,
                                       ifelse(measuredElementSuaFbs == params$wasteCode, -1,
                                       ifelse(measuredElementSuaFbs == params$seedCode, -1,
                                       ifelse(measuredElementSuaFbs == params$industrialCode, -1,
                                       ifelse(measuredElementSuaFbs == params$touristCode, -1, 0))))))))))),
                 by = c(params$geoVar,params$yearVar,params$parentVar,params$childVar)]
}else{
    
##Simple availability that we interpret as FOOD PROCESSING: this availability is based only on PRODUCTION, IMPORT and EXPORT
dataMergeTree[, params$availVar := sum(ifelse(is.na(Value), 0, Value) *
                                       ifelse(measuredElementSuaFbs == params$productionCode, 1,
                                       ifelse(measuredElementSuaFbs == params$importCode, 1,
                                       ifelse(measuredElementSuaFbs == params$exportCode , -1, 
                                       ifelse(measuredElementSuaFbs == params$stockCode, -1,
                                  ##     ifelse(measuredElementSuaFbs == params$foodCode, -1,
                                  ##     ifelse(measuredElementSuaFbs == params$feedCode , -1,
                                  ##     ifelse(measuredElementSuaFbs == params$wasteCode, -1,
                                       ifelse(measuredElementSuaFbs == params$seedCode, -1 ,0)))))),
                                  ##     ifelse(measuredElementSuaFbs == params$industrialCode, -1,
                                  ##     ifelse(measuredElementSuaFbs == params$touristCode, -1, 0))))))))))),
              by = c(params$geoVar,params$yearVar,params$parentVar,params$childVar)]

##The validation process of the final output requests to check all the components of the SUA tables: I print also the whole 
## table with all the components and the resulting availability (row 74)
}
##-------------------------------------------------------------------------------------------------------
##Deviate the negative agailability to be manually checked

if(printNegativeAvailability){
if(is.null(printDirectory)){
    message("No validation files have been created, please specify the directory to allocate intermediate validation files")}
    else
    {
nagativeAvailability=dataMergeTree[availability<1]
nagativeAvailability=nagativeAvailability[,.(measuredItemParentCPC,	geographicAreaM49, availability,timePointYears)]
nagativeAvailability=unique(nagativeAvailability)
nagativeAvailability[,measuredItemParentCPC:=paste0("'", measuredItemParentCPC)]
##directory= paste0("C:/Users/Rosa/Desktop/ProcessedCommodities/BatchExpandedItems/","Batch",batchNumber)
dir.create(paste0(printDirectory, "/nagativeAvailability"), recursive=TRUE)
dir.create(paste0(printDirectory,"/nagativeAvailability/", currentGeo), recursive=TRUE)

if(nrow(nagativeAvailability)>0){
write.csv(nagativeAvailability, paste0(printDirectory, "/nagativeAvailability/", "/",currentGeo, "/nagativeAvailability",lev,".csv"), sep=";",row.names = F)
}
##I print also the whole table in order to allow the procedure
dataMergeTreeToPrint=copy(dataMergeTree)
dataMergeTreeToPrint[,measuredItemChildCPC:=paste0("'", measuredItemChildCPC)]
dataMergeTreeToPrint[,measuredItemParentCPC:=paste0("'", measuredItemParentCPC)]
write.csv(dataMergeTreeToPrint, paste0(printDirectory, "/nagativeAvailability/", "/",currentGeo, "/InputSUATable",lev,".csv"), sep=";",row.names = F)
}
}

####------------------------------------------------------------------------------------------------------  

## I keep just the columns I need, note I am excluding the Element column because I am interested into te AVAILABITLITY
## I do not need all the components (prod, import, export, tourism, industial, stock... ) anymore. 
dataMergeTree=dataMergeTree[,c(params$parentVar,  params$geoVar, params$yearVar, params$childVar, params$extractVar,   
                              params$level, params$availVar), with=FALSE] 
## At this point we still have the availabilities repeated for each element of the original SUA table.
## For example if an Item X entered in the SUA table through 4 elements (prod, export, feed and stock), 
## we still have 4 rows with same availability
dataMergeTree = dataMergeTree[, list(availability = mean(get(params$availVar), na.rm = TRUE)),
                              by = c(params$parentVar,params$geoVar,params$yearVar,params$childVar,params$extractVar,  params$level)]



## In order to continue runnung the module even if some availability are lower than 0
## we make the assumption that: if the availability is equal to ZERO (or negative), it means that
## the productive process of its derived products cannot be activated (in any case it is the case
## to check the intial SUA table because there might be somethig wrong) 
dataMergeTree[get(params$availVar)<1,params$availVar:=0]				

## Express the whole availability of each parent in terms of child equivalent:
dataMergeTree[,availabilitieChildEquivalent:=get(params$availVar)* get(params$extractVar)]
## Sum of the availabilities express in terms of child eq. by child
dataMergeTree[, sumAvail:=sum(availabilitieChildEquivalent), by=c(params$childVar,params$yearVar,params$geoVar)]

## I create the column that has to be populated
dataMergeTree[,params$shareDownUp:=NA_real_]
dataMergeTree[,params$shareDownUp:=availabilitieChildEquivalent/sumAvail]

## Add a warning if the sum (by parent) of the shareDownUp is not equal to ONE, it should give a warning!!!

dataMergeTree[, check:=sum(shareDownUp), by=c(params$childVar,params$yearVar,params$geoVar)]
if(any( dataMergeTree[!is.na(check),check]>1)){
    
    warning("Some Share from down to up are greater than one!!")
    
    toCheck=dataMergeTree[dataMergeTree[!is.na(check),check]>1]
}


## Delete the columns I do not need anymore
dataMergeTree[,availabilitieChildEquivalent:=NULL]
dataMergeTree[,sumAvail:=NULL]
dataMergeTree[,check:=NULL]


return(dataMergeTree)


}

