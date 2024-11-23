##' Function to compute Processing sharing: share that identify the quantity of primary availability 
##' that it is allocated in different productive processes.
##'
##' @param data data table containing all the columns to compute processingSharing
##' @param params defaultProcessedItamParams parameters, object which contains the parameters  
##'
##' @export
##'


calculateProcessingShare=function(data, printSharesGraterThan1=FALSE, param){
                                  
    
##Check that data contains all the necessary columns    
stopifnot(c(param$geoVar, param$yearVar, param$childVar, param$parentVar,
            param$extractVar, param$shareDownUp ,params$value, param$availVar) %in% colnames(data))

##data[, processingShare:=(((get(param$value)/get(param$extractVar))*get(param$shareDownUp))/get( param$availVar ))]
data[,param$processingShare:= (( get(params$value)/get (param$extractVar) )* get(param$shareDownUp) )/get((param$availVar))] 


## This merge eventually used the processing tree (the tree extracte dby Cristina from)
##data=merge(data,processingTree, by=c("geographicAreaM49", "measuredItemChildCPC", "timePointYears", "measuredItemParentCPC", "extractionRate"))



##data[flagObservationStatus=="E" & flagMethod=="h" & get(param$processingShare)>1.02, processingShare:=1]
##if processing share are Inf, it means that the availability is 0, so the share must be zero as well.
data[processingShare==Inf,processingShare:=0 ]


## Force the processing share not exceeding 1 

data[processingShare>1,processingShare:=1 ]

##Attempt to use shares coming from the old system, with shares here we mean the share down up (processing shares)
##data[!is.na(pShare), processingShare:=pShare]

##-------------------------------------------------------------------------------------------------------
##Here we should perform an imputation of the imputation on 
processingShareParamenters=defaultImputationParameters()
processingShareParamenters$imputationValueColumn="processingShare"
processingShareParamenters$imputationFlagColumn="processingShareFlagObservationStatus"
processingShareParamenters$imputationMethodColumn="processingShareFlagMethod"
processingShareParamenters$byKey=c("geographicAreaM49", "measuredItemChildCPC", "measuredItemParentCPC")
processingShareParamenters$estimateNoData=FALSE


processingShareParamenters$ensembleModels$defaultExp=NULL
##processingShareParamenters$ensembleModels$defaultLogistic=NULL
processingShareParamenters$ensembleModels$defaultLoess=NULL
processingShareParamenters$ensembleModels$defaultSpline=NULL
processingShareParamenters$ensembleModels$defaultMars=NULL
processingShareParamenters$ensembleModels$defaultMixedModel=NULL
##processingShareParamenters$ensembleModels$defaultMovingAverage=NULL
##processingShareParamenters$ensembleModels$defaultArima=NULL

##dataInpute=copy(data)
data[,processingShareFlagObservationStatus:="M"]
data[,processingShareFlagMethod:="u"]

## I am flagging the just computed flags with a protected flag combination
## in ordert to use them as training set to produce imputations
data[!is.na(processingShare),processingShareFlagObservationStatus:="T"]
data[!is.na(processingShare),processingShareFlagMethod:="-"]
data[processingShare=="NaN", processingShare:=NA_real_]

##Remove series with no data
counts = data[, sum(!is.na(processingShare)),
               by = c(processingShareParamenters$byKey)]
counts=counts[V1!=0]
counts=counts[,c(param$geoVar, param$childVar, param$parentVar), with=FALSE]
data=data[counts, ,on=c(param$geoVar, param$childVar, param$parentVar)]

## impute processingSharing
data=imputeVariable(data,processingShareParamenters)



## Force the processing share not exceeding 1 
data[processingShare>1,processingShare:=1 ]
##-------------------------------------------------------------------------------------------------------


return(data)

}

