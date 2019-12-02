##' 
##'
##' **Author: Amsata Niang**
##' **Author: Aydan Selek**
##' 
##' **Description:**
##'
##' This module is designed to identify and automatically correct the outliers of MILK production.
##'



# Import libraries
suppressMessages({
    library(data.table)
    library(faosws)
    library(faoswsFlag)
    library(faoswsUtil)
    library(faoswsImputation)
    library(faoswsProduction)
    library(faoswsProcessing)
    library(faoswsEnsure)
    library(magrittr)
    library(dplyr)
    library(sendmailR)
    
})



R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
    
    library(faoswsModules)
    SETTINGS = ReadSettings("C:/Users/Selek/Dropbox/1-FAO-DROPBOX/faoswsProduction/modules/outlierDetectionProd/sws.yml")
    
    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH = SETTINGS[["share"]]
    
    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])
    
    ## Get session information from SWS. Token must be obtained from web interface
    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])
    
}


# No need now: offtake rates are now calculated
dir_to_save <- file.path(R_SWS_SHARE_PATH, "Livestock",paste0("validationFAODOMAIN_rosa"))
# if(!file.exists(dir_to_save)){
#     dir.create(dir_to_save, recursive = TRUE)
# }

# Load and check the computation parameters
imputationSelection = swsContext.computationParams$imputation_selection
if(!imputationSelection %in% c("session", "all"))
    stop("Incorrect imputation selection specified")

imputationTimeWindow = swsContext.computationParams$imputation_timeWindow
if(!imputationTimeWindow %in% c("all", "lastThree"))
    stop("Incorrect imputation selection specified")

# Get data configuration and session
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

# ##' Build processing parameters
# processingParameters =
#     productionProcessingParameters(datasetConfig = datasetConfig)



# lastYear=max(as.numeric(swsContext.computationParams$last_year))

##' Obtain the complete imputation key
# completeImputationKey = getCompleteImputationKey("production")

# completeImputationKey@dimensions$timePointYears@keys <-
# as.character(min(completeImputationKey@dimensions$timePointYears@keys):lastYear)



##' Extract the animal parent to child commodity mapping table
##'
##' This table contains the parent item/element code which maps to the child
##' item/element code. For example, the slaughtered animal element for cattle is
##' 5315, while the slaughtered animal for cattle meat is 5320.
##'
##  Ideally, the two elements should be merged and have a single
##  code in the classification. This will eliminate the change of
##  code in the transfer procedure.

# animalMeatMappingTable = ReadDatatable("animal_parent_child_mapping")
#animalMilkCorr = ReadDatatable('animal_milk_correspondence')
# ##When pulled from the SWS the datatable header cannot contain capital letters
#setnames(animalMilkCorr,c("animal_item_cpc", "milk_item_cpc"), c("measuredItemAnimalCPC", "measuredItemMilkCPC"))

# animalMeatMappingTable= animalMeatMappingTable[,.(measuredItemParentCPC, measuredElementParent,
#                                                   measuredItemChildCPC, measuredElementChild)]

##' Here we expand the session to include all the parent and child items. That
##' is, we expand to the particular livestock tree.
##'
##' For example, if 02111 (Cattle) is in the session, then the session will be
##' expanded to also include 21111.01 (meat of cattle, freshor chilled), 21151
##' (edible offal of cattle, fresh, chilled or frozen), 21512 (cattle fat,
##' unrendered), and 02951.01 (raw hides and skins of cattle).
##'
##' The elements are also expanded to the required triplet.
# 
# livestockImputationItems =
#     completeImputationKey %>%
#     expandMeatSessionSelection(oldKey = .,
#                                selectedMeatTable = animalMeatMappingTable) %>%
#     getQueryKey("measuredItemCPC", datasetkey = .) %>%
#     selectMeatCodes(itemCodes = .)
# 
# sessionItems =
#     sessionKey %>%
#     expandMeatSessionSelection(oldKey = .,
#                                selectedMeatTable = animalMeatMappingTable) %>%
#     getQueryKey("measuredItemCPC", datasetkey = .) %>%
#     selectMeatCodes(itemCodes = .)
# 
# ##' Select the range of items based on the computational parameter.
# selectedMeatCode =
#     switch(imputationSelection,
#            session = sessionItems,
#            all = livestockImputationItems)
# 
# 

## Get the animal data (NB: preProcessing: manage NA M and transform timePointYears)
## Get the MILK data (NB: preProcessing: manage NA M and transform timePointYears)


# triplet of livestock with stocks in head
milk_triplet_lst_1 <- list(input="5111", output="5318", productivity="9999")
milk_triplet_lst_2 <- list(input="5318", output="5510", productivity="5417")
# 
#  # triplet of livestock with stocks in head
#  livestock_triplet_lst_1 <- list(input="5111", output="5315", productivity="9999")
#  livestock_triplet_lst_2 <- list(input="5320", output="5510", productivity="5417") 
#  
#  #Livestock triplet with stock unit in 1000 head
#   livestock_triplet_lst_1bis <- list(input="5112", output="5316", productivity="9999") 
#   livestock_triplet_lst_2bis <- list(input="5321", output="5510", productivity="5424") 
#  
#  
#  #triplets for crops
#   crop_triplet_lst<-list(input="5312",output="5510",productivity="5421")
#  


#to remove
# triplets<-c("5312","5510","5421","5111","5315",
#             "5320","5417", "5112","5316",
#             "5321","5510","5424")


#to remove
# animalKey = completeImputationKey
# animalKey@dimensions$measuredItemCPC@keys = currentAnimalItem
# animalKey@dimensions$measuredElement@keys =triplets
#     with(animalFormulaParameters,
#          c(productionCode))

# sessionKey@dimensions$geographicAreaM49<-completeImputationKey@dimensions$geographicAreaM49

#reading outputs of estimation session data
animalData =
    sessionKey %>%
    GetData(key = .) #%>%
#preProcessing(data = .)

#clean the subset of country it was just for testing
data=animalData #[geographicAreaM49 %in% c("840","426","440")]

#new parameters outliers_threshold, imputation_selection, last_year,outliers_threshold

# startYear = as.numeric(swsContext.computationParams$startYear)
endYear = as.numeric(swsContext.computationParams$last_year)
geoM49 = swsContext.computationParams$geom49

# Set-up the parameters
# startYear <- 2014
# endYear <- 2018
THRESHOLD_IMPUTED <-as.numeric(swsContext.computationParams$outliers_threshold)
window <- 5
startYear<-endYear-window+1
yearVals<-startYear:endYear

interval <- (startYear-window):(startYear-1)
# yearVals <- (startYear-window):endYear

# Read the data needed
flagValidTable <- ReadDatatable("valid_flags")

data1 <- data[measuredItemCPC %in% c('02211', '02212', '02291', '02292', '02293') & measuredElement == '5318',]
mapping <- ReadDatatable('animal_milk_correspondence')
setnames(mapping,c("animal_item_cpc", "milk_item_cpc"), c("measuredItemAnimalCPC", "measuredItemCPC"))

data2 <- merge(data1, mapping, by='measuredItemCPC')
data2[, measuredItemCPC:= NULL]
setnames(data2,"measuredItemAnimalCPC", "measuredItemCPC")

data<-rbind(data,data2)

#the FBS tree is used to map only meat items but not offals and fats
# message("Download fbsTree from SWS...")
# fbsTree=ReadDatatable("fbs_tree")
# fbsTree=data.table(fbsTree)
# setnames(fbsTree,colnames(fbsTree),c( "fbsID1", "fbsID2", "fbsID3","fbsID4", "measuredItemSuaFbs"))
# setcolorder(fbsTree,c("fbsID4", "measuredItemSuaFbs", "fbsID1", "fbsID2", "fbsID3"))

#extract meat items from the fbstree
# meatItem<-fbsTree[fbsID3=="2943",get("measuredItemSuaFbs")]
# 
# mapping<-mapping[measured_item_child_cpc %in% meatItem]




#triplets <- ReadDatatable("item_type_yield_elements")
#triplets$element_41[19] = "9999"# delete after off-take rate saved
# data <- readRDS(paste0("//hqlprsws1.hq.un.fao.org/sws_r_share/mongeau/tmp/production_outliers/data/production/", COUNTRY, ".rds"))

# itemMap <- GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredItemCPC")
# itemMap <- itemMap[!is.na(type), .(measuredItemCPC = code, item_type = type)]  
# 
# # Identify the triplets of: Input-Outpot-Productivity
# item_map_factor <-
#     triplets[
#         nchar(element_41) == 4
#         ][,
#           .(item_type, input = element_31, productivity = element_41, output = element_51, factor)
#           ]
# 
# item_map_factor <-
#     melt(
#         item_map_factor,
#         id.vars = c("item_type", "factor"),
#         value.name = "measuredElement"
#     )
# 
# map_items <-
#     merge(
#         item_map_factor[, .(item_type, measuredElement)],
#         itemMap,
#         by = "item_type",
#         all.x = TRUE,
#         allow.cartesian = TRUE
#     )
# 
# map_items <-
#     merge(
#         map_items, 
#         item_map_factor[, .(item_type, variable, measuredElement)], 
#         by = c("item_type", "measuredElement")
#     )
# 
# map_items[, item_type := NULL]
# 
# data <- merge(data, map_items, by = c("measuredElement", "measuredItemCPC"), all.x = TRUE)


#estimating offtake rates for livestock stocks in head
# Estimating the ratio between Live animals and Milk animals (these animals are only in head)
dataEstYield <-data[measuredElement %in% c("5111","5318")]

dataEstYield <- dcast.data.table(dataEstYield, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value'))
dataEstYield[,Value:=`5111`/`5318`]
dataEstYield[,`5111`:=NULL]
dataEstYield[,`5318`:=NULL]
dataEstYield[,measuredElement:=9999]

dataEstYield[,flagObservationStatus:="I"]
dataEstYield[,flagMethod:="i"]

dataEstYield<-dataEstYield[,names(data),with=FALSE]
dataEstYield<-unique(dataEstYield,by=c(colnames(data)))

data<-rbind(data,dataEstYield)


#Offtake rates for livestock in 1000 heads
# dataEstYield2<-data[measuredElement %in% c("5112","5316")]
# 
# dataEstYield2<-  dcast.data.table(dataEstYield2, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value'))
# dataEstYield2[,Value:=`5316`/`5112`]
# dataEstYield2[,`5112`:=NULL]
# dataEstYield2[,`5316`:=NULL]
# dataEstYield2[,measuredElement:=9999]
# 
# dataEstYield2[,flagObservationStatus:="I"]
# dataEstYield2[,flagMethod:="i"]
# 
# dataEstYield2<-dataEstYield2[,names(data),with=FALSE]
# dataEstYield2<-unique(dataEstYield2,by=c(colnames(data)))
# 
# data<-rbind(data,dataEstYield2)


## To be deleted after off-take inserted
# pathtoofftake<-pathtoofftake<-paste0(dir_to_save,"/",list.files(dir_to_save))
# 
# 
# data_list <- lapply(pathtoofftake,read.csv)
# 
# for(i in seq_along(data_list)) {
#   df<-data_list[[i]]
#   df<-as.data.table(df)
#   
#   data_list[[i]] <- df[,list(geographicAreaM49,timePointYears,
#                              measuredItemCPC=paste0("0",measuredItemCPC),
#                              takeOffRate,TakeOffFlagObservationStatus,
#                              TakeOffRateFlagMethod)]
#   
# }
# 
# 
# data_offtake <- do.call("rbind",data_list)
# data_offtake <- data_offtake[geographicAreaM49==840,]
# setnames(data_offtake, "TakeOffFlagObservationStatus", "flagObservationStatus")
# setnames(data_offtake, "TakeOffRateFlagMethod", "flagMethod")
# data_offtake$geographicAreaM49 <- as.character(data_offtake$geographicAreaM49)
# data_offtake$timePointYears <- as.character(data_offtake$timePointYears)
# setnames(data_offtake,"takeOffRate","Value")
# 
# data_offtake[,measuredElement:=9999] #arbitrary
# 
# data_offtake<-data_offtake[,Value:=mean(Value,na.rm = TRUE),
#                            by=c("geographicAreaM49","timePointYears","measuredItemCPC")]
# 
# data_offtake[,flagObservationStatus:="E"] #arbitrary
# data_offtake[,flagMethod:="n"] #arbitrary
# 
# data_offtake<-unique(data_offtake,by=c(colnames(data_offtake)))
# 
# data_offtakecomplete <-
#     CJ(
#         geographicAreaM49 = unique(data_offtake$geographicAreaM49),
#         measuredElement = 9999,
#         timePointYears = unique(data_offtake$timePointYears),
#         measuredItemCPC = unique(data_offtake$measuredItemCPC)
#     )
# 
# data_offtake<-merge(
#     data_offtakecomplete,
#     data_offtake,
#     by=c("geographicAreaM49","timePointYears","measuredItemCPC","measuredElement"),
#     all.x =TRUE
# )
# 
# data_offtake[,flagObservationStatus:="E"] #arbitrary
# data_offtake[,flagMethod:="n"] #arbitrary
# data_offtake<-data_offtake[,names(data),with=FALSE] #arbitrary
# 
# 
# data<-rbind(data,data_offtake)
# 
# data_ititial<-copy(data)

data <- merge(
    data,
    flagValidTable,
    by=c("flagObservationStatus","flagMethod")
)

data[is.na(Protected),Protected:=FALSE]
# data <- merge(data, data_offtake, by = c("geographicAreaM49", "timePointYears", "measuredItemCPC", "flagObservationStatus", "flagMethod"), all.x = TRUE)
#complete triplet


# livestock_triplet_lst_1bisvec <- c("5112","5316","9999") # delete after
# inteminput<-unique(data[measuredElement %in% livestock_triplet_lst_1bisvec,get("measuredItemCPC")])
# 
# 
# trippletcomplete <-
#     CJ(
#         geographicAreaM49 = unique(data$geographicAreaM49),
#         measuredElement = livestock_triplet_lst_1bisvec,
#         timePointYears = unique(data$timePointYears),
#         measuredItemCPC = inteminput
#     )
# 
# trippletcomplete<-merge(
#     data,
#     trippletcomplete,
#     by=c("geographicAreaM49","timePointYears","measuredItemCPC","measuredElement"),
#     all.y = TRUE
# )
# 
# trippletcomplete<-trippletcomplete[,names(data),with=FALSE]
# 
# 
# data<-rbind(
#     data,
#     trippletcomplete[!data, on=c('geographicAreaM49', 'timePointYears',
#                                  'measuredItemCPC', 'measuredElement')]
# )


# this function complete a triplet in case one or two element are missng
complete_triplet<-function(data,triplets){
    
    #complete livestocks triplt
    itemProd<-unique(data[measuredElement %in% triplets$productivity ,get("measuredItemCPC")])
    
    trippletcomplete <-
        CJ(
            geographicAreaM49 = unique(data$geographicAreaM49),
            measuredElement = unlist(triplets,use.names = FALSE),
            timePointYears = unique(data$timePointYears),
            measuredItemCPC = itemProd
        )
    
    trippletcomplete<-merge(
        data,
        trippletcomplete,
        by=c("geographicAreaM49","timePointYears","measuredItemCPC","measuredElement"),
        all.y = TRUE
    )
    
    trippletcomplete<-trippletcomplete[,names(data),with=FALSE]
    
    data<-rbind(
        data,
        trippletcomplete[!data, on=c('geographicAreaM49', 'timePointYears',
                                     'measuredItemCPC', 'measuredElement')]
    )
    return(data)
    
}

#this may not be important when reading sessions after imputation
#because all should be complete
#data<-complete_triplet(data,crop_triplet_lst)
data<-complete_triplet(data,milk_triplet_lst_1)
data<-complete_triplet(data,milk_triplet_lst_2)
# data<-complete_triplet(data,livestock_triplet_lst_1bis)
# data<-complete_triplet(data,livestock_triplet_lst_2bis)


#### FUNCTIONS #####

`%!in%` = Negate(`%in%`)
# rollavg() is a rolling average function that uses computed averages
# to generate new values if there are missing values (and FOCB/LOCF).
# I.e.:
# vec <- c(NA, 2, 3, 2.5, 4, 3, NA, NA, NA)
#
#> RcppRoll::roll_mean(myvec, 3, fill = 'extend', align = 'right')
#[1]       NA       NA       NA 2.500000 3.166667 3.166667       NA       NA       NA 
#
#> rollavg(myvec)
#[1] 2.000000 2.000000 3.000000 2.500000 4.000000 3.000000 3.166667 3.388889 3.185185

rollavg <- function(x, order = 3) {
    # order should be > 2
    stopifnot(order >= 3)
    
    non_missing <- sum(!is.na(x))
    
    # For cases that have just two non-missing observations
    order <- ifelse(order > 2 & non_missing == 2, 2, order)
    
    if (non_missing == 1) {
        x[is.na(x)] <- na.omit(x)[1]
    } else if (non_missing >= order) {
        n <- 1
        while(any(is.na(x)) & n <= 10) { # 10 is max tries
            movav <- suppressWarnings(RcppRoll::roll_mean(x, order, fill = 'extend', align = 'right'))
            movav <- data.table::shift(movav)
            x[is.na(x)] <- movav[is.na(x)]
            n <- n + 1
        }
        
        x <- zoo::na.fill(x, 'extend')
    }
    
    return(x)
}


#This function allow to transfert slaughter from parent livestock triplet: (stocks,slaughtered,off_take_rate)
#to child triplet: (slaughtered,carcass_weight,production of meat)

# update_slaughter<-function(data, mappingData,sendTo,from="parent"){
#     
#     mapping1<-copy(mappingData)
#     
#     stopifnot(sendTo %in% c("parent","child"))
#     stopifnot(from %in% c("parent","child"))
#     
#     element<-paste0("measured_element_",sendTo)
#     item<-paste0("measured_item_",sendTo,"_cpc")
#     
#     element1<-paste0("measured_element_",from)
#     item1<-paste0("measured_item_",from,"_cpc")
#     
#     setnames(mapping1,c(item),c("measuredItemCPC"))
#     setnames(mapping1,c(element),"measuredElement")
#     
#     datamerged<-merge(
#         data,
#         mapping1,
#         by=c("measuredElement", "measuredItemCPC")
#     )
#     
#     datamerged<-datamerged[,c("geographicAreaM49","timePointYears",
#                               element1,
#                               item1,
#                               "Value","flagObservationStatus","flagMethod",
#                               "Valid","Protected"),with=FALSE
#                            ]
#     setnames(datamerged,c(item1),c("measuredItemCPC"))
#     setnames(datamerged,c(element1),"measuredElement")
#     
#     
#     datamerged<-datamerged[,names(data),with=FALSE]
#     
#     data<-rbind(
#         data,
#         datamerged[!data, on=c('geographicAreaM49', 'timePointYears',
#                                'measuredItemCPC', 'measuredElement')]
#     )
#     
#     return(data)
#     
# }

#this function take a wide data of  a triplet and creat boolean outlier variable for the element of the triplet
Label_outlier<-function(data=data, element, type){
    Value <- paste0("Value_", element)
    Protected <- paste0("Protected_", element)
    outlier <- paste0("isOutlier_", type)
    
    data[,timeCondition:=ifelse(timePointYears %in% interval ,TRUE,FALSE)]
    
    data[,proCondition:=ifelse(timePointYears %in% yearVals & get(Protected)==TRUE,TRUE,FALSE)]
    
    data[,meanCondition:=ifelse(timeCondition==TRUE & proCondition==TRUE,TRUE,FALSE)]
    
    
    data[,
         Meanold := mean(get(Value)[meanCondition==TRUE], na.rm = TRUE),
         by = c('geographicAreaM49', 'measuredItemCPC')
         ]
    
    data[is.na(get(Value)), c(Value) := 0]
    
    data[, ratio := get(Value) / Meanold]
    
    data[,c(outlier):=ifelse(abs(ratio-1) > THRESHOLD_IMPUTED & get(Protected)==FALSE,TRUE,FALSE)]
    
    data[,`:=`(Meanold=NULL,ratio=NULL)]
    
    return(data)
}


# imput_with_average() function find the outliers  the imputation for given elements (in this case only for Productivity)
#no more used: to clean
imput_with_average <- function(data,element){
    
    data_element<-data[measuredElement %in% element]
    
    data_element[,
                 Meanold := mean(Value[timePointYears %in% interval], na.rm = TRUE),
                 by = c('geographicAreaM49', 'measuredItemCPC', 'measuredElement')
                 ]
    
    data_element[is.na(Value), Value := 0]
    
    data_element[, ratio := Value / Meanold]
    
    data_element[,is_outlier:=ifelse(abs(ratio-1) > THRESHOLD_IMPUTED & Protected==FALSE,TRUE,FALSE)]
    # 
    data_element[,value_new:=Value]
    data_element[is_outlier==TRUE,value_new:=NA]
    
    data_element <-
        data_element[
            order(geographicAreaM49, measuredElement, measuredItemCPC, timePointYears),
            value_avg := rollavg(value_new, order = 3),
            by = c("geographicAreaM49", "measuredElement", "measuredItemCPC")
            ]
    setkey(data_element, NULL)
    
    
    data_element[is_outlier==TRUE,`:=`(Value=value_avg,
                                       flagObservationStatus="I",
                                       flagMethod="e")]
    
    data_element<-data_element[,colnames(data),with=FALSE]
    
    data<-rbind(
        data_element,
        data[!data_element,on=c("geographicAreaM49","measuredElement")]
    )
    return(data)
}


#compute moving avarage variable foran element of a triplet
#the varname will be: movav_element example movav_input
compute_movav<-function(data=data_crop,element="5510",type="output") {
    Value<-paste0("Value_",element)
    movag<-paste0("movav_",type)
    
    outlier <- paste0("isOutlier_", type)
    
    data[,value_new:=get(Value)]
    
    #compute moving average without outlier
    data[get(outlier)==TRUE,value_new:=NA]
    
    data <-
        data[
            order(geographicAreaM49,measuredItemCPC, timePointYears),
            c(movag) := rollavg(value_new, order = 3),
            by = c("geographicAreaM49","measuredItemCPC")
            ]
    
    data[,value_new:=NULL]
    return(data)
}


#Correcting outliers
#Parametrize input var, output var and productivity var

correctInputOutput<-function(data=data,
                             triplet=milk_triplet_lst_1,
                             partial=FALSE,
                             factor=1
) {
    
    #TO DO: quality check
    
    
    # data_triplet<-copy(data)
    
    data_triplet <- data[measuredElement %in% triplet]
    
    # data_triplet<-imput_with_average(data_triplet,triplet$productivity)
    
    inteminput<-data_triplet[measuredElement %in% triplet$input,get("measuredItemCPC")]
    
    data_triplet<-data_triplet[measuredItemCPC %in% inteminput]
    
    
    data_triplet<- dcast.data.table(data_triplet, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value', 'Protected'))
    
    
    Label_outlier(data=data_triplet,element=triplet$output,type="output")
    compute_movav(data=data_triplet,element=triplet$output,type="output")
    
    Label_outlier(data=data_triplet,element=triplet$input,type="input")
    compute_movav(data=data_triplet,element=triplet$input,type="input")
    
    Label_outlier(data=data_triplet,element=triplet$productivity,type="productivity")
    compute_movav(data=data_triplet,element=triplet$productivity,type="productivity")
    
    
    input<-paste0("Value_",triplet$input)
    output<-paste0("Value_",triplet$output)
    productivity<-paste0("Value_",triplet$productivity)
    
    
    data_triplet[isOutlier_productivity==TRUE,c(productivity):=movav_productivity]
    
    # data_triplet[isOutlier_input==TRUE & isOutlier_output==FALSE,c(input):=ifelse(get(productivity)!=0,get(output)/get(productivity)*factor,NA)]
    # data_triplet[isOutlier_input==TRUE & isOutlier_output==FALSE,isOutlier_input:=FALSE]
    
    data_triplet[isOutlier_input==FALSE & isOutlier_output==TRUE,
                 c(output):=get(input)*get(productivity)/factor]
    
    data_triplet[isOutlier_input==FALSE & isOutlier_output==TRUE,
                 isOutlier_output:=TRUE]
    
    if (partial==FALSE) {
        
        data_triplet[isOutlier_input==TRUE & isOutlier_output==TRUE,
                     c(output):=movav_output]
        data_triplet[isOutlier_input==TRUE & isOutlier_output==TRUE,
                     c(input):=movav_output/get(productivity)*factor]
        
        data_triplet[isOutlier_input==TRUE & isOutlier_output==TRUE,
                     `:=`(isOutlier_input=FALSE,isOutlier_output=FALSE)]
        
        #update the productivity
        data_triplet[,c(productivity):=get(output)/get(input)*factor]
        
        data_triplet[get(output)==0,c(productivity):=0]
        data_triplet[is.na(get(output)),c(productivity):=0]
        
        
        
        
    }
    
    
    #Putting the data in the initial format
    
    outlierdata<-data_triplet[,list(geographicAreaM49,timePointYears,measuredItemCPC,isOutlier_productivity)]
    
    id.vars=c("geographicAreaM49","timePointYears","measuredItemCPC")
    data_triplet<-data_triplet[,c(id.vars,grep("^Value",names(data_triplet),value = TRUE)),with=FALSE]
    
    data_triplet<-melt(data_triplet, id.vars=c("geographicAreaM49","timePointYears",
                                               "measuredItemCPC"), grep("^Value",names(data_triplet),value = TRUE),
                       variable.name = "measuredElement", value.name = "value_new")
    
    data_triplet[,measuredElement:=substr(measuredElement,start = 7,stop = 10)]
    data_triplet[value_new==Inf,value_new:=NA_real_]
    
    data<-merge(
        data,
        data_triplet,
        by=c("geographicAreaM49","timePointYears","measuredElement", "measuredItemCPC"),
        all.x = TRUE
    )
    
    data<-merge(
        data,
        outlierdata,
        by=c("geographicAreaM49","timePointYears", "measuredItemCPC"),
        all.x = TRUE
    )
    
    
    data[,difference:=value_new-Value]
    
    # data[,check:=ifelse(Protected==FALSE & !is.na(value_new) &
    #                         round(value_new,6)!=round(Value,6) & 
    #                         timePointYears %in% yearVals,TRUE,FALSE)]
    
    data[is.na(Protected),Protected:=FALSE]
    data[is.na(isOutlier_productivity),isOutlier_productivity:=FALSE]
    
    data[(measuredElement %!in% triplet$productivity) & (Protected==FALSE) & (!is.na(value_new)) & (round(value_new,6)!=round(Value,6)) &
             (timePointYears %in% yearVals) ,`:=`(Value=value_new,
                                                  flagObservationStatus="E",
                                                  flagMethod="e")]
    
    data[measuredElement %in% triplet$productivity & Protected==FALSE & !is.na(value_new) & round(value_new,6)!=round(Value,6) &
             timePointYears %in% yearVals ,`:=`(Value=value_new,
                                                flagObservationStatus=ifelse(isOutlier_productivity==TRUE,"E",flagObservationStatus
                                                ),
                                                flagMethod=ifelse(isOutlier_productivity==TRUE,"i",flagMethod))]
    
    # data[measuredElement %in% triplet$productivity & Protected==FALSE & !is.na(value_new) & round(value_new,6)!=round(Value,6) &
    #          timePointYears %in% yearVals ,Value:=value_new]
    data[,value_new:=NULL]
    data[,difference:=NULL]
    data[,isOutlier_productivity:=NULL]
    
    
    return(data)
    
}

# data<-correctInputOutput(data,triplet = crop_triplet_lst,partial = FALSE)
# 
# #livestocks type 1: stocks in head
data<-correctInputOutput(data,triplet = milk_triplet_lst_1,partial = TRUE)
# 
#data<-update_slaughter(data=data,mappingData = mapping,sendTo = "child",from = "parent") ### CHECK THIS
# 
data<-correctInputOutput(data,triplet = milk_triplet_lst_2,partial = FALSE,factor = 1000)

#data<-update_slaughter(data=data,mappingData = mapping,sendTo = "parent",from = "child")  ### CHECK THIS

# data<-correctInputOutput(data,triplet = livestock_triplet_lst_1,partial = FALSE)
# 
# 
# 
# #livestock second type: stock in 1000 heads
# 
# livestock_triplet_lst_1bis <- list(input="5112", output="5316", productivity="9999") # delete after


# data<-correctInputOutput(data,triplet = livestock_triplet_lst_1bis,partial = TRUE)
# 
# data<-update_slaughter(data=data,mappingData = mapping,sendTo = "child",from = "parent")
# 
# data<-correctInputOutput(data,triplet = livestock_triplet_lst_2bis,partial = FALSE,factor = 1000)
# 
# data<-update_slaughter(data=data,mappingData = mapping,sendTo = "parent",from = "child")
# 
# data<-correctInputOutput(data,triplet = milk_triplet_lst_1,partial = FALSE)


dataf <- data[measuredElement!="9999"]
dataf <- dataf[!is.na(Value)]

dataf <- dataf[timePointYears %in% yearVals,c(names(animalData)), with=FALSE]

SaveData(domain = datasetConfig$domain,
         dataset = datasetConfig$dataset,
         data = dataf, waitTimeout = 2000000)


# Productivity outliers after update

productivityVector <- c("5417", "9999")

data_element<-data[measuredElement %in% productivityVector]

data_element[,
             Meanold := mean(Value[timePointYears %in% interval], na.rm = TRUE),
             by = c('geographicAreaM49', 'measuredItemCPC', 'measuredElement')
             ]

data_element[is.na(Value), Value := 0]

data_element[, ratio := Value / Meanold]

data_element[,is_outlier:=ifelse(abs(ratio-1) > THRESHOLD_IMPUTED & Protected==FALSE,TRUE,FALSE)]

data_outlier <- data_element[is_outlier==TRUE & timePointYears %in% yearVals]

data_outlier <- nameData(datasetConfig$domain,datasetConfig$dataset,data_outlier)

write.csv(data_outlier,"Outliers.csv")
