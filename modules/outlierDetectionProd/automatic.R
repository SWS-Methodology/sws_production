##' 
##'
##' **Author: Amsata Niang**
##' **Author: Aydan Selek**
##' 
##' **Description:**
##'
##' This module is designed to identify and automatically correct the outliers in the production environement.
##' It can detect any triplets (so called productivity, input and output) in Crop or Livestock.
##'

message("plug-in starts to run")

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
    SETTINGS = ReadSettings("C:\\Users\\NIANGAM\\Dropbox\\GITHUB\\faoswsProduction\\modules\\outlierDetectionProd/sws.yml")
    
    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH = SETTINGS[["share"]]
    
    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])
    
    ## Get session information from SWS. Token must be obtained from web interface
    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])
    
}

# Off-take rates are written on ..... 
# No need now: offtake rates are now calculated
# dir_to_save <- file.path(R_SWS_SHARE_PATH, "Livestock",paste0("validationFAODOMAIN_rosa"))
# if(!file.exists(dir_to_save)){
#     dir.create(dir_to_save, recursive = TRUE)
# }

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


# This function allows to transfert slaughter from parent livestock triplet: (stocks, slaughtered, off_take_rate)
# to child triplet: (slaughtered, carcass_weight, production of meat)

update_slaughter <- function(data, mappingData, sendTo, from = "parent"){

    stopifnot(sendTo %in% c("parent","child"))
    stopifnot(from %in% c("parent","child"))  
    
    mapping1 <- copy(mappingData)
    
    element <- paste0("measured_element_", sendTo)
    item <- paste0("measured_item_", sendTo, "_cpc")
    
    element1 <- paste0("measured_element_", from)
    item1 <- paste0("measured_item_", from, "_cpc")
    
    setnames(mapping1, c(item), c("measuredItemCPC"))
    setnames(mapping1, c(element), "measuredElement")
    
    datamerged <- merge(
        data,
        mapping1,
        by = c("measuredElement", "measuredItemCPC")
    )
    
    datamerged <- datamerged[, c("geographicAreaM49","timePointYears",
                                 element1,
                                 item1,
                                 "Value", "flagObservationStatus", "flagMethod",
                                 "Valid", "Protected", "EF"),with=FALSE
                           ]
    setnames(datamerged, c(item1), c("measuredItemCPC"))
    setnames(datamerged, c(element1), "measuredElement")
    
    datamerged <- datamerged[, names(data), with = FALSE]
    
    data <- rbind(
        data,
        datamerged[!data, on=c('geographicAreaM49', 'timePointYears',
                               'measuredItemCPC', 'measuredElement')]
    )
    
    return(data)
    
}

# This function takes a wide data of a triplet and creates boolean outlier variable for the element of the triplet
Label_outlier <- function(data = data, element, type){
  Value <- paste0("Value_", element)
  Protected <- paste0("Protected_", element)
  outlier <- paste0("isOutlier_", type)
  
  data[, timeCondition:= ifelse(timePointYears %in% interval, TRUE, FALSE)]
  
  data[, proCondition:= ifelse(timePointYears %in% yearVals & get(Protected) == TRUE, TRUE, FALSE)]
  
  data[, meanCondition:= ifelse(timeCondition == TRUE | proCondition == TRUE, TRUE, FALSE)]
  
  data[,
       Meanold := mean(get(Value)[meanCondition == TRUE], na.rm = TRUE),
       by = c('geographicAreaM49', 'measuredItemCPC')
       ]
  
  data[is.na(get(Value)), c(Value):= 0]
  
  data[, ratio:= get(Value) / Meanold]
  
  data[, c(outlier):= ifelse(abs(ratio-1) > THRESHOLD_IMPUTED & get(Protected) == FALSE, TRUE, FALSE)]
  
  data[,`:=`(Meanold = NULL, ratio = NULL)]
  
  return(data)
}


# imput_with_average() function finds the outliers the imputation for given elements (in this case only for Productivity)
# no more used: TO CLEAN
imput_with_average <- function(data,element){
  
  data_element <- data[measuredElement %in% element]
  
  data_element[,
               Meanold := mean(Value[timePointYears %in% interval], na.rm = TRUE),
               by = c('geographicAreaM49', 'measuredItemCPC', 'measuredElement')
               ]

  data_element[is.na(Value), Value:= 0]

  data_element[, ratio:= Value / Meanold]

  data_element[, is_outlier:= felse(abs(ratio-1) > THRESHOLD_IMPUTED & Protected == FALSE, TRUE, FALSE)]
  # 
  data_element[, value_new:= Value]
  data_element[is_outlier == TRUE, value_new:= NA]
  
  data_element <-
    data_element[
      order(geographicAreaM49, measuredElement, measuredItemCPC, timePointYears),
      value_avg := rollavg(value_new, order = 3),
      by = c("geographicAreaM49", "measuredElement", "measuredItemCPC")
      ]
  setkey(data_element, NULL)
  
  data_element[is_outlier == TRUE, `:=`(Value = value_avg,
                                                 flagObservationStatus = "I",
                                                 flagMethod = "e")]
  
  data_element <- data_element[, colnames(data), with = FALSE]
  
  data <- rbind(
    data_element,
    data[!data_element, on = c("geographicAreaM49", "measuredElement")]
  )
  return(data)
}


# Compute moving avarage variable for an element of a triplet
# the varname will be: movav_element example movav_input
compute_movav <- function(data = data_crop, element = "5510", type = "output") {
  Value <- paste0("Value_",element)
  movag <- paste0("movav_",type)
  
  outlier <- paste0("isOutlier_", type)
  
  data[, value_new:= get(Value)]
  
  #compute moving average without outlier
  data[get(outlier) == TRUE, value_new:= NA]
  
  data <-
    data[
      order(geographicAreaM49,measuredItemCPC, timePointYears),
      c(movag) := rollavg(value_new, order = 3),
      by = c("geographicAreaM49","measuredItemCPC")
      ]
  
  data[, value_new:= NULL]
  return(data)
}


# Correcting outliers
# Parametrize input var, output var and productivity var
correctInputOutput <- function(data = data,
                             triplet = livestock_triplet_lst_1,
                             partial = FALSE,
                             factor = 1
                             ) {
        
    
  # data_triplet<-copy(data)
  data[flagObservationStatus=="M",Value:=NA_real_]
    
  data_triplet <- data[measuredElement %in% triplet]
    
  # data_triplet<-imput_with_average(data_triplet,triplet$productivity)
  
  inteminput <- data_triplet[measuredElement %in% triplet$input,get("measuredItemCPC")]
  
  data_triplet <- data_triplet[measuredItemCPC %in% inteminput]
  
  data_triplet <- dcast.data.table(data_triplet, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value', 'Protected', 'EF'))
  
  
  Label_outlier(data = data_triplet, element = triplet$output, type = "output")
  compute_movav(data = data_triplet, element = triplet$output, type = "output")
  
  Label_outlier(data = data_triplet, element = triplet$input, type = "input")
  compute_movav(data = data_triplet, element = triplet$input, type = "input")
  
  Label_outlier(data = data_triplet, element = triplet$productivity, type = "productivity")
  compute_movav(data = data_triplet, element = triplet$productivity, type = "productivity")
  
  
  input <- paste0("Value_", triplet$input)
  output <- paste0("Value_", triplet$output)
  productivity < -paste0("Value_", triplet$productivity)
  
  ef_input <- paste0("EF_", triplet$input)
  ef_output <- paste0("EF_", triplet$output)
  ef_productivity <- paste0("EF_", triplet$productivity)
  
  data_triplet[get(ef_input) == TRUE, isOutlier_input:= TRUE]
  data_triplet[get(ef_output) == TRUE,isOutlier_output:= TRUE]
  data_triplet[get(ef_productivity) == TRUE,isOutlier_productivity:= TRUE]
  
  data_triplet[isOutlier_productivity == TRUE, c(productivity):= movav_productivity]
  
  data_triplet[isOutlier_input == TRUE & isOutlier_output == FALSE, c(input):= ifelse(get(productivity) != 0, get(output)/get(productivity)*factor, NA)]
  data_triplet[isOutlier_input == TRUE & isOutlier_output == FALSE, isOutlier_input:= FALSE]
  
  data_triplet[isOutlier_input == FALSE & isOutlier_output == TRUE,
       c(output):= get(input)*get(productivity)/factor]
  
  data_triplet[isOutlier_input == FALSE & isOutlier_output == TRUE,
       isOutlier_output:= FALSE]
  
  if (partial == FALSE) {
      
      data_triplet[isOutlier_input == TRUE & isOutlier_output == TRUE,
           c(output):= movav_output]
      data_triplet[isOutlier_input == TRUE & isOutlier_output == TRUE,
           c(input):= movav_output/get(productivity)*factor]
      
      data_triplet[isOutlier_input == TRUE & isOutlier_output == TRUE,
           `:=`(isOutlier_input = FALSE, isOutlier_output = FALSE)]
      
      #update the productivity
      data_triplet[, c(productivity):= get(output)/get(input)*factor]
      
      data_triplet[get(output) == 0, c(productivity):= 0]
      data_triplet[is.na(get(output)), c(productivity):= 0]
      
  }
  

  # Putting the data in the initial format
  
  outlierdata <- data_triplet[, list(geographicAreaM49, timePointYears, measuredItemCPC, isOutlier_productivity)]
  
  id.vars = c("geographicAreaM49", "timePointYears", "measuredItemCPC")
  data_triplet <- data_triplet[, c(id.vars,grep("^Value",names(data_triplet), value = TRUE)), with=FALSE]
  
  data_triplet <- melt(data_triplet, id.vars=c("geographicAreaM49","timePointYears",
                                       "measuredItemCPC"), grep("^Value",names(data_triplet), value = TRUE),
                  variable.name = "measuredElement", value.name = "value_new")
  
  data_triplet[, measuredElement:= substr(measuredElement, start = 7, stop = 10)]
  data_triplet[value_new ==Inf, value_new:= NA_real_]
  
  data <- merge(
      data,
      data_triplet,
      by = c("geographicAreaM49", "timePointYears", "measuredElement", "measuredItemCPC"),
      all.x = TRUE
  )
  
  data <- merge(
      data,
      outlierdata,
      by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"),
      all.x = TRUE
  )
  
  data[, difference:= value_new - Value]
  
  data[flagObservationStatus %in% c("M"), Value:= 0]
  
  # data[,check:=ifelse(Protected==FALSE & !is.na(value_new) &
  #                         round(value_new,6)!=round(Value,6) & 
  #                         timePointYears %in% yearVals,TRUE,FALSE)]
  
  data[is.na(Protected), Protected:= FALSE]
  data[is.na(isOutlier_productivity), isOutlier_productivity:= FALSE]
  
  data[(measuredElement %!in% triplet$productivity) & (Protected == FALSE) & (!is.na(value_new)) & (round(value_new,6) != round(Value,6)) &
           (timePointYears %in% yearVals) ,`:=`(Value = value_new,
                                                    flagObservationStatus = "E",
                                                    flagMethod = "e")]
  
  #dealing with nput flag of productivity
  dataflagInput <- data[measuredElement %in% triplet$input, list(geographicAreaM49, timePointYears, measuredItemCPC,
                                                               flagOinput = flagObservationStatus,
                                                               flagMinput = flagMethod)]
  
  dataflagOutput <- data[measuredElement %in% triplet$output, list(geographicAreaM49, timePointYears, measuredItemCPC,
                                                               flagOoutput = flagObservationStatus,
                                                               flagMoutput = flagMethod)]
  
  data <- merge(
      data,
      dataflagInput,
      by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"),
      all.x = TRUE
  )
  
  data <- merge(
      data,
      dataflagOutput,
      by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"),
      all.x = TRUE
  )
  
  data[, weakFlagO:= NA_character_]
  
  data[, weakFlagO:= ifelse((flagOinput %in% "I") | (flagOoutput %in% "I"), "I", weakFlagO)]
  data[, weakFlagO:= ifelse((flagOinput == "") & (flagOoutput == ""), "", weakFlagO)]
  data[, weakFlagO:= ifelse((flagOinput == "E") & (flagOoutput == "E"), "E", weakFlagO)]
  data[, weakFlagO:= ifelse((flagOinput == "T") & (flagOoutput == "T"), "T", weakFlagO)]
  
  # blank and other 
  data[, weakFlagO:= ifelse((flagOinput == "") & (flagOoutput %!in% c("","T")), flagOoutput, weakFlagO)]
  data[, weakFlagO:= ifelse((flagOinput %!in% c("", "T")) & (flagOoutput == ""), flagOinput, weakFlagO)]
  
  data[, weakFlagO:= ifelse((flagOinput == "T") & (flagOoutput %!in% c("","T")), flagOoutput, weakFlagO)]
  data[, weakFlagO:= ifelse((flagOinput %!in% c("","T")) & (flagOoutput=="T"), flagOinput, weakFlagO)]
  
  data[, weakFlagO:= ifelse((flagOinput == "E") & (flagOoutput == "I"), "I", weakFlagO)]
  data[, weakFlagO:= ifelse((flagOinput == "I") & (flagOoutput == "I"), "I", weakFlagO)]
  
  data[measuredElement %in% triplet$productivity & Protected == FALSE & !is.na(value_new) & round(value_new,1) != round(Value,1) &
           timePointYears %in% yearVals, `:=`(Value = value_new,
                                              flagObservationStatus = weakFlagO,
                                              flagMethod = "i"
                                              )]
  
  # data[measuredElement %in% triplet$productivity & Protected==FALSE & !is.na(value_new) & round(value_new,6)!=round(Value,6) &
  #          timePointYears %in% yearVals ,Value:=value_new]
  data[, value_new:= NULL]
  data[, difference:= NULL]
  data[, isOutlier_productivity:= NULL]
  data[, flagOinput:= NULL]
  data[, flagMinput:= NULL]
  data[, flagOoutput:= NULL]
  data[, flagMoutput:= NULL]
  data[, weakFlagO:= NULL] 
  
  return(data)
  
}

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

completeImputationKey = getCompleteImputationKey("production")


# triplet of livestock with stocks in head
livestock_triplet_lst_1 <- list(input="5111", output="5315", productivity="9999")
livestock_triplet_lst_2 <- list(input="5320", output="5510", productivity="5417") 
 
#Livestock triplet with stock unit in 1000 head
livestock_triplet_lst_1bis <- list(input="5112", output="5316", productivity="9999") 
livestock_triplet_lst_2bis <- list(input="5321", output="5510", productivity="5424") 
 
#triplets for crops
crop_triplet_lst<-list(input="5312", output="5510", productivity="5421")
 
#to remove
# triplets<-c("5312","5510","5421","5111","5315",
#            "5320","5417", "5112","5316",
#            "5321","5510","5424")
 
 
animalData =
    sessionKey %>%
    GetData(key = .) #%>%
    #preProcessing(data = .)

#clean the subset of country it was just for testing
data = animalData #[geographicAreaM49 %in% c("840","426","440")]

# CROP,LIVESTOCK or MILK
imputation_selection <- swsContext.computationParams$imputation_selection
endYear = as.numeric(swsContext.computationParams$last_year)
geoM49 = swsContext.computationParams$geom49

# Set-up the parameters
# startYear <- 2014
# endYear <- 2018
THRESHOLD_IMPUTED <- as.numeric(swsContext.computationParams$outliers_threshold)
window <- 5
startYear <- endYear - window + 1
yearVals <- startYear:endYear

interval <- (startYear-window):(startYear-1)

# Read the data needed
flagValidTable <- ReadDatatable("valid_flags")
mapping <- ReadDatatable("animal_parent_child_mapping")

# The FBS tree is used to map only meat items but not offals and fats
message("Download fbsTree from SWS...")
fbsTree = ReadDatatable("fbs_tree")
fbsTree = data.table(fbsTree)
setnames(fbsTree, colnames(fbsTree) , c( "fbsID1", "fbsID2", "fbsID3","fbsID4", "measuredItemSuaFbs"))
setcolorder(fbsTree, c("fbsID4", "measuredItemSuaFbs", "fbsID1", "fbsID2", "fbsID3"))

# Extract meat items from the fbstree
meatItem <- fbsTree[fbsID3 == "2943", get("measuredItemSuaFbs")]

mapping <- mapping[measured_item_child_cpc %in% meatItem]

message("Estimation of off-take rates...")

if(imputation_selection=="LIVESTOCK") {
  #Offtake rates for livestock in heads
  dataEstOff <- data[measuredElement %in% c("5111","5315")]
  dataEstOff <- dcast.data.table(dataEstOff, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value'))
  dataEstOff[, Value:= `5315`/`5111`]
  dataEstOff[, `5111`:= NULL]
  dataEstOff[, `5315`:= NULL]
  dataEstOff[, measuredElement:= 9999]
  dataEstOff[, flagObservationStatus:="I"]
  dataEstOff[, flagMethod:="i"]
  dataEstOff <- dataEstOff[, names(data), with=FALSE]
  dataEstOff <- unique(dataEstOff, by=c(colnames(data)))
  data <- rbind(data,dataEstOff)

}

if(imputation_selection=="LIVESTOCK1000") {
  #Offtake rates for livestock in 1000 heads
  dataEstOff2 <- data[measuredElement %in% c("5112","5316")]
  dataEstOff2 <- dcast.data.table(dataEstOff2, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value'))
  dataEstOff2[, Value:= `5316`/`5112`]
  dataEstOff2[, `5112`:= NULL]
  dataEstOff2[, `5316`:= NULL]
  dataEstOff2[, measuredElement:= 9999]
  dataEstOff2[,flagObservationStatus:="I"]
  dataEstOff2[,flagMethod:="i"]
  dataEstOff2 <- dataEstOff2[, names(data), with=FALSE]
  dataEstOff2 <- unique(dataEstOff2, by=c(colnames(data)))
  data <- rbind(data, dataEstOff2)

}

data <- merge(
    data,
    flagValidTable,
    by=c("flagObservationStatus","flagMethod")
)

data[is.na(Protected), Protected:=FALSE]
data[flagObservationStatus == "E" & flagMethod == "e", Protected:= FALSE]
data[flagObservationStatus == "T" & flagMethod == "q", Protected:= FALSE]

data[, EF:= ifelse(flagObservationStatus == "E" & flagMethod == "e", TRUE, FALSE)]

data_ef <- data[,list(geographicAreaM49, timePointYears,
                    measuredElement, measuredItemCPC, EF)]

#this may not be important when reading sessions after imputation
#because all should be complete

if ( sum(unlist(crop_triplet_lst) %in% data$measuredElement) == 3) {
    data <- complete_triplet(data,crop_triplet_lst)
}

if ( sum(unlist(livestock_triplet_lst_1) %in% data$measuredElement) == 3) {
    data <- complete_triplet(data,livestock_triplet_lst_1)
}

if ( sum(unlist(livestock_triplet_lst_2) %in% data$measuredElement) == 3) {
    data <- complete_triplet(data,livestock_triplet_lst_2)
}

if ( sum(unlist(livestock_triplet_lst_1bis)%in% data$measuredElement) == 3) {
    data <- complete_triplet(data,livestock_triplet_lst_1bis)
}

if ( sum(unlist(livestock_triplet_lst_2bis)%in% data$measuredElement) == 3) {
    data <- complete_triplet(data, livestock_triplet_lst_2bis)
}

# data<-correctInputOutput(data,triplet = crop_triplet_lst,partial = FALSE)

message("Correction...")

if (imputation_selection == "CROP") {
  
  data <- correctInputOutput(data, triplet = crop_triplet_lst, partial = FALSE)

} else if(imputation_selection == "LIVESTOCK") {

  # livestocks type 1: stocks in heads
  data <- correctInputOutput(data, triplet = livestock_triplet_lst_1, partial = TRUE)

  data <- update_slaughter(data = data, mappingData = mapping, sendTo = "child", from = "parent")

  data <- correctInputOutput(data, triplet = livestock_triplet_lst_2, partial = FALSE, factor = 1000)

  data <- update_slaughter(data = data, mappingData = mapping, sendTo = "parent", from = "child")

  data <- correctInputOutput(data, triplet = livestock_triplet_lst_1, partial = FALSE)

} else {

  # livestock type 2: stock in 1000 heads
  data <- correctInputOutput(data, triplet = livestock_triplet_lst_1bis, partial = TRUE)

  data <- update_slaughter(data = data, mappingData = mapping, sendTo = "child", from = "parent")

  data <- correctInputOutput(data, triplet = livestock_triplet_lst_2bis, partial = FALSE, factor = 1000)

  data <- update_slaughter(data = data, mappingData = mapping, sendTo = "parent", from = "child")

  data <- correctInputOutput(data, triplet = livestock_triplet_lst_1bis, partial = FALSE)

}


dataf <- data[measuredElement != "9999"]
dataf <- dataf[!is.na(Value)]

dataf <- dataf[timePointYears %in% yearVals, c(names(animalData)), with=FALSE]

SaveData(domain = datasetConfig$domain,
         dataset = datasetConfig$dataset,
         data = dataf, waitTimeout = 2000000)


#productivity outliers after update

productivityVector <- c("5421",
                      "9999",
                      "5424",
                      "5417")


data_element <- data[measuredElement %in% productivityVector]

data_element[,
             Meanold:= mean(Value[timePointYears %in% interval], na.rm = TRUE),
             by = c('geographicAreaM49', 'measuredItemCPC', 'measuredElement')
             ]

data_element[is.na(Value), Value:= 0]

data_element[, ratio:= Value / Meanold]

data_element[, is_outlier:= ifelse(abs(ratio-1) > THRESHOLD_IMPUTED & Protected == FALSE,TRUE,FALSE)]

data_outlier <- data_element[is_outlier == TRUE & timePointYears %in% yearVals]

data_outlier <- nameData(datasetConfig$domain, datasetConfig$dataset, data_outlier)

write.csv(data_outlier, "Productivity_Outliers_Livestock.csv")