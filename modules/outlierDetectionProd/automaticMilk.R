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
    SETTINGS = ReadSettings(file.path(getwd(),"modules","outlierDetectionProd","sws.yml"))
    
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


# Get the MILK data (NB: preProcessing: manage NA M and transform timePointYears)

milk_triplet_lst_1 <- list(input="5111", output="5318", productivity="9999")
milk_triplet_lst_2 <- list(input="5318", output="5510", productivity="5417")



#to remove
# animalKey = completeImputationKey
# animalKey@dimensions$measuredItemCPC@keys = currentAnimalItem
# animalKey@dimensions$measuredElement@keys =triplets
#     with(animalFormulaParameters,
#          c(productionCode))

# sessionKey@dimensions$geographicAreaM49<-completeImputationKey@dimensions$geographicAreaM49

# Reading outputs of estimation session data
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

THRESHOLD_IMPUTED <-as.numeric(swsContext.computationParams$outliers_threshold)
window <- 5
startYear<-endYear-window+1
yearVals<-startYear:endYear

interval <- (startYear-window):(startYear-1)

# Read the flag data
flagValidTable <- ReadDatatable("valid_flags")

# This chunk of the help us to create an artifical triplet which is actually not exist. We create a triplet with Live Animal, Milk Animal and Yield (9999)
mapping <- ReadDatatable('animal_milk_correspondence')
setnames(mapping,c("animal_item_cpc", "milk_item_cpc"), c("measuredItemAnimalCPC", "measuredItemCPC"))

data2 <- merge(data[measuredElement == '5318'], mapping, by='measuredItemCPC')
data2[, measuredItemCPC:= NULL]
setnames(data2,"measuredItemAnimalCPC", "measuredItemCPC")

data2<-data2[,colnames(data),with=FALSE]

data<-rbind(data,data2[!data,on=c("geographicAreaM49","timePointYears",
                                  "measuredElement","measuredItemCPC")])




# Estimating the ratio between Live animals and Milk animals (these animals are only in head). We give flag I,i for this new variable.
dataEstYield <-data[measuredElement %in% c("5111","5318")]

dataEstYield <- dcast.data.table(dataEstYield, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value'))
dataEstYield[,Value:=`5318`/`5111`]
dataEstYield[,`5111`:=NULL]
dataEstYield[,`5318`:=NULL]
dataEstYield[,measuredElement:=9999]

dataEstYield[,flagObservationStatus:="I"]
dataEstYield[,flagMethod:="i"]

dataEstYield <- dataEstYield[,names(data),with=FALSE]
dataEstYield <- unique(dataEstYield,by=c(colnames(data)))

data <- rbind(data,dataEstYield)



data <- merge(
    data,
    flagValidTable,
    by=c("flagObservationStatus","flagMethod")
)

data[is.na(Protected),Protected:=FALSE]

# We do not have a flag E,e in our flag data, but this flag will come to our session from the previous imputation module. We need to check and correct 
# the outliers coming form the imputation as well. For this reason we assign Protected = FALSE, for this new flag.
data[flagObservationStatus=="E" & flagMethod=="e",Protected:=FALSE]
data[,EF:=ifelse(flagObservationStatus=="E" & flagMethod=="e",TRUE,FALSE)]

data_ef <- data[,list(geographicAreaM49,timePointYears,
                    measuredElement,measuredItemCPC,EF)]


#data[flagObservationStatus=='M', Value:= NA_real_]

# This function complete a triplet in case one or two element are missng
complete_triplet <- function(data,triplets){
    
    itemProd <- unique(data[measuredElement %in% triplets$productivity ,get("measuredItemCPC")])
    
    trippletcomplete <-
        CJ(
            geographicAreaM49 = unique(data$geographicAreaM49),
            measuredElement = unlist(triplets,use.names = FALSE),
            timePointYears = unique(data$timePointYears),
            measuredItemCPC = itemProd
        )
    
    trippletcomplete <- merge(
        data,
        trippletcomplete,
        by=c("geographicAreaM49","timePointYears","measuredItemCPC","measuredElement"),
        all.y = TRUE
    )
    
    trippletcomplete <- trippletcomplete[,names(data),with=FALSE]
    
    data<-rbind(
        data,
        trippletcomplete[!data, on=c('geographicAreaM49', 'timePointYears',
                                     'measuredItemCPC', 'measuredElement')]
    )
    return(data)
    
}

# This may not be important when reading sessions after imputation
data<-complete_triplet(data,milk_triplet_lst_1)
data<-complete_triplet(data,milk_triplet_lst_2)

data[flagObservationStatus == 'M', Protected := TRUE]

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




# This function take a wide data of  a triplet and creat boolean outlier variable for the element of the triplet
Label_outlier<-function(data=data, element, type){
    Value <- paste0("Value_", element)
    Protected <- paste0("Protected_", element)
    outlier <- paste0("isOutlier_", type)
    
    data[,timeCondition:=ifelse(timePointYears %in% interval,TRUE,FALSE)]
    
    data[,proCondition:=ifelse(timePointYears %in% yearVals & get(Protected)==TRUE,TRUE,FALSE)]
    
    data[,meanCondition:=ifelse(timeCondition==TRUE | proCondition==TRUE,TRUE,FALSE)]
    
    
    data[,
         Meanold := mean(get(Value)[meanCondition==TRUE], na.rm = TRUE),
         by = c('geographicAreaM49', 'measuredItemCPC')
         ]
    
    #data[is.na(get(Value)), c(Value) := 0]
    
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


# This function computes moving avarage variable for an element of a triplet
# the varname will be: movav_element example movav_input
compute_movav <- function(data=data_crop,element="5510",type="output") {
    Value <- paste0("Value_",element)
    movag <- paste0("movav_",type)
    
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


# This function corrects the outliers
# Parametrize input var, output var and productivity var

correctInputOutput <- function(data=data,
                             triplet=milk_triplet_lst_1,
                             factor=1
) {
    
    data[flagObservationStatus=="M",Value:=NA_real_]
    
    data_triplet <- data[measuredElement %in% triplet]
    
    inteminput<-data_triplet[measuredElement %in% triplet$input,get("measuredItemCPC")]
    
    data_triplet<-data_triplet[measuredItemCPC %in% inteminput]
    
    
    data_triplet<- dcast.data.table(data_triplet, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value', 'Protected', 'EF'))
    
    
    Label_outlier(data=data_triplet,element=triplet$output,type="output")
    compute_movav(data=data_triplet,element=triplet$output,type="output")
    
    Label_outlier(data=data_triplet,element=triplet$input,type="input")
    compute_movav(data=data_triplet,element=triplet$input,type="input")
    
    Label_outlier(data=data_triplet,element=triplet$productivity,type="productivity")
    compute_movav(data=data_triplet,element=triplet$productivity,type="productivity")
    
    
    input <- paste0("Value_",triplet$input)
    output <- paste0("Value_",triplet$output)
    productivity <- paste0("Value_",triplet$productivity)
    
    ef_input <- paste0("EF_",triplet$input)
    ef_output <- paste0("EF_",triplet$output)
    ef_productivity <- paste0("EF_",triplet$productivity)
    
    data_triplet[get(ef_input)==TRUE,isOutlier_input:=TRUE]
    data_triplet[get(ef_output)==TRUE,isOutlier_output:=TRUE]
    data_triplet[get(ef_productivity)==TRUE,isOutlier_productivity:=TRUE]
    
    # Number of Milk Animal cannot be higher than Live Animal: This module do not correct the Milk Animal number with the officil milk production
    # though, It is possible that will create an outlier for the Milk production Yield. The results of productivity outliers
    # will send to the user to control the official outlier on Milk Production so that they can change the number of Milk Animal, not higher than Live Animal.
    data_triplet[isOutlier_productivity==TRUE,c(productivity):= ifelse(movav_productivity<=1, movav_productivity, 1)]
    
    # if (factor==1000){
    #     data_triplet[isOutlier_input==TRUE & isOutlier_output==FALSE,c(input):=ifelse(get(productivity)!=0,get(output)/get(productivity)*factor,NA)]
    #     data_triplet[isOutlier_input==TRUE & isOutlier_output==FALSE,isOutlier_input:=FALSE]
    # }
    
    data_triplet[isOutlier_output==TRUE,
                 c(output):=get(input)*get(productivity)/factor]
    
    data_triplet[isOutlier_output==TRUE,
                 isOutlier_output:=FALSE]
    
    #if (partial==FALSE) {
        
        # data_triplet[isOutlier_input==TRUE & isOutlier_output==TRUE,
        #              c(output):=movav_output]
        # data_triplet[isOutlier_input==TRUE & isOutlier_output==TRUE,
        #              c(input):=movav_output/get(productivity)*factor]
        # 
        # data_triplet[isOutlier_input==TRUE & isOutlier_output==TRUE,
        #              `:=`(isOutlier_input=FALSE,isOutlier_output=FALSE)]
        
        # Update the productivity
        data_triplet[,c(productivity):=get(output)/get(input)*factor]
        
        data_triplet[get(output)==0,c(productivity):=0]
        data_triplet[is.na(get(output)),c(productivity):=0]
        
    #}  
    
    
    
    # Putting the data in the initial format
    
    outlierdata <- data_triplet[,list(geographicAreaM49,timePointYears,measuredItemCPC,isOutlier_productivity)]
    
    id.vars = c("geographicAreaM49","timePointYears","measuredItemCPC")
    data_triplet <- data_triplet[,c(id.vars,grep("^Value",names(data_triplet),value = TRUE)),with=FALSE]
    
    data_triplet <- melt(data_triplet, id.vars=c("geographicAreaM49","timePointYears",
                                               "measuredItemCPC"), grep("^Value",names(data_triplet),value=TRUE),
                       variable.name="measuredElement", value.name="value_new")
    
    data_triplet[,measuredElement:=substr(measuredElement,start = 7,stop = 10)]
    data_triplet[value_new==Inf,value_new:=NA_real_]
    
    data <- merge(
        data,
        data_triplet,
        by=c("geographicAreaM49","timePointYears","measuredElement", "measuredItemCPC"),
        all.x = TRUE
    )
    
    data <- merge(
        data,
        outlierdata,
        by=c("geographicAreaM49","timePointYears", "measuredItemCPC"),
        all.x = TRUE
    )
    
    
    data[,difference:=value_new-Value]
    data[flagObservationStatus %in% c('M'), Value:=0]
    # data[,check:=ifelse(Protected==FALSE & !is.na(value_new) &
    #                         round(value_new,6)!=round(Value,6) & 
    #                         timePointYears %in% yearVals,TRUE,FALSE)]
    
    data[is.na(Protected),Protected:=FALSE]
    data[is.na(isOutlier_productivity),isOutlier_productivity:=FALSE]
    # 
    # data[(measuredElement %!in% triplet$productivity) & (Protected==FALSE) & (!is.na(value_new)) & (round(value_new,6)!=round(Value,6)) &
    #          (timePointYears %in% yearVals) ,`:=`(Value=value_new,
    #                                               flagObservationStatus="E",
    #                                               flagMethod="e")]
    # 
    # data[measuredElement %in% triplet$productivity & Protected==FALSE & !is.na(value_new) & round(value_new,6)!=round(Value,6) &
    #          timePointYears %in% yearVals ,`:=`(Value=value_new,
    #                                             flagObservationStatus=ifelse(isOutlier_productivity==TRUE,"E",flagObservationStatus
    #                                             ),
    #                                             flagMethod=ifelse(isOutlier_productivity==TRUE,"i",flagMethod))]
    # 
    # # data[measuredElement %in% triplet$productivity & Protected==FALSE & !is.na(value_new) & round(value_new,6)!=round(Value,6) &
    # #          timePointYears %in% yearVals ,Value:=value_new]
    # data[,value_new:=NULL]
    # data[,difference:=NULL]
    # data[,isOutlier_productivity:=NULL]
    # 
    # 
    
    data[(measuredElement %!in% triplet$productivity) & (Protected==FALSE) & (!is.na(value_new)) & (round(value_new,6)!=round(Value,6)) &
             (timePointYears %in% yearVals) ,`:=`(Value=value_new,
                                                  flagObservationStatus="E",
                                                  flagMethod="e")]
    
    #dealing with nput flag of productivity
    dataflagInput<-data[measuredElement %in% triplet$input, list(geographicAreaM49,timePointYears,measuredItemCPC,
                                                                 flagOinput=flagObservationStatus,
                                                                 flagMinput=flagMethod)]
    
    dataflagOutput<-data[measuredElement %in% triplet$output, list(geographicAreaM49,timePointYears,measuredItemCPC,
                                                                   flagOoutput=flagObservationStatus,
                                                                   flagMoutput=flagMethod)]
    
    data<-merge(
        data,
        dataflagInput,
        by=c("geographicAreaM49","timePointYears", "measuredItemCPC"),
        all.x = TRUE
    )
    
    data<-merge(
        data,
        dataflagOutput,
        by=c("geographicAreaM49","timePointYears", "measuredItemCPC"),
        all.x = TRUE
    )
    
    data[,weakFlagO:=NA_character_]
    
    data[,weakFlagO:=ifelse((flagOinput %in% "I") | (flagOoutput %in% "I"),"I",weakFlagO)]
    data[,weakFlagO:=ifelse((flagOinput=="") & (flagOoutput==""),"",weakFlagO)]
    data[,weakFlagO:=ifelse((flagOinput=="E") & (flagOoutput=="E"),"E",weakFlagO)]
    data[,weakFlagO:=ifelse((flagOinput=="T") & (flagOoutput=="T"),"T",weakFlagO)]
    
    # blank and other 
    data[,weakFlagO:=ifelse((flagOinput=="") & (flagOoutput %!in% c("","T")),flagOoutput,weakFlagO)]
    data[,weakFlagO:=ifelse((flagOinput%!in% c("","T")) & (flagOoutput==""),flagOinput,weakFlagO)]
    
    data[,weakFlagO:=ifelse((flagOinput=="T") & (flagOoutput %!in% c("","T")),flagOoutput,weakFlagO)]
    data[,weakFlagO:=ifelse((flagOinput%!in% c("","T")) & (flagOoutput=="T"),flagOinput,weakFlagO)]
    
    data[,weakFlagO:=ifelse((flagOinput=="E") & (flagOoutput=="I"),"I",weakFlagO)]
    data[,weakFlagO:=ifelse((flagOinput=="I") & (flagOoutput=="I"),"I",weakFlagO)]
    
    data[measuredElement %in% triplet$productivity & Protected==FALSE & !is.na(value_new) & round(value_new,1)!=round(Value,1) &
             timePointYears %in% yearVals ,`:=`(Value=value_new,
                                                flagObservationStatus=weakFlagO,
                                                flagMethod="i"
             )]
    
    # data[measuredElement %in% triplet$productivity & Protected==FALSE & !is.na(value_new) & round(value_new,6)!=round(Value,6) &
    #          timePointYears %in% yearVals ,Value:=value_new]
    data[, c('value_new', 'difference', 'isOutlier_productivity', 'flagOinput', 'flagMinput', 'flagOoutput', 'flagMoutput', 'weakFlagO'):= NULL]
    
    return(data)
    
}



data<-correctInputOutput(data,triplet = milk_triplet_lst_1)
 
data<-correctInputOutput(data,triplet = milk_triplet_lst_2,factor = 1000)


### Final

dataf1 <- data[(measuredElement %in% c("5318")) & (measuredItemCPC %in% mapping[,get("measuredItemAnimalCPC")])]
dataf <- data[!dataf1, on = c(names(data))]

dataf <- dataf[measuredElement %!in% c("9999","5111")]

dataf <- dataf[!is.na(Value)]

dataf <- dataf[timePointYears %in% yearVals,c(names(animalData)), with=FALSE]

SaveData(domain = datasetConfig$domain,
         dataset = datasetConfig$dataset,
         data = dataf, waitTimeout = 2000000)


# Productivity outliers after update

#productivityVector <- c("5417", "9999", "5318", "5111", "5510")
productivityVector <- c("5417", "9999")

data_element<-data[measuredElement %in% productivityVector]

data_element[,
             Meanold := mean(Value[timePointYears %in% interval], na.rm = TRUE),
             by = c('geographicAreaM49', 'measuredItemCPC', 'measuredElement')
             ]

#data_element[is.na(Value), Value := 0]

data_element[, ratio := Value / Meanold]

data_element[,is_outlier:=ifelse(abs(ratio-1) > THRESHOLD_IMPUTED & Protected==FALSE,TRUE,FALSE)]

data_outlier <- data_element[is_outlier==TRUE & timePointYears %in% yearVals]

data_outlier <- nameData(datasetConfig$domain,datasetConfig$dataset,data_outlier)

write.csv(data_outlier,"OutliersMilk.csv")
