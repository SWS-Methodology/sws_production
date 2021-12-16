##' 
##'
##' **Author: Livia Lombardi**
##' 
##' 
##' **Description:**
##'
##' This module is designed to identify and automatically correct the outliers in the production environement.
##' It can detect any triplets (so called productivity, input and output) in Crop, Livestock, Milk or Egg.
##'
##' It was designed to correct the anomalous behaviour of the imputation plugin 
##'
##' IMPORTANT: the session should always have 5 years before the startYear !
##' Documentation at : 

message("plug-in starts to run")

# Import libraries
suppressMessages({
    library(data.table)
    library(faosws)
    library(RcppRoll)
    library(faoswsFlag)
    library(faoswsUtil)
    library(faoswsImputation)
    library(faoswsProduction)
    library(faoswsProcessing)
    library(faoswsEnsure)
    library(magrittr)
    library(dplyr)
    library(sendmailR)
    library(faoswsStandardization)
    library(openxlsx)
})


R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if (CheckDebug()) {
    
    library(faoswsModules)
    SETTINGS <- ReadSettings("modules/Crop_Livestock_Milk_Outlier_Correction/sws.yml")
    
    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH <- SETTINGS[["share"]]
    
    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])
    
    ## Get session information from SWS. Token must be obtained from web interface
    
    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])
}
# Get data configuration and session
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

completeImputationKey = getCompleteImputationKey("production")

# CROP,LIVESTOCK or MILK
##'Imputation selection rimane per definire la categoria di item
imputation_selection <- swsContext.computationParams$imputation_selection
##'Da aggiungere lo start Year
#startYear = as.numeric(swsContext.computationParams$start_year)
#startYear = 2014
startYear = as.numeric(swsContext.computationParams$start_year)
endYear = as.numeric(swsContext.computationParams$last_year)
#geoM49 = swsContext.computationParams$geom49

range_carcass <- ReadDatatable("range_carcass_weight")

eggs_factors <- ReadDatatable("eggs_technical_conversion_factors")
eggs_factors[, notes := NULL]
# Set-up the parameters

THRESHOLD_IMPUTED <- as.numeric(swsContext.computationParams$outliers_threshold)

if (swsContext.computationParams$imputation_timeWindow=="lastThree") {
    window <- 3 
} else {
    window <- 5 
}


if (swsContext.computationParams$animal_type=="both") {
    animal_type <- "both"
} else if (swsContext.computationParams$animal_type=="none") {
    animal_type <- "none"
} else if (swsContext.computationParams$animal_type=="big_animals"){
    animal_type <- "big_animals"
} else {
    animal_type <- "small_animals"
}

yearVals <- startYear:endYear

interval <- (startYear-window):(startYear-1)

originalData =
    sessionKey %>%
    GetData(key = .) #%>%

originalData <- originalData[timePointYears %in% as.character(c(yearVals,interval, (interval-3))),]

data = copy(originalData) 

mailto = swsContext.userEmail

# Livestock triplets with stocks in head
livestock_triplet_lst_1 <- list(input="5111", output="5315", productivity="9999")
livestock_triplet_lst_2 <- list(input="5320", output="5510", productivity="5417") 

# Livestock triplets with stock unit in 1000 head
livestock_triplet_lst_1bis <- list(input="5112", output="5316", productivity="9999") 
livestock_triplet_lst_2bis <- list(input="5321", output="5510", productivity="5424") 

# Crop triplet
crop_triplet_lst<-list(input="5312", output="5510", productivity="5421")

# Milk triplet
milk_triplet_lst_1 <- list(input="5111", output="5318", productivity="9999")
milk_triplet_lst_2 <- list(input="5318", output="5510", productivity="5417")


# Egg triplet

egg_triplet_lst <-list(input="5313", output1="5510",output2="5513", productivity="5424")


# TMP file for anomalies
USER <- regmatches(
    swsContext.username,
    regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
)

TMP_DIR <- file.path(tempdir(), USER)
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)

tmp_file_incomp <- file.path(TMP_DIR, paste0("Livestock_outlierincompatibilities.xlsx"))

#dt to append incompatibilites
incomp_dt <- data.table(geographicAreaM49=character(), timePointYears=character(), measuredItemCPC=character())
incomp_dt2 <- data.table(geographicAreaM49=character(), timePointYears=character(), measuredItemCPC=character())
protection_dt <- data.table(geographicAreaM49=character(), timePointYears=character(), measuredItemCPC=character())
protection_dt2 <- data.table(geographicAreaM49=character(), timePointYears=character(), measuredItemCPC=character())
case2_dt <- data.table(geographicAreaM49=character(), timePointYears=character(), measuredItemCPC=character())
case2_dt2 <- data.table(geographicAreaM49=character(), timePointYears=character(), measuredItemCPC=character())
# Quality check

if (imputation_selection == "CROP") {
    
    stopifnot(sum(unlist(crop_triplet_lst) %in% unique(data$measuredElement))==3)
    
    # if (sum(unlist(crop_triplet_lst) %in% unique(data$measuredElement))!=3){
    #   message = paste("Please run the plug-in with all Crop Items:", crop_triplet_lst, sep='\n')
    #   sendmailR::sendmail(from = "no-reply@fao.org", to = mailto, subject = "missing triplet", body = message, remove = TRUE)
    # }
    
} else if (imputation_selection == "LIVESTOCK") {
    
    if(animal_type == "both"){
        
        stopifnot(sum(c(livestock_triplet_lst_1$input,livestock_triplet_lst_1$output) %in% unique(data$measuredElement))==2)
        
        stopifnot(sum(unlist(livestock_triplet_lst_2) %in% unique(data$measuredElement))==3)
        
        stopifnot(sum(c(livestock_triplet_lst_1bis$input,livestock_triplet_lst_1bis$output) %in% unique(data$measuredElement))==2)
        
        stopifnot(sum(unlist(livestock_triplet_lst_2bis) %in% unique(data$measuredElement))==3)
        
    } else if (animal_type == "big_animals"){
        
        stopifnot(sum(c(livestock_triplet_lst_1$input,livestock_triplet_lst_1$output) %in% unique(data$measuredElement))==2)
        
        stopifnot(sum(unlist(livestock_triplet_lst_2) %in% unique(data$measuredElement))==3)  
        
    } else if (animal_type == "small_animals"){
        
        stopifnot(sum(c(livestock_triplet_lst_1bis$input,livestock_triplet_lst_1bis$output) %in% unique(data$measuredElement))==2)
        
        stopifnot(sum(unlist(livestock_triplet_lst_2bis) %in% unique(data$measuredElement))==3) 
        
    }
    
    
} else if (imputation_selection == "EGG"){
    
    
    stopifnot(sum(unlist(egg_triplet_lst)  %in% unique(data$measuredElement))==4)

    
} else {
    
    stopifnot(sum(c(milk_triplet_lst_1$input,milk_triplet_lst_1$output) %in% unique(data$measuredElement))==2)
    
    stopifnot(sum(unlist(milk_triplet_lst_2) %in% unique(data$measuredElement))==3)
    
    # if ((sum(c(milk_triplet_lst_1$input,milk_triplet_lst_1$output) %in% unique(data$measuredElement))!=2) |
    #     (sum(unlist(milk_triplet_lst_2) %in% unique(data$measuredElement))!=3)) {
    #   message = paste("Please run the plug-in with all Milk Items:", milk_triplet_lst_1, milk_triplet_lst_2, sep='\n')
    #   sendmailR::sendmail(from = "no-reply@fao.org", to = mailto, subject = "missing triplet", body = message, remove = TRUE)
    # }
}

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

update_slaughter <- function(data, mappingData, sendTo, from){
    
    stopifnot(sendTo %in% c("parent","child"))
    stopifnot(from %in% c("parent","child"))  
    
    datacopied <- copy(data)
    
    cols_datacopied <- names(datacopied)
    
    #datacopied <- datacopied[, cols_datacopied[cols_datacopied != "Value"], with = FALSE]
    
    # datacopied <- datacopied[, c("geographicAreaM49","timePointYears", "measuredItemCPC", 
    #                              "measuredElement", "flagObservationStatus", "flagMethod",
    #                              "Valid", "Protected", "EF", "Case"), with = FALSE]
    
    mapping1 <- copy(mappingData)
    
    if(sendTo %in% "child"){
        
        data_sl <- merge(datacopied, mapping1, by.x = c("measuredItemCPC","measuredElement"), 
                         by.y = c("measured_item_parent_cpc","measured_element_parent"), all.x = TRUE)
        
        data_sl <- data_sl[flagObservationStatus %in% "E" & flagMethod %in% "e" & timePointYears %in% yearVals & 
                               measuredItemCPC %in% mapping1$measured_item_parent_cpc & measuredElement %in% unique(mapping1$measured_element_parent),]
        
        
        data_sl <- data_sl[, c("measuredItemCPC", "measuredElement", "measured_item_child_cpc_description", "measured_item_parent_cpc_description",
                               "animal_group_code"):= NULL]
        
        setnames(data_sl, c("measured_item_child_cpc","measured_element_child"), c("measuredItemCPC", "measuredElement"))
        
        data_sl[, flagMethod := "c"]
        
        if("Case" %in% cols_datacopied){
            
            data_cs <- merge(datacopied, mapping1, by.x = c("measuredItemCPC"), 
                             by.y = c("measured_item_parent_cpc"), all.x = TRUE)
            
            
            data_cs <- data_cs[!is.na(Case) & measuredItemCPC %in% mapping1$measured_item_parent_cpc,]
            
            data_cs <- data_cs[, c("measuredItemCPC", "measuredElement", "measured_item_child_cpc_description", "measured_item_parent_cpc_description",
                                   "animal_group_code", "measured_element_parent"):= NULL]
            
            setnames(data_cs, c("measured_item_child_cpc","measured_element_child"), c("measuredItemCPC", "measuredElement"))
            
            data_cs <- data_cs[, c("geographicAreaM49","measuredItemCPC", "measuredElement", "timePointYears", "Case"),with=F]
            
            data_cs <- unique(data_cs)
            
        }
        
        
        if("Case" %in% cols_datacopied){
            
            updated <- rbind(datacopied[! data_sl , on = c("measuredElement","geographicAreaM49",
                                                           "timePointYears", "measuredItemCPC")], data_sl)
            
            updated <- merge(updated, data_cs, by = c("geographicAreaM49","measuredItemCPC", "measuredElement", "timePointYears"), all.x= TRUE)
            
            
            updated[measuredItemCPC %in% mapping1$measured_item_child_cpc, Case.x := Case.y]
            
            setnames(updated, c("Case.x"), c("Case"))
            
            updated[, "Case.y" := NULL]
            
            
            
        }else{
            
            updated <- rbind(datacopied[! data1 , on = c("measuredElement","geographicAreaM49",
                                                         "timePointYears", "measuredItemCPC")], data1)
            
            
            
        }
        
    }else{
        
        
        data_sl <- merge(datacopied, mapping1, by.x = c("measuredItemCPC","measuredElement"), 
                         by.y = c("measured_item_child_cpc","measured_element_child"), all.x = TRUE)
        
        data_sl <- data_sl[flagObservationStatus %in% "E" & flagMethod %in% "e" & timePointYears %in% yearVals & 
                               measuredItemCPC %in% mapping1$measured_item_child_cpc & measuredElement %in% unique(mapping1$measured_element_child),]
        
        
        data_sl <- data_sl[, c("measuredItemCPC", "measuredElement", "measured_item_parent_cpc_description", "measured_item_child_cpc_description",
                               "animal_group_code"):= NULL]
        
        setnames(data_sl, c("measured_item_parent_cpc","measured_element_parent"), c("measuredItemCPC", "measuredElement"))
        
        data_sl[, flagMethod := "c"]
        
        updated <- rbind(datacopied[!  data_sl , on = c("measuredElement","geographicAreaM49",
                                                        "timePointYears", "measuredItemCPC")],  data_sl)
        
        
    }
    
    
    
    return(updated)    

    
}

update_milk <- function(data, mappingData){
    
    
    datacopied <- copy(data)
    
    cols_datacopied <- names(datacopied)
    
    mapping1 <- copy(mappingData)
    
    setnames(mapping1, "measuredItemCPC","measuredItemMilkCPC")
    
    dataMerged <- datacopied[measuredItemCPC %in% mapping1$measuredItemAnimalCPC,]
    
    dataMerged <- merge(datacopied, mapping1, by.x = "measuredItemCPC", by.y = "measuredItemAnimalCPC", all.x = TRUE)
    
    dataMerged <- dataMerged[measuredElement %in% "5318", ]
    
    dataMerged <- dataMerged[, c("geographicAreaM49","timePointYears","measuredElement","Case","measuredItemMilkCPC"), with = FALSE]
    
    setnames(dataMerged, "measuredItemMilkCPC", "measuredItemCPC")
    
    complete_data <- merge(datacopied, dataMerged, by = c("geographicAreaM49","timePointYears","measuredItemCPC","measuredElement"),
                           all.x = TRUE)
    
    complete_data[measuredItemCPC %in% mapping1$measuredItemMilkCPC, Case.x := Case.y]
    
    complete_data[,Case.y := NULL]
    
    setnames(complete_data, "Case.x", "Case")
    
    return(complete_data)   
    
}


#Label_outlier(data_label = data_triplet, element = triplet$output, type = "output")
#data_label = data_triplet
Label_outlier <- function(data_label = data, element, type){
    Value <- paste0("Value_", element)
    Protected <- paste0("Protected_", element)
    outlier <- paste0("isOutlier_", type)
    
    ##' New condition to label data as outliers
    ##' - if in current range of years there are at least 2 protected values, these will be the mean to compare other imputations
    ##' - if ionly 1 protected value, the comparison mean will be the sum of past interval values + the protected value
    ##' - if no current protected (all imputations) , the old mean will be the classic old mean of the past interval
    
    data_label[, proCondition:= ifelse(timePointYears %in% yearVals & get(Protected) == TRUE, TRUE, FALSE)]
    
    data_label[, numProtected := sum(proCondition),
               by = c('geographicAreaM49', 'measuredItemCPC')
               ]
    
    data_label[, meanCondition:= FALSE]
    
    
    # data_label[, meanCondition := ifelse(numProtected > 1 & proCondition[timePointYears %in% yearVals] == TRUE, 
    #                                      FALSE, FALSE),
    #            by = c('geographicAreaM49', 'measuredItemCPC')]
    
    
    data_label[numProtected > 1 & timePointYears %in% yearVals & get(Protected) == TRUE, meanCondition:= TRUE,
               by = c('geographicAreaM49', 'measuredItemCPC')]
    #data_protected <- data_label[numProtected >1, ]
    ######################################
    #### SECOND TYPE OF CONDITION ########
    ######################################
    #if only one protected in yearVal and it is in the last year, leave it like it is becasue imputation works fine I dont do anything! Is fine
    # data_label[, meanCondition := ifelse(numProtected == 1 & proCondition[timePointYears %in% as.character(endYear)] == TRUE, 
    #                                      FALSE, FALSE),
    #            by = c('geographicAreaM49', 'measuredItemCPC')]
    #if only one protected in yearVal and it is in the penultimate year, take the three year before to compare the mean
    data_label[, meanCondition := ifelse(numProtected == 1 & proCondition[timePointYears %in% as.character(endYear-1)] == TRUE 
                                         & timePointYears %in% as.character((endYear-4):(endYear-1)),TRUE, meanCondition),
               by = c('geographicAreaM49', 'measuredItemCPC')]
    
    
    #data_protected[, meanCondition:= ifelse(timePointYears %in% yearVals & get(Protected) == TRUE, TRUE, FALSE)]
    ############################################################
    #        DA sistemare i CASI == 1
    ############################################################
    #data_semi_protected <- data_label[numProtected ==1, ]&
    #timePointYears %in% as.character((endYear-3):(endYear-1))
    #if the only protected value is in the year before the last year -> the mean is calculated based on the last 3 years before the very last one
    
    
    
    # data_label[numProtected == 1 &  get(Protected) == TRUE & timePointYears %in% as.character(endYear-1), 
    #            meanCondition := TRUE, by = c('geographicAreaM49', 'measuredItemCPC')
    #            ][timePointYears %in% as.character((endYear-3):(endYear-1)), meanCondition := TRUE]
    
    
    #if the only protected value is in the last year. no mean condition true since imputtions work fine
    
    
    #if the only protected is in a year that is not the last two years -> moving average rules
    range <- length(startYear:endYear) - 2
    
    counter = 0
    
    for (i in seq_len(range)) {
        # 
        # data_label[numProtected == 1 &  Protected[get("timePointYears") %in% as.character(endYear -2 - counter)] == TRUE &
        #                timePointYears %in% as.character((endYear -2 - counter -3):(endYear -2 - counter)), 
        #            meanCondition:= TRUE,
        #            by = c('geographicAreaM49', 'measuredItemCPC')]
        # 
        
        #it stop working, no idea
        # data_label[, meanCondition := ifelse(numProtected == 1 & proCondition[timePointYears %in% as.character(endYear -2 - counter)] == TRUE 
        #                                      & timePointYears %in% as.character((endYear -2 - counter -3):(endYear -2 - counter -1)),TRUE, meanCondition),
        #            by = c('geographicAreaM49', 'measuredItemCPC')]
        
        
        
        data_label[numProtected == 1 & proCondition == TRUE & timePointYears %in% as.character(endYear -2 - counter) | 
                       numProtected == 1 & timePointYears %in%  as.character((endYear -2 - counter -3):(endYear -2 - counter -1)), 
                   meanCondition := TRUE]
        
        counter = counter + 1
    }
    # data_label[numProtected == 1 &  get(Protected[timePointYears %!in% as.character((endYear-1):(endYear))]) == TRUE, 
    #            meanCondition:= FALSE,
    #            by = c('geographicAreaM49', 'measuredItemCPC')]
    
    #data_not_protected <- data_label[numProtected == 0,]
    
    data_label[numProtected == 0 & timePointYears %in% interval, meanCondition:= TRUE,
               by = c('geographicAreaM49', 'measuredItemCPC')]
    
    #other_data <- data_label[is.na(numProtected), ]
    
    
    
    #data_label <- rbind(data_protected,data_semi_protected,data_not_protected,other_data)
    
    data_label[,
               Meanold := mean(get(Value)[meanCondition == TRUE], na.rm = TRUE),
               by = c('geographicAreaM49', 'measuredItemCPC')
               ]
    
    data_label[is.na(get(Value)), c(Value):= 0]
    
    data_label[, ratio:= get(Value) / Meanold]
    
    
    
    if(imputation_selection == "LIVESTOCK" | imputation_selection == "CROP"){
        
        data_label[, c(outlier):= FALSE]
        #if element <- è productivity ... if 9999 (then is an off take rate) then theshold is 0.1
        # if any other than 9999 (it means that is a yield) then is 0.2
        # then apply rules for other elements based on magnitude of the numbers
        if(element %!in% unique(c(crop_triplet_lst$productivity, livestock_triplet_lst_1$productivity,
                                  livestock_triplet_lst_1bis$productivity, livestock_triplet_lst_2$productivity,
                                  livestock_triplet_lst_2bis$productivity, milk_triplet_lst_1$productivity,
                                  milk_triplet_lst_2$productivity))){
            #devo assaegnare non devo mettere ifesle!
            
            data_label[get(Value) <100 & abs(ratio-1) > 9 & get(Protected) == FALSE & timePointYears %in% yearVals, 
                       c(outlier):= TRUE]
            
            data_label[get(Value) >= 100 & get(Value) < 1000 & abs(ratio-1) > 5 & get(Protected) == FALSE & timePointYears %in% yearVals, 
                       c(outlier):= TRUE]
            
            data_label[get(Value) >= 1000 & get(Value) < 10000 & abs(ratio-1) > 1 & get(Protected) == FALSE & timePointYears %in% yearVals,
                       c(outlier):= TRUE]
            
            data_label[get(Value) >= 10000 & get(Value) < 100000 & abs(ratio-1) > 0.6 & get(Protected) == FALSE & timePointYears %in% yearVals, 
                       c(outlier):= TRUE]
            
            data_label[get(Value) >= 100000 & get(Value) < 500000 & abs(ratio-1) > 0.5 & get(Protected) == FALSE & timePointYears %in% yearVals, 
                       c(outlier):= TRUE]
            
            data_label[get(Value) >= 500000 & get(Value) < 3000000 & abs(ratio-1) > 0.4 & get(Protected) == FALSE & timePointYears %in% yearVals, 
                       c(outlier):= TRUE]
            
            data_label[get(Value) >= 3000000 & get(Value) < 20000000 & abs(ratio-1) > 0.3 & get(Protected) == FALSE & timePointYears %in% yearVals, 
                       c(outlier):= TRUE]
            
            data_label[get(Value) >= 20000000 & get(Value) < 50000000 & abs(ratio-1) > 0.15 & get(Protected) == FALSE & timePointYears %in% yearVals,
                       c(outlier):= TRUE]
            
            data_label[get(Value) >=50000000 & abs(ratio-1) > 0.1 & get(Protected) == FALSE & timePointYears %in% yearVals,
                       c(outlier):= TRUE]
            
        }else if(element %in% unique(c(crop_triplet_lst$productivity))){
            
            data_label[, c(outlier):= ifelse(abs(ratio-1) > 0.15 & 
                                                 get(Protected) == FALSE, TRUE, FALSE) &
                           timePointYears %in% yearVals]
            
            
        }else if(element %in% unique(c(livestock_triplet_lst_2$productivity,
                                       livestock_triplet_lst_2bis$productivity,milk_triplet_lst_2$productivity))){
            
            data_label[, c(outlier):= ifelse(abs(ratio-1) > 0.2 & 
                                                 get(Protected) == FALSE, TRUE, FALSE) &
                           timePointYears %in% yearVals]
            
            
        }else if(element %in% unique(c(livestock_triplet_lst_1$productivity,livestock_triplet_lst_1bis$productivity,
                                       milk_triplet_lst_1$productivity))){
            
            data_label[, c(outlier):= ifelse(abs(ratio-1) > 0.1 & 
                                                 get(Protected) == FALSE, TRUE, FALSE) &
                           timePointYears %in% yearVals]
        }
        
        
    }else if(imputation_selection == "EGG" | imputation_selection == "MILK"){
        
        data_label[, c(outlier):= ifelse(abs(ratio-1) > THRESHOLD_IMPUTED & 
                                             get(Protected) == FALSE, TRUE, FALSE) &
                       timePointYears %in% yearVals]
        
    }
    #data_label <- data_label[order(geographicAreaM49, measuredItemCPC, -timePointYears)]
    
    #if next year the flag is protected don t check the current value as outlier SUBStituted WITH NEXT RULE
    #data_label[, Nextflag := shift(proCondition), by = c ("geographicAreaM49","measuredItemCPC")]
    #data_label[Nextflag == TRUE, c(outlier) := FALSE]
    
    ##' if the value is imputed in the middle of two protected values remove outlier label!
    ##' ## 
    
    data_label[,centralCondition := copy(get(outlier))]
    
    data_label[,variableProtection := copy(get(Protected))]
    
    
    
    if (imputation_selection == "LIVESTOCK" | imputation_selection == "CROP" | imputation_selection == "MILK"){
        
        for (i in (interval[1] + 1): (tail(yearVals,1))){
            data_label[, centralCondition := ifelse( sum( variableProtection[timePointYears %in% as.character((interval[1] : (i -1)))] ) >= 1 & 
                                                         sum( variableProtection[timePointYears %in% as.character((i + 1) : (tail(yearVals,1)))] ) >= 1 &
                                                         timePointYears %in% as.character(i),
                                                     FALSE, centralCondition),
                       by = c('geographicAreaM49', 'measuredItemCPC')]
            
        }
        
        #data_label[, c(outlier) := centralCondition]
        data_label[,c(outlier)  := copy(centralCondition)]
        #data_label[, c(outlier)] <- data_label[, centralCondition]
        
        #data_label[, c("centralCondition","variableProtection") := NULL]
        
        
        #data_label <- data_label[order(geographicAreaM49, measuredItemCPC, timePointYears)]
    }
    
    data_label[,`:=`(ratio = NULL, centralCondition = NULL, variableProtection = NULL)]
    
    return(data_label)
}
# imput_with_average() function finds the outliers the imputation for given elements (in this case only for Productivity)
# no more used: TO CLEAN


# Compute moving avarage variable for an element of a triplet
# the varname will be: movav_element example movav_input
compute_movav <- function(data = data_crop, element, type ) {
    Value <- paste0("Value_",element)
    movag <- paste0("movav_",type)
    
    outlier <- paste0("isOutlier_", type)
    
    
    #data[, c(movag) := ifelse(get(outlier) == TRUE, Meanold, get(Value))]
    
    data[, c(movag) :=  Meanold]
    
    
    # data[, value_new:= get(Value)]
    # 
    # #compute moving average without outlier
    # data[get(outlier) == TRUE, value_new:= NA]
    # 
    # data <-
    #     data[
    #         order(geographicAreaM49,measuredItemCPC, timePointYears),
    #         c(movag) := rollavg(value_new, order = 3),
    #         by = c("geographicAreaM49","measuredItemCPC")
    #         ]
    
    #data[, value_new:= NULL]
    return(data)
}


# Correcting outliers
# Parametrize input var, output var and productivity var
correctInputOutput <- function(data = data,
                               triplet = livestock_triplet_lst_1,
                               partial = FALSE,
                               factor = 1,
                               last = FALSE,
                               type = "big"
) {
    
    
    # data_triplet<-copy(data)
    # tranform to na data falgged as missing 
    data[flagObservationStatus=="M",Value:=NA_real_]
    #protect the productivity values coming from manual estimated input or output or the production in tonnes for eggs coming from 1000 value
    data[flagObservationStatus %in% "T" & flagMethod %in% "i", Protected := TRUE]
    
    data[flagObservationStatus %in% "E" & flagMethod %in% "i", Protected := TRUE]
    
    if (imputation_selection == "LIVESTOCK") {
        
        #take the coordinets of live animals who has protected production in the meat triplette
        protected_meat <- merge(data, meat_type[, c("cpc_live_animal","cpc_meat"), with = F], by.x = "measuredItemCPC", by.y = "cpc_live_animal",
                                all.x = TRUE)
        #merging with the meat items of the original dataset, to catch the protected productions
        complete_protected_meat <- merge(protected_meat, 
                                         data[measuredElement %in% "5510", c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears","Protected"),with=F], 
                                         by.x = c("cpc_meat","geographicAreaM49","timePointYears"), 
                                         by.y = c("measuredItemCPC","geographicAreaM49","timePointYears"), 
                                         all.x = TRUE)
        
        setnames(complete_protected_meat, c("measuredElement.x", "Protected.x","Protected.y"), c("measuredElement", "Protected", "Protected_meat"))
        
        # complete_protected_meat[, Protected_meat := ifelse( Protected.y[measuredElement %in% "5510"] == TRUE, TRUE, FALSE),
        #                         by = c("geographicAreaM49","measuredItemCPC","timePointYears")]
        
        complete_protected_meat[, measuredElement.y := NULL]
        
        complete_protected_meat <- complete_protected_meat[measuredElement %in% triplet & measuredItemCPC %in% meat_type$cpc_live_animal,]
        
        #filter only data with element in the current triplet
        if(type == "big"){
            data_triplet <- data[measuredElement %in% triplet & measuredItemCPC %in% big_list]
        } else {
            data_triplet <- data[measuredElement %in% triplet & measuredItemCPC %in% small_list]
        }
        
    }else if(imputation_selection == "MILK"){
        
        milk_mapping <- copy(mapping)
        
        setnames(milk_mapping, "measuredItemCPC","measuredItemMilkCPC")
        
        protected_milk <- merge(data, milk_mapping, by.x = "measuredItemCPC", by.y = "measuredItemAnimalCPC",
                                all.x = TRUE)
        
        complete_protected_milk <- merge(protected_milk, 
                                         data[measuredElement %in% "5510", c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears","Protected"),with=F], 
                                         by.x = c("measuredItemMilkCPC","geographicAreaM49","timePointYears"), 
                                         by.y = c("measuredItemCPC","geographicAreaM49","timePointYears"), 
                                         all.x = TRUE)
        
        setnames(complete_protected_milk, c("measuredElement.x", "Protected.x","Protected.y"), c("measuredElement", "Protected", "Protected_milk"))
        
        
        complete_protected_milk[, measuredElement.y := NULL]
        
        complete_protected_milk <- complete_protected_milk[measuredElement %in% triplet & measuredItemCPC %in%milk_mapping$measuredItemAnimalCPC,]
        
        
        data_triplet <- data[measuredElement %in% triplet]
        
    }else{
        
        data_triplet <- data[measuredElement %in% triplet]
    }
    # data_triplet<-imput_with_average(data_triplet,triplet$productivity)
    #take the cpc list of items that have input 
    inteminput <- data_triplet[measuredElement %in% triplet$input,get("measuredItemCPC")]
    
    data_triplet <- data_triplet[measuredItemCPC %in% unique(inteminput)]
    
    
    #First condition: if meat round accept the new column Case created in the previous animal detection round
    cols_initial_data_triplet <- names(data_triplet)
    
    if ("Case" %in% cols_initial_data_triplet){
        data_triplet <- dcast.data.table(data_triplet, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value', 'Protected', 'EF', 'Case'))
    } else {
        data_triplet <- dcast.data.table(data_triplet, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value', 'Protected', 'EF'))
    }
    
    
    if(imputation_selection %in% c("CROP", "LIVESTOCK", "MILK")){
        
        Label_outlier(data_label = data_triplet, element = triplet$output, type = "output")
        compute_movav(data = data_triplet, element = triplet$output, type = "output")
        
        Label_outlier(data_label = data_triplet, element = triplet$input, type = "input")
        compute_movav(data = data_triplet, element = triplet$input, type = "input")
        
        Label_outlier(data_label = data_triplet, element = triplet$productivity, type = "productivity")
        compute_movav(data = data_triplet, element = triplet$productivity, type = "productivity")
        
        
        input <- paste0("Value_", triplet$input)
        output <- paste0("Value_", triplet$output)
        productivity <- paste0("Value_", triplet$productivity)
        
        ef_input <- paste0("EF_", triplet$input)
        ef_output <- paste0("EF_", triplet$output)
        ef_productivity <- paste0("EF_", triplet$productivity)
        
        data_triplet[get(ef_input) == TRUE, isOutlier_input:= TRUE]
        data_triplet[get(ef_output) == TRUE,isOutlier_output:= TRUE]
        data_triplet[get(ef_productivity) == TRUE,isOutlier_productivity:= TRUE]
        
    }else{
        #need to be mangaaed separately being quartet and not a triplet actually
        Label_outlier(data_label = data_triplet, element = triplet$output1, type = "output1")
        compute_movav(data = data_triplet, element = triplet$output1, type = "output1")
        
        Label_outlier(data_label = data_triplet, element = triplet$output2, type = "output2")
        compute_movav(data = data_triplet, element = triplet$output2, type = "output2")
        
        Label_outlier(data_label = data_triplet, element = triplet$input, type = "input")
        compute_movav(data = data_triplet, element = triplet$input, type = "input")
        
        Label_outlier(data_label = data_triplet, element = triplet$productivity, type = "productivity")
        compute_movav(data = data_triplet, element = triplet$productivity, type = "productivity")
        
        
        input <- paste0("Value_", triplet$input)
        output1 <- paste0("Value_", triplet$output1)
        output2 <- paste0("Value_", triplet$output2)
        productivity <- paste0("Value_", triplet$productivity)
        
        ef_input <- paste0("EF_", triplet$input)
        ef_output1 <- paste0("EF_", triplet$output1)
        ef_output2 <- paste0("EF_", triplet$output2)
        ef_productivity <- paste0("EF_", triplet$productivity)
        
        data_triplet[get(ef_input) == TRUE, isOutlier_input:= TRUE]
        data_triplet[get(ef_output1) == TRUE,isOutlier_output1:= TRUE]
        data_triplet[get(ef_output2) == TRUE,isOutlier_output2:= TRUE]
        data_triplet[get(ef_productivity) == TRUE,isOutlier_productivity:= TRUE]
        
    }
    
    # Number of Milk Animal cannot be higher than Live Animal: This module do not correct the Milk Animal number with the officil milk production
    # though, It is possible that will create an outlier for the Milk production Yield. The results of productivity outliers
    # will send to the user to control the official outlier on Milk Production so that they can change the number of Milk Animal, not higher than Live Animal.
    
    ##' GENERAL for all productivity at starting point
    
    # if (imputation_selection == "MILK"){
    #     data_triplet[isOutlier_productivity==TRUE,c(productivity):= ifelse(movav_productivity<=1, movav_productivity, 1)]
    # } else if(imputation_selection == "LIVESTOCK" & last == FALSE){
    #     data_triplet[isOutlier_productivity==TRUE,c(productivity):=movav_productivity]
    # } else if (imputation_selection == "LIVESTOCK" & last == TRUE){
    #     data_triplet[isOutlier_productivity==TRUE & get(as.character(paste0("Case_", triplet$input))) %in% "3",c(productivity):=movav_productivity]
    # }
    
    # data_triplet[isOutlier_productivity == TRUE, c(productivity):= movav_productivity]
    if (imputation_selection == "CROP") {
        
        
        ######################################################################
        ######################################################################
        #ADDITIONAL part to adjust the tails
        
        data_triplet[, tailCheck := ifelse(sum(Protected_5312[timePointYears %in% as.character(startYear:endYear)]) == 0 & 
                                               sum(Protected_5510[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                     by = c("measuredItemCPC", "geographicAreaM49")]
        
        
        
        data_triplet[tailCheck == TRUE,
                     tailOutlier_input := ifelse(abs((get(paste0("Value_", triplet$input)) / movav_input)-1) < 0.05, 
                                                 FALSE, TRUE)]    
        
        data_triplet[tailCheck == TRUE,
                     tailOutlier_output := ifelse(abs((get(paste0("Value_", triplet$output)) / movav_output)-1) < 0.05, 
                                                  FALSE, TRUE)]    
        
        ###########################################################
        
        data_triplet[, newInput := get(paste0("Value_", triplet$input))]
        
        data_triplet[tailCheck == TRUE & tailOutlier_input == TRUE &
                         timePointYears %in% as.character(startYear:endYear), newInput := NA]
        data_triplet <-
            data_triplet[
                order(geographicAreaM49,measuredItemCPC, timePointYears),
                tailMovag_input := rollavg(newInput, order = 3),
                by = c("geographicAreaM49","measuredItemCPC")
                ]
        
        data_triplet[, newOutput := get(paste0("Value_", triplet$output))]
        
        data_triplet[tailCheck == TRUE & tailOutlier_output == TRUE &
                         timePointYears %in% as.character(startYear:endYear), newOutput := NA]
        data_triplet <-
            data_triplet[
                order(geographicAreaM49,measuredItemCPC, timePointYears),
                tailMovag_output := rollavg(newOutput, order = 3),
                by = c("geographicAreaM49","measuredItemCPC")
                ]
        #########################################################
        
        
        
        # data_triplet <-
        #     data_triplet[
        #         order(geographicAreaM49,measuredItemCPC, timePointYears),
        #         tailMovag_input := roll_meanr(get(paste0("Value_", triplet$input)),  5),
        #         by = c("geographicAreaM49","measuredItemCPC")
        #         ]
        # 
        # data_triplet <-
        #     data_triplet[
        #         order(geographicAreaM49,measuredItemCPC, timePointYears),
        #         tailMovag_output := roll_meanr(get(paste0("Value_", triplet$output)), 5),
        #         by = c("geographicAreaM49","measuredItemCPC")
        #         ]
        
        #
        #
        
        data_triplet[tailCheck == TRUE & tailOutlier_input == TRUE & tailOutlier_output == FALSE,
                     c(input):= ifelse(!is.na(tailMovag_input), tailMovag_input, get(paste0("Value_", triplet$input)))]
        
        data_triplet[tailCheck == TRUE & tailOutlier_output == TRUE & tailOutlier_input == FALSE,
                     c(output):= ifelse(!is.na(tailMovag_output), tailMovag_output, get(paste0("Value_", triplet$output)))]
        
        #both outliers
        data_triplet[tailCheck == TRUE & tailOutlier_output == TRUE & tailOutlier_input == TRUE,
                     c(input):= ifelse(!is.na(tailMovag_input), tailMovag_input, get(paste0("Value_", triplet$input)))]
        
        data_triplet[tailCheck == TRUE & tailOutlier_output == TRUE & tailOutlier_input == TRUE,
                     c(output):= ifelse(!is.na(tailMovag_output), tailMovag_output, get(paste0("Value_", triplet$output)))]
        
        ######################################################################
        ######################################################################
        data_triplet[is.na(tailCheck), tailCheck := FALSE]
        
        #changing a priori outliers productivity
        data_triplet[isOutlier_productivity==TRUE,c(productivity):=movav_productivity]
        
        #manage input as outlier
        data_triplet[tailCheck == FALSE & isOutlier_input == TRUE & isOutlier_output == FALSE & 
                         get(paste0("Protected_", triplet$input)) == FALSE, 
                     c(input):= ifelse(get(productivity) != 0, get(output)/get(productivity)*factor, NA)]
        
        
        #manage output as outlier
        data_triplet[tailCheck == FALSE & isOutlier_input == FALSE & isOutlier_output == TRUE & 
                         get(paste0("Protected_", triplet$output)) == FALSE,
                     c(output):= get(input)*get(productivity)/factor]
        
        
        ##' Manage when both inut and output are outlier. For crops this is done at the first round because it has unique triplet
        ##' Last parameter is need for livestock
        if (partial == FALSE & last != TRUE) {
            
            data_triplet[tailCheck == FALSE & isOutlier_input == TRUE & isOutlier_output == TRUE &
                             get(paste0("Protected_", triplet$input)) == FALSE & get(paste0("Protected_", triplet$output)) == FALSE,
                         c(output):= movav_output]
            
            data_triplet[isOutlier_input == TRUE & isOutlier_output == TRUE,
                         c(input):= movav_output/get(productivity)*factor]
            
            
        }
        
        data_triplet[, Case := NA_character_]
        
        data_triplet[get(paste0("Protected_", triplet$input)) == TRUE & get(paste0("Protected_", triplet$output)) == TRUE &
                         isOutlier_productivity == TRUE, Case := "1_alert"]
        
        data_triplet[, c("tailCheck","tailOutlier_input","tailOutlier_output","tailMovag_input","tailMovag_output") := NULL]
        
    }
    
    if (imputation_selection == "MILK") {
        
        
        if (partial == TRUE) {
            
            data_triplet[, Case:= NA_character_]
            
            data_triplet <- merge(data_triplet, unique(complete_protected_milk[,c("geographicAreaM49","measuredItemCPC","timePointYears", "Protected_milk"), with=F]),
                                  by = c("geographicAreaM49","measuredItemCPC", "timePointYears"), all.x = TRUE)
            
            #if milk triplet all free the imputation plugin works good so we do not touch it => case 1
            data_triplet[, tailCheck:= ifelse(sum(Protected_5318[timePointYears %in% as.character(startYear:endYear)]) == 0 & 
                                                  sum(Protected_milk[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                         by = c("measuredItemCPC", "geographicAreaM49")]
            #if milk triplet all protected  we do not touch it =Case 1
            data_triplet[, proCheck:= ifelse(sum(Protected_5318[timePointYears %in% as.character(startYear:endYear)]) == (endYear-startYear+1) & 
                                                 sum(Protected_milk[timePointYears %in% as.character(startYear:endYear)]) == (endYear-startYear+1) , TRUE, FALSE),
                         by = c("measuredItemCPC", "geographicAreaM49")]
            
            
            data_triplet[tailCheck == TRUE, Case := "1"]
            
            data_triplet[proCheck == TRUE, Case := "1"]
            
            data_triplet[, stockOutlier := FALSE]
            
            #last minute change. we would check all indepentendtly from stock being outlier or not
            # data_triplet[isOutlier_input == TRUE, stockOutlier := TRUE]
            # 
            data_triplet[, stockCheck:= ifelse(sum(stockOutlier[timePointYears %in% as.character(startYear:endYear)]) > 0, TRUE, FALSE),
                         by = c("measuredItemCPC", "geographicAreaM49")]
            
            
            data_triplet[Case %!in% "1"  & stockCheck == FALSE & Protected_5318 == FALSE & Protected_milk == FALSE, Case := "2_base"]
            
            data_triplet[Case %!in% "1"  & stockCheck == TRUE  & Protected_5318 == FALSE & Protected_milk == FALSE , Case := "2_out"]
            
            
            data_triplet[Case %!in% "1"  & stockCheck == FALSE & Protected_5318 == TRUE & Protected_milk == FALSE |
                             Case %!in% "1"  & stockCheck == FALSE & Protected_5318 == FALSE & Protected_milk == TRUE, Case := "3_base"]
            
            data_triplet[Case %!in% "1"  & stockCheck == TRUE  & Protected_5318 == TRUE & Protected_milk == FALSE |
                             Case %!in% "1"  & stockCheck == TRUE  & Protected_5318 == FALSE & Protected_milk == TRUE  , Case := "3_out"]
            
            
            
            data_triplet[, c("Protected_milk","tailCheck","stockOutlier","stockCheck","proCheck") := NULL]
            
        }else if (partial == FALSE){
            
            data_triplet[, newInput := get(paste0("Value_", triplet$input))]
            
            data_triplet[, newOutput := get(paste0("Value_", triplet$output))]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("2_base", "3_base") & 
                             get(paste0("Protected_", triplet$input)) == FALSE &
                             timePointYears %in% as.character(startYear:endYear), newInput := NA]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("2_base", "3_base") & 
                             get(paste0("Protected_", triplet$output)) == FALSE &
                             timePointYears %in% as.character(startYear:endYear), newOutput := NA]
            
            
            data_triplet <-
                data_triplet[
                    order(geographicAreaM49,measuredItemCPC, timePointYears),
                    avg_Input := rollavg(newInput, order = 3),
                    by = c("geographicAreaM49","measuredItemCPC")
                    ]
            
            data_triplet <-
                data_triplet[
                    order(geographicAreaM49,measuredItemCPC, timePointYears),
                    avg_Output := rollavg(newOutput, order = 3),
                    by = c("geographicAreaM49","measuredItemCPC")
                    ]
            
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("2_base", "3_base") & 
                             get(paste0("Protected_", triplet$input)) == FALSE & isOutlier_input == TRUE &
                             timePointYears %in% as.character(startYear:endYear), 
                         c(input) := avg_Input]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("2_base", "3_base") & 
                             get(paste0("Protected_", triplet$output)) == FALSE & isOutlier_output == TRUE &
                             timePointYears %in% as.character(startYear:endYear), 
                         c(output) := avg_Output]
            
            
            data_triplet[, c("newInput","newOutput","avg_Input","avg_Output"):= NULL]
            
            
            
            
        }
        
    }
    
    
    if (imputation_selection == "EGG") {
        
        eggs_factors[, world_avg := mean(avg_weight, na.rm = TRUE)]
        eggs_factors$avg_weight <- as.numeric(eggs_factors$avg_weight)
        eggs_factors[is.na(avg_weight), avg_weight := world_avg]
        eggs_factors <- eggs_factors[item %in% "0231",]
        eggs_factors$avg_weight <- eggs_factors$avg_weight/1000
        eggs_factors[, world_avg := NULL]
        
        data_triplet <- merge(data_triplet, eggs_factors[, c("m49","avg_weight"), with = FALSE], by.x = "geographicAreaM49", 
                              by.y = "m49", all.x = TRUE)
        
        #mov average for productivity outliers 
        data_triplet[, newProductivity := get(paste0("Value_", triplet$productivity))]
        
        data_triplet[isOutlier_productivity == TRUE & 
                         get(paste0("Protected_", triplet$productivity)) == FALSE &
                         timePointYears %in% as.character(startYear:endYear), newProductivity := NA]
        data_triplet <-
            data_triplet[
                order(geographicAreaM49,measuredItemCPC, timePointYears),
                avg_Productivity := rollavg(newProductivity, order = 3),
                by = c("geographicAreaM49","measuredItemCPC")
                ]
        
        #movaverage for 5510 outliers
        data_triplet[, newOutput1 := get(paste0("Value_", triplet$output1))]
        
        data_triplet[isOutlier_output1 == TRUE & 
                         get(paste0("Protected_", triplet$output1)) == FALSE &
                         timePointYears %in% as.character(startYear:endYear), newOutput1 := NA]
        data_triplet <-
            data_triplet[
                order(geographicAreaM49,measuredItemCPC, timePointYears),
                avg_Output1 := rollavg(newOutput1, order = 3),
                by = c("geographicAreaM49","measuredItemCPC")
                ]
        
        #mov average for input outliers 
        data_triplet[, newInput := get(paste0("Value_", triplet$input))]
        
        data_triplet[isOutlier_input == TRUE & 
                         get(paste0("Protected_", triplet$input)) == FALSE &
                         timePointYears %in% as.character(startYear:endYear), newInput := NA]
        data_triplet <-
            data_triplet[
                order(geographicAreaM49,measuredItemCPC, timePointYears),
                avg_Input := rollavg(newInput, order = 3),
                by = c("geographicAreaM49","measuredItemCPC")
                ]
        
        
        #intialize case column
        data_triplet[, Case := NA_character_]
        # if both output blocked -> check input 
        
        data_triplet[get(paste0("Protected_", triplet$output1)) == TRUE & get(paste0("Protected_", triplet$output2)) == TRUE
                     & get(paste0("Protected_", triplet$input)) == FALSE, 
                     Case := "1_base"]
        
        #if outputs are protected, increase the yield threshold 
        
        data_triplet[Case %in% "1_base", 
                     isOutlier_productivity := ifelse(abs((get(paste0("Value_", triplet$productivity)) / movav_productivity)-1) < 0.2, 
                                                      FALSE, isOutlier_productivity)]
        
        data_triplet[Case %in% "1_base" & isOutlier_productivity == TRUE , 
                     c(productivity) := avg_Productivity]
        
        data_triplet[Case %in% "1_base" & isOutlier_productivity == TRUE , 
                     c(input):= ifelse(get(productivity) != 0, get(output1)/get(productivity)*factor, NA)]
        
        #send it by email within the incompatibilities
        data_triplet[get(paste0("Protected_", triplet$output1)) == TRUE & get(paste0("Protected_", triplet$output2)) == TRUE
                     & get(paste0("Protected_", triplet$input)) == TRUE & isOutlier_productivity == TRUE, 
                     Case := "1_out"]
        
        
        # if output 2 blocked but not output1 recalculate output 1 and block it -> then check input
        data_triplet[get(paste0("Protected_", triplet$output1)) == FALSE & get(paste0("Protected_", triplet$output2)) == TRUE, 
                     Case := "2.1"]
        #update 5510 with the weight and 5513
        data_triplet[Case %in% "2.1", c(output1) := ifelse(get(output2) != 0, get(output2)*avg_weight, NA)]
        
        #protect 5510
        data_triplet[Case %in% "2.1", c(paste0("Protected_", triplet$output1)) := TRUE]
        
        # if output 1 blocked but not output1 recalculate output 2 and block it -> then check input
        data_triplet[get(paste0("Protected_", triplet$output1)) == TRUE & get(paste0("Protected_", triplet$output2)) == FALSE, 
                     Case := "2.2"]
        
        #update 5513 with the weight and 5510
        data_triplet[Case %in% "2.2", c(output2) := ifelse(get(output1) != 0, get(output1)/avg_weight, NA)]
        
        #protect 5513
        data_triplet[Case %in% "2.2", c(paste0("Protected_", triplet$output2)) := TRUE]
        
        
        data_triplet[Case %in% c("2.1","2.2"), 
                     isOutlier_productivity := ifelse(abs((get(paste0("Value_", triplet$productivity)) / movav_productivity)-1) < 0.2, 
                                                      FALSE, isOutlier_productivity)]
        
        
        data_triplet[Case %in% c("2.1","2.2") & get(paste0("Protected_", triplet$input)) == FALSE & isOutlier_productivity == TRUE , 
                     c(productivity) := avg_Productivity]
        
        data_triplet[Case %in% c("2.1","2.2") & get(paste0("Protected_", triplet$input)) == FALSE & isOutlier_productivity == TRUE , 
                     c(input):= ifelse(get(productivity) != 0, get(output1)/get(productivity)*factor, NA)]
        
        #send it by email within the incompatibilities
        data_triplet[Case %in% c("2.1","2.2") & get(paste0("Protected_", triplet$input)) == TRUE & isOutlier_productivity == TRUE, 
                     Case := "2_out"]
        
        
        
        #if output2 and output 1 not blocked 
        data_triplet[get(paste0("Protected_", triplet$output1)) == FALSE & get(paste0("Protected_", triplet$output2)) == FALSE, 
                     Case := "3_base"]
        #the first step is to check if 5510 is outlier and laying not outlier. 
        data_triplet[Case %in% "3_base" & isOutlier_output1 == TRUE & isOutlier_input == FALSE, 
                     Case := "3_out_output"]
        
        #then change 5510 
        data_triplet[Case %in% "3_out_output", 
                     c(output1) := avg_Output1]
        
        #and recalculate 5513
        data_triplet[Case %in% "3_out_output", c(output2) := ifelse(get(output1) != 0, get(output1)/avg_weight, NA)]
        
        #then we check if 5510 is not outlier and laying is outlier and not protected. 
        data_triplet[Case %in% "3_base" & isOutlier_output1 == FALSE & isOutlier_input == TRUE 
                     & get(paste0("Protected_", triplet$input)) == FALSE, 
                     Case := "3_out_input"]
        
        #then change 5510 and recalculate 5513
        data_triplet[Case %in% "3_out_input", 
                     c(input) := avg_Input]
        
        #IF BOTH OUTLIERS
        data_triplet[Case %in% "3_base" & isOutlier_output1 == TRUE & isOutlier_input == TRUE 
                     & get(paste0("Protected_", triplet$input)) == FALSE, 
                     Case := "3_out_both"]
        
        
        data_triplet[Case %in% "3_out_both", 
                     c(input) := avg_Input]
        
        #then change 5510 
        data_triplet[Case %in% "3_out_both", 
                     c(output1) := avg_Output1]
        
        #and recalculate 5513
        data_triplet[Case %in% "3_out_both", c(output2) := ifelse(get(output1) != 0, get(output1)/avg_weight, NA)]
        
        
        #where input and output are not outlier but yield is outlier 
        
        data_triplet[Case %in% "3_base" & isOutlier_output1 == FALSE & isOutlier_input == FALSE & isOutlier_productivity == TRUE, 
                     Case := "3_alert"]
        
        data_triplet[, c("avg_weight","newProductivity","avg_Productivity","newOutput1","avg_Output1","newInput", "avg_Input") := NULL]
    }
    
    
    
    
    if (imputation_selection == "LIVESTOCK") {
        ##' First condition we are in Live Animal triplet. The productivity is already smoothed
        ##' Initialize the cases. The column will be then useful to decide what to do under meat
        
        ##' partial == TRUE represent the first round of live animals check where also cases are defined
        if (partial == TRUE) {
            
            data_triplet[, Case:= NA_character_]
            # da inserire case con solo yield outlier and neither input and output are outlier
            
            ##' Case 1: Stocks (input) are protected and slaughtered (output) are outliers and not protected
            ##' if off take rate has been changed because it was outlier we should alert the case under meat
            #all_EstOff_f <- copy(all_EstOff)
            
            #use round 3. otherwise it sees equal two different off take rate
            #data_triplet <- merge(data_triplet, all_EstOff_f[,list(geographicAreaM49,measuredItemCPC,timePointYears, Value_oldOff= Value)],
            #by = c("geographicAreaM49","measuredItemCPC", "timePointYears"), all.x = TRUE)
            
            
            
            #add the column with protection of the meat 
            data_triplet <- merge(data_triplet, unique(complete_protected_meat[,c("geographicAreaM49","measuredItemCPC","timePointYears", "Protected_meat"), with=F]),
                                  by = c("geographicAreaM49","measuredItemCPC", "timePointYears"), all.x = TRUE)
            
            
            # we have 1 options: stock is protected and prod of meat protected as well. We do not consider the off take rate.
            #it is flagged as alert here so we can apply a larger threshold when we check in meat.
            
            data_triplet[get(paste0("Protected_", triplet$input)) == TRUE & get(paste0("Protected_", triplet$output)) == FALSE &
                             Protected_meat == TRUE, 
                         Case := "1_alert"]
            
            
            # we have 2 options: stock is protected and prod of meat is not protected. We consider the off take:
            #it can be outlier -> 1_base_out
            #or not be outlier -> 1_base
            
            data_triplet[get(paste0("Protected_", triplet$input)) == TRUE & get(paste0("Protected_", triplet$output)) == FALSE &
                             Protected_meat == FALSE & isOutlier_productivity == FALSE, 
                         Case := "1_base"]
            
            # data_triplet[isOutlier_input == FALSE & isOutlier_output == TRUE & 
            #                  get(paste0("Protected_", triplet$input)) == TRUE & get(paste0("Protected_", triplet$output)) == FALSE &
            #                  Value_9999 != Value_oldOff, 
            #              c(output):= ifelse(get(productivity) != 0, get(input)*get(productivity)/factor, NA)]
            
            
            
            data_triplet[get(paste0("Protected_", triplet$input)) == TRUE & get(paste0("Protected_", triplet$output)) == FALSE &
                             Protected_meat == FALSE & isOutlier_productivity == TRUE, 
                         Case := "1_base_out"]
            
            data_triplet[, prodCondition := ifelse(sum(Protected_meat[timePointYears %in% as.character(startYear:endYear)]) >= 1,
                                                   TRUE, FALSE),
                         by = c('geographicAreaM49', 'measuredItemCPC')]
            
            #if off take rate outlier but the triplet in production of meat has protected values around then increase outlier range
            
            data_triplet[Case %in% '1_base_out' & prodCondition == TRUE, 
                         isOutlier_productivity := ifelse(abs((get(paste0("Value_", triplet$productivity)) / movav_productivity)-1) < 0.15, 
                                                          FALSE, isOutlier_productivity)]
            
            data_triplet[Case %in% '1_base_out' & prodCondition == TRUE & isOutlier_productivity == FALSE, Case := NA_character_]
            
            data_triplet[Case %in% '1_base_out',c(productivity):=movav_productivity]
            
            data_triplet[Case %in% '1_base_out',c(output):= ifelse(get(productivity) != 0, get(input)*get(productivity)/factor, NA)]
            
            
            #off take rate è outlier and both input and output are protected -> send by email
            
            data_triplet[get(paste0("Protected_", triplet$input)) == TRUE & get(paste0("Protected_", triplet$output)) == TRUE & 
                             isOutlier_productivity == TRUE, 
                         Case := "1_alert_protection"]
            
            data_triplet[Case %in% '1_alert_protection', 
                         isOutlier_productivity := ifelse(abs((get(paste0("Value_", triplet$productivity)) / movav_productivity)-1) < 0.2, 
                                                          FALSE, isOutlier_productivity)]
            
            data_triplet[Case %in% '1_alert_protection' & isOutlier_productivity == FALSE, Case := NA_character_]
            
            # case where slaughtered is protected and stock is not
            #if is outlier off take rate with enlarged threshold, send it by email
            
            data_triplet[get(paste0("Protected_", triplet$output)) == TRUE & get(paste0("Protected_", triplet$input)) == FALSE
                         & isOutlier_productivity== TRUE, 
                         Case := "2_out"]
            
            #if oktake not outlier with 30% threshold unflag it
            data_triplet[Case %in% "2_out" , 
                         isOutlier_productivity := ifelse(abs((get(paste0("Value_", triplet$productivity)) / movav_productivity)-1) < 0.3, 
                                                          FALSE, isOutlier_productivity)]
            
            data_triplet[Case %in% "2_out" & is.na(isOutlier_productivity), isOutlier_productivity := FALSE]
            
            data_triplet[Case %in% "2_out" & isOutlier_productivity== FALSE, 
                         Case := "2_base"]
            
            #even if off take rate not outlier flag as case 2  to check the meat triplette
            
            data_triplet[get(paste0("Protected_", triplet$output)) == TRUE & get(paste0("Protected_", triplet$input)) == FALSE
                         & isOutlier_productivity== FALSE, 
                         Case := "2_base"]
            
            
            #all free case 3
            
            #cases with off take rate outier and meat protected in the meat triplette -> directly check the meat
            data_triplet[get(paste0("Protected_", triplet$input)) == FALSE & get(paste0("Protected_", triplet$output)) == FALSE &
                             Protected_meat == TRUE & isOutlier_productivity == TRUE, 
                         Case := "3_alert"]
            
            
            #cases where all free and no meat production protected. We act on the live animal
            data_triplet[get(paste0("Protected_", triplet$input)) == FALSE & get(paste0("Protected_", triplet$output)) == FALSE &
                             Protected_meat == FALSE , 
                         Case := "3_base"]
            
            #get(paste0("Protected_", triplet$output))
            
            if(triplet$input == "5111"){
                
                data_triplet[, tailCheck := ifelse(sum(Protected_5111[timePointYears %in% as.character(startYear:endYear)]) == 0 & 
                                                       sum(Protected_5315[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                             by = c("measuredItemCPC", "geographicAreaM49")]
                
            }else{
                
                data_triplet[, tailCheck := ifelse(sum(Protected_5112[timePointYears %in% as.character(startYear:endYear)]) == 0 & 
                                                       sum(Protected_5316[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                             by = c("measuredItemCPC", "geographicAreaM49")]
                
            }    
            
            
            
            data_triplet[Case %in% '3_base' & tailCheck == TRUE,
                         tailOutlier_input := ifelse(abs((get(paste0("Value_", triplet$input)) / movav_input)-1) < 0.05, 
                                                     FALSE, TRUE)]    
            
            data_triplet[Case %in% '3_base' & tailCheck == TRUE,
                         tailOutlier_output := ifelse(abs((get(paste0("Value_", triplet$output)) / movav_output)-1) < 0.05, 
                                                      FALSE, TRUE)]    
            
            
            
            data_triplet <-
                data_triplet[
                    order(geographicAreaM49,measuredItemCPC, timePointYears),
                    tailMovag_input := roll_meanr(get(paste0("Value_", triplet$input)),  5),
                    by = c("geographicAreaM49","measuredItemCPC")
                    ]
            
            data_triplet <-
                data_triplet[
                    order(geographicAreaM49,measuredItemCPC, timePointYears),
                    tailMovag_output := roll_meanr(get(paste0("Value_", triplet$output)), 5),
                    by = c("geographicAreaM49","measuredItemCPC")
                    ]
            
            
            
            #for cases that are not tails but an element can be outlier: we take roll average without imputed figured that are potential outliers
            #new input
            data_triplet[, newInput := get(paste0("Value_", triplet$input))]
            
            data_triplet[Case %in% "3_base" & 
                             tailCheck == FALSE &
                             timePointYears %in% as.character(startYear:endYear), newInput := NA]
            data_triplet <-
                data_triplet[
                    order(geographicAreaM49,measuredItemCPC, timePointYears),
                    lastMovag_input := rollavg(newInput, order = 3),
                    by = c("geographicAreaM49","measuredItemCPC")
                    ]
            
            data_triplet[, newOutput := get(paste0("Value_", triplet$output))]
            
            data_triplet[Case %in% "3_base" & 
                             tailCheck == FALSE &
                             timePointYears %in% as.character(startYear:endYear), newOutput := NA]
            data_triplet <-
                data_triplet[
                    order(geographicAreaM49,measuredItemCPC, timePointYears),
                    lastMovag_output := rollavg(newOutput, order = 3),
                    by = c("geographicAreaM49","measuredItemCPC")
                    ]
            
            
            # data_triplet <-
            #     data_triplet[
            #         order(geographicAreaM49,measuredItemCPC, timePointYears),
            #         lastMovag_input := roll_meanr(get(paste0("Value_", triplet$input)),  3),
            #         by = c("geographicAreaM49","measuredItemCPC")
            #         ]
            # 
            # data_triplet <-
            #     data_triplet[
            #         order(geographicAreaM49,measuredItemCPC, timePointYears),
            #         lastMovag_output := roll_meanr(get(paste0("Value_", triplet$output)), 3),
            #         by = c("geographicAreaM49","measuredItemCPC")
            #         ]
            
            #cases where the validation years have only inputed values so tails outliers can appear
            #if stock outlier and slaughtered not outlier let s change stock
            
            data_triplet[Case %in% '3_base' &  tailCheck == TRUE & tailOutlier_input == TRUE & tailOutlier_output == FALSE,
                         c(input):= tailMovag_input]
            
            data_triplet[Case %in% '3_base' & tailCheck == TRUE & tailOutlier_input == TRUE & tailOutlier_output == FALSE,
                         Case := "3.1"]
            
            #if slaughtered outlier and stock not outlier let s change slaughtered
            
            data_triplet[Case %in% '3_base' & tailCheck == TRUE & tailOutlier_input == FALSE & tailOutlier_output == TRUE,
                         c(output):= tailMovag_output]
            
            data_triplet[Case %in% '3_base' & tailCheck == TRUE & tailOutlier_input == FALSE & tailOutlier_output == TRUE,
                         Case := "3.2"]
            
            #if both stock and sloughtered outlier let s change both of them
            data_triplet[Case %in% '3_base' & tailCheck == TRUE & tailOutlier_input == TRUE & tailOutlier_output == TRUE, 
                         c(input):= tailMovag_input]
            
            data_triplet[Case %in% '3_base' & tailCheck == TRUE & tailOutlier_input == TRUE & tailOutlier_output == TRUE,
                         c(output):= tailMovag_output]
            
            data_triplet[Case %in% '3_base'& tailCheck == TRUE & tailOutlier_input == TRUE & tailOutlier_output == TRUE,
                         Case := "3.3"]
            
            
            # cases where at least some protected values are around in the series
            #if stock outlier and slaughtered not outlier let s change stock
            
            data_triplet[Case %in% '3_base' &  tailCheck == FALSE & isOutlier_input == TRUE & isOutlier_output == FALSE,
                         c(input):= lastMovag_input]
            
            data_triplet[Case %in% '3_base' &  tailCheck == FALSE & isOutlier_input == TRUE & isOutlier_output == FALSE,
                         Case := "3.1"]
            
            #if slaughtered outlier and stock not outlier let s change slaughtered
            
            data_triplet[Case %in% '3_base'  &  tailCheck == FALSE & isOutlier_input == FALSE & isOutlier_output == TRUE,
                         c(output):= lastMovag_output]
            
            data_triplet[Case %in% '3_base'  &  tailCheck == FALSE & isOutlier_input == FALSE & isOutlier_output == TRUE,
                         Case := "3.2"]
            
            
            #if both stock and sloughtered outlier let s change both of them
            data_triplet[Case %in% '3_base'  &  tailCheck == FALSE & isOutlier_input == TRUE & isOutlier_output == TRUE |
                             Case %in% '3_base'  &  tailCheck == FALSE & isOutlier_productivity == TRUE, 
                         c(input):= lastMovag_input]
            
            data_triplet[Case %in% '3_base' &  tailCheck == FALSE & isOutlier_input == TRUE & isOutlier_output == TRUE |
                             Case %in% '3_base'  &  tailCheck == FALSE & isOutlier_productivity == TRUE ,
                         c(output):= lastMovag_output]
            
            data_triplet[Case %in% '3_base' &  tailCheck == FALSE & isOutlier_input == TRUE & isOutlier_output == TRUE |
                             Case %in% '3_base'  &  tailCheck == FALSE & isOutlier_productivity == TRUE ,
                         Case := "3.3"]
            
            #data_triplet[Case %in% "3", Case := NA_character_]
            
            #data_triplet[Case %in% "3.1", Case := NA_character_]
            
            data_triplet[, c("Protected_meat","prodCondition","tailCheck","tailOutlier_input", 
                             "tailOutlier_output","tailMovag_input","tailMovag_output",
                             "lastMovag_input","lastMovag_output","newInput","newOutput"):= NULL]
            
            
        }
        
        #' partial == FALSE and last == FALSE represent the meat round
        #' slaughtered figures are already synch under meat thanks to update-slaughtered function
        if (partial == FALSE & last != TRUE) {
            ###############################
            ########### CASE 1 ############
            ###############################
            
            
            # attaching the carcass table min and max to control the yield levels
            
            data_triplet <- merge(data_triplet, range_carcass, by.x = "measuredItemCPC", by.y= "meat_item_cpc", all.x = TRUE)
            # caso1 alert
            
            #if the slaughtered is outlier for cases that have stock and production protected then we enlarge the thresholds, always
            #given that is in the carcass ranges
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_alert" & isOutlier_productivity == TRUE, 
                         outlier := ifelse(abs((get(paste0("Value_", triplet$productivity)) / movav_productivity)-1) < 0.3, FALSE, TRUE)]
            #if with the new threshold they are not outlier, unlabel them and discard. they no need check on yieds level since were not touched
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_alert" & isOutlier_productivity == TRUE &
                             outlier == FALSE, isOutlier_productivity := FALSE ]
            #unflag
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_alert" & isOutlier_productivity == FALSE, 
                         c(paste0("Case_", triplet$input)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$output))) %in% "1_alert" & isOutlier_productivity == FALSE, 
                         c(paste0("Case_", triplet$output)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$productivity))) %in% "1_alert" & isOutlier_productivity == FALSE, 
                         c(paste0("Case_", triplet$productivity)) := NA_character_ ]
            
            #if still outlier apply old mean yield to slaughter
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_alert" & isOutlier_productivity == TRUE &
                             outlier == TRUE, c(productivity):=movav_productivity]
            
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_alert" & isOutlier_productivity == TRUE &
                             outlier == TRUE, c(input):= ifelse(get(productivity) != 0, get(output)/get(productivity)*factor, NA) ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_alert" & isOutlier_productivity == TRUE &
                             outlier == TRUE, carcass_outlier := 
                             ifelse(get(paste0("Value_", triplet$productivity)) < carcass_weight_min & !is.na(carcass_weight_min) |
                                        get(paste0("Value_", triplet$productivity)) > carcass_weight_max & !is.na(carcass_weight_min) , TRUE, FALSE) ]
            
            #check that the new yield is in the carcass range #
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_alert" & 
                             carcass_outlier == TRUE & get(paste0("Value_", triplet$productivity)) < carcass_weight_min, 
                         c(productivity):= carcass_weight_min]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_alert" &
                             carcass_outlier == TRUE & get(paste0("Value_", triplet$productivity)) > carcass_weight_max, 
                         c(productivity):= carcass_weight_max]
            
            #recalculate input given new productivities
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_alert" & 
                             carcass_outlier == TRUE, 
                         c(input):= ifelse(get(productivity) != 0, get(output)/get(productivity)*factor, NA)]
            
            #unflag the outliers since it was re-calculated
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_alert" & isOutlier_productivity == TRUE &
                             carcass_outlier == TRUE, 
                         carcass_outlier := FALSE]
            
            #still keeping trace of cases 1_alert and outlier productivity true and outlier true.. since are cases where we changed yield and so slaughtered 
            #need to be updated in live animals
            
            #caso 1 base out
            
            #I check also the yiled of triplette with yield not outlier. Since we apply a lower threshold than the standard one of 0.2
            # data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_base_out" & isOutlier_productivity == TRUE, 
            #              outlier := ifelse(abs((get(paste0("Value_", triplet$productivity)) / movav_productivity)-1) < 0.15, FALSE, TRUE)]
            
            data_triplet[, exceptions := FALSE]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_base_out" , 
                         outlier := ifelse(abs((get(paste0("Value_", triplet$productivity)) / movav_productivity)-1) < 0.15, FALSE, TRUE)]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_base_out"  &
                             outlier == FALSE, isOutlier_productivity := FALSE ]
            
            
            #they are exceptions, since the new slaughterd is coming from changes in animals. Just modifing the yield out of range
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_base_out" &
                             outlier == TRUE, exceptions:= TRUE]
            #check that the new yield is in the carcass range #
            #check the limit of yied for all changed slaughtered even the ones that didn t result as outliers... (yield could have been at limit)
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_base_out" & exceptions == TRUE |
                             get(as.character(paste0("Case_", triplet$input))) %in% "1_base_out" & isOutlier_productivity == FALSE , 
                         carcass_outlier := 
                             ifelse(get(paste0("Value_", triplet$productivity)) < carcass_weight_min & !is.na(carcass_weight_min) |
                                        get(paste0("Value_", triplet$productivity)) > carcass_weight_max & !is.na(carcass_weight_min) , TRUE, FALSE) ]
            
            #if yield still outlier or not in carcass ranges checnge productivity with mov average of yield #
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_base_out" & carcass_outlier == TRUE | 
                             get(as.character(paste0("Case_", triplet$input))) %in% "1_base_out" & exceptions == TRUE , 
                         c(productivity):=movav_productivity]
            
            #then change production
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_base_out" & carcass_outlier == TRUE | 
                             get(as.character(paste0("Case_", triplet$input))) %in% "1_base_out" & exceptions == TRUE , 
                         c(output):= ifelse(get(productivity) != 0, get(input)*get(productivity)/factor, NA)]
            
            
            #unflag
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "1_base_out" , 
                         c(paste0("Case_", triplet$input)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$output))) %in% "1_base_out" , 
                         c(paste0("Case_", triplet$output)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$productivity))) %in% "1_base_out" , 
                         c(paste0("Case_", triplet$productivity)) := NA_character_ ]
            
            #1_base
            data_triplet[get(as.character(paste0("Case_", triplet$input))) == "1_base" & isOutlier_productivity == TRUE, 
                         outlier := ifelse(abs((get(paste0("Value_", triplet$productivity)) / movav_productivity)-1) < 0.25, FALSE, TRUE)]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) == "1_base" & isOutlier_productivity == TRUE &
                             outlier == FALSE, isOutlier_productivity := FALSE ]
            
            #they are exceptions, since the new slaughterd is coming from changes in animals. Just modifing the yield out of range
            #removed from filter isOutlierProductivity == TRUE -> redudant
            data_triplet[get(as.character(paste0("Case_", triplet$input))) == "1_base"  &
                             outlier == TRUE, exceptions:= TRUE]
            
            #if yield still outlier or not in carcass ranges checnge productivity with mov average of yield #
            data_triplet[get(as.character(paste0("Case_", triplet$input))) == "1_base" & exceptions == TRUE , 
                         c(productivity):=movav_productivity]
            
            #then change production
            data_triplet[get(as.character(paste0("Case_", triplet$input))) == "1_base" & exceptions == TRUE , 
                         c(output):= ifelse(get(productivity) != 0, get(input)*get(productivity)/factor, NA)]
            
            
            #unflag
            data_triplet[get(as.character(paste0("Case_", triplet$input))) == "1_base" , 
                         c(paste0("Case_", triplet$input)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$output))) == "1_base" , 
                         c(paste0("Case_", triplet$output)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$productivity))) == "1_base" , 
                         c(paste0("Case_", triplet$productivity)) := NA_character_ ]
            
            
            
            # ###############################
            # ########### CASE 2 ############
            # ###############################
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("2_base","2_out") & isOutlier_productivity == TRUE , 
                         outlier := ifelse(abs((get(paste0("Value_", triplet$productivity)) / movav_productivity)-1) < 0.3, FALSE, TRUE)]
            
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("2_base","2_out") & outlier == TRUE &
                             get(as.character(paste0("Protected_", triplet$output))) == FALSE, 
                         c(productivity):=movav_productivity]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("2_base","2_out") & outlier == TRUE &
                             get(as.character(paste0("Protected_", triplet$output))) == FALSE , 
                         c(output):= ifelse(get(productivity) != 0, get(input)*get(productivity)/factor, NA)]
            
            
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("2_base","2_out") & outlier == TRUE &
                             get(as.character(paste0("Protected_", triplet$output))) == TRUE, 
                         c(paste0("Case_", triplet$input)) := "2_production_protection"]
            
            
            
            
            data_triplet[get(as.character(paste0("Protected_", triplet$input))) == TRUE & get(as.character(paste0("Protected_", triplet$output))) == TRUE &
                             isOutlier_productivity == TRUE, c(paste0("Case_", triplet$input)) := "2_alert_protection" ]
            
            
            #unflag
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("2_base","2_out") ,
                         c(paste0("Case_", triplet$input)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$output))) %in% c("2_base","2_out") ,
                         c(paste0("Case_", triplet$output)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$productivity))) %in% c("2_base","2_out") ,
                         c(paste0("Case_", triplet$productivity)) := NA_character_ ]
            
            # 
            # ###############################
            # ########### CASE 3 ############
            # ###############################
            
            #
            
            
            #they are exceptions, since the new slaughterd is coming from changes in animals. Just modifing the yield out of range
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("3.2","3.3") & isOutlier_productivity == TRUE , 
                         exceptions:= TRUE]
            #check that the new yield is in the carcass range #
            
            #check the modified yield for all, even not outliers
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("3.2","3.3") & exceptions == TRUE |
                             get(as.character(paste0("Case_", triplet$input))) %in% c("3.2","3.3") & isOutlier_productivity == FALSE, 
                         carcass_outlier :=
                             ifelse(get(paste0("Value_", triplet$productivity)) < carcass_weight_min & !is.na(carcass_weight_min) |
                                        get(paste0("Value_", triplet$productivity)) > carcass_weight_max & !is.na(carcass_weight_min) , TRUE, FALSE) ]
            
            #if yield still outlier or not in carcass ranges checnge productivity with mov average of yield #
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("3.2","3.3") & carcass_outlier == TRUE |
                             get(as.character(paste0("Case_", triplet$input))) %in% c("3.2","3.3") & exceptions == TRUE ,
                         c(productivity):=movav_productivity]
            
            #then change production
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("3.2","3.3") & carcass_outlier == TRUE |
                             get(as.character(paste0("Case_", triplet$input))) %in% c("3.2","3.3") & exceptions == TRUE ,
                         c(output):= ifelse(get(productivity) != 0, get(input)*get(productivity)/factor, NA)]
            
            
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("3.1","3_base") & isOutlier_productivity == TRUE , 
                         c(productivity):=movav_productivity]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("3.1","3_base") & isOutlier_productivity == TRUE ,
                         c(output):= ifelse(get(productivity) != 0, get(input)*get(productivity)/factor, NA)]
            
            
            
            
            #unflag
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("3.2","3.3","3.1","3_base") ,
                         c(paste0("Case_", triplet$input)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$output))) %in% c("3.2","3.3","3.1","3_base") ,
                         c(paste0("Case_", triplet$output)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$productivity))) %in% c("3.2","3.3","3.1","3_base") ,
                         c(paste0("Case_", triplet$productivity)) := NA_character_ ]
            # 
            # 3 alert
            # 
            
            #if the slaughtered is outlier for cases that have stock and production protected then we enlarge the thresholds, always
            #given that is in the carcass ranges
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "3_alert" & isOutlier_productivity == TRUE, 
                         outlier := ifelse(abs((get(paste0("Value_", triplet$productivity)) / movav_productivity)-1) < 0.25, FALSE, TRUE)]
            #if with the new threshold they are not outlier, unlabel them and discard. they no need check on yieds level since were not touched
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "3_alert" & isOutlier_productivity == TRUE &
                             outlier == FALSE, isOutlier_productivity := FALSE ]
            #unflag
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "3_alert" & isOutlier_productivity == FALSE, 
                         c(paste0("Case_", triplet$input)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$output))) %in% "3_alert" & isOutlier_productivity == FALSE, 
                         c(paste0("Case_", triplet$output)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$productivity))) %in% "3_alert" & isOutlier_productivity == FALSE, 
                         c(paste0("Case_", triplet$productivity)) := NA_character_ ]
            
            #if still outlier apply old mean yield to slaughter
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "3_alert" & isOutlier_productivity == TRUE &
                             outlier == TRUE, c(productivity):=movav_productivity]
            
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "3_alert" & isOutlier_productivity == TRUE &
                             outlier == TRUE, c(input):= ifelse(get(productivity) != 0, get(output)/get(productivity)*factor, NA) ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "3_alert" & isOutlier_productivity == TRUE &
                             outlier == TRUE, carcass_outlier := 
                             ifelse(get(paste0("Value_", triplet$productivity)) < carcass_weight_min & !is.na(carcass_weight_min) |
                                        get(paste0("Value_", triplet$productivity)) > carcass_weight_max & !is.na(carcass_weight_min) , TRUE, FALSE) ]
            
            #check that the new yield is in the carcass range #
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "3_alert" & isOutlier_productivity == TRUE &
                             carcass_outlier == TRUE & get(paste0("Value_", triplet$productivity)) < carcass_weight_min, 
                         c(productivity):= carcass_weight_min]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "3_alert" & isOutlier_productivity == TRUE &
                             carcass_outlier == TRUE & get(paste0("Value_", triplet$productivity)) > carcass_weight_max, 
                         c(productivity):= carcass_weight_max]
            
            #recalculate input given new productivities
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "3_alert" & isOutlier_productivity == TRUE &
                             carcass_outlier == TRUE, 
                         c(input):= ifelse(get(productivity) != 0, get(output)/get(productivity)*factor, NA)]
            
            #unflag the outliers since it was re-calculated
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "3_alert" & isOutlier_productivity == TRUE &
                             carcass_outlier == TRUE, 
                         carcass_outlier := FALSE]
            
            
            #cleaning and preparing dt
            data_triplet[, c("carcass_weight_min","carcass_weight_max","outlier","carcass_outlier","exceptions") := NULL]
            
            #unflag the live animals
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% 
                             c("1_alert","1_base_out","1_base","1_alert_protection","2_base","2_out","3_alert","3.2","3.3","3.1","3_base") & 
                             measuredItemCPC %!in% meat_type$cpc_meat, 
                         c(paste0("Case_", triplet$input)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$output))) %in% 
                             c("1_alert","1_base_out","1_base","1_alert_protection","2_base","2_out","3_alert","3.2","3.3","3.1","3_base") & 
                             measuredItemCPC %!in% meat_type$cpc_meat, 
                         c(paste0("Case_", triplet$output)) := NA_character_ ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$productivity))) %in% 
                             c("1_alert","1_base_out","1_base","1_alert_protection","2_base","2_out","3_alert","3.2","3.3","3.1","3_base") & 
                             measuredItemCPC %!in% meat_type$cpc_meat, 
                         c(paste0("Case_", triplet$productivity)) := NA_character_ ]
            
            #data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% "3", Case := "3"]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("1_alert"), 
                         Case := "1_alert" ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("3_alert"), 
                         Case := "3_alert" ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("2_production_protection"), 
                         Case := "2_production_protection" ]
            
            #"2_production_protection"
            #3_alert cases need to be added
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("1_alert_protection"), 
                         Case := "1_alert_protection" ]
            
            data_triplet[get(as.character(paste0("Case_", triplet$input))) %in% c("2_alert_protection"), 
                         Case := "2_alert_protection" ]
            # 
            # data_triplet[exceptions == "ERROR" , Case := "ERROR"]
            # 
            # data_triplet[exceptions == "ERROR_PRODUCTION" , Case := "ERROR_PRODUCTION"]
            # 
            # data_triplet[,c("proposal_case1","proposal_case2","proposal_case3","proposal_case4", "exceptions",paste0("Case_", triplet$input),
            #                 paste0("Case_", triplet$output),paste0("Case_", triplet$productivity)) := NULL]
            
            data[, Case:= NULL]
            # se è 3 aggiungi colonna alla fine con i numero 3 poi posso eliminare
            #data_triplet[get(paste0("Case_", triplet$input)) %in% "3", Case := "3"]
            #data_triplet[,c(paste0("Case_", triplet$input),paste0("Case_", triplet$output),paste0("Case_", triplet$productivity)):= NULL]
            #quindi nell'if successivo inserisco anche if input di carne! farei prima a dire se crop altrimenti considera case
            ##########################################################################################################################
            ###IMPORTANTE RIMUOVI LE COLONNE DEI CASI!!! PERO DEVO CAPIRE COME DISTINGUERE I CASI 3 da portarmi all ultimo giro!! ####
            ##########################################################################################################################
            
        }
        #qua gestisco l ultimo giro di outlier per i casi = 3
        if (partial == FALSE & last == TRUE & "Case" %in% cols_initial_data_triplet) {
            
            #manage the alert cases for which slaughterd was changed in the meat triplet. need to check them in live animal with larger range
            #take also cases with protected yield. Yiled can be outlier but not flagged as outlier only for the fact that comes from protected input/output
            data_triplet[get(as.character(paste0("Case_", triplet$output))) %in% c("1_alert", "3_alert") & isOutlier_productivity == TRUE |
                             get(as.character(paste0("Case_", triplet$output))) %in% c("1_alert", "3_alert") & get(as.character(paste0("Protected_", triplet$productivity))), 
                         outlier := ifelse(abs((get(paste0("Value_", triplet$productivity)) / movav_productivity)-1) < 0.15, FALSE, TRUE)]
            
            
            
            
            cols_final_dt <- names(data_triplet)
            
            if("outlier" %in% cols_final_dt){
                data_triplet[get(as.character(paste0("Case_", triplet$output))) %in% c("1_alert","3_alert") & outlier == TRUE, 
                             Case := "off_take_alert" ]
                
                
                #data[, outlier:= NULL]
                #unflag other cases
                data_triplet[get(as.character(paste0("Case_", triplet$output))) %in% c("1_alert","3_alert") & outlier != TRUE, 
                             c(paste0("Case_", triplet$output)) := NA_character_ ]
                #data[, Case:= "off_take_alert"]
                
                data_triplet[, outlier:= NULL]
                
                data[, Case:= NULL]
                
            }else{
                
                data[, Case:= NULL]
            }
            
            
            
            
        }
        
        
    }
    
    # Putting the data in the initial format
    #in outlier data also cases are saved in livestock correction round
    cols_data_triplet <- names(data_triplet)
    
    if ("Case" %!in% cols_data_triplet) {
        outlierdata <- data_triplet[, list(geographicAreaM49, timePointYears, measuredItemCPC, isOutlier_productivity)]
    } else {
        outlierdata <- data_triplet[, list(geographicAreaM49, timePointYears, measuredItemCPC, isOutlier_productivity, Case)]
    }
    
    
    
    
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
    #(round(value_new,2) != round(Value,2))
    data[(measuredElement %!in% triplet$productivity) & (Protected == FALSE) & (!is.na(value_new)) & round(value_new) != round(Value) &
             (timePointYears %in% yearVals) ,`:=`(Value = value_new,
                                                  flagObservationStatus = "E",
                                                  flagMethod = "e")]
    
    
    data[, c('value_new', 'difference', 'isOutlier_productivity'):= NULL]
    
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

yield_function <- function(data_table) {
    
    data <- copy(data_table)
    
    datasetConfig <- GetDatasetConfig(domainCode = "agriculture",
                                      datasetCode = "aproduction")
    
    processingParameters <-
        productionProcessingParameters(datasetConfig = datasetConfig)
    
    if(imputation_selection == "LIVESTOCK"){
      sessionItems <- c(intersect(unique(data$measuredItemCPC),meat_type$cpc_meat))
    }else{
      sessionItems <- c(unique(data$measuredItemCPC))
    }
    # sessionItems <-
    #     getQueryKey("measuredItemCPC", session_key)
    
    final_data <- data.table(geographicAreaM49=character(), measuredElement=character(), measuredItemCPC=character(),
                             timePointYears=character(),Value=numeric(),flagObservationStatus=character(),
                             flagMethod=character())
    
    for (iter in seq(sessionItems)) {
        
        imputationProcess <- try({
            
            set.seed(070416)
            
            currentItem <- sessionItems[iter]
            
            print(currentItem)
            
            liveStockItems <-
                getAnimalMeatMapping(R_SWS_SHARE_PATH = R_SWS_SHARE_PATH,
                                     onlyMeatChildren = FALSE)
            
            liveStockItems <- unique(liveStockItems$measuredItemParentCPC)
            #if the code is an animal code: skip the iteration (they have no yield)
            if(currentItem %in%liveStockItems) next 
            ## Obtain the formula and remove indigenous and biological meat.
            
            # suppressMessages({
            #     formulaTable <-
            #         getProductionFormula(itemCode = currentItem) %>%
            #         removeIndigenousBiologicalMeat(formula = .)
            # })
            
            suppressWarnings(formulaTable <- removeIndigenousBiologicalMeat(getProductionFormula(currentItem)))
            
            if (nrow(formulaTable) > 1) {
                stop("Imputation should only use one formula")
            }
            
            ## Create the formula parameter list
            formulaParameters <-
                with(formulaTable,
                     productionFormulaParameters(datasetConfig = datasetConfig,
                                                 productionCode = output,
                                                 areaHarvestedCode = input,
                                                 yieldCode = productivity,
                                                 unitConversion = unitConversion)
                )
            
            
            
            extractedData <-data[measuredItemCPC %in% currentItem,]
            
            extractedData <- extractedData[measuredElement %!in% formulaTable$productivity,]
            
            if (nrow(extractedData) == 0) {
                message("Item : ", currentItem, " does not contain any data")
                next
            }
            
            if (nrow(extractedData[measuredElement %in% formulaTable$productivity & flagObservationStatus %in% "" 
                                   & flagMethod %in% "q",]) > 0) {
                message("Item : ", currentItem, " has a protected productivity value")
                next
            }
            
            processedData <-
                extractedData %>%
                preProcessing(data = .)    
            
            
            processedData <-
                denormalise(
                    normalisedData = processedData,
                    denormaliseKey = "measuredElement",
                    fillEmptyRecords = TRUE
                )
            
            
            
            processedData <-
                createTriplet(
                    data = processedData,
                    formula = formulaTable
                )
            
            
            processedData[
                get(formulaParameters$yieldObservationFlag) == processingParameters$missingValueObservationFlag,
                ":="(
                    c(formulaParameters$yieldMethodFlag),
                    list(processingParameters$missingValueMethodFlag))
                ]
            
            
            if (typeof(processedData[, formulaParameters$yieldValue]) == "character"){
                
                processedData[, formulaParameters$yieldValue := NULL]
                
                processedData[, formulaParameters$yieldValue:= NA_real_]
                
            }
            
            
            computed_Data = computeYield(processedData,
                                         processingParameters = processingParameters,
                                         formulaParameters = formulaParameters)
            
            
            computed_Data <-
                normalise(
                    computed_Data,
                    removeNonExistingRecords = FALSE
                )
            
            final_data <- rbind(final_data,computed_Data[measuredElement %in% formulaTable$productivity,])
            
            #data_to_save <- data_to_save[!is.na(Value),]
            
            #data_to_save[flagObservationStatus %in% "M" & flagMethod %in% "c", flagMethod := "u"]
            
            # SaveData(domain = sessionKey@domain,
            #          dataset = sessionKey@dataset,
            #          data =  data_to_save)
        })
        
    }
    return(final_data)
}
#calculate yield on just updated data from the animals . return data in the same format produced by correct input output function

calculate_yield <- function(data, type = "big"){
    
    main_dataset <- copy(data)
    
    yield_data <- yield_function(main_dataset)
    
    yield_data <- merge(
        yield_data,
        flagValidTable,
        by=c("flagObservationStatus","flagMethod")
    )
    
    yield_data[is.na(Protected), Protected:=FALSE]
    
    yield_data[flagObservationStatus == "E" & flagMethod == "e", Protected:= FALSE]
    yield_data[flagObservationStatus == "T" & flagMethod == "q", Protected:= FALSE]
    
    yield_data[, EF:= ifelse(flagObservationStatus == "E" & flagMethod == "e", TRUE, FALSE)]
    
    yield_data <- merge(yield_data, main_dataset[,c("geographicAreaM49","timePointYears","measuredItemCPC","measuredElement","Case"),with = F], 
                        by = c("geographicAreaM49","timePointYears","measuredItemCPC","measuredElement"), all.x = TRUE)
    
    if(type %in% "big"){
        
        yield_data <- yield_data[measuredItemCPC %in% intersect(big_list, meat_type$cpc_meat),]
        
    }else{
        
        yield_data <- yield_data[measuredItemCPC %in% intersect(small_list, meat_type$cpc_meat),]
        
    }
    
    
    
    final <- rbind(main_dataset[! yield_data , on = c("measuredElement","geographicAreaM49",
                                                      "timePointYears", "measuredItemCPC")], yield_data)
    
    
    return(final)
    
}

offtake_function <- function(data_table) {
    
    data <- copy(data_table)
    
    datasetConfig <- GetDatasetConfig(domainCode = "agriculture",
                                      datasetCode = "aproduction")
    
    processingParameters <-
        productionProcessingParameters(datasetConfig = datasetConfig)
    
    #session items were changed for the livestock correction to not recompute meat yields
    if(imputation_selection == "LIVESTOCK"){
      sessionItems <- c(intersect(unique(data$measuredItemCPC),meat_type$cpc_live_animal))
    }else{
      sessionItems <- c(unique(data$measuredItemCPC))
    }
    # sessionItems <-
    #     getQueryKey("measuredItemCPC", session_key)
    
    final_data <- data.table(geographicAreaM49=character(), measuredElement=character(), measuredItemCPC=character(),
                             timePointYears=character(),Value=numeric(),flagObservationStatus=character(),
                             flagMethod=character())
    
    for (iter in seq(sessionItems)) {
        
        imputationProcess <- try({
            
            set.seed(070416)
            
            currentItem <- sessionItems[iter]
            
            print(currentItem)
            
            
            ## Obtain the formula and remove indigenous and biological meat.
            
            
            suppressWarnings(formulaTable <- removeIndigenousBiologicalMeat(getProductionFormula(currentItem)))
            
            formulaTable$productivity <- "5077"
            
            if (nrow(formulaTable) > 1) {
                stop("Imputation should only use one formula")
            }
            
            ## Create the formula parameter list
            formulaParameters <-
                with(formulaTable,
                     productionFormulaParameters(datasetConfig = datasetConfig,
                                                 productionCode = output,
                                                 areaHarvestedCode = input,
                                                 yieldCode = productivity,
                                                 unitConversion = unitConversion)
                )
            
            #New code for the off take rate. We add it as the productivity element in live animals
            
            
            extractedData <-data[measuredItemCPC %in% currentItem,]
            
            extractedData <- extractedData[measuredElement %!in% formulaTable$productivity,]
            
            if (nrow(extractedData) == 0) {
                message("Item : ", currentItem, " does not contain any data")
                next
            }
            
            if (nrow(extractedData[measuredElement %in% formulaTable$productivity & flagObservationStatus %in% "" 
                                   & flagMethod %in% "q",]) > 0) {
                message("Item : ", currentItem, " has a protected productivity value")
                next
            }
            
            processedData <-
                extractedData %>%
                preProcessing(data = .)    
            
            
            processedData <-
                denormalise(
                    normalisedData = processedData,
                    denormaliseKey = "measuredElement",
                    fillEmptyRecords = TRUE
                )
            
            
            
            processedData <-
                createTriplet(
                    data = processedData,
                    formula = formulaTable
                )
            
            
            processedData[
                get(formulaParameters$yieldObservationFlag) == processingParameters$missingValueObservationFlag,
                ":="(
                    c(formulaParameters$yieldMethodFlag),
                    list(processingParameters$missingValueMethodFlag))
                ]
            
            
            if (typeof(processedData[, formulaParameters$yieldValue]) == "character"){
                
                processedData[, formulaParameters$yieldValue := NULL]
                
                processedData[, formulaParameters$yieldValue:= NA_real_]
                
            }
            
            ## Data quality check
            suppressMessages({
                ensureProductionInputs(processedData,
                                       processingParameters = processingParameters,
                                       formulaParameters = formulaParameters,
                                       returnData = FALSE,
                                       normalised = FALSE)
            })
            
            
            missingYield =
                is.na(processedData[[formulaParameters$yieldValue]])&
                processedData[[formulaParameters$yieldMethodFlag]]!="-"
            nonMissingProduction =
                !is.na(processedData[[formulaParameters$productionValue]]) &
                processedData[[formulaParameters$productionObservationFlag]] != processingParameters$missingValueObservationFlag
            nonMissingAreaHarvested =
                !is.na(processedData[[formulaParameters$areaHarvestedValue]]) &
                processedData[[formulaParameters$areaHarvestedObservationFlag]] != processingParameters$missingValueObservationFlag
            
            feasibleFilter =
                missingYield &
                nonMissingProduction &
                nonMissingAreaHarvested
            
            nonZeroProductionFilter =
                (processedData[[formulaParameters$productionValue]] != 0)
            
            
            #computation of the off take rate
            processedData[feasibleFilter, `:=`(c(formulaParameters$yieldValue),
                                               computeRatio(get(formulaParameters$areaHarvestedValue),
                                                            get(formulaParameters$productionValue)))]
            
            
            
            processedData[feasibleFilter & nonZeroProductionFilter,
                          `:=`(c(formulaParameters$yieldObservationFlag),
                               aggregateObservationFlag(get(formulaParameters$productionObservationFlag),
                                                        get(formulaParameters$areaHarvestedObservationFlag)))]
            
            
            processedData[feasibleFilter & !nonZeroProductionFilter,
                          `:=`(c(formulaParameters$yieldObservationFlag),
                               processingParameters$missingValueObservationFlag)]
            
            processedData[feasibleFilter & !nonZeroProductionFilter,
                          `:=`(c(formulaParameters$yieldMethodFlag),
                               processingParameters$missingValueMethodFlag)]
            
            ## Assign method flag i to that ratio with areaHarvested!=0
            processedData[feasibleFilter & nonZeroProductionFilter,
                          `:=`(c(formulaParameters$yieldMethodFlag),
                               processingParameters$balanceMethodFlag)]
            
            
            ## If  Prod or Area Harvested is (M,-) also yield should be flagged as (M,-)
            
            MdashProduction =  processedData[,get(formulaParameters$productionObservationFlag)==processingParameters$missingValueObservationFlag
                                             & get(formulaParameters$productionMethodFlag)=="-"]
            
            blockFilterProd= MdashProduction & missingYield
            
            processedData[blockFilterProd ,
                          `:=`(c(formulaParameters$yieldValue,formulaParameters$yieldObservationFlag,formulaParameters$yieldMethodFlag),
                               list(NA_real_,processingParameters$missingValueObservationFlag, "-"))]
            
            
            MdashAreaHarvested= processedData[,get(formulaParameters$areaHarvestedObservationFlag)==processingParameters$missingValueObservationFlag
                                              & get(formulaParameters$areaHarvestedMethodFlag)=="-"]
            
            blockFilterAreaHarv= MdashAreaHarvested & missingYield
            
            processedData[blockFilterAreaHarv ,
                          `:=`(c(formulaParameters$yieldValue,formulaParameters$yieldObservationFlag,formulaParameters$yieldMethodFlag),
                               list(NA_real_,processingParameters$missingValueObservationFlag, "-"))]
            
            #Normalize the computed data
            
            computed_Data <-
                normalise(
                    processedData,
                    removeNonExistingRecords = FALSE
                )
            
            final_data <- rbind(final_data,computed_Data[measuredElement %in% formulaTable$productivity,])
            
            #data_to_save <- data_to_save[!is.na(Value),]
            
            #data_to_save[flagObservationStatus %in% "M" & flagMethod %in% "c", flagMethod := "u"]
            
            # SaveData(domain = sessionKey@domain,
            #          dataset = sessionKey@dataset,
            #          data =  data_to_save)
        })
        
    }
    return(final_data)
}

#calculate off take rate and keep the structure of elaborated data in correct input output
#this function is useful to update productivities and its flags given that it could have been modified by the outlier detector
calculate_off_take <- function(data, type = "big"){
    
    main_dataset <- copy(data)
    
    off_take_data <- offtake_function(main_dataset)
    
    off_take_data <- merge(
        off_take_data,
        flagValidTable,
        by=c("flagObservationStatus","flagMethod")
    )
    
    off_take_data[is.na(Protected), Protected:=FALSE]
    
    off_take_data[flagObservationStatus == "E" & flagMethod == "e", Protected:= FALSE]
    off_take_data[flagObservationStatus == "T" & flagMethod == "q", Protected:= FALSE]
    
    off_take_data[, EF:= ifelse(flagObservationStatus == "E" & flagMethod == "e", TRUE, FALSE)]
    
    off_take_data[, measuredElement := "9999"]
    
    off_take_data <- merge(off_take_data, main_dataset[,c("geographicAreaM49","timePointYears","measuredItemCPC","measuredElement","Case"),with = F], 
                           by = c("geographicAreaM49","timePointYears","measuredItemCPC","measuredElement"), all.x = TRUE)
    
    if(type %in% "big"){
        
        off_take_data <- off_take_data[measuredItemCPC %in% intersect(big_list, meat_type$cpc_live_animal),]
        
    }else{
        
        off_take_data <- off_take_data[measuredItemCPC %in% intersect(small_list, meat_type$cpc_live_animal),]
        
    }
    
    
    
    final <- rbind(main_dataset[! off_take_data , on = c("measuredElement","geographicAreaM49",
                                                         "timePointYears", "measuredItemCPC")], off_take_data)
    
    
    return(final)
    
}

send_mail <- function(from = NA, to = NA, subject = NA,
                      body = NA, remove = FALSE) {
    
    if (missing(from)) from <- 'no-reply@fao.org'
    
    if (missing(to)) {
        if (exists('swsContext.userEmail')) {
            to <- swsContext.userEmail
        }
    }
    
    if (is.null(to)) {
        stop('No valid email in `to` parameter.')
    }
    
    if (missing(subject)) stop('Missing `subject`.')
    
    if (missing(body)) stop('Missing `body`.')
    
    if (length(body) > 1) {
        body <-
            sapply(
                body,
                function(x) {
                    if (file.exists(x)) {
                        # https://en.wikipedia.org/wiki/Media_type 
                        file_type <-
                            switch(
                                tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
                                txt  = 'text/plain',
                                csv  = 'text/csv',
                                png  = 'image/png',
                                jpeg = 'image/jpeg',
                                jpg  = 'image/jpeg',
                                gif  = 'image/gif',
                                xls  = 'application/vnd.ms-excel',
                                xlsx = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                doc  = 'application/msword',
                                docx = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                                pdf  = 'application/pdf',
                                zip  = 'application/zip',
                                # https://stackoverflow.com/questions/24725593/mime-type-for-serialized-r-objects
                                rds  = 'application/octet-stream'
                            )
                        
                        if (is.null(file_type)) {
                            stop(paste(tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
                                       'is not a supported file type.'))
                        } else {
                            res <- sendmailR:::.file_attachment(x, basename(x), type = file_type)
                            
                            if (remove == TRUE)    {
                                unlink(x)
                            }
                            
                            return(res)
                        }
                    } else {
                        return(x)
                    }
                }
            )
    } else if (!is.character(body)) {
        stop('`body` should be either a string or a list.')
    }
    
    sendmailR::sendmail(from, to, subject, as.list(body))
}

## END fuction-----



# Read the data needed
flagValidTable <- ReadDatatable("valid_flags")
flagValidTable[is.na(flagObservationStatus), flagObservationStatus := ""]
#let s unprotect imputations
flagValidTable[flagObservationStatus == 'I' & flagMethod == 'c', Protected := FALSE]
#let s protect official yields
flagValidTable[flagObservationStatus == '' & flagMethod == 'i', Protected := TRUE]



if (imputation_selection=="MILK") {
    
    # This chunk of the help us to create an artifical triplet which is actually not exist. We create a triplet with Live Animal, Milk Animal and Yield (9999)
    mapping <- ReadDatatable('animal_milk_correspondence')
    setnames(mapping,c("animal_item_cpc", "milk_item_cpc"), c("measuredItemAnimalCPC", "measuredItemCPC"))
    
    data2 <- merge(data[measuredElement == '5318'], mapping, by='measuredItemCPC')
    data2[, measuredItemCPC:= NULL]
    setnames(data2,"measuredItemAnimalCPC", "measuredItemCPC")
    
    data2<-data2[,colnames(data),with=FALSE]
    
    data<-rbind(data,data2[!data,on=c("geographicAreaM49","timePointYears",
                                      "measuredElement","measuredItemCPC")])
} else {
    
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
    
}


message("Transformation of off-take rates...")

#saving all off take rate to compare later in the function if the productivity has changed
#all_EstOff <- data[measuredElement %in% "5077",]
#all_EstOff <- all_EstOff[, measuredElement := "9999"]
#data changing productivity element to not change all the structure of the functions
data <- data[measuredElement %in% "5077", measuredElement := "9999"]

if(imputation_selection=="MILK") {
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
    
}

#exclude european countries

geographic_table <- ReadDatatable("eurostat_m49")
setnames(geographic_table, c("m49","eurostat"), c("geographicAreaM49","eurostatGeographic"))

eu_countries <- c("AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","EL","HU","IE","IT","LV","LT","LU","MT","NL",
                  "PL","PT","RO","SK","SI","ES","SE")

geographic_table <- geographic_table[eurostatGeographic %in% eu_countries,]

eu_list <- geographic_table[, geographicAreaM49]

data <- data[geographicAreaM49 %!in% eu_list,]
#merging data with flag table

data <- merge(
    data,
    flagValidTable,
    by=c("flagObservationStatus","flagMethod")
)

data[is.na(Protected), Protected:=FALSE]

data[flagObservationStatus == "E" & flagMethod == "e", Protected:= FALSE]
data[flagObservationStatus == "T" & flagMethod == "q", Protected:= FALSE]

data[, EF:= ifelse(flagObservationStatus == "E" & flagMethod == "e", TRUE, FALSE)]

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

if ( sum(unlist(milk_triplet_lst_1) %in% data$measuredElement) == 3) {
    data <- complete_triplet(data,milk_triplet_lst_1)
}

if ( sum(unlist(milk_triplet_lst_2) %in% data$measuredElement) == 3) {
    data <- complete_triplet(data,milk_triplet_lst_2)
}

# data<-correctInputOutput(data,triplet = crop_triplet_lst,partial = FALSE)

message("Correction...")

if (imputation_selection == "CROP") {
    
    data <- correctInputOutput(data, triplet = crop_triplet_lst, partial = FALSE)
    
    cols_data <- names(data)
    
    if ("Case" %in% cols_data) {
        
        incomp_dt <- unique(data[Case %in% c("1_alert"), 
                                 c("geographicAreaM49", "timePointYears", "measuredItemCPC"), with=FALSE])
        
        
    } 
    
} else if(imputation_selection == "LIVESTOCK") {
    
    meat_type <- ReadDatatable("meat")
    #fbsTree[fbsID3 == "2943", get("measuredItemSuaFbs")]
    big_list <- unlist(meat_type[unit %in% "head", list(cpc_live_animal, cpc_meat)], use.names = FALSE)
    
    small_list <- unlist(meat_type[unit %in% "1000 head", list(cpc_live_animal, cpc_meat)], use.names = FALSE)
    
    if (animal_type == "both") {
        # livestocks type 1: stocks in heads
        data <- correctInputOutput(data, triplet = livestock_triplet_lst_1, partial = TRUE, type = "big")
        
        data <- calculate_off_take(data, type = "big")

        data <- update_slaughter(data = data, mappingData = mapping, sendTo = "child", from = "parent")
        
        #recalculate yields that were potentially changed if slaughtered 
        data <- calculate_yield(data, type = "big")
        
        cols_data <- names(data)
        
        if ("Case" %in% cols_data) {
            
            case2_dt <- unique(data[Case %in% "2_out", c("geographicAreaM49", "timePointYears", "measuredItemCPC"), with=FALSE])
            
            #data[Case %in% "2_out", Case := NA_character_]
            
        } 
        
        data <- correctInputOutput(data, triplet = livestock_triplet_lst_2, partial = FALSE, factor = 1000, type = "big")
        
        cols_data <- names(data)
        
        if ("Case" %in% cols_data) {
            
            protection_dt <- unique(data[Case %in% c("1_alert_protection","2_alert_protection","2_production_protection"), 
                                         c("geographicAreaM49", "timePointYears", "measuredItemCPC"), with=FALSE])

            
        } 
        
        #recalculates yield, since we just acted on the meat triplette
        data <- calculate_yield(data, type = "big")
        #funzion deve essere applicata solo ai casi 3 o 4
        data <- update_slaughter(data = data, mappingData = mapping, sendTo = "parent", from = "child")
        #recalculate off take rate, since some slaughtered might show up as outlier in meat trplette.
        data <- calculate_off_take(data, type = "big")
        
        data <- correctInputOutput(data, triplet = livestock_triplet_lst_1, partial = FALSE, last = TRUE, type = "big")
        #non dovrebbe servire piu l'ultimo update di carne. Devo solo fare un ricalcolo finale di yield e off take rate!
        #data <- update_slaughter(data = data, mappingData = mapping, sendTo = "child", from = "parent")
        cols_data <- names(data)
        
        if ("Case" %in% cols_data) {
            
            incomp_dt <- unique(data[Case %in% "off_take_alert", c("geographicAreaM49", "timePointYears", "measuredItemCPC"), with=FALSE])
            
            data[, Case := NULL]
            
        } 
        
        # livestock type 2: stock in 1000 heads
        data <- correctInputOutput(data, triplet = livestock_triplet_lst_1bis, partial = TRUE, type = "small")
        
        data <- calculate_off_take(data, type = "small")
        
        data <- update_slaughter(data = data, mappingData = mapping, sendTo = "child", from = "parent")
        
        data <- calculate_yield(data, type = "small")
        
        cols_data <- names(data)
        
        if ("Case" %in% cols_data) {
            
            case2_dt2 <- unique(data[Case %in% "2_out", c("geographicAreaM49", "timePointYears", "measuredItemCPC"), with=FALSE])
            
           # data[Case %in% "2_out", Case := NA_character_]
            
        } 
        
        data <- correctInputOutput(data, triplet = livestock_triplet_lst_2bis, partial = FALSE, factor = 1000, type = "small")
        
        cols_data <- names(data)
        
        if ("Case" %in% cols_data) {
            
            protection_dt2 <- unique(data[Case %in% c("1_alert_protection", "2_alert_protection","2_production_protection"), 
                                          c("geographicAreaM49", "timePointYears", "measuredItemCPC"), with=FALSE])
            
        } 
        
        data <- calculate_yield(data, type = "small")
        
        data <- update_slaughter(data = data, mappingData = mapping, sendTo = "parent", from = "child")
        
        data <- calculate_off_take(data, type = "small")
        
        data <- correctInputOutput(data, triplet = livestock_triplet_lst_1bis, partial = FALSE, last = TRUE, type = "small")
        
        cols_data <- names(data)
        
        if ("Case" %in% cols_data) {
            
            incomp_dt2 <- unique(data[Case %in% "off_take_alert", c("geographicAreaM49", "timePointYears", "measuredItemCPC"), with=FALSE])
            
            data[, Case := NULL]
            
        } 
        
        #data <- update_slaughter(data = data, mappingData = mapping, sendTo = "child", from = "parent")
        
        #to finally update 1000 heads slaughtered under meats *** but is the productivity then updated?
        #data <- update_slaughter(data = data, mappingData = mapping, sendTo = "child", from = "parent")
    } else if (animal_type == "big_animals"){
        
        # livestocks type 1: stocks in heads
        data <- correctInputOutput(data, triplet = livestock_triplet_lst_1, partial = TRUE, type = "big")
        
        data <- calculate_off_take(data, type = "big")
        
        data <- update_slaughter(data = data, mappingData = mapping, sendTo = "child", from = "parent")
        
        #recalculate yields that were potentially changed if slaughtered 
        data <- calculate_yield(data, type = "big")
        
        data <- correctInputOutput(data, triplet = livestock_triplet_lst_2, partial = FALSE, factor = 1000, type = "big")
        
        cols_data <- names(data)
        
        if ("Case" %in% cols_data) {
            
            protection_dt <- unique(data[Case %in% "1_alert_protection", c("geographicAreaM49", "timePointYears", "measuredItemCPC"), with=FALSE])
            
            
        } 
        
        #recalculates yield, since we just acted on the meat triplette
        data <- calculate_yield(data, type = "big")
        #funzion deve essere applicata solo ai casi 3 o 4
        data <- update_slaughter(data = data, mappingData = mapping, sendTo = "parent", from = "child")
        #recalculate off take rate, since some slaughtered might show up as outlier in meat trplette.
        data <- calculate_off_take(data, type = "big")
        
        data <- correctInputOutput(data, triplet = livestock_triplet_lst_1, partial = FALSE, last = TRUE, type = "big")
        #non dovrebbe servire piu l'ultimo update di carne. Devo solo fare un ricalcolo finale di yield e off take rate!
        #data <- update_slaughter(data = data, mappingData = mapping, sendTo = "child", from = "parent")
        cols_data <- names(data)
        
        if ("Case" %in% cols_data) {
            
            incomp_dt <- unique(data[Case %in% "off_take_alert", c("geographicAreaM49", "timePointYears", "measuredItemCPC"), with=FALSE])
            
            data[, Case := NULL]
            
        } 
        
    } else if (animal_type == "small_animals"){
        
        # livestock type 2: stock in 1000 heads
        data <- correctInputOutput(data, triplet = livestock_triplet_lst_1bis, partial = TRUE, type = "small")
        
        data <- calculate_off_take(data, type = "small")
        
        data <- update_slaughter(data = data, mappingData = mapping, sendTo = "child", from = "parent")
        
        data <- calculate_yield(data, type = "small")
        
        data <- correctInputOutput(data, triplet = livestock_triplet_lst_2bis, partial = FALSE, factor = 1000, type = "small")
        
        cols_data <- names(data)
        
        if ("Case" %in% cols_data) {
            
            protection_dt2 <- unique(data[Case %in% "1_alert_protection", c("geographicAreaM49", "timePointYears", "measuredItemCPC"), with=FALSE])
            
        } 
        
        data <- calculate_yield(data, type = "small")
        
        data <- update_slaughter(data = data, mappingData = mapping, sendTo = "parent", from = "child")
        
        data <- calculate_off_take(data, type = "small")
        
        data <- correctInputOutput(data, triplet = livestock_triplet_lst_1bis, partial = FALSE, last = TRUE, type = "small")
        
        cols_data <- names(data)
        
        if ("Case" %in% cols_data) {
            
            incomp_dt2 <- unique(data[Case %in% "off_take_alert", c("geographicAreaM49", "timePointYears", "measuredItemCPC"), with=FALSE])
            
            data[, Case := NULL]
            
        } 
        

    }
    
} else if (imputation_selection == "MILK"){
    
    data<-correctInputOutput(data,triplet = milk_triplet_lst_1, partial = TRUE)
    
    anomalies <- data[Case %in% c("2_out", "3_out"),]
    
    data <- update_milk(data = data, mappingData = mapping)
    
    data<-correctInputOutput(data,triplet = milk_triplet_lst_2, partial = FALSE, factor = 1000)
    
    
    #check that off take rate with new milking heads is compatible
    data_to_check <- data[measuredItemCPC %in% mapping$measuredItemCPC & measuredElement %in% "5318" & 
                              timePointYears %in% as.character(startYear:endYear) & flagObservationStatus %in% "E" & flagMethod %in% "e",]
    
    
    data_to_check <- data_to_check[, c(names(originalData)), with=FALSE]
    
    data_to_check <- merge(data_to_check, mapping, by = "measuredItemCPC", all.x = TRUE)
    
    stock_data <- originalData[measuredElement %in% "5111", ]
    
    stock_data <- stock_data[, c("geographicAreaM49","measuredItemCPC","timePointYears","Value"), with = FALSE]
    
    setnames(stock_data, "Value", "stockValue")
    
    data_to_check <- merge(data_to_check, stock_data, by.x = c("geographicAreaM49","measuredItemAnimalCPC","timePointYears"),
                           by.y = c("geographicAreaM49","measuredItemCPC","timePointYears"), all.x = TRUE)
    
    data_to_check[, off_take := Value/stockValue]
    
    data_to_check <- data_to_check[off_take > 1, ]
    
    incomp_dt <- data_to_check[, c("geographicAreaM49","measuredItemAnimalCPC","timePointYears","measuredItemCPC","measuredElement"), with = FALSE]
    #missing last check on milk off take rates < 1. If milking heads were mmodified need to be labeled and checked
    
}else if (imputation_selection == "EGG"){
    #other birds eggs are done separately
    data <- data[measuredItemCPC %in% "0231",]
    
    data <- correctInputOutput(data, triplet = egg_triplet_lst, partial = FALSE, factor = 1000)
    
    cols_data <- names(data)
    
    if ("Case" %in% cols_data) {
        
        incomp_dt <- unique(data[Case %in% c("1_out","2_out","3_alert"), 
                                 c("geographicAreaM49", "timePointYears", "measuredItemCPC"), with=FALSE])

        
    } 
    
    #update flag of both output if one of the two was protected +  SAME RULE FOR 3_OUT_OUTPUT, as 2.2
    
    data[Case %in% "2.1", flagObs_5513 := flagObservationStatus[measuredElement %in% "5513"], 
         by = c("geographicAreaM49","measuredItemCPC", "timePointYears")]
    
    data[Case %in% "2.1" & measuredElement %in% "5510", `:=` (flagObservationStatus = flagObs_5513, flagMethod = "i")]
    
    data[Case %in% "2.1" & measuredElement %in% "5510" & flagObs_5513 == "",  flagObservationStatus := "T"]
    
    
    data[Case %in% c("2.2", "3_out_output"), flagObs_5510 := flagObservationStatus[measuredElement %in% "5510"], 
         by = c("geographicAreaM49","measuredItemCPC", "timePointYears")]
    
    data[Case %in% c("2.2", "3_out_output") & measuredElement %in% "5513", `:=` (flagObservationStatus = flagObs_5510, flagMethod = "i")]
    
    data[Case %in% c("2.2", "3_out_output") & measuredElement %in% "5513" & flagObs_5513 == "",  flagObservationStatus := "T"]
    
    data[, c("flagObs_5513", "flagObs_5510") := NULL]
    

    
}

if (imputation_selection=="CROP"){
    
    dataf <- copy(data)
    dataf <- dataf[!is.na(Value)]
    dataf <- dataf[, c(names(originalData)), with=FALSE]

    new_yields <- yield_function(dataf)
    
    dataf <- rbind(dataf[! new_yields , on = c("measuredElement","geographicAreaM49",
                                               "timePointYears", "measuredItemCPC")], new_yields)
    
    data_to_save <- dataf[!(flagObservationStatus %in% "M" & flagMethod %in% "u"),]
    
    data_to_save <- data_to_save[timePointYears %in% yearVals,]
    
    data_to_save <- merge(data_to_save, originalData[,c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears","Value"),with = F], 
                          by = c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears"),
                          all.x = TRUE)
    
    
    data_to_save[, keep := ifelse(measuredElement%in%"5421" & round(Value.x,3) == round(Value.y,3),FALSE, TRUE)]
    
    data_to_save <- data_to_save[keep==TRUE,]
    
    data_to_save[, c("keep", "Value.y"):= NULL]
    
    setnames(data_to_save, "Value.x", "Value")
    
   
    
}else if (imputation_selection=="MILK"){
    
    dataf <- copy(data)
    dataf <- dataf[!is.na(Value)]
    dataf <- dataf[, c(names(originalData)), with=FALSE]
    
    dataf <- dataf[measuredItemCPC %in% mapping$measuredItemCPC,]

    
    new_yields <- yield_function(dataf)
    
    dataf <- rbind(dataf[! new_yields , on = c("measuredElement","geographicAreaM49",
                                             "timePointYears", "measuredItemCPC")], new_yields)
    
    data_to_save <- dataf[!(flagObservationStatus %in% "M" & flagMethod %in% "u"),]
    
    data_to_save <- data_to_save[timePointYears %in% yearVals,]
    
    data_to_save <- merge(data_to_save, originalData[,c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears","Value"),with = F], 
                          by = c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears"),
                          all.x = TRUE)
    
    
    data_to_save[, keep := ifelse(measuredElement%in%"5417" & round(Value.x,3) == round(Value.y,3),FALSE, TRUE)]
    
    data_to_save <- data_to_save[keep==TRUE,]
    
    data_to_save[, c("keep", "Value.y"):= NULL]
    
    setnames(data_to_save, "Value.x", "Value")
   
    
} else if(imputation_selection == "LIVESTOCK"){
    #dataf <- data[measuredElement %!in% "9999",]
    dataf <- copy(data)
    dataf <- dataf[!is.na(Value)]
    dataf <- dataf[, c(names(originalData)), with=FALSE]
    
    data_to_save <- dataf[measuredElement %in% "9999", measuredElement := "5077"]
    
    data_to_save <- data_to_save[!(flagObservationStatus %in% "M" & flagMethod %in% "u"),]
    
    data_to_save <- data_to_save[timePointYears %in% yearVals,]
    
    #remove qty if thet are rounded the same of what already in sws
    
    data_to_save <- merge(data_to_save, originalData[,c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears","Value"),with = F], 
                          by = c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears"),
                          all.x = TRUE)
    
    
    data_to_save[, keep := ifelse(measuredElement%in%"5077" & round(Value.x,3) == round(Value.y,3),FALSE, TRUE)]
    #check off take rates che hanno NA
    data_to_save <- data_to_save[keep==TRUE,]
    
    data_to_save[, c("keep", "Value.y"):= NULL]
    
    setnames(data_to_save, "Value.x", "Value")
    
}else if(imputation_selection == "EGG"){
    
    dataf <- copy(data)
    dataf <- dataf[!is.na(Value)]
    dataf <- dataf[, c(names(originalData)), with=FALSE]

    
    new_yields <- yield_function(dataf)
    new_yields <- new_yields[!is.na(Value)]
    
    dataf <- rbind(dataf[! new_yields , on = c("measuredElement","geographicAreaM49",
                                               "timePointYears", "measuredItemCPC")], new_yields)
    
    data_to_save <- dataf[!(flagObservationStatus %in% "M" & flagMethod %in% "u"),]
    
    data_to_save <- data_to_save[timePointYears %in% yearVals,]
    
    data_to_save <- merge(data_to_save, originalData[,c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears","Value"),with = F], 
                          by = c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears"),
                          all.x = TRUE)
    
    
    data_to_save[, keep := ifelse(measuredElement%in%"5424" & round(Value.x,3) == round(Value.y,3),FALSE, TRUE)]
    
    data_to_save <- data_to_save[keep==TRUE,]
    
    data_to_save[, c("keep", "Value.y"):= NULL]
    
    setnames(data_to_save, "Value.x", "Value")
}



SaveData(domain = datasetConfig$domain,
         dataset = datasetConfig$dataset,
         data = data_to_save, waitTimeout = 2000000)


# Send the productivity outliers (plus an extra csv file which contains productivity > 1) to the user 
if(imputation_selection == "LIVESTOCK"){
    
    incomp_dt_final <- rbind(incomp_dt,incomp_dt2)
    
    protection_dt_final <- rbind(protection_dt,protection_dt2)
    
    case2_dt_final <- rbind(case2_dt, case2_dt2)
    
    
    if(exists("incomp_dt_final")){
        incomp_dt_final <- nameData("aproduction", "aproduction", incomp_dt_final)
    } 
    
    if(exists("protection_dt_final")){
        protection_dt_final <- nameData("aproduction", "aproduction", protection_dt_final)
    }
    
    if(exists("case2_dt_final")){
        case2_dt_final <- nameData("aproduction", "aproduction", case2_dt_final)
        
        case2_dt_final[, Value:= "X"]
        
        d_cast_cas2 <- dcast(case2_dt_final, geographicAreaM49 + geographicAreaM49_description + measuredItemCPC + measuredItemCPC_description ~ timePointYears, 
                             value.var = c("Value"))
    }
    




}else{
    
    if(exists("incomp_dt")){
        incomp_dt_final <- nameData("aproduction", "aproduction", incomp_dt)
    } 
    
}




wb <- createWorkbook(USER)

if(exists("incomp_dt_final")){
    
    if(nrow(incomp_dt_final) != 0){
    
    addWorksheet(wb, "Anomalies")
    writeDataTable(wb, "Anomalies",incomp_dt_final)
    
    }
    
}

if(exists("protection_dt_final")){
    
    if(nrow(protection_dt_final) != 0){
    
    addWorksheet(wb, "Outlier_Protection")
    writeDataTable(wb, "Outlier_Protection",protection_dt_final)
    
    }
    
}

if(exists("d_cast_cas2")){
    
    if(nrow(d_cast_cas2) != 0){
    
    addWorksheet(wb, "Livestocks case 2")
    writeDataTable(wb, "Livestocks case 2",d_cast_cas2)
    
    }
    
}




library(devtools)
Sys.setenv(PATH = paste("C:/Rtools/bin", Sys.getenv("PATH"), sep=";"))
Sys.setenv(BINPREF = "C:/Rtools/mingw_$(WIN)/bin/")

saveWorkbook(wb, tmp_file_incomp, overwrite = TRUE)



body_message = paste("Plugin completed. Production outliers anomalies file attached.
                     ",
                     sep='\n')

send_mail(from = "no-reply@fao.org", 
          to = swsContext.userEmail,
          subject = paste0("Production outliers anomalies result"), 
          body = c(body_message, tmp_file_incomp))

unlink(TMP_DIR, recursive = TRUE)

print('Plug-in Completed')