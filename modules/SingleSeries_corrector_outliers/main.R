##' 
##'
##' **Author: Livia Lombardi**
##' 
##' 
##' **Description:**
##'
##' This module is designed to identify and automatically correct the outliers in the production environement.
##' It can detect any triplets (so called productivity, input and output) in single triplet for livestock and not livestock
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

single_triplet_lst_1 <- list(input="5510", output="0000", productivity="9999") 
single_triplet_lst_2  <- list(input="5114", output="0000", productivity="5424") 


# TMP file for anomalies
USER <- regmatches(
    swsContext.username,
    regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
)

if (imputation_selection == "LIVESTOCK") {
    
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
    
    
    
    if(imputation_selection == "LIVESTOCK" | imputation_selection == "NOT_LIVESTOCK"){
        
        data_label[, c(outlier):= FALSE]
        #if element <- è productivity ... if 9999 (then is an off take rate) then theshold is 0.1
        # if any other than 9999 (it means that is a yield) then is 0.2
        # then apply rules for other elements based on magnitude of the numbers
        if(element %!in% unique(c(livestock_triplet_lst_1$productivity,
                                  livestock_triplet_lst_1bis$productivity, 
                                  livestock_triplet_lst_2$productivity,
                                  livestock_triplet_lst_2bis$productivity,
                                  single_triplet_lst_1,
                                  single_triplet_lst_2))){
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
            
        }else if(element %in% unique(c(livestock_triplet_lst_2$productivity,
                                       livestock_triplet_lst_2bis$productivity))){
            
            data_label[, c(outlier):= ifelse(abs(ratio-1) > 0.2 & 
                                                 get(Protected) == FALSE, TRUE, FALSE) &
                           timePointYears %in% yearVals]
            
            
        }else if(element %in% unique(c(livestock_triplet_lst_1$productivity,livestock_triplet_lst_1bis$productivity))){
            
            data_label[, c(outlier):= ifelse(abs(ratio-1) > 0.1 & 
                                                 get(Protected) == FALSE, TRUE, FALSE) &
                           timePointYears %in% yearVals]
        }
        
        
    }
    
    data_label[,centralCondition := copy(get(outlier))]
    
    data_label[,variableProtection := copy(get(Protected))]
    
    
    
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
    
    
    data_label[,`:=`(ratio = NULL, centralCondition = NULL, variableProtection = NULL)]
    
    return(data_label)
}
# imput_with_average() function finds the outliers the imputation for given elements (in this case only for Productivity)
# no more used: TO CLEAN
#data1 <- copy(data)
correctSingleTriplet <- function(data = data,
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
        
        
        #filter only data with element in the current triplet
        if(type == "big"){
            data_triplet <- data[measuredElement %in% triplet & measuredItemCPC %in% big_list]
        } else {
            data_triplet <- data[measuredElement %in% triplet & measuredItemCPC %in% small_list]
        }
        
        # data_triplet<-imput_with_average(data_triplet,triplet$productivity)
        #take the cpc list of items that have input 
        inteminput <- data_triplet[measuredElement %in% c(triplet$input, triplet$output),get("measuredItemCPC")]
        
        data_triplet <- data_triplet[measuredItemCPC %in% unique(inteminput)]
        
        
        #First condition: if meat round accept the new column Case created in the previous animal detection round
        cols_initial_data_triplet <- names(data_triplet)
        
        
        data_triplet <- dcast.data.table(data_triplet, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value', 'Protected', 'EF'))
        
        
        #take only single series
        
        data_single <- data_triplet[is.na(get(paste0("Value_", triplet$input))) & !is.na(get(paste0("Value_", triplet$output))) 
                                    & timePointYears %in% as.character(startYear:endYear) |
                                        !is.na(get(paste0("Value_", triplet$input))) & is.na(get(paste0("Value_", triplet$output))) 
                                    & timePointYears %in% as.character(startYear:endYear),
                                    ]
        
        
        #combination_of_interest <- unique(data_single[, .(geographicAreaM49,measuredItemCPC)])
        
        combination_of_interest <- unique(data_single[,c("geographicAreaM49","measuredItemCPC"), with = FALSE])
        
        combination_of_interest[, Single := TRUE]
        
        data_triplet <- merge(data_triplet, combination_of_interest, by = c("geographicAreaM49","measuredItemCPC"), all.x = TRUE)
        
        data_triplet <- data_triplet[Single == TRUE,]
        
        data_triplet[, Single := NULL]
        
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
        
    }else if(imputation_selection == "NOT_LIVESTOCK"){
        
        if(partial == FALSE){
            data_triplet <- data[measuredElement %in% triplet$input & measuredItemCPC %in% c("02910","02920","02960.01","21119.90","21170.02","02941")]
        } else {
            data_triplet <- data[measuredElement %in% triplet$input & measuredItemCPC %in% "02196"]
        }
        
        # data_triplet<-imput_with_average(data_triplet,triplet$productivity)
        #take the cpc list of items that have input 
        inteminput <- data_triplet[measuredElement %in% triplet$input,get("measuredItemCPC")]
        
        data_triplet <- data_triplet[measuredItemCPC %in% unique(inteminput)]
        
        
        #First condition: if meat round accept the new column Case created in the previous animal detection round
        cols_initial_data_triplet <- names(data_triplet)
        
        
        data_triplet <- dcast.data.table(data_triplet, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value', 'Protected', 'EF'))
        
        
        Label_outlier(data_label = data_triplet, element = triplet$input, type = "input")
        compute_movav(data = data_triplet, element = triplet$input, type = "input")
        
        
        
        input <- paste0("Value_", triplet$input)
        
        
        ef_input <- paste0("EF_", triplet$input)
        
        
        data_triplet[get(ef_input) == TRUE, isOutlier_input:= TRUE]
        
        
    }
    
    
    
    if (imputation_selection == "LIVESTOCK") {
        # remove both zero cases
        
        data_triplet[, check := FALSE]
        
        data_triplet[get(paste0("Value_", triplet$input)) == 0 & get(paste0("Value_", triplet$output)) == 0, check := TRUE]
        
        data_triplet <- data_triplet[check == FALSE,]
        
        
        ##' divide series with input from the one with output
        data_triplet_input <- data_triplet[get(paste0("Value_", triplet$input)) != 0,]
        
        data_triplet_output <- data_triplet[get(paste0("Value_", triplet$output)) != 0,]
        
        
        
        #check all imputed saries to correct tails
        
        if(triplet$input == "5111"){
            
            data_triplet_input[, tailCheck := ifelse(sum(Protected_5111[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                               by = c("measuredItemCPC", "geographicAreaM49")]
            
            data_triplet_output[, tailCheck := ifelse(sum(Protected_5315[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                                by = c("measuredItemCPC", "geographicAreaM49")]
            
        }else if(triplet$input == "5112"){
            
            data_triplet_input[, tailCheck := ifelse(sum(Protected_5112[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                               by = c("measuredItemCPC", "geographicAreaM49")]
            
            data_triplet_output[, tailCheck := ifelse(sum(Protected_5316[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                                by = c("measuredItemCPC", "geographicAreaM49")]
            
        }else if(triplet$input == "5320"){
            
            data_triplet_input[, tailCheck := ifelse(sum(Protected_5320[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                               by = c("measuredItemCPC", "geographicAreaM49")]
            
            data_triplet_output[, tailCheck := ifelse(sum(Protected_5510[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                                by = c("measuredItemCPC", "geographicAreaM49")]
            
            
        }else if(triplet$input == "5321"){
            
            data_triplet_input[, tailCheck := ifelse(sum(Protected_5321[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                               by = c("measuredItemCPC", "geographicAreaM49")]
            
            data_triplet_output[, tailCheck := ifelse(sum(Protected_5510[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                                by = c("measuredItemCPC", "geographicAreaM49")]
            
            
        }    
        
        
        data_triplet_input[tailCheck == TRUE,
                           tailOutlier_input := ifelse(abs((get(paste0("Value_", triplet$input)) / movav_input)-1) < 0.05, 
                                                       FALSE, TRUE)]  
        
        
        
        data_triplet_input<-
            data_triplet_input[
                order(geographicAreaM49,measuredItemCPC, timePointYears),
                tailMovag_input := roll_meanr(get(paste0("Value_", triplet$input)),  5),
                by = c("geographicAreaM49","measuredItemCPC")
                ]
        
        
        cols_triplet_input <- names(data_triplet_input)
        
        if("tailOutlier_input" %!in% cols_triplet_input){
            data_triplet_input[,tailOutlier_input := FALSE]
        }
        #correct input as outlier with tails
        data_triplet_input[tailCheck == TRUE & tailOutlier_input == TRUE,
                           c(input):= tailMovag_input]
        
        #correct input as outliers not tails cases
        data_triplet_input[tailCheck == FALSE & isOutlier_input == TRUE,
                           c(input):= movav_input]
        
        
        # doing the same for output
        
        
        data_triplet_output[tailCheck == TRUE,
                            tailOutlier_output := ifelse(abs((get(paste0("Value_", triplet$output)) / movav_output)-1) < 0.05, 
                                                         FALSE, TRUE)]    
        
        
        data_triplet_output <-
            data_triplet_output[
                order(geographicAreaM49,measuredItemCPC, timePointYears),
                tailMovag_output := roll_meanr(get(paste0("Value_", triplet$output)), 5),
                by = c("geographicAreaM49","measuredItemCPC")
                ]
        
        
        cols_triplet_output <- names(data_triplet_output)
        
        if("tailOutlier_output" %!in% cols_triplet_output){
            data_triplet_output[,tailOutlier_output := FALSE]
        }
        #correct input as outlier with tails
        data_triplet_output[tailCheck == TRUE & tailOutlier_output == TRUE,
                            c(output):= tailMovag_output]
        
        #correct input as outliers not tails cases
        data_triplet_output[tailCheck == FALSE & isOutlier_output == TRUE,
                            c(output):= movav_output]
        
        
        
        data_triplet_input[, c("check","tailCheck","tailOutlier_input","tailMovag_input") := NULL]
        
        data_triplet_output[, c("check","tailCheck","tailOutlier_output","tailMovag_output") := NULL]
        
        data_triplet <- rbind(data_triplet_input, data_triplet_output)
        
    } else if( imputation_selection == "NOT_LIVESTOCK"){
        
        if(triplet$input == "5510"){
            
            data_triplet[, tailCheck := ifelse(sum(Protected_5510[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                         by = c("measuredItemCPC", "geographicAreaM49")]
            
            
        }else if(triplet$input == "5114"){
            
            data_triplet[, tailCheck := ifelse(sum(Protected_5114[timePointYears %in% as.character(startYear:endYear)]) == 0 , TRUE, FALSE),
                         by = c("measuredItemCPC", "geographicAreaM49")]
            
            
        }
        
        
        
        data_triplet[tailCheck == TRUE,
                     tailOutlier_input := ifelse(abs((get(paste0("Value_", triplet$input)) / movav_input)-1) < 0.05, 
                                                 FALSE, TRUE)]  
        
        
        
        data_triplet<-
            data_triplet[
                order(geographicAreaM49,measuredItemCPC, timePointYears),
                tailMovag_input := roll_meanr(get(paste0("Value_", triplet$input)),  5),
                by = c("geographicAreaM49","measuredItemCPC")
                ]
        
        
        cols_triplet <- names(data_triplet)
        
        if("tailOutlier_input" %!in% cols_triplet){
            data_triplet[,tailOutlier_input := FALSE]
        }
        #correct input as outlier with tails
        data_triplet[tailCheck == TRUE & tailOutlier_input == TRUE,
                     c(input):= tailMovag_input]
        
        #correct input as outliers not tails cases
        data_triplet[tailCheck == FALSE & isOutlier_input == TRUE,
                     c(input):= movav_input]
        
        
        
        
        data_triplet[, c("tailCheck","tailOutlier_input","tailMovag_input") := NULL]
        
        
        
    }
    
    # Putting the data in the initial format
    #in outlier data also cases are saved in livestock correction round
    cols_data_triplet <- names(data_triplet)
    
    if(imputation_selection == "LIVESTOCK"){
        outlierdata <- data_triplet[, list(geographicAreaM49, timePointYears, measuredItemCPC,isOutlier_productivity)]
    }else{
        outlierdata <- data_triplet[, list(geographicAreaM49, timePointYears, measuredItemCPC)] 
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
    
    if(imputation_selection == "LIVESTOCK"){
        
        data[is.na(isOutlier_productivity), isOutlier_productivity:= FALSE]
        #(round(value_new,2) != round(Value,2))
        data[(measuredElement %!in% triplet$productivity) & (Protected == FALSE) & (!is.na(value_new)) & round(value_new) != round(Value) &
                 (timePointYears %in% yearVals) ,`:=`(Value = value_new,
                                                      flagObservationStatus = "E",
                                                      flagMethod = "e")]
        
        
        data[, c('value_new', 'difference', 'isOutlier_productivity'):= NULL]
        
    }else{
        
        #(round(value_new,2) != round(Value,2))
        data[(Protected == FALSE) & (!is.na(value_new)) & round(value_new) != round(Value) &
                 (timePointYears %in% yearVals) ,`:=`(Value = value_new,
                                                      flagObservationStatus = "E",
                                                      flagMethod = "e")]
        
        
        data[, c('value_new', 'difference'):= NULL]
        
    }
    
    return(data)
    
}

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



if (imputation_selection=="LIVESTOCK") {
    
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
    
    message("Transformation of off-take rates...")
    
    data <- data[measuredElement %in% "5077", measuredElement := "9999"]
  
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

# 
# 
# if ( sum(unlist(livestock_triplet_lst_1) %in% data$measuredElement) == 3) {
#     data <- complete_triplet(data,livestock_triplet_lst_1)
# }
# 
# if ( sum(unlist(livestock_triplet_lst_2) %in% data$measuredElement) == 3) {
#     data <- complete_triplet(data,livestock_triplet_lst_2)
# }
# 
# if ( sum(unlist(livestock_triplet_lst_1bis)%in% data$measuredElement) == 3) {
#     data <- complete_triplet(data,livestock_triplet_lst_1bis)
# }
# 
# if ( sum(unlist(livestock_triplet_lst_2bis)%in% data$measuredElement) == 3) {
#     data <- complete_triplet(data, livestock_triplet_lst_2bis)
# }



# data<-correctInputOutput(data,triplet = crop_triplet_lst,partial = FALSE)

message("Correction...")

if(imputation_selection == "LIVESTOCK") {
    
    meat_type <- ReadDatatable("meat")
    #fbsTree[fbsID3 == "2943", get("measuredItemSuaFbs")]
    big_list <- unlist(meat_type[unit %in% "head", list(cpc_live_animal, cpc_meat)], use.names = FALSE)
    
    small_list <- unlist(meat_type[unit %in% "1000 head", list(cpc_live_animal, cpc_meat)], use.names = FALSE)
    
    if (animal_type == "both") {
        # livestocks type 1: stocks in heads
        data <- correctSingleTriplet(data, triplet = livestock_triplet_lst_1,  type = "big")
        
        data <- correctSingleTriplet(data, triplet = livestock_triplet_lst_2,  factor = 1000, type = "big")
        
        
        # livestock type 2: stock in 1000 heads
        data <- correctSingleTriplet(data, triplet = livestock_triplet_lst_1bis,  type = "small")
        
      
        data <- correctSingleTriplet(data, triplet = livestock_triplet_lst_2bis,  factor = 1000, type = "small")
        
      
    }
    
} else if (imputation_selection == "NOT_LIVESTOCK"){
    
    data <- data[measuredElement %in% c("5510","5114"),]
    
    data <- correctSingleTriplet(data, triplet = single_triplet_lst_1, partial = FALSE)
    
    data <- correctSingleTriplet(data, triplet = single_triplet_lst_2, partial = TRUE)
}


if(imputation_selection == "LIVESTOCK"){
    #dataf <- data[measuredElement %!in% "9999",]
    dataf <- copy(data)
    dataf <- dataf[!is.na(Value)]
    dataf <- dataf[, c(names(originalData)), with=FALSE]
    
   
    
    data_to_save <- dataf[(flagObservationStatus %in% "E" & flagMethod %in% "e"),]
    
    data_to_save <- data_to_save[timePointYears %in% yearVals,]
    
    #remove qty if thet are rounded the same of what already in sws
   
    
}else if(imputation_selection == "NOT_LIVESTOCK"){
    
    dataf <- copy(data)
    dataf <- dataf[!is.na(Value)]
    dataf <- dataf[, c(names(originalData)), with=FALSE]
    
    data_to_save <- dataf[(flagObservationStatus %in% "E" & flagMethod %in% "e"),]
    
    data_to_save <- data_to_save[timePointYears %in% yearVals,]
}



SaveData(domain = datasetConfig$domain,
         dataset = datasetConfig$dataset,
         data = data_to_save, waitTimeout = 2000000)



print('Plug-in Completed')