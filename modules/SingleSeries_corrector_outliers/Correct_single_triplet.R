# Correcting outliers
# Parametrize input var, output var and productivity var
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
    data[flagObservationStatus %in% c("T", "X") & flagMethod %in% "i", Protected := TRUE]
    
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