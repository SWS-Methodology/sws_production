#'computeTotStock
#'
#'
#' This function has been created to compute the totStock that has to be used as
#' reference to compue the take off rates and consequently the number of slaughtered  animals.
#' In order to compute the total animal stocks, trade has to be taken into account
#' 
#' @param data datataset containing the the numeber of animal stocks (trade not included)
#' @return Returns a dataset with the the tot stock 
#' 
#' @export

computeTotSlaughtered = function(data, FormulaParameters=animalFormulaParameters, ...){

    # List additional arguments
    a <- list(...)

    ## Data quality checks    
    ## 1) Check if the columns are all there
    ## 2) This function works only if data are dernormalized
    
    flagValidTable1=copy(flagValidTable)
    flagValidTable1[flagObservationStatus=="I" & flagMethod=="c", Protected:=FALSE]
    flagValidTable1[flagObservationStatus=="E" & flagMethod=="c", Protected:=FALSE]
    flagValidTable1[flagObservationStatus=="E" & flagMethod=="h", Protected:=FALSE]
   
    
    ## Before computing the takeOffRate, replace the non -protected slaughtered animals value with NA,M,u
    data=removeNonProtectedFlag(data, valueVar =animalFormulaParameters$areaHarvestedValue,
                                observationFlagVar= animalFormulaParameters$areaHarvestedObservationFlag,
                                methodFlagVar=animalFormulaParameters$areaHarvestedMethodFlag,
                                flagValidTable = flagValidTable1)
        
    data[,takeOffRate:= get(animalFormulaParameters$areaHarvestedValue)/get(animalFormulaParameters$productionValue)]
    
    data[takeOffRate==Inf,takeOffRate:=NA]
    
    ##Associate a Flat to the new variable "TakeOffRate"
    data[,TakeOffFlagObservationStatus:="M"]
    ##All those OffTake rate that are not NA should have the observation flag resulting from the aggregation of the two variables used to compute it: 
    ##animalStock and slaughteredAnimal.
    
    data[!is.na(takeOffRate)& 
             !is.na(get(animalFormulaParameters$areaHarvestedObservationFlag)) &
             !is.na(get(animalFormulaParameters$productionObservationFlag)),
         TakeOffFlagObservationStatus:=aggregateObservationFlag(get(animalFormulaParameters$areaHarvestedObservationFlag),
                                                                get(animalFormulaParameters$productionObservationFlag))]
    
    data[,TakeOffRateFlagMethod:="u"]
    ## I chose the "c" method flag just to "protect" the computed Off take rate
    data[!is.na(takeOffRate),TakeOffRateFlagMethod:="c"]
    
    
    
    
    ## Impute the missing takeOffRates
    
    takeOffImputationParamenters=defaultImputationParameters()
    takeOffImputationParamenters$imputationValueColumn="takeOffRate"
    takeOffImputationParamenters$imputationFlagColumn="TakeOffFlagObservationStatus"
    takeOffImputationParamenters$imputationMethodColumn="TakeOffRateFlagMethod"
    takeOffImputationParamenters$byKey=c("geographicAreaM49","measuredItemCPC")
    takeOffImputationParamenters$estimateNoData=FALSE
    
    takeOffImputed=data[,.(geographicAreaM49, measuredItemCPC, timePointYears,takeOffRate, TakeOffFlagObservationStatus, TakeOffRateFlagMethod)]
    
    takeOffImputed=removeNoInfo(takeOffImputed,"takeOffRate", "TakeOffFlagObservationStatus",
                                byKey = c("geographicAreaM49", "measuredItemCPC" ))
    
    # It was decided on November 2019 that the ensemble approach for the
    # offtake rate should be killed, favouring the average of previous
    # five years. The previous code is commented with #@#

    takeOffImputed[,
      movav :=
        rollavg(
          get(takeOffImputationParamenters$imputationValueColumn),
          order = 3
        ),
        by = c(takeOffImputationParamenters$byKey)
    ]

    takeOffImputed[
      is.na(takeOffRate) & !is.na(movav),
      `:=`(
        c("takeOffRate",
        takeOffImputationParamenters$imputationFlagColumn,
        takeOffImputationParamenters$imputationMethodColumn),
        # Flags are Ee to differentiate them from Ie
        list(movav, "E", "e")
      )
    ]

    takeOffImputed[, movav := NULL]
    
    #@# takeOffImputed <-
    #@#   imputeVariable(
    #@#     takeOffImputed,
    #@#     imputationParameters = takeOffImputationParamenters
    #@#   )
    #@# 
    #@# if (!is.null(a$FIX_OUTLIERS) && a$FIX_OUTLIERS == TRUE) {

    #@#   orig_cols <- names(takeOffImputed)

    #@#   if (a$imputationTimeWindow == "all") {
    #@#     val_years <- a$completeImputationKey@dimensions$timePointYears@keys
    #@#   } else {
    #@#     val_years <-
    #@#       lastYear - 0:switch(a$imputationTimeWindow, "lastThree" = 2, "lastFive" = 4)
    #@#   }

    #@#   # These will be used as Protected = FALSE.
    #@#   # TODO: parameterise
    #@#   checkOut <-
    #@#     data[
    #@#       timePointYears %in% val_years & is.na(Value_measuredElement_5315),
    #@#       .(geographicAreaM49, measuredItemCPC, timePointYears, Protected = FALSE)
    #@#     ]

    #@#   takeOffImputed <-
    #@#     merge(
    #@#       takeOffImputed,
    #@#       checkOut,
    #@#       by = c("geographicAreaM49", "measuredItemCPC", "timePointYears"),
    #@#       all.x = TRUE
    #@#     )

    #@#   takeOffImputed[is.na(Protected), Protected := TRUE]

    #@#   takeOffImputed[,
    #@#     `:=`(
    #@#       mean_old =
    #@#         mean(
    #@#           get(takeOffImputationParamenters$imputationValueColumn)[
    #@#             get(takeOffImputationParamenters$yearValue) %in% a$AVG_YEARS
    #@#           ]
    #@#         )
    #@#     ),
    #@#     by = c(takeOffImputationParamenters$byKey)
    #@#   ]

    #@#   takeOffImputed[is.nan(mean_old), mean_old := NA_real_]

    #@#   # NOTE: some NA ratios are due to M- series
    #@#   takeOffImputed[,
    #@#     `:=`(
    #@#       ratio = get(takeOffImputationParamenters$imputationValueColumn) / mean_old
    #@#     )
    #@#   ]

    #@#   takeOffImputed[is.infinite(ratio), ratio := NA_real_]

    #@#   takeOffImputed[, outlier := FALSE]

    #@#   takeOffImputed[
    #@#     get(takeOffImputationParamenters$yearValue) %in% val_years &
    #@#       Protected == FALSE,
    #@#     outlier := abs(ratio - 1) > a$THRESHOLD
    #@#   ]

    #@#   takeOffImputed[
    #@#     outlier == TRUE,
    #@#     (takeOffImputationParamenters$imputationValueColumn) := NA_real_
    #@#   ]

    #@#   takeOffImputed[,
    #@#     movav :=
    #@#       rollavg(
    #@#         get(takeOffImputationParamenters$imputationValueColumn),
    #@#         order = 3
    #@#       ),
    #@#       by = c(takeOffImputationParamenters$byKey)
    #@#   ]

    #@#   takeOffImputed[
    #@#     outlier == TRUE & !is.na(movav),
    #@#     `:=`(
    #@#       c(takeOffImputationParamenters$imputationValueColumn,
    #@#       takeOffImputationParamenters$imputationFlagColumn,
    #@#       takeOffImputationParamenters$imputationMethodColumn),
    #@#       # Flags are Ee to differentiate them from Ie
    #@#       list(movav, "E", "e")
    #@#     )
    #@#   ]

    #@#   # Keep these to change flags later
    #@#   takeOff_outliers <-
    #@#     takeOffImputed[
    #@#       TakeOffFlagObservationStatus == "E" & TakeOffRateFlagMethod == "e",
    #@#       .(geographicAreaM49, measuredItemCPC, timePointYears, takeOffOut = TRUE)
    #@#     ]

    #@#   takeOffImputed <- takeOffImputed[, orig_cols, with = FALSE]
    #@# }
    
    data[,takeOffRate:=NULL]
    data[,TakeOffFlagObservationStatus:=NULL]
    data[,TakeOffRateFlagMethod:=NULL]
    
    ##When I make the merge I can recuperate those series when the slaughtered existes but there are no info for stock numbers (e.g. :Pacific Is)
    
    takeOffImputed=merge(data,takeOffImputed,
                         by=c("geographicAreaM49", "measuredItemCPC", "timePointYears"), all.x = TRUE)
    
    
    ##compute the new slaughtered
    takeOffImputed[,newSlaughtered:=(takeOffRate * get(animalFormulaParameters$productionValue))]  
    
    ## if we are running the module locally this two line are useful to visualizeintermediate output.
    ##ToSave=nameData("agriculture", "aproduction",takeOffImputed)
    ##write.csv(ToSave, paste0("C:/Users/Rosa/Desktop/LivestockFinalDebug/takeOffImputed",currentMeatItem,".csv"))
    ## I have already deleted all the Non protected values from the animalSlaughtered series, 
    ## actually all the values associated to "I,c"/"E,c"/"E,h"/"T,p"/"T,h"/"T,-" flag combinations 
    ## have been removed even if they were not originally protected.
    
    ##OverWrite the newSlaughter into the missing value of the slaughterd column:
    ##Be careful to not overwrite slaugheter that were (M,-)
    takeOffImputed[
      is.na(get(animalFormulaParameters$areaHarvestedValue)) &
        !is.na(newSlaughtered) &
        get(animalFormulaParameters$areaHarvestedMethodFlag) != "-",
      animalFormulaParameters$areaHarvestedObservationFlag := "I"
    ]
    
    #@# if (!is.null(a$FIX_OUTLIERS) && a$FIX_OUTLIERS == TRUE) {
    #@#   if (nrow(takeOff_outliers) > 0) {
    #@#     takeOffImputed <-
    #@#       merge(
    #@#         takeOffImputed,
    #@#         takeOff_outliers,
    #@#         by = c("geographicAreaM49", "measuredItemCPC", "timePointYears"),
    #@#         all.x = TRUE
    #@#       )

    #@#     takeOffImputed[
    #@#       takeOffOut == TRUE,
    #@#       animalFormulaParameters$areaHarvestedObservationFlag := "E"
    #@#     ]

    #@#     takeOffImputed[, takeOffOut := NULL]
    #@#   }
    #@# }
    
    takeOffImputed[is.na(get(animalFormulaParameters$areaHarvestedValue)) &
                       !is.na(newSlaughtered)&
                       get(animalFormulaParameters$areaHarvestedMethodFlag)!="-",
                   animalFormulaParameters$areaHarvestedMethodFlag:="e"]
    
    
    takeOffImputed[is.na(get(animalFormulaParameters$areaHarvestedValue)) &
                       !is.na(newSlaughtered)&
                       get(animalFormulaParameters$areaHarvestedMethodFlag)!="-",
                   animalFormulaParameters$areaHarvestedValue:=newSlaughtered]
    
    #slaughteredParentData=takeOffImputed[,c("geographicAreaM49",
    #                                        "measuredItemCPC",
    #                                        "timePointYears",
    #                                        animalFormulaParameters$areaHarvestedValue,
    #                                        animalFormulaParameters$areaHarvestedObservationFlag,
    #                                        animalFormulaParameters$areaHarvestedMethodFlag), with=FALSE]
    #
    #slaughteredParentData=normalise(slaughteredParentData, removeNonExistingRecords=FALSE)
    
    return(takeOffImputed)
    
    
}
