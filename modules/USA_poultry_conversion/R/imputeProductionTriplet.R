##' This function imputes the whole production domain.
##'
##' The function will impute production, area harvested and yield at the same
##' time.
##'
##' Transformation in the yield formula is not allowed and will not be taken
##' into account.
##'
##' @param data The data
##' @param processingParameters A list of the parameters for the production
##'     processing algorithms. See defaultProductionParameters() for a starting
##'     point.
##' @param formulaParameters A list holding the names and parmater of formulas.
##'     See \code{productionFormulaParameters}.
##' @param imputationParameters A list holding the imputation parameters, see
##'     \code{getImputationParameters}.
##' @export
##'
##' @import faoswsImputation
##' @import data.table
##'

imputeProductionTriplet = function(data,
                                  processingParameters,
                                  formulaParameters,
                                  imputationParameters, ...){

    originDataType = sapply(data, FUN = typeof)

    ################## ADDED TO HANDLE OUTLIERS

    # List additional arguments
    a <- list(...)

    # TODO: Should be moved to R/
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

    # function added in order to keep values in line
    fix_out <- function(data,
                        completeImputationKey,
                        imputationTimeWindow,
                        formula_parameters,
                        imputation_parameters,
                        flagValidTable,
                        THRESHOLD,
                        AVG_YEARS) {

      orig_cols <- names(data)

      mydata <- copy(data)

      if (imputationTimeWindow == "all") {
        val_years <- completeImputationKey@dimensions$timePointYears@keys
        completeImputationKey@domain <- sessionKey@domain
        completeImputationKey@dataset <- sessionKey@dataset
      } else {
        val_years <-
          lastYear - 0:switch(imputationTimeWindow, "lastThree" = 2, "lastFive" = 4)
      }

      mydata <-
        flagValidTable[
          mydata,
          on = c("flagObservationStatus" = formula_parameters$productionObservationFlag,
                 "flagMethod" = formula_parameters$productionMethodFlag)
        ]

      setnames(
        mydata,
        c("flagObservationStatus", "flagMethod"),
        c(formula_parameters$productionObservationFlag, formula_parameters$productionMethodFlag)
      )

      mydata[,
        `:=`(
          mean_old =
            mean(
              get(formula_parameters$productionValue)[
                get(imputation_parameters$yearValue) %in% AVG_YEARS
              ]
            ),
          mean_protected =
            mean(
              get(formula_parameters$productionValue)[
                get(imputation_parameters$yearValue) %in% (max(AVG_YEARS) + 1):max(val_years) &
                  sum(Protected[get(imputation_parameters$yearValue) %in% (max(AVG_YEARS) + 1):max(val_years)]) >= 2 &
                  Protected == TRUE
              ]
            )
        ),
        by = c(imputation_parameters$byKey)
      ]

      mydata[is.nan(mean_old), mean_old := NA_real_]
      mydata[is.nan(mean_protected), mean_protected := NA_real_]

      # NOTE: some NA ratios are due to M- series
      mydata[,
        `:=`(
          ratio_old       = get(formula_parameters$productionValue) / mean_old,
          ratio_protected = get(formula_parameters$productionValue) / mean_protected
        )
      ]

      mydata[is.infinite(ratio_old), ratio_old := NA_real_]
      mydata[is.infinite(ratio_protected), ratio_protected := NA_real_]

      mydata[, ratio := ifelse(!is.na(ratio_protected), ratio_protected, ratio_old)]

      mydata[, outlier := FALSE]

      mydata[
        get(imputation_parameters$yearValue) %in% val_years &
          Protected == FALSE,
        outlier := abs(ratio - 1) > THRESHOLD
      ]

      mydata[
        outlier == TRUE,
        (formula_parameters$productionValue) := NA_real_
      ]

      mydata[,
        movav :=
          rollavg(
            get(formula_parameters$productionValue),
            order = 3
          ),
          by = c(imputation_parameters$byKey)
      ]

      mydata[
        outlier == TRUE & !is.na(movav),
        `:=`(
          c(formula_parameters$productionValue,
          formula_parameters$productionObservationFlag,
          formula_parameters$productionMethodFlag),
          # Flags are Ee to differentiate them from Ie
          list(movav, "E", "e")
        )
      ]

      return(mydata[, orig_cols, with = FALSE])
    }
    ################## / ADDED TO HANDLE OUTLIERS

    areaHarvestedImputationParameters = imputationParameters$areaHarvestedParams
    yieldImputationParameters = imputationParameters$yieldParams
    productionImputationParameters = imputationParameters$productionParams

    message("Initializing ... ")
    dataCopy = copy(data)
    
    ##filter out (m-) from the imputation process
    
    
    
    ## Data Quality Checks
    suppressMessages({
        ensureImputationInputs(data = dataCopy,
                               imputationParameters = yieldImputationParameters)
        ensureImputationInputs(data = dataCopy,
                               imputationParameters =
                                   productionImputationParameters)
  
        ensureProductionInputs(dataCopy,
                               processingParameters = processingParameters,
                               formulaParameters = formulaParameters,
                               returnData = FALSE,
                               normalised = FALSE)
    })

    setkeyv(x = dataCopy, cols = c(processingParameters$areaVar,
                                   processingParameters$yearValue))


    dataCopy = computeYield(dataCopy,
                            processingParameters = processingParameters,
                            formulaParameters = formulaParameters,flagTable = ReadDatatable("ocs2023_flagweight"))
    ## Check whether all values are missing
    allYieldMissing = all(is.na(dataCopy[[formulaParameters$yieldValue]]))
    allProductionMissing = all(is.na(dataCopy[[formulaParameters$productionValue]]))
    allAreaMissing = all(is.na(dataCopy[[formulaParameters$areaHarvestedValue]]))


    if(!all(allYieldMissing)){
        ## Step two: Impute Yield
        message("Imputing Yield ...")
        n.missYield = sum(is.na(dataCopy[[formulaParameters$yieldValue]]))
        ## if(!missing(yieldFormula))
        ##     yieldFormula =
        ##     as.formula(gsub(yearValue, "yearValue",
        ##                     gsub(yieldValue, "yieldValue",
        ##                          deparse(yieldFormula))))

        dataCopy = imputeVariable(data = dataCopy,
                       imputationParameters = yieldImputationParameters)

        if (!is.null(a$FIX_OUTLIERS) && a$FIX_OUTLIERS == TRUE) {
          dataCopy <- fix_out(data                  = dataCopy,
                              completeImputationKey = a$completeImputationKey,
                              # completeImputationKey@domain = sessionKey@domain,
                              # completeImputationKey@dataset = sessionKey@dataset,
                              imputationTimeWindow  = a$imputationTimeWindow,
                              formula_parameters    = formulaParameters,
                              imputation_parameters = yieldImputationParameters,
                              flagValidTable        = a$flagValidTable,
                              THRESHOLD             = a$THRESHOLD,
                              AVG_YEARS             = a$AVG_YEARS)
        }

        ## TODO (Michael): Remove imputed zero yield as yield can not be zero by
        ##                 definition. This probably should be handled in the
        ##                 imputation parameter.
        ## Francesca: there is no reson why the zero yields have to be deleted!!
        ## It is the opposite: team B/C do not want to have yield when there is no production
        ## no areaHarvested!
        ##dataCopy =
        ##    removeZeroYield(dataCopy,
        ##                    yieldValue = formulaParameters$yieldValue,
        ##                    yieldObsFlag = formulaParameters$yieldObservationFlag,
        ##                    yieldMethodFlag = formulaParameters$yieldMethodFlag)
        n.missYield2 = length(which(is.na(
            dataCopy[[formulaParameters$yieldValue]])))
        message("Number of values imputed: ", n.missYield - n.missYield2)
        message("Number of values still missing: ", n.missYield2)

        ## Balance production now using imputed yield
        dataCopy =
            balanceProduction(data = dataCopy,
                              processingParameters = processingParameters,
                              formulaParameters = formulaParameters,flagTable = ReadDatatable("ocs2023_flagweight"))

        ## NOTE (Michael): Check again whether production is available
        ##                 now after it is balanced.
        allProductionMissing = all(is.na(dataCopy[[formulaParameters$productionValue]]))
    } else {
        warning("The input dataset contains insufficient data to impute yield!")
    }

    if(!all(allProductionMissing)){
        ## step three: Impute production
        message("Imputing Production ...")
        n.missProduction = length(which(is.na(
            dataCopy[[formulaParameters$productionValue]])))

        dataCopy = imputeVariable(data = dataCopy,
                       imputationParameters = productionImputationParameters)

        if (!is.null(a$FIX_OUTLIERS) && a$FIX_OUTLIERS == TRUE) {
          dataCopy <- fix_out(data                  = dataCopy,
                              completeImputationKey = a$completeImputationKey,
                              # completeImputationKey@domain = sessionKey@domain,
                              # completeImputationKey@dataset = sessionKey@dataset,
                              imputationTimeWindow  = a$imputationTimeWindow,
                              formula_parameters    = formulaParameters,
                              imputation_parameters = productionImputationParameters,
                              flagValidTable        = a$flagValidTable,
                              THRESHOLD             = a$THRESHOLD,
                              AVG_YEARS             = a$AVG_YEARS)
        }

        n.missProduction2 = length(which(is.na(
            dataCopy[[formulaParameters$productionValue]])))
        message("Number of values imputed: ",
            n.missProduction - n.missProduction2)
        message("Number of values still missing: ", n.missProduction2)
    } else {
        warning("The input dataset contains insufficient data to impute production!")
    }

    ## step four: balance area harvested
    message("Imputing Area Harvested ...")
    n.missAreaHarvested =
        length(which(is.na(
            dataCopy[[formulaParameters$areaHarvestedValue]])))

    dataCopy =
        balanceAreaHarvested(data = dataCopy,
                             processingParameters = processingParameters,
                             formulaParameters = formulaParameters,flagTable = ReadDatatable("ocs2023_flagweight"))
    allAreaMissing = all(is.na(dataCopy[[formulaParameters$areaHarvestedValue]]))

    if(!all(allAreaMissing)){
        ## HACK (Michael): This is to ensure the area harvested are also
        ##                 imputed. Then we delete all computed yield and
        ##                 then balance again. This causes the yield not
        ##                 comforming to the imputation model.
        ##
        ##                 This whole function should be re-writtened so
        ##                 that an algorithm similar to the EM algorithm
        ##                 estimates and impute the triplet in a conherent
        ##                 way.
        ##
        ##                 Issue #88
       
        dataCopy = imputeVariable(data = dataCopy,
                      imputationParameters = areaHarvestedImputationParameters)

        if (!is.null(a$FIX_OUTLIERS) && a$FIX_OUTLIERS == TRUE) {
          dataCopy <- fix_out(data                  = dataCopy,
                              completeImputationKey = a$completeImputationKey,
                              # completeImputationKey@domain = sessionKey@domain,
                              # completeImputationKey@dataset = sessionKey@dataset,
                              imputationTimeWindow  = a$imputationTimeWindow,
                              formula_parameters    = formulaParameters,
                              imputation_parameters = areaHarvestedImputationParameters,
                              flagValidTable        = a$flagValidTable,
                              THRESHOLD             = a$THRESHOLD,
                              AVG_YEARS             = a$AVG_YEARS)
        }
        
    ## It was this part that caused the double "i" in methodFlag in the same triplet:
    ## beacuse I was deliting those non-protected yields even if I had used them to compute
    ## as identity the other variables.   
    ##   dataCopy[!is.na(get(formulaParameters$areaHarvestedValue)) &
    ##            !is.na(get(formulaParameters$productionValue)) &
    ##            !(combineFlag(.SD,
    ##                          formulaParameters$yieldObservationFlag,
    ##                          formulaParameters$yieldMethodFlag) %in%
    ##              getProtectedFlag()),
    ##            `:=`(c(formulaParameters$yieldValue,
    ##                   formulaParameters$yieldObservationFlag,
    ##                   formulaParameters$yieldMethodFlag),
    ##                 list(NA, "M", "u"))]
        dataCopy =
            computeYield(dataCopy,
                         processingParameters = processingParameters,
                         formulaParameters = formulaParameters,flagTable = ReadDatatable("ocs2023_flagweight"))
        dataCopy = imputeVariable(data = dataCopy,
                       imputationParameters = yieldImputationParameters)

        if (!is.null(a$FIX_OUTLIERS) && a$FIX_OUTLIERS == TRUE) {
          dataCopy <- fix_out(data                  = dataCopy,
                              completeImputationKey = a$completeImputationKey,
                              # completeImputationKey@domain = sessionKey@domain,
                              # completeImputationKey@dataset = sessionKey@dataset,
                              imputationTimeWindow  = a$imputationTimeWindow,
                              formula_parameters    = formulaParameters,
                              imputation_parameters = yieldImputationParameters,
                              flagValidTable        = a$flagValidTable,
                              THRESHOLD             = a$THRESHOLD,
                              AVG_YEARS             = a$AVG_YEARS)
        }
       
    } ## End of HACK.
    n.missAreaHarvested2 =
        length(which(is.na(
            dataCopy[[formulaParameters$areaHarvestedValue]])))
    message("Number of values imputed: ",
        n.missAreaHarvested - n.missAreaHarvested2)
    message("Number of values still missing: ", n.missAreaHarvested2)


    ## This is to ensure the data type of the output is identical to
    ## the input data.
    dataCopy[, `:=`(colnames(dataCopy),
                    lapply(colnames(dataCopy),
                           FUN = function(x){
                        if(x %in% names(originDataType)){
                            as(.SD[[x]], originDataType[[x]])
                        } else {
                            .SD[[x]]
                        }
                    }))]
    dataCopy
}
