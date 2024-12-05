##' # Imputation of Non-Livestock Commodities
##'
##' **Author: Josh Browning, Michael C. J. Kao**
##'
##' **Revisited by: Livia Lombardi**
##'
##' **Description:**
##'
##' This module imputes the triplet (input, productivity and output) for a given
##' non-livestock commodity.
##'
##' **Inputs:**
##'
##' * Production domain
##' * Complete Key Table
##' * Livestock Element Mapping Table
##' * Identity Formula Table
##'
##' **Flag assignment:**
##'
##' | Procedure | Observation Status Flag | Method Flag|
##' | --- | --- | --- |
##' | Balance by Production Identity | `<flag aggregation>` | i |
##' | Imputation | I | e |
##'
##' **Data scope**
##'
##' * GeographicAreaM49: All countries specified in the `Complete Key Table`.
##'
##' * measuredItemCPC: Depends on the session selection. If the selection is
##'   "session", then only items selected in the session will be imputed. If the
##'   selection is "all", then all the items listed in the `Complete Key Table`
##'   excluding the live stock item in the `Livestock Element Mapping Table`
##'   will be imputed.
##'
##' * measuredElement: Depends on the measuredItemCPC, all cooresponding
##'   elements in the `Identity Formula Table`.
##'
##' * timePointYears: All years specified in the `Complete Key Table`.
##'
##' ---
##' Main changes:
##' - removed the outlier part- integrates the original imputation function inside the plugin
##' - removing imputation before first official value (before uprotected values and Nan values were mixed)
##' - managing M- time series

##' ## Initialisation
##'
#source("R/imputeProductionTriplet_v2.R")
##' Load the libraries
suppressMessages({
    library(faosws)
    library(faoswsUtil)
    library(faoswsFlag)
    library(faoswsImputation)
    library(faoswsProduction)
    library(faoswsProcessing)
    library(faoswsEnsure)
    library(magrittr)
    library(dplyr)
    #library(sendmailR)
})

##' Set up for the test environment and parameters
R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

if (CheckDebug()) {

    library(faoswsModules)
    SETTINGS <- ReadSettings("modules/Crop Production Imputation/sws.yml")

    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH <- SETTINGS[["share"]]

    ## Get session information from SWS. Token must be obtained from web interface

    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])
}

##' FUNCTIONS -------------------------------------------------------------------------------------------------------
# send_mail <- function(from = NA, to = NA, subject = NA,
#                       body = NA, remove = FALSE) {
# 
#     if (missing(from)) from <- 'no-reply@fao.org'
# 
#     if (missing(to)) {
#         if (exists('swsContext.userEmail')) {
#             to <- swsContext.userEmail
#         }
#     }
# 
#     if (is.null(to)) {
#         stop('No valid email in `to` parameter.')
#     }
# 
#     if (missing(subject)) stop('Missing `subject`.')
# 
#     if (missing(body)) stop('Missing `body`.')
# 
#     if (length(body) > 1) {
#         body <-
#             sapply(
#                 body,
#                 function(x) {
#                     if (file.exists(x)) {
#                         # https://en.wikipedia.org/wiki/Media_type
#                         file_type <-
#                             switch(
#                                 tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
#                                 txt  = 'text/plain',
#                                 csv  = 'text/csv',
#                                 png  = 'image/png',
#                                 jpeg = 'image/jpeg',
#                                 jpg  = 'image/jpeg',
#                                 gif  = 'image/gif',
#                                 xls  = 'application/vnd.ms-excel',
#                                 xlsx = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
#                                 doc  = 'application/msword',
#                                 docx = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
#                                 pdf  = 'application/pdf',
#                                 zip  = 'application/zip',
#                                 # https://stackoverflow.com/questions/24725593/mime-type-for-serialized-r-objects
#                                 rds  = 'application/octet-stream'
#                             )
# 
#                         if (is.null(file_type)) {
#                             stop(paste(tolower(sub('.*\\.([^.]+)$', '\\1', basename(x))),
#                                        'is not a supported file type.'))
#                         } else {
#                             res <- sendmailR:::.file_attachment(x, basename(x), type = file_type)
# 
#                             if (remove == TRUE)    {
#                                 unlink(x)
#                             }
# 
#                             return(res)
#                         }
#                     } else {
#                         return(x)
#                     }
#                 }
#             )
#     } else if (!is.character(body)) {
#         stop('`body` should be either a string or a list.')
#     }
# 
#     sendmailR::sendmail(from, to, subject, as.list(body))
# }



imputeProductionTripletOriginal = function(data,
                                           processingParameters,
                                           formulaParameters,
                                           imputationParameters){
    originDataType = sapply(data, FUN = typeof)

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
                            formulaParameters = formulaParameters)
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

        dataCopy =imputeVariable(data = dataCopy,
                                 imputationParameters = yieldImputationParameters)
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
                              formulaParameters = formulaParameters)

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
                             formulaParameters = formulaParameters)
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
                         formulaParameters = formulaParameters)
        dataCopy = imputeVariable(data = dataCopy,
                                  imputationParameters = yieldImputationParameters)


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
##'---------------------------------------------------------------------------------------------------------------
#Parameters


USER <- regmatches(
    swsContext.username,
    regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
)



TMP_DIR <- file.path(tempdir(), USER)
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)

tmp_file_no_imputed <- file.path(TMP_DIR, "non_livestock_imputation_result.csv")

##' Get user specified imputation selection
imputationSelection <- swsContext.computationParams$imputation_selection

#Time window substituted with Origin Year
#imputationTimeWindow <- swsContext.computationParams$imputation_timeWindow

# DEBUG

imputationStartYear <- as.numeric(swsContext.computationParams$start_year)
#imputationStartYear <- 2000
##' Check the validity of the computational parameter
stopifnot(imputationStartYear >= 1991)

#DEBUG
#imputationSelection = "session"

if(!imputationSelection %in% c("session", "all"))
    stop("Incorrect imputation selection specified")

##' Removing parameters used for Outlier detection
# FIX_OUTLIERS <- as.logical(swsContext.computationParams$fix_outliers)
# THRESHOLD <- as.numeric(swsContext.computationParams$outliers_threshold)
# AVG_YEARS <- 2009:2013

##' Get data configuration and session
sessionKey <- swsContext.datasets[[1]]
datasetConfig <- GetDatasetConfig(domainCode = sessionKey@domain,
                                  datasetCode = sessionKey@dataset)

##' Build processing parameters
processingParameters <-
    productionProcessingParameters(datasetConfig = datasetConfig)
processingParameters$domain <- sessionKey@domain
processingParameters$dataset <- sessionKey@dataset

lastYear=as.numeric(swsContext.computationParams$last_year)

#DEBUG
#lastYear=2021


## Inserting the list of EU countries declared in the MoU. If the user decide to exclude them from the imputation
#Eu countries if excluded will not excluded from the imputation process, just from the "save back"
`%!in%` <- Negate(`%in%`)

eu_parameter <- swsContext.computationParams$eurostat

geographic_table <- ReadDatatable("eurostat_m49")
setnames(geographic_table, c("m49","eurostat"), c("geographicAreaM49","eurostatGeographic"))

eu_countries <- c("AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","EL","HU","IE","IT","LV","LT","LU","MT","NL",
                  "PL","PT","RO","SK","SI","ES","SE")

geographic_table <- geographic_table[eurostatGeographic %in% eu_countries,]

eu_list <- geographic_table[, geographicAreaM49]


##' Get the full imputation Datakey
completeImputationKey <- getCompleteImputationKey("production")
completeImputationKey@domain <- sessionKey@domain
completeImputationKey@dataset <- sessionKey@dataset

# Exclude 835, which is in QA but not in LIVE

completeImputationKey@dimensions$geographicAreaM49@keys <-
    setdiff(completeImputationKey@dimensions$geographicAreaM49@keys, "835")

completeImputationKey@dimensions$timePointYears@keys <-
    as.character(min(completeImputationKey@dimensions$timePointYears@keys):lastYear)


##' **NOTE (Michael): Since the animal/meat are currently imputed by the
##'                   imputed_slaughtered and synchronise slaughtered module, so
##'                   they should be excluded here.**
##'
##'
##' NOTE (Sebastian): The share path isn't actually used here - it's
liveStockItems <-
    getAnimalMeatMapping(R_SWS_SHARE_PATH = R_SWS_SHARE_PATH,
                         onlyMeatChildren = FALSE)
liveStockItems = liveStockItems[,.(measuredItemParentCPC, measuredItemChildCPC)]
liveStockItems = unlist(x = liveStockItems, use.names = FALSE)
liveStockItems = unique(x = liveStockItems )


##' This is the complete list of items that are in the imputation list
nonLivestockImputationItems <-
    getQueryKey("measuredItemCPC", completeImputationKey) %>%
    setdiff(., liveStockItems)

##' These are the items selected by the users
sessionItems <-
    getQueryKey("measuredItemCPC", sessionKey) %>%
    intersect(., nonLivestockImputationItems)

##' Select the commodities based on the user input parameter
selectedItemCode <-
    switch(imputationSelection,
           "session" = sessionItems,
           "all" = nonLivestockImputationItems)

flagValidTable <- ReadDatatable("valid_flags")
stopifnot(nrow(flagValidTable) > 0)

#' ---
##' ## Perform Imputation
imputationResult <- data.table()

#lastYear=max(as.numeric(completeImputationKey@dimensions$timePointYears@keys))

# logConsole1=file("log.txt",open = "w")
# sink(file = logConsole1, append = TRUE, type = c( "message"))

##' Loop through the commodities to impute the items individually.

for (iter in seq(selectedItemCode)) {

    imputationProcess <- try({
        set.seed(070416)

        currentItem <- selectedItemCode[iter]


        ## Obtain the formula and remove indigenous and biological meat.
        ##
        ## NOTE (Michael): Biological and indigenous meat are currently
        ##                 removed, as they have incorrect data
        ##                 specification. They should be separate item with
        ##                 different item code rather than under different
        ##                 element under the meat code.
        formulaTable <-
            getProductionFormula(itemCode = currentItem) %>%
            removeIndigenousBiologicalMeat(formula = .)

        ## NOTE (Michael): Imputation should be performed on only 1 formula,
        ##                 if there are multiple formulas, they should be
        ##                 calculated based on the values imputed. For
        ##                 example, if one of the formula has production in
        ##                 tonnes while the other has production in
        ##                 kilo-gram, then we should impute the production
        ##                 in tonnes, then calculate the production in
        ##                 kilo-gram.
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

        ## Update the item/element key according to the current commodity
        subKey <- completeImputationKey

        subKey@dimensions$measuredItemCPC@keys <- currentItem

        subKey@dimensions$measuredElement@keys <-
            with(formulaParameters,
                 c(productionCode, areaHarvestedCode, yieldCode))

        ## Start the imputation
        message("Imputation for item: ", currentItem, " (",  iter, " out of ",
                length(selectedItemCode),")")

        ## Build imputation parameter
        imputationParameters <-
            with(formulaParameters,
                 getImputationParameters(productionCode = productionCode,
                                         areaHarvestedCode = areaHarvestedCode,
                                         yieldCode = yieldCode)
            )

        ## Extract the data, and skip the imputation if the data contains no entry.
        extractedData <- GetData(subKey)

        ##extractedData= expandDatasetYears(extractedData, processingParameters,
        ##                                 swsContext.computationParams$startYear,swsContext.computationParams$endYear )

        if (nrow(extractedData) == 0) {
            message("Item : ", currentItem, " does not contain any data")
            next
        }

        ## Process the data.
        processedData <-
            extractedData %>%
            preProcessing(data = .)

        processedData <- processedData[timePointYears >= 1991,]
        ## expandYear(data = .,
        ##            areaVar = processingParameters$areaVar,
        ##            elementVar = processingParameters$elementVar,
        ##            itemVar = processingParameters$itemVar,
        ##            valueVar = processingParameters$valueVar,
        ##            newYears= lastYear) %>%

        ##' RemoveNon protected flag applied in the range of the starting Year
        processedData <- removeNonProtectedFlag(processedData, keepDataUntil = (lastYear - (lastYear - imputationStartYear)))
        #
        # if (imputationTimeWindow == "all") {
        #     processedData <- removeNonProtectedFlag(processedData)
        # } else if (imputationTimeWindow == "lastThree") {
        #     processedData <- removeNonProtectedFlag(processedData, keepDataUntil = (lastYear - 2))
        # } else if (imputationTimeWindow == "lastFive") {
        #     processedData <- removeNonProtectedFlag(processedData, keepDataUntil = (lastYear - 4))
        # }

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

        ##    if(imputationTimeWindow=="lastThree"){
        ##        processedDataLast=processedData[get(processingParameters$yearVar) %in% c(lastYear, lastYear-1, lastYear-2)]
        ##        processedDataAll=processedData[!get(processingParameters$yearVar) %in% c(lastYear, lastYear-1, lastYear-2)]
        ##                        processedDataLast=processProductionDomain(data = processedDataLast,
        ##                                processingParameters = processingParameters,
        ##                                formulaParameters = formulaParameters) %>%
        ##        ensureProductionInputs(data = .,
        ##                               processingParam = processingParameters,
        ##                               formulaParameters = formulaParameters,
        ##                               normalised = FALSE)
        ##        processedData=rbind(processedDataLast,processedDataAll)
        ##
        ##    }else{
        ##
        ##        processedData =  processProductionDomain(data = processedData,
        ##                                processingParameters = processingParameters,
        ##                                formulaParameters = formulaParameters) %>%
        ##            ensureProductionInputs(data = .,
        ##                                   processingParam = processingParameters,
        ##                                   formulaParameters = formulaParameters,
        ##                                   normalised = FALSE)
        ##
        ##              }
        ##
        ##imputationParameters$productionParams$plotImputation="prompt"
        ##------------------------------------------------------------------------------------------------------------------------

        ## We have to remove (M,-) from yield: since yield is usually computed ad identity,
        ## it results inusual that it exists a last available protected value different from NA and when we perform
        ## the function expandYear we risk to block the whole time series. I replace all the (M,-) in yield with
        ## (M,u) the triplet will be sychronized by the imputeProductionTriplet function.

        processedData[
            get(formulaParameters$yieldObservationFlag) == processingParameters$missingValueObservationFlag,
            ":="(
                c(formulaParameters$yieldMethodFlag),
                list(processingParameters$missingValueMethodFlag))
            ]

        processedData <- normalise(processedData)
        ##' We want to avoid imputations for items which have official data starting from certain year.
        ##' M u at this point corresponds to not protected values to be reimputed
        ##' assigning specific flag to those data to get the difference between the empty series expanded with
        ##' expandYear function

        toBeImputed <- processedData[get(processingParameters$flagObservationVar) == processingParameters$missingValueObservationFlag &
                          get(processingParameters$flagMethodVar) == processingParameters$missingValueMethodFlag,]


        officialData <- setdiff(processedData,toBeImputed)

        officialData_expanded <-
            expandYear(
                data       = officialData,
                areaVar    = processingParameters$areaVar,
                elementVar = processingParameters$elementVar,
                itemVar    = processingParameters$itemVar,
                valueVar   = processingParameters$valueVar,
                newYears   = lastYear
            )

        toBeImputed_expanded <-
            expandYear(
                data       = toBeImputed,
                areaVar    = processingParameters$areaVar,
                elementVar = processingParameters$elementVar,
                itemVar    = processingParameters$itemVar,
                valueVar   = processingParameters$valueVar,
                newYears   = lastYear
            )

        ##------------------------------------------------------------------------------------------------------------##

        ##' if the country start an official production we don t want to impute the production before that date
        pre_official <- copy(officialData_expanded)


        pre_official <- pre_official[(flagObservationStatus == "") | (flagObservationStatus == "E" & flagMethod == "f")|
                                         (flagObservationStatus == "T"),
                                     .SD[which.min(timePointYears)], by = c("measuredElement", "geographicAreaM49")]

        ##' to do : check the merge criteria with a hole imputed time serie ... with Ie present of course!
        ##' if no official appear, null col firstOfficial should be managed
        pre_official_merge <- merge(officialData_expanded, pre_official, all.x = T,
                                    by = c("measuredItemCPC","measuredElement", "geographicAreaM49"))

        setnames(pre_official_merge, c("timePointYears.x","Value.x","flagObservationStatus.x","flagMethod.x","timePointYears.y"),
                 c("timePointYears","Value","flagObservationStatus","flagMethod","firstOfficial"))

        pre_official_merge<- pre_official_merge[, c("geographicAreaM49","measuredElement","measuredItemCPC",
                                                    "timePointYears","Value","flagObservationStatus","flagMethod",
                                                    "firstOfficial"), with = FALSE]

        ##' let s take off rows with firstOfficial NA, it means the whole time serie doesn t have an official flag
        ##' for this reason it should be attached later to be imputed

        pre_official_merge_na <- pre_official_merge[is.na(firstOfficial),]

        pre_official_merge <- pre_official_merge[!is.na(firstOfficial) & timePointYears >= firstOfficial,]

        pre_official_merge <- rbind(pre_official_merge,pre_official_merge_na)

        pre_official_merge[,firstOfficial := NULL]


        ##' binding with the not protected values that we want to impute
        selectedData <- rbind(toBeImputed_expanded[! pre_official_merge, on = c("measuredElement","geographicAreaM49",
        "timePointYears", "measuredItemCPC")], pre_official_merge)
        ##--------------------------------------------------------------------------------------------------------------###
        processedData <-
            expandYear(
                data       = processedData,
                areaVar    = processingParameters$areaVar,
                elementVar = processingParameters$elementVar,
                itemVar    = processingParameters$itemVar,
                valueVar   = processingParameters$valueVar,
                newYears   = lastYear
            )

        processedData <- denormalise(processedData, denormaliseKey = "measuredElement")

        ##------------------------------------------------------------------------------------------------------------------------
        ## Perform imputation
        imputed <-
            imputeProductionTripletOriginal(
                data                  = processedData,
                processingParameters  = processingParameters,
                formulaParameters     = formulaParameters,
                imputationParameters  = imputationParameters
            )


        ##------------------------------------------------------------------------------------------------------------------------
        ## By now we did not have touched those situation in which production or areaHarvested are ZERO:
        ## we may have some yield different from zero even if productio or areaHarvested or the both are ZERO.
        ##
        ## I apply this modification to:
        ##    1) Production = zero
        ##    2) or AreaHarveste= zero
        ##    3) and non zero yield only.
        ##
        zeroProd <- imputed[, get(formulaParameters$productionValue) == 0 ]

        zeroreHArv <- imputed[, get(formulaParameters$areaHarvestedValue) == 0]

        nonZeroYield <- imputed[, (get(formulaParameters$yieldValue) != 0)]

        myfilter <- (zeroProd|zeroreHArv) & nonZeroYield

        imputed[myfilter, ":="(c(formulaParameters$yieldValue), list(0))]

        imputed[
            myfilter,
            ":="(
                c(formulaParameters$yieldObservationFlag),
                aggregateObservationFlag(
                    get(formulaParameters$productionObservationFlag),
                    get(formulaParameters$areaHarvestedObservationFlag)
                )
            )
            ]

        imputed[myfilter, ":="(c(formulaParameters$yieldMethodFlag),list(processingParameters$balanceMethodFlag))]

        ##------------------------------------------------------------------------------------------------------------------------

        ## Filter timePointYears for which it has been requested to compute imputations
        ## timeWindow= c(as.numeric(swsContext.computationParams$startYear):as.numeric(swsContext.computationParams$endYear))
        ## imputed= imputed[timePointYears %in% timeWindow,]

        ## Save the imputation back to the database.

        ##  Records containing invalid dates are
        ##  excluded, for example, South Sudan only came
        ##  into existence in 2011. Thus although we can
        ##  impute it, they should not be saved back to
        ##  the database.
        imputed <-
            removeInvalidDates(data = imputed, context = sessionKey) %>%
            ensureProductionOutputs(
                data = .,
                processingParameters = processingParameters,
                formulaParameters = formulaParameters,
                normalised = FALSE
            ) %>%
            normalise(.)

        #ensureProductionBalanced(imputed)
        #ensureProtectedData(imputed)

        ##' Here we select which data shoul be kept for the saving given that we don't want to impute values before
        ##' official/semi-official data appears

        final_imputed <- semi_join(imputed, selectedData,
                                    by = c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears"))

        ##' remove all the productivity elements where input or output is not present
        final_imputed <- as.data.table(final_imputed)

        prova <- denormalise(final_imputed, denormaliseKey = "measuredElement")

        naOutput <- prova[, is.na(get(formulaParameters$productionValue)) &
                            is.na(get(formulaParameters$productionMethodFlag))]

        naInput <- prova[, is.na(get(formulaParameters$areaHarvestedValue)) &
                           is.na(get(formulaParameters$areaHarvestedMethodFlag))]

        nafilter <- (naOutput|naInput)

        prova <- prova[!nafilter,]

        final_imputed <- normalise(prova, removeNonExistingRecords = TRUE)

        anomalies <- final_imputed[flagMethod == "",]

        ## Only data with method flag "i" for balanced,
        ## or flag combination (I, e) for imputed are
        ## saved back to the database.
        ##Also the new (M,-) data have to be sent back, series must be blocked!!!

        final_imputed <-
            final_imputed[
                (flagMethod == "i" | (flagObservationStatus == "I" & flagMethod == "e")) |
                    (flagObservationStatus == "M" & flagMethod == "-") |
                    (flagObservationStatus == "E" & flagMethod == "e"),]

        ##I should send to the data.base also the (M,-) value added in the last year in order to highlight that the series is closed.

        # if (imputationTimeWindow == "lastThree") {
        #     imputed <- imputed[get(processingParameters$yearVar) %in% (lastYear - 0:2)]
        # } else if (imputationTimeWindow == "lastFive") {
        #     imputed <- imputed[get(processingParameters$yearVar) %in% (lastYear - 0:4)]
        # }
        final_imputed <- final_imputed[get(processingParameters$yearVar) %in% (lastYear - 0:(lastYear - imputationStartYear))]

        final_imputed <- postProcessing(data = final_imputed)

        if (eu_parameter == "no") {

            final_imputed=final_imputed[geographicAreaM49 %!in% eu_list, ]

            SaveData(domain = sessionKey@domain,
                     dataset = sessionKey@dataset,
                     data =  final_imputed)

        }else{

        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data =  final_imputed)
        }

    })

    ## Capture the items that failed
    if(inherits(imputationProcess, "try-error"))
        imputationResult <-
            rbind(imputationResult,
                  data.table(item = currentItem,
                             error = imputationProcess[iter]))

}
message("Production Crops Imputation loop ended")
##' ---
##' ## Return Message

if (exists("imputationResult") && nrow(imputationResult) > 0) {

    write.csv(imputationResult, tmp_file_no_imputed)

    # ## Initiate email
    # from <- "sws@fao.org"
    # to <- swsContext.userEmail
    # subject <- "Imputation Result"
    # body <- paste0("The following items failed, please inform the maintainer "
    #                , "of the module")
    #
    # errorAttachmentName <- "non_livestock_imputation_result.csv"
    #
    # errorAttachmentPath <-
    #     paste0(R_SWS_SHARE_PATH, "/kao/", errorAttachmentName)
    #
    # write.csv(imputationResult, file = errorAttachmentPath,
    #           row.names = FALSE)
    #
    # errorAttachmentObject <- mime_part(x = errorAttachmentPath,
    #                                    name = errorAttachmentName)
    #
    # bodyWithAttachment <- list(body, errorAttachmentObject)
    #
    # sendmail(from = from, to = to, subject = subject, msg = bodyWithAttachment)
    #
    # stop("Production imputation incomplete, check following email to see where ",
    #      " it failed")
}

#msg <- "Imputation Completed Successfully"

# if (!CheckDebug()) {
#     ## Initiate email
#     from <- "sws@fao.org"
#     to <- swsContext.userEmail
#     subject <- "Crop-production imputation plugin has correctly run"
#     body <- paste0("The plug-in has saved the Production imputation in your session. Session number: ",  sessionKey@sessionId)
#
#     sendmail(from = from, to = to, subject = subject, msg = body)
# }

message("Production Crop Imputation: preparing email")

## Initiate email

body_message <- sprintf(
    "Imputation Completed. Production imputation can be incomplete if some items failed in imputation,
    please check non_livestock_imputation_result.csv")

if (!CheckDebug()) {
    # send_mail(
    #     from <- "sws@fao.org",
    #     to <- swsContext.userEmail,
    #     subject <- "Crops module",
    #     body = c(body_message,
    #              tmp_file_no_imputed
    #     )
    # )
}

unlink(TMP_DIR, recursive = TRUE)

message("Production Crops Imputation: END")
