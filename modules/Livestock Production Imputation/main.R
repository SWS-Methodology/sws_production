##' # Imputation and Synchronisation of Livestock Commodities
##'
##' **Author: Francesca Rosa**
##'
##' **Description:**
##'
##' The animals slaughtered for production of meat, offals, fats and hides must
##' be available before running the production imputation code. These numbers,
##' however, are not guaranteed to be available, and in the case of missing
##' data, an imputation method must be applied.
##'
##' The decision was to use the production figures of meat, if available, to
##' compute the missing animals slaughtered. If these figures are also missing,
##' they should be imputed using the production imputation methodology. Of
##' course, in the case of currently available data in the animal element, that
##' data should be transferred to the quantity of animals slaughtered for meat
##' and then the imputation ran. We also decided to save the imputations for
##' meat so as to retain consistency with the animal figures.
##'
##' Although the procedure is called transfer, however, the value is actually
##' calculated. To transfer value from animal (parent) to meat (child), we copy
##' the value, then multiplied by a `share`. The meaning of the variable is the
##' share of the slaughtered animal that is used as input for the children. In
##' most cases they are 100%, however, take cattle in India for example, they
##' can be less then 100 as not all cattle slaughtered are used to produce meat
##' due to the holy nature of the animal.
##'
##' **Inputs:**
##'
##' * Production domain
##' * Complete Key Table
##' * Livestock Element Mapping Table
##' * Identity Formula table
##' * Share table
##' * Elements code table
##' * Range Carcass Weight table
##'
##' **Steps:**
##'
##' 1. Impute Livestock Numbers
##'
##' 2. Impute Number of Slaughtered animal (assiciated to the animal item)
##'
##' 3. Transfer the animal slaughtered from animal commodity (parent) to the
##'    meat commodity (child)
##'
##' 4. Impute the meat triplet (production/animal slaughtered/carcass weight)
##'    based on the same logic as all other production imputation procedure.
##'
##' 5. Transfer the slaughtered animal from the meat back to the animal, as now
##'    certain slaughtered animal is imputed in step 3.
##'
##' 6. Transfer the slaughtered animal from the animal to all other child
##'    commodities. This includes items such as offals, fats and hides and
##'    impute missing values for non-meat commodities.
##'
##' **Flag assignment:**
##'
##' | Procedure | Observation Status Flag | Method Flag|
##' | --- | --- | --- |
##' | Tranasfer between animal and meat commodity | `<Same as origin>` | c |
##' | Balance by Production Identity | `<flag aggregation>` | i |
##' | Imputation | I | e |
##'
##' **NOTE (Michael): Currently the transfer has flag 'c' indicating it is
##' copied, however, they should be replaced with a new flag as it is calculated
##' by not by identity.**
##'
##' **Data scope**
##'
##' * GeographicAreaM49: All countries specified in the `Complete Key Table`.
##'
##' * measuredItemCPC: Depends on the session selection. If the selection is
##'   "session", then only items selected in the session will be imputed. If the
##'   selection is "all", then all the items listed in the `Livestock Element
##'   Mapping Table` will be imputed.
##'
##' * measuredElement: Depends on the measuredItemCPC, all cooresponding
##'   elements in the `Identity Formula Table` and also all elements listed in
##'   the `Livestock Element Mapping Table`.
##'
##' * timePointYears: All years specified in the `Complete Key Table`.
##'
##'
##' **Flow chart:**
##' ![livestock Flow](livestock_flow.jpg?raw=true "livestock Flow")
##' ---

##' ## Initialisation
##'

message("Step 0: Setup")

##' Load the libraries
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
})



##' removed. rshared folder no longer exist
#dir_to_save <- file.path(R_SWS_SHARE_PATH, "Livestock", paste0("validation", gsub("/", "_",swsContext.username)))
# if (!file.exists(dir_to_save)) {
#     dir.create(dir_to_save, recursive = TRUE)
# }
# TODO: Should be moved to R/
## -----------------------------------------------------------------------------------------------------
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

##------------------------------------------------------------------------------------------------------


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

##-------------------------------------------------------------------------------------------------------
##' Get the shared path
R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

if (CheckDebug()) {
    
    library(faoswsModules)
    
    SETTINGS <- ReadSettings("C:/Users/lombardi/OneDrive - Food and Agriculture Organization/Livia/faoswsProduction/modules/Livestock Production Imputation/sws.yml")
    
    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH <- SETTINGS[["share"]]
    
    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])
    #SetClientFiles("C:/Users/Lombardili/OneDrive - Food and Agriculture Organization/Livia/certificates/production")
    
    ## Get session information from SWS. Token must be obtained from web interface
    GetTestEnvironment(SETTINGS[["server"]], SETTINGS[["token"]])
}

USER <- regmatches(
    swsContext.username,
    regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
)


##' Load and check the computation parameters
imputationSelection <- swsContext.computationParams$imputation_selection
if (!imputationSelection %in% c("session", "all")) {
    stop("Incorrect imputation selection specified")
}

# imputationTimeWindow <- swsContext.computationParams$imputation_timeWindow
# if (!imputationTimeWindow %in% c("all", "lastThree", "lastFive")) {
#   stop("Incorrect imputation selection specified")
# }

imputationStartYear <- as.numeric(swsContext.computationParams$start_year)
##' Check the validity of the computational parameter
stopifnot(imputationStartYear >= 1991)


TMP_DIR <- file.path(tempdir(), USER)
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)

tmp_file_no_ls <- file.path(TMP_DIR, "non_livestock_imputation_result.csv")
tmp_file_LivestockTriplet <- file.path(TMP_DIR, "LivestockTriplet.csv")
tmp_file_Not_balanced_triplet <- file.path(TMP_DIR, "Not_balanced_Triplet.csv")
tmp_file_ToBeChecked <- file.path(TMP_DIR, "ToBeChecked.csv")

##' Get data configuration and session
sessionKey <- swsContext.datasets[[1]]
datasetConfig <- GetDatasetConfig(domainCode = sessionKey@domain,
                                  datasetCode = sessionKey@dataset)

##' Build processing parameters
processingParameters <-
    productionProcessingParameters(datasetConfig = datasetConfig)

lastYear <- max(as.numeric(swsContext.computationParams$last_year))



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

##' Obtain the complete imputation key
completeImputationKey <- getCompleteImputationKey("production")

completeImputationKey@dimensions$timePointYears@keys <-
    as.character(min(completeImputationKey@dimensions$timePointYears@keys):lastYear)


##' Extract the animal parent to child commodity mapping table
##'
##' This table contains the parent item/element code which maps to the child
##' item/element code. For example, the slaughtered animal element for cattle is
##' 5315, while the slaughtered animal for cattle meat is 5320.
##'
##  Ideally, the two elements should be merged and have a single
##  code in the classification. This will eliminate the change of
##  code in the transfer procedure.

animalMeatMappingTable <- ReadDatatable("animal_parent_child_mapping")

## When pulled from the SWS the datatable header cannot contain capital letters
animalMeatMappingTable <-
    animalMeatMappingTable[,
                           .(
                               measuredItemParentCPC = measured_item_parent_cpc,
                               measuredElementParent = measured_element_parent,
                               measuredItemChildCPC  = measured_item_child_cpc,
                               measuredElementChild  = measured_element_child
                           )
                           ]

##' Here we expand the session to include all the parent and child items. That
##' is, we expand to the particular livestock tree.
##'
##' For example, if 02111 (Cattle) is in the session, then the session will be
##' expanded to also include 21111.01 (meat of cattle, freshor chilled), 21151
##' (edible offal of cattle, fresh, chilled or frozen), 21512 (cattle fat,
##' unrendered), and 02951.01 (raw hides and skins of cattle).
##'
##' The elements are also expanded to the required triplet.

livestockImputationItems <-
    expandMeatSessionSelection(
        oldKey = completeImputationKey,
        selectedMeatTable = animalMeatMappingTable
    ) %>%
    getQueryKey("measuredItemCPC", datasetkey = .) %>%
    selectMeatCodes(itemCodes = .)

sessionItems <-
    expandMeatSessionSelection(
        oldKey = sessionKey,
        selectedMeatTable = animalMeatMappingTable
    ) %>%
    getQueryKey("measuredItemCPC", datasetkey = .) %>%
    selectMeatCodes(itemCodes = .)

##' Select the range of items based on the computational parameter.
selectedMeatCode <-
    switch(
        imputationSelection,
        session = sessionItems,
        all     = livestockImputationItems
    )

# lastYear=max(as.numeric(completeImputationKey@dimensions$timePointYears@keys))

##' ---
##' ## Perform Synchronisation and Imputation

##' Here we iterate through the the meat item to perform the steps described in
##' the description. Essentially, we are looping over different livestock trees.
if (CheckDebug()) {
    logConsole1 <- file("log.txt",open = "w")
    sink(file = logConsole1, append = TRUE, type = "message")
}

# NOTE: this used to come from the faoswsFlag package.
# XXX: There are some discrepancies in the two tables (pkg and SWS)
flagValidTable <- ReadDatatable("valid_flags")
stopifnot(nrow(flagValidTable) > 0)

imputationResult <- data.table()

for (iter in seq(selectedMeatCode)) {
    
    imputationProcess <- try({
        
        message("Processing livestock tree (", iter, " out of ",
                length(selectedMeatCode), ")")
        
        set.seed(070416)
        
        ## Extact the current ANIMAL,MEAT and NON-MEATcodes with their relative formula and mapping table
        
        ##meat
        currentMeatItem <- selectedMeatCode[iter]
        
        currentMappingTable <-
            animalMeatMappingTable[measuredItemChildCPC == currentMeatItem, ]
        
        ##animal
        currentAnimalItem <- currentMappingTable[, measuredItemParentCPC]
        
        ##all derived
        currentAllDerivedProduct <-
            animalMeatMappingTable[measuredItemParentCPC == currentAnimalItem, measuredItemChildCPC]
        
        ##derived non meat
        currentNonMeatItem <-
            currentAllDerivedProduct[currentAllDerivedProduct != currentMeatItem]
        
        itemMap <- GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredItemCPC")
        stopifnot(nrow(itemMap) > 0)
        
        # Remove offals, hides and skins as there is a dedicated plugin
        currentNonMeatItem <-
            setdiff(
                currentNonMeatItem,
                itemMap[
                    type %in% c("HIDE", "PSKN", "OFF", "POFF") |
                        (type == "DERA" & grepl("\\b(offal|skin|hide|fat)", description)),
                    code
                    ]
            )
        
        message("Extracting the shares tree")
        
        shareData <-
            getShareData(
                geographicAreaM49     = getQueryKey("geographicAreaM49", completeImputationKey),
                measuredItemChildCPC  = currentAllDerivedProduct,
                measuredItemParentCPC = currentAnimalItem,
                timePointYearsSP      = getQueryKey("timePointYears", completeImputationKey)
            ) %>%
            setnames(x = .,
                     old = c("Value", "timePointYearsSP"),
                     new = c("share", "timePointYears")) %>%
            mutate(timePointYears = as.numeric(timePointYears))
        
        shareData <- as.data.table(shareData)
        
        ## note: all the shares are equalt to 1
        
        ## ---------------------------------------------------------------------
        message("Extracting animal data ", currentAnimalItem, " (Animal)")
        
        ## Get the animal formula
        animalFormulaTable <-
            getProductionFormula(itemCode = currentAnimalItem) %>%
            removeIndigenousBiologicalMeat(formula = .)
        
        if (nrow(animalFormulaTable) > 1) {
            stop("Imputation should only use one formula")
        }
        
        ## Create the formula parameter list
        animalFormulaParameters <-
            with(animalFormulaTable,
                 productionFormulaParameters(datasetConfig = datasetConfig,
                                             productionCode = output,
                                             areaHarvestedCode = input,
                                             yieldCode = productivity,
                                             unitConversion = unitConversion))
        
        
        ## Get the animal key, we take the complete key and then modify the element
        ## and item dimension to extract the current meat item and it's
        ## corresponding elements.
        ## Francesca: it is not necessary to extract the triplet, but just Livestock and
        ## Slaughtered, the element that should play the role of the YIEL is, in this case
        ## the off-take  rate that is endogenously computed (eventually using trade) and then imputed.
        
        animalKey <- completeImputationKey
        
        animalKey@dimensions$measuredItemCPC@keys <- currentAnimalItem
        
        animalKey@dimensions$measuredElement@keys <-
            animalFormulaParameters$productionCode
        
        ## Get the animal data (NB: preProcessing: manage NA M and transform timePointYears)
        animalData <-
            animalKey %>%
            GetData(key = .) %>%
            preProcessing(data = .)
        
        ## This condition allow to use also the NON-protected data to build the imputations
        ## for last three years in case you have chosed to produce imputations only for last
        ## three years
        animalData <- removeNonProtectedFlag(animalData, keepDataUntil = (lastYear - (lastYear - imputationStartYear)))
        
        # if (imputationTimeWindow == "all") {
        #   animalData <- removeNonProtectedFlag(animalData)
        # } else if (imputationTimeWindow == "lastThree") {
        #   animalData <- removeNonProtectedFlag(animalData, keepDataUntil = (lastYear - 2))
        # } else if (imputationTimeWindow == "lastFive") {
        #   animalData <- removeNonProtectedFlag(animalData, keepDataUntil = (lastYear - 4))
        # }
        
        animalData <-
            expandYear(
                data       = animalData,
                areaVar    = processingParameters$areaVar,
                elementVar = processingParameters$elementVar,
                itemVar    = processingParameters$itemVar,
                valueVar   = processingParameters$valueVar,
                newYears   = lastYear
            )
        
        ## ---------------------------------------------------------------------
        ##  The idea is to include the TRADE domain into the livestock imputation process. The basic hypothesis
        ##  is that Countries import livestock just for slaughtering purposes.
        ##  We made several test including and excluding trade data which in many cases was the source of outliers
        ##  apparently non feasible fluctuations into the meat production.
        
        ##  Get new trade data
        
        itemMap <- itemMap[, .(measuredItemCPC = code,type)]
        
        data <- merge(animalData, itemMap, by = "measuredItemCPC")
        
        ## This two lines contains info on the trade elements to be pulled in case it will be decided in the future to
        ## use trade to compute the number of animal Slaughtered
        #itemCodeKey = ReadDatatable("element_codes")
        #tradeElements = itemCodeKey[itemtype== unique(data[,type]),c(imports, exports)]
        #factor= itemCodeKey[itemtype== unique(data[,type]),c(factor)] # this is a conversion factor to be used in computing one element of the triplet from the others as identity
        
        ## I prefer to get the conversion factor from the data table: item_type_yield_elements which is the same where also the
        ## fuction getProductionFormula takes it.
        
        getFactor <- ReadDatatable(table = "item_type_yield_elements")
        
        factor <- getFactor[item_type == unique(data[,type]), c(factor)]
        
        ##  Pull trade data for the current Animal Item
        ##  In case you decide to use the trade: build the key using the most updated dataset!!!!
        
        ##   tradeData <- GetData(key = key)
        ##
        ##   setnames(tradeData, c("measuredElementTrade", "measuredItemCPC"),
        ##            c("measuredElement", "measuredItemCPC"))
        ##
        ##   tradeData=preProcessing(tradeData)
        ##
        ##   stockTrade=rbind(tradeData, animalData)
        ## At the moment it has been decided to NOT use trade data
        ##    stockTrade=animalData
        ##    stockTrade=denormalise(stockTrade, denormaliseKey = "measuredElement", fillEmptyRecords=TRUE )
        
        animalData <- denormalise(animalData, denormaliseKey = "measuredElement", fillEmptyRecords = TRUE )
        
        ## ---------------------------------------------------------------------
        ## Imputation of animal Stock
        ## To impute livestock numbers we follow excatly the same approach (the ensemble approach)
        ## already developped. Here we are building the parameters
        animalStockImputationParameters <- defaultImputationParameters()
        
        ## I am modifing the animalStockImputationParameters in order to specify that the variable to be imputed
        ## is the livestock (5111 for big animals, 5112 for small animals)
        animalStockImputationParameters$imputationValueColumn <- animalFormulaParameters$productionValue
        animalStockImputationParameters$imputationFlagColumn <- animalFormulaParameters$productionObservationFlag
        animalStockImputationParameters$imputationMethodColumn <- animalFormulaParameters$productionMethodFlag
        animalStockImputationParameters$byKey <- c("geographicAreaM49", "measuredItemCPC")
        animalStockImputationParameters$estimateNoData <- TRUE
        
        ##This code is to see the charts of the emsemble approach
        ##animalStockImputationParameters$plotImputation="prompt"
        
        message("Step 1: Impute missing values for livestock: item ", currentAnimalItem,
                " (Animal)")
        
        stockImputed <- imputeVariable(animalData, imputationParameters = animalStockImputationParameters)
        
        #M- issue
        ##---------------------------------------------------------------------------------------------------------
        
        ##Pull slaughtered Animail (code referrig to ANIMAL)
        slaughterdKey <- animalKey
        
        slaughterdKey@dimensions$measuredElement@keys <-
            with(animalFormulaParameters,c(areaHarvestedCode))
        
        slaughteredAnimalData <-
            slaughterdKey %>%
            GetData(key = .) %>%
            preProcessing(data = .)
        
        slaughteredAnimalData <- removeNonProtectedFlag(slaughteredAnimalData, 
                                                        keepDataUntil = (lastYear - (lastYear - imputationStartYear)))
        
        # if (imputationTimeWindow == "all") {
        #   slaughteredAnimalData <- removeNonProtectedFlag(slaughteredAnimalData)
        # } else if (imputationTimeWindow == "lastThree") {
        #   slaughteredAnimalData <- removeNonProtectedFlag(slaughteredAnimalData, keepDataUntil = (lastYear - 2))
        # } else if (imputationTimeWindow == "lastFive") {
        #   slaughteredAnimalData <- removeNonProtectedFlag(slaughteredAnimalData, keepDataUntil = (lastYear - 4))
        # }
        
        slaughteredAnimalData <-
            removeNonProtectedFlag(slaughteredAnimalData) %>%
            expandYear(
                data = .,
                areaVar    = processingParameters$areaVar,
                elementVar = processingParameters$elementVar,
                itemVar    = processingParameters$itemVar,
                valueVar   = processingParameters$valueVar,
                newYears   = lastYear
            )
        
        slaughteredAnimalData <-
            denormalise(
                slaughteredAnimalData,
                denormaliseKey = "measuredElement",
                fillEmptyRecords = TRUE
            )
        
        ## Prepare the table to be used to compute TOT slaughtered Animal: this approach has been follow in order to
        ## use trade data. In theory for some countries it would have been necessary to compute the Total number of animal
        ## slaughterd including the trade flows. The alternative, would have been to use the usual triplet approach using
        ## functions as imputeProductionTriplet.
        
        ## For some countries we may have slaughtered AnimalData, but not stockImputed
        ## Be careful with this merge:
        
        stockSlaughtered <-
            merge(
                stockImputed,
                slaughteredAnimalData,
                by    = c("geographicAreaM49", "measuredItemCPC", "timePointYears"),
                all.x =  TRUE,
                all.y =  TRUE
            )
        
        ##---------------------------------------------------------------------------------------------------------
        
        message("Step 2: Impute Number of Slaughtered animal for ", currentAnimalItem, " (Animal)")
        
        ## The function computeTot
        # Imputations of offtake are here
        slaughteredParentData <-
            computeTotSlaughtered(
                data              = stockSlaughtered,
                FormulaParameters = animalFormulaParameters
            )
        
        # re assign M- to slaughtered values since the function computeToSlaughtered is turning M- to Mu
        slaughteredParentData <- merge(slaughteredParentData, stockSlaughtered[, list(geographicAreaM49,measuredItemCPC,timePointYears,
                                                                                        flagObservationStatus_measuredElement_sl_original = get(paste0(animalFormulaParameters$areaHarvestedObservationFlag)),
                                                                                        flagMethod_measuredElement_sl_original =get(paste0(animalFormulaParameters$areaHarvestedMethodFlag)))], 
                                         by= c("geographicAreaM49","measuredItemCPC","timePointYears"),
                                         all.x = TRUE)
        
        slaughteredParentData[flagObservationStatus_measuredElement_sl_original == "M" &
                                    flagMethod_measuredElement_sl_original == "-", 
                               animalFormulaParameters$areaHarvestedValue := NA]
        
        slaughteredParentData[flagObservationStatus_measuredElement_sl_original == "M" &
                                  flagMethod_measuredElement_sl_original == "-",
                              animalFormulaParameters$areaHarvestedObservationFlag := "M",]
        
        slaughteredParentData[flagObservationStatus_measuredElement_sl_original == "M" &
                                  flagMethod_measuredElement_sl_original == "-",
                              animalFormulaParameters$areaHarvestedMethodFlag := "-"]
        
        slaughteredParentData[, flagObservationStatus_measuredElement_sl_original:= NULL]
        
        slaughteredParentData[, flagMethod_measuredElement_sl_original:= NULL]
        
        # Before Saving this data in the shared folder I change the off-take method flag which is: "i". It is now "c"
        # because it was useful to protect it.
        slaughteredParentData[TakeOffRateFlagMethod == "c", TakeOffRateFlagMethod := "i"]
        
        
        
        write.csv(slaughteredParentData, tmp_file_LivestockTriplet)
        # if (!CheckDebug()) {
        #     write.csv(
        #       slaughteredParentData,
        #       file.path(dir_to_save, paste0("LivestockTriplet_", currentAnimalItem, ".csv")),
        #       row.names = FALSE
        #     )
        # }
        
        slaughteredParentData <-
            slaughteredParentData[,
                                  c("geographicAreaM49", "measuredItemCPC", "timePointYears",
                                    animalFormulaParameters$areaHarvestedValue,
                                    animalFormulaParameters$areaHarvestedObservationFlag,
                                    animalFormulaParameters$areaHarvestedMethodFlag),
                                  with = FALSE
                                  ]
        
        slaughteredParentData <-
            normalise(
                slaughteredParentData,
                removeNonExistingRecords = FALSE
            )
        #M- issue
        ##---------------------------------------------------------------------------------------------------------
        ## --------------------------------------------------------------------------------------------------------
        ## Check if all the slaughtered series have been imputed. If the animal stocks series is not present
        ## there would not be the series of animal slaughtered.
        
        ## This is the dataset containig the slaughted
        slaughteredAnimalData <- normalise(slaughteredAnimalData)
        
        sel_cols <- c("geographicAreaM49", "measuredItemCPC", "timePointYears", "measuredElement")
        
        imputed <- slaughteredParentData[, sel_cols, with = FALSE]
        
        orginalSlaughterd <- slaughteredAnimalData[, sel_cols, with = FALSE]
        
        diff <- setdiff(orginalSlaughterd,imputed)
        
        if (nrow(diff) > 0) {
            seriesToAdd <- slaughteredAnimalData[diff,,on = sel_cols]
            slaughteredParentData <- rbind(slaughteredParentData, seriesToAdd)
        }
        
        ##---------------------------------------------------------------------------------------------------------
        ## --------------------------------------------------------------------------------------------------------
        message("Extracting production triplet for item ", currentMeatItem,
                " (Meat)")
        
        ## Get the meat formula
        meatFormulaTable <-
            getProductionFormula(itemCode = currentMeatItem) %>%
            removeIndigenousBiologicalMeat(formula = .)
        
        ##Associated to each commodity we MUST have just ONE formula
        if (nrow(meatFormulaTable) > 1) {
            stop("Imputation should only use one formula")
        }
        
        ## Create the formula parameter list
        meatFormulaParameters <-
            with(meatFormulaTable,
                 productionFormulaParameters(datasetConfig = datasetConfig,
                                             productionCode = output,
                                             areaHarvestedCode = input,
                                             yieldCode = productivity,
                                             unitConversion = unitConversion)
            )
        
        ## Get the meat key, we take the complete key and then modify the element
        ## and item dimension to extract the current meat item and it's
        ## corresponding elements.
        ##
        ## We extract the triplet so that we can perform the check
        ## on whether the triplet are balanced already. Eventhough
        ## only the animal slaughtered element is transferred.
        meatKey <- completeImputationKey
        
        meatKey@dimensions$measuredItemCPC@keys <- currentMeatItem
        
        meatKey@dimensions$measuredElement@keys <-
            unique( with(meatFormulaParameters,
                         c(productionCode, areaHarvestedCode, yieldCode,
                           currentMappingTable$measuredElementChild)))
        
        ## Get the meat data
        meatData <- GetData(key = meatKey)
        meatData <- preProcessing(data = meatData)
        meatData <- removeInvalidFlag(meatData)
        
        meatData <- removeNonProtectedFlag(meatData, keepDataUntil = (lastYear - (lastYear - imputationStartYear)))
        
        # if (imputationTimeWindow == "all") {
        #   meatData <- removeNonProtectedFlag(meatData)
        # } else if (imputationTimeWindow == "lastThree") {
        #   meatData <- removeNonProtectedFlag(meatData, keepDataUntil = (lastYear - 2))
        # } else if (imputationTimeWindow == "lastFive") {
        #   meatData <- removeNonProtectedFlag(meatData, keepDataUntil = (lastYear - 4))
        # }
        
        meatData <-
            denormalise(
                normalisedData = meatData,
                denormaliseKey = "measuredElement"
            )
        
        ## We have to remove (M,-) from the carcass weight: since carcass weight is usually computed ad identity,
        ## it results inusual that the last available value is protected and different from NA. We risk that, when we perform
        ## the function expandYear, we erroneously block the whole time series. I replace all the (M,-) carcass weight with
        ## (M,-). The triplet will be sychronized by the imputeProductionTriplet function.
        
        meatData[
            get(meatFormulaParameters$yieldObservationFlag) == processingParameters$missingValueObservationFlag,
            ":="(
                c(meatFormulaParameters$yieldMethodFlag),
                list(processingParameters$missingValueMethodFlag)
            )
            ]
        
        meatData <- createTriplet(data = meatData, formula = meatFormulaTable)
        
        ## The slaughtered must be all synchronized from the animal
        meatData[,
                 ":="(
                     c(meatFormulaParameters$areaHarvestedValue,
                       meatFormulaParameters$areaHarvestedObservationFlag,
                       meatFormulaParameters$areaHarvestedMethodFlag),
                     list(NA_real_,"M", "u"))
                 ]
        
        ensureProductionInputs(
            data                 = meatData,
            processingParameters = processingParameters,
            formulaParameters    = meatFormulaParameters,
            normalised           = FALSE,
            returnData           = FALSE
        )
        
        meatData <- normalise(meatData)
        
        meatData <-
            expandYear(
                data       = meatData,
                areaVar    = processingParameters$areaVar,
                elementVar = processingParameters$elementVar,
                itemVar    = processingParameters$itemVar,
                valueVar   = processingParameters$valueVar,
                newYears   = lastYear
            )
        
        ## ---------------------------------------------------------------------
        message("Step 3: Transferring animal slaughtered from animal to meat commodity")
        
        animalMeatMappingShare <-
            merge(currentMappingTable, shareData, all.x = TRUE,
                  by = c("measuredItemParentCPC", "measuredItemChildCPC"))
        
        ## Transfer the animal slaughtered number from animal to the meat.
        slaughteredTransferedToMeatData <-
            transferParentToChild(
                parentData = slaughteredParentData,
                childData = meatData,
                mappingTable = animalMeatMappingShare,
                transferMethodFlag = "c",
                imputationObservationFlag = "I",
                parentToChild = TRUE
            )
        
        ensureCorrectTransfer(
            parentData   = slaughteredParentData,
            childData    = slaughteredTransferedToMeatData,
            mappingTable = animalMeatMappingShare,
            returnData   = FALSE
        )
        
        ## ---------------------------------------------------------------------
        
        message("Step 4: Perform Imputation on the Meat Triplet")
        
        ## Start the imputation
        ## Build imputation parameter
        imputationParameters <-
            with(meatFormulaParameters,
                 getImputationParameters(productionCode    = productionCode,
                                         areaHarvestedCode = areaHarvestedCode,
                                         yieldCode         = yieldCode)
            )
        
        message("Performing Imputation")
        
        meatImputed <- slaughteredTransferedToMeatData
        
        meatImputed <-
            denormalise(
                normalisedData  = meatImputed,
                denormaliseKey  = "measuredElement",
                fillEmptyRecord = TRUE
            )
        
        #meatImputed =processProductionDomain(data = meatImputed,
        #                                     processingParameters = processingParameters,
        #                                     formulaParameters = meatFormulaParameters)
        
        ## Since we have syncronized and protected "slaugtered animal"
        ## and we have protected some of the carcass weight copied from the old
        ## system in order to stabilize the imputation of this variable, it is possible that
        ## we have some all protected triplets and we have to check:
        ## 1. the three elements are balanced
        ## 2. if only slaughtered and production are balanced, the resulting
        ##    carcass weight is within the ranges
        
        ## I add to the already existing formula parameters the flagComb columns because I have to work
        ## with PROTECTED flag combinations
        
        ## Enlarge the meatFormulaParameters just to include Flag checks:
        meatFormParams <-
            c(meatFormulaParameters,
              list(
                  areaHarvestedFlagComb = paste0("flagComb_", meatFormulaParameters$areaHarvestedCode),
                  productionFlagComb    = paste0("flagComb_", meatFormulaParameters$productionCode),
                  yieldFlagComb         = paste0("flagComb_", meatFormulaParameters$yieldCode)
              )
            )
        
        ##Obtain a vector containing all the protected flag combinations
        ProtectedFlag <- getProtectedFlag()
        
        ##I have to exclude (M,-) from the protected flag combinations. Doing the checks for the carcass weight to
        ##free, otherwise I risk to open closed series:
        ProtectedFlag <- ProtectedFlag[ProtectedFlag != "(M, -)"]
        
        ##Add the flag combination column for each element of the triplet
        meatImputed[,
                    meatFormParams$areaHarvestedFlagComb :=
                        combineFlag(
                            meatImputed,
                            meatFormParams$areaHarvestedObservationFlag,
                            meatFormParams$areaHarvestedMethodFlag
                        )
                    ]
        
        meatImputed[,
                    meatFormParams$productionFlagComb :=
                        combineFlag(
                            meatImputed,
                            meatFormParams$productionObservationFlag,
                            meatFormParams$productionMethodFlag
                        )
                    ]
        
        meatImputed[,
                    meatFormParams$yieldFlagComb :=
                        combineFlag(
                            meatImputed,
                            meatFormParams$yieldObservationFlag,
                            meatFormParams$yieldMethodFlag
                        )
                    ]
        
        meatImputed[,
                    yield := (get(meatFormParams$productionValue) / get(meatFormParams$areaHarvestedValue)) * factor
                    ]
        
        ##If two elements of the triplet are all protected (Meat and Slaughtered) I have to compute again the resulting Carcass Weight
        
        meatANDSlaughteredProtectedEl <-
            meatImputed[,
                        get(meatFormParams$productionFlagComb) %in% ProtectedFlag &
                            get(meatFormParams$areaHarvestedFlagComb) %in% ProtectedFlag
                        ]
        
        ##Overwrite the carcass weight with the just computed, and consequently update the Flags
        meatImputed[
            meatANDSlaughteredProtectedEl ,
            ":="(
                c(meatFormParams$yieldValue,
                  meatFormParams$yieldObservationFlag,
                  meatFormParams$yieldMethodFlag),
                list(NA_real_, "M", "u"))
            ]
        
        ##I remove the flagComb columns that  have created just to make these checks
        meatImputed[, meatFormParams$areaHarvestedFlagComb := NULL]
        meatImputed[, meatFormParams$yieldFlagComb := NULL]
        meatImputed[, meatFormParams$productionFlagComb := NULL]
        meatImputed[, yield := NULL]
        
        ## ---------------------------------------------------------------------
        ## Check if all the Carcass Weight are within feasible ranges
        rangeCarcassWeight <- ReadDatatable("range_carcass_weight")
        
        currentRange <- rangeCarcassWeight[meat_item_cpc == currentMeatItem,]
        
        meatImputed[
            get(meatFormParams$yieldValue) > currentRange[, carcass_weight_max] |
                get(meatFormParams$yieldValue) < currentRange[, carcass_weight_min],
            ":="(
                c(meatFormParams$areaHarvestedValue,
                  meatFormParams$areaHarvestedObservationFlag,
                  meatFormParams$areaHarvestedMethodFlag),
                list(NA_real_,"M","u"))
            ]
        
        ## ---------------------------------------------------------------------
        ## Perform imputation using the standard imputation function
        
        meatImputed <-
            imputeProductionTripletOriginal(
                data                 = meatImputed,
                processingParameters = processingParameters,
                imputationParameters = imputationParameters,
                formulaParameters    = meatFormulaParameters
            )
        
        ensureProductionOutputs(
            data                 = meatImputed,
            processingParameters = processingParameters,
            formulaParameters    = meatFormulaParameters,
            returnData           = FALSE,
            normalised           = FALSE
        )
        
        #if (lastYear - imputationStartYear > 5) {
        
        noBalanced <-
            ensureProductionBalanced(
                meatImputed,
                meatFormParams$areaHarvestedValue,
                meatFormParams$yieldValue,
                meatFormParams$productionValue,
                factor,
                normalised = FALSE,
                getInvalidData = TRUE
            )
        
        if (nrow(noBalanced) > 0) {
            message("Warning: the triplet is not balanced after imputeProductionTriplet!")
            
            # if (!CheckDebug()) {
            #     
            # 
            #     # 
            #     # createErrorAttachmentObject <- function(testName,
            #     #                                        testResult,
            #     #                                        R_SWS_SHARE_PATH){
            #     #     errorAttachmentName = paste0(testName, ".csv")
            #     #     errorAttachmentPath =
            #     #         paste0(R_SWS_SHARE_PATH, "/rosa/", errorAttachmentName)
            #     #     write.csv(testResult, file = errorAttachmentPath,
            #     #               row.names = FALSE)
            #     #     errorAttachmentObject = mime_part(x = errorAttachmentPath,
            #     #                                       name = errorAttachmentName)
            #     #     errorAttachmentObject
            #     # }
            #     #write.csv(noBalanced,tmp_file_Not_balanced_triplet)
            #     # bodyWithAttachmentNoBalanced <-
            #     #     createErrorAttachmentObject(paste0("Not_balanced_Triplet_", currentMeatItem),
            #     #                                 noBalanced,
            #     #                                 R_SWS_SHARE_PATH)
            # 
            #     send_mail(from = "sws@fao.org",
            #              to = swsContext.userEmail,
            #              subject = "Some triplet are not balanced",
            #              body = tmp_file_Not_balanced_triplet)
            # 
            # }
        }
        #}
        
        #' Check if the resulting Carcass weights are within a feasible range!
        #' We are currently use the table range stored in the SWS
        
        ##Select the row corresponding to the current meat item from the range-table
        
        message("Check the Carcass weights")
        
        ##currentRange=rangeCarcassWeight[meat_item_cpc==currentMeatItem,]
        ## I am checking only those series where the Value is different from NA:
        ## it means that is cannot overwrite (M,-) figures in the carcass weigth series.
        
        ## Identify the rows out of range
        outOfRange <-
            meatImputed[
                get(imputationParameters$yieldParams$imputationValueColumn) >  currentRange[,carcass_weight_max] |
                    get(imputationParameters$yieldParams$imputationValueColumn) <  currentRange[,carcass_weight_min]
                ]
        
        if (nrow(outOfRange) > 0) {
            
            message("Number out rows out of range: ", nrow(outOfRange))
            
            ## Replace the values of carcass weight outside from the range with the extremes of the range
            
            ## Impose the outOfRange values below the minimum equal to the
            ## lower extreme of the range and  the outOfRange Values up the max equal to upper extreme of the range
            
            meatImputed[
                get(imputationParameters$yieldParams$imputationValueColumn) >  currentRange[,carcass_weight_max] &
                    get(imputationParameters$yieldParams$imputationFlagColumn) != "M",
                ":="(
                    c(imputationParameters$yieldParams$imputationValueColumn,
                      imputationParameters$yieldParams$imputationFlagColumn,
                      imputationParameters$yieldParams$imputationMethodColumn),
                    list(
                        currentRange[,carcass_weight_max],
                        "I",
                        "e"
                    )
                )
                ]
            
            meatImputed[
                get(imputationParameters$yieldParams$imputationValueColumn) <  currentRange[,carcass_weight_min] &
                    get(imputationParameters$yieldParams$imputationFlagColumn) != "M",
                ":="(
                    c(imputationParameters$yieldParams$imputationValueColumn,
                      imputationParameters$yieldParams$imputationFlagColumn,
                      imputationParameters$yieldParams$imputationMethodColumn),
                    list(
                        currentRange[,carcass_weight_min],
                        "I",
                        "e"
                    )
                )
                ]
            
            ## We should free the number of animal slaughtered and recalculate this variable as identity
            
            meatImputed[,
                        newS :=
                            factor * computeRatio(
                                get(imputationParameters$productionParams$imputationValueColumn),
                                get(imputationParameters$yieldParams$imputationValueColumn)
                            )
                        ]
            
            #OverWrite the Slaughtered animal element if PRODUCTION had NOT been computed as identity
            
            meatImputed[
                get(imputationParameters$productionParams$imputationMethodColumn) != "i" &
                    (newS > (get(imputationParameters$areaHarvestedParams$imputationValueColumn) + 1e-6) |
                         newS < (get(imputationParameters$areaHarvestedParams$imputationValueColumn) -  1e-6)),
                ":="(
                    c(imputationParameters$areaHarvestedParams$imputationValueColumn,
                      imputationParameters$areaHarvestedParams$imputationFlagColumn,
                      imputationParameters$areaHarvestedParams$imputationMethodColumn),
                    list(
                        newS,
                        aggregateObservationFlag(
                            get(imputationParameters$yieldParams$imputationFlagColumn),
                            get(imputationParameters$productionParams$imputationFlagColumn)
                        ),
                        "i"
                    )
                )
                ]
            
            meatImputed[,
                        newP := (get(imputationParameters$yieldParams$imputationValueColumn) * get(imputationParameters$areaHarvestedParams$imputationValueColumn)) / factor
                        ]
            
            #OverWrite the Production animal element if SLAUGHTERED had NOT been computed as identity
            meatImputed[
                (newP > (get(imputationParameters$productionParams$imputationValueColumn) + 1e-6) |
                     newP < (get(imputationParameters$productionParams$imputationValueColumn) -  1e-6) ) &
                    get(imputationParameters$areaHarvestedParams$imputationMethodColumn) != "i",
                ":="(
                    c(imputationParameters$productionParams$imputationValueColumn,
                      imputationParameters$productionParams$imputationFlagColumn,
                      imputationParameters$productionParams$imputationMethodColumn),
                    list(
                        newP,
                        aggregateObservationFlag(
                            get(imputationParameters$yieldParams$imputationFlagColumn),
                            get(imputationParameters$productionParams$imputationFlagColumn)
                        ),
                        "i"
                    )
                )
                ]
            
            meatImputed[, c("newS", "newP") := NULL]
            
            ## table(meatImputed[,.(flagMethod_measuredElement_5320,flagMethod_measuredElement_5417,flagMethod_measuredElement_5510)])
        }
        
        meatImputed <- normalise(meatImputed)
        
        ## ---------------------------------------------------------------------
        message("Step 3: Transfer animal slaughtered back from meat to animal commodity")
        
        ## Transfer the animal slaughtered from meat back to animal, this can be
        ## done by specifying parentToChild equal to FALSE.
        ##
        ## NOTE (Michael): We only subset the new calculated or imputed values to be
        ##                 transfer back to the animal (parent) commodity. See issue
        ##                 #180.
        ##
        ## NOTE (Michael): Since the animal element is not imputed nor balanced , we
        ##                 will not test whether it is imputed or the identity
        ##                 calculated.
        
        ## I am filtering meatImputed in order to avoid issue 180
        
        meatImputedFilterd <-
            meatImputed[flagMethod == "i" | (flagObservationStatus == "I" & flagMethod == "e") | (flagObservationStatus == "E" & flagMethod == "e"), ]
        
        slaughteredTransferedBackToAnimalData <-
            transferParentToChild(
                parentData                = slaughteredParentData,
                childData                 = meatImputedFilterd,
                mappingTable              = animalMeatMappingShare,
                transferMethodFlag        = "c",
                imputationObservationFlag = "I",
                parentToChild             = FALSE
            )
        
        ## Not all the tranfered figures have to be sent back to the SWS, bacause there are situation where
        ## only the flag is changed, and it would be better to keep the protected flag combination coming from the
        ## parent-data "slaughteredParentData"
        
        ensureProductionOutputs(
            data                 = meatImputed,
            processingParameters = processingParameters,
            formulaParameters    = meatFormulaParameters,
            testImputed          = FALSE,
            testCalculated       = FALSE,
            normalised           = TRUE,
            returnData           = FALSE
        )
        
        ## ---------------------------------------------------------------------
        
        ##message("Testing transfers are applied correctly")
        ## WARNING (Michael): We currently only check the synchronisation between
        ##                    animal and the meat as this processed is applied in
        ##                    the module. The animal slaughtered si transferred from
        ##                    animal to non-meat items, but not the reverse so we
        ##                    can not expect them to be synchronised. However, we
        ##                    need to also ensure the synchronisation happen between
        ##                    other the animal and non-meat child. How to do this
        ##                    specifically, I have no immediate idea. This is
        ##                    related to issue 178.(SOLVED)
        ##
        
        ##Slaughtered trasfered back from meat item to animal are those that should be
        ##checked.
        
        ##meatImputed
        #ensureCorrectTransfer(parentData = slaughteredTransferedBackToAnimalData,
        #                      childData = meatImputed,
        #                      mappingTable = animalMeatMappingShare,
        #                      returnData = FALSE)
        
        ## Here I am building the file to be sent as email attachement to be checked.
        ## The problem is that only re-computed figures with a different intial Value have to be sent back to the SWS
        
        ## I have to send back to the SWS the following elements:
        ## 1.Livestock numbers stockImputed
        ## 2.Slaughtered animal associated to ANIMAL (slaughteredTransferedBackToAnimalData)
        
        ## 3.4.5. The meat triplet contained in meatImputed
        livestockNumbers <- normalise(stockImputed)
        
        message("Saving the synchronised and imputed data back")
        
        syncedData <-
            rbind(
                meatImputed,
                livestockNumbers,
                slaughteredTransferedBackToAnimalData
            )
        
        ##Maybe it is better to send back also the (M,-) series otherwise it seems they are not updated!
        syncedData <- syncedData[(flagMethod!="u"),]
        
        ##write.csv(syncedData, paste0("C:/Users/Rosa/Desktop/LivestockFinalDebug/syncedData/",currentMeatItem,".csv"), row.names = FALSE)
        
        ## The transfer can over-write official and
        ## semi-official figures in the processed commodities as
        ## indicated by in the previous synchronise slaughtered
        ## module.
        ##
        
        syncedData = syncedData[get(processingParameters$yearVar) %in% (lastYear - 0:(lastYear - imputationStartYear))]
        
        # if (imputationTimeWindow == "lastThree") {
        #   syncedData = syncedData[get(processingParameters$yearVar) %in% (lastYear - 0:2)]
        # } else if (imputationTimeWindow == "lastFive") {
        #   syncedData = syncedData[get(processingParameters$yearVar) %in% (lastYear - 0:4)]
        # }
        
        syncedData <- postProcessing(data = syncedData)
        
        syncedData <- removeInvalidDates(syncedData)
        
        ProtectedOverwritten <-
            ensureProtectedData(
                syncedData[
                    (flagObservationStatus =="I" & flagMethod == "e") |
                        flagMethod == "i" |
                        flagMethod == "c",
                    ],
                getInvalidData = TRUE
            )
        
        ProtectedOverwritten <- ProtectedOverwritten[measuredElement != imputationParameters$areaHarvestedParams$variable]
        
        ProtectedOverwritten <- ProtectedOverwritten[Value != i.Value]
        #saving data and pulling back the M- (if we want to just block withot pulling, just make it NA and remove it)
        if (eu_parameter == "no") {
            
            syncedData=syncedData[geographicAreaM49 %!in% eu_list, ]
            
            data_to_save <- copy(syncedData)
            
            data_to_save[, M_first_year :=  max(as.numeric(timePointYears[flagObservationStatus %in% "M" & flagMethod %in% "-"])),
                  by = c("geographicAreaM49","measuredItemCPC","measuredElement")]
            
            # data_to_save[as.numeric(timePointYears) < as.numeric(M_first_year), `:=` (Value = 0 , flagObservationStatus = "M",
            #                                                                  flagMethod = "-"),
            #       by = c("geographicAreaM49","measuredItemCPC","measuredElement")]
            
            data_to_save[, to_remove := FALSE]
            
            data_to_save[as.numeric(timePointYears) < as.numeric(M_first_year) & !(flagObservationStatus %in% "M" & flagMethod %in% "-"), 
                         to_remove := TRUE,
                         by = c("geographicAreaM49","measuredItemCPC","measuredElement")]
            
            data_to_save <- data_to_save[to_remove == FALSE,]
            
            data_to_save[, to_remove := NULL]
            
            data_to_save[, M_first_year := NULL]
            
            SaveData(domain = sessionKey@domain,
                     dataset = sessionKey@dataset,
                     data =  data_to_save)
            #if 0 M - have been pulled back, then fill up to 1991 for coherence: to discuss. if the 0 M- represent the start year of closure
            # if(imputationStartYear > 1991){
            #     
            #     zeros_data <- copy(data_to_save)
            #     
            #     zeros_data <- zeros_data[!is.infinite(M_first_year), ]
            #     
            #     zeros_data_to_save <- zeros_data %>% distinct(geographicAreaM49,measuredItemCPC,measuredElement,M_first_year)
            #     
            #     zeros_data_to_save <- as.data.table(zeros_data_to_save)[,.(year=seq(1991,max(M_first_year))),by=.(geographicAreaM49,measuredItemCPC,measuredElement)]
            #     
            #     
            # }else{
            #     
            #     data_to_save[, M_first_year := NULL]
            #     
            #     SaveData(domain = sessionKey@domain,
            #              dataset = sessionKey@dataset,
            #              data =  data_to_save)
            #     
            # }
            
          
            
        }else{
            
            data_to_save <- copy(syncedData)
            
            data_to_save[, M_first_year :=  max(as.numeric(timePointYears[flagObservationStatus %in% "M" & flagMethod %in% "-"])),
                         by = c("geographicAreaM49","measuredItemCPC","measuredElement")]
            
            data_to_save[, to_remove := FALSE]
            
            data_to_save[as.numeric(timePointYears) < as.numeric(M_first_year) & !(flagObservationStatus %in% "M" & flagMethod %in% "-"), 
                         to_remove := TRUE,
                         by = c("geographicAreaM49","measuredItemCPC","measuredElement")]
            
            data_to_save <- data_to_save[to_remove == FALSE,]
            
            
            data_to_save[, to_remove := NULL]
            
            data_to_save[, M_first_year := NULL]
            
            SaveData(domain = sessionKey@domain,
                     dataset = sessionKey@dataset,
                     data =  data_to_save)
            

        }
        
        
        # SaveData(domain = sessionKey@domain,
        #          dataset = sessionKey@dataset,
        #          data = syncedData)
        
        #---------------------------------------------------------------------
        
        #         if (!CheckDebug() & length(ProtectedOverwritten) > 0) {
        # 
        # 
        # 
        #             write.csv(ProtectedOverwritten,tmp_file_ToBeChecked)
        # 
        # 
        #             send_mail(from = "sws@fao.org",
        #                       to = swsContext.userEmail,
        #                       subject = "Some protected figures have been overwritten",
        #                       body = tmp_file_ToBeChecked)
        # #
        # #             bodyWithAttachment <-
        # #                 createErrorAttachmentObject(paste0("ToBeChecked_", currentMeatItem),
        # #                                             ProtectedOverwritten,
        # #                                             R_SWS_SHARE_PATH)
        # #
        # #             sendmail(from = "sws@fao.org",
        # #                      to = swsContext.userEmail,
        # #                      subject = "Some protected figures have been overwritten",
        # #                      msg = bodyWithAttachment)
        #         }
        
        ## Now that we have computed and synchronized all the slaughtered we can proceed
        ##computig other derived items
        ## ---------------------------------------------------------------------
        if (length(currentNonMeatItem) > 0) {
            
            nonMeatImputedList <- list()
            
            message("Step 6: Transfer the slaughtered animal from the animal to all other child
                    commodities. This includes items such as offals, fats and hides and
                    impute missing values for non-meat commodities.")
            
            ## Different triplet for different non-meat items, we need to loop through the
            ## different non-meat items
            
            for (j in seq(currentNonMeatItem)) {
                currentNonMeatItemLoop = currentNonMeatItem[j]
                
                message("Extracting production triplet for item ",
                        paste0(currentNonMeatItemLoop, collapse = ", "),
                        " (Non-meat Child)")
                
                ## Get the non Meat formula
                currentNonMeatFormulaTable <-
                    getProductionFormula(itemCode = currentNonMeatItemLoop) %>%
                    removeIndigenousBiologicalMeat(formula = .)
                
                ## Build the non meat key
                currentNonMeatKey <- completeImputationKey
                currentNonMeatKey@dimensions$measuredItemCPC@keys = currentNonMeatItemLoop
                currentNonMeatKey@dimensions$measuredElement@keys =
                    with(currentNonMeatFormulaTable,
                         unique(c(input, output, productivity)))
                
                nonMeatMeatFormulaParameters <-
                    with(currentNonMeatFormulaTable,
                         productionFormulaParameters(datasetConfig = datasetConfig,
                                                     productionCode = output,
                                                     areaHarvestedCode = input,
                                                     yieldCode = productivity,
                                                     unitConversion = unitConversion)
                    )
                
                ## Get the non meat data
                
                nonMeatData <-
                    currentNonMeatKey %>%
                    GetData(key = .) %>%
                    preProcessing(data = .) %>%
                    denormalise(normalisedData = .,
                                denormaliseKey = "measuredElement") %>%
                    createTriplet(data = .,
                                  formula = currentNonMeatFormulaTable)
                
                ## We have to remove (M,-) from the carcass weight: since carcass weght is usually computed ad identity,
                ## it results inutial that it exists a last available protected value different from NA and when we perform
                ## the function expandYear we risk to block the whole time series. I replace all the (M,-) carcass wight with
                ## (M,-) the triplet will be sychronized by the imputeProductionTriplet function.
                
                nonMeatData[
                    get(nonMeatMeatFormulaParameters$yieldObservationFlag) == processingParameters$missingValueObservationFlag,
                    ":="(
                        c(nonMeatMeatFormulaParameters$yieldMethodFlag),
                        list(processingParameters$missingValueMethodFlag)
                    )
                    ]
                
                nonMeatData <- normalise(denormalisedData = nonMeatData,
                                         removeNonExistingRecords = FALSE)
                
                nonMeatData <-
                    expandYear(
                        data       = nonMeatData,
                        areaVar    = processingParameters$areaVar,
                        elementVar = processingParameters$elementVar,
                        itemVar    = processingParameters$itemVar,
                        valueVar   = processingParameters$valueVar,
                        newYears   = lastYear
                    )
                
                message("Transfer Animal Slaughtered to All Child Commodities")
                
                nonMeatMappingTable <-
                    animalMeatMappingTable[measuredItemChildCPC %in% currentNonMeatItemLoop, ]
                
                animalNonMeatMappingShare <-
                    merge(nonMeatMappingTable, shareData, all.x = TRUE,
                          by = c("measuredItemParentCPC", "measuredItemChildCPC"))
                
                
                ## In this tipology of commodity, there are still present old FAOSTAT imputations flagged as (I,-).
                ## At the moment the best we can do is to keep those figures as protected.
                ## We delete the figures flagged ad (I,e) end computed ad identity figures (method="i") coming from previus run of themodule:
                
                modifiedFlagTable <- copy(flagValidTable)
                
                modifiedFlagTable[flagObservationStatus == "I" & flagMethod == "-" , Protected := TRUE]
                
                nonMeatData <- removeNonProtectedFlag(nonMeatData, flagValidTable = modifiedFlagTable,
                                                      keepDataUntil = (lastYear - (lastYear - imputationStartYear)))
                
                # if (imputationTimeWindow == "all") {
                #   nonMeatData = removeNonProtectedFlag(nonMeatData, flagValidTable = modifiedFlagTable)
                # } else if (imputationTimeWindow == "lastThree") {
                #   nonMeatData = removeNonProtectedFlag(nonMeatData, flagValidTable = modifiedFlagTable, keepDataUntil = (lastYear-2))
                # } else if (imputationTimeWindow == "lastFive") {
                #   nonMeatData = removeNonProtectedFlag(nonMeatData, flagValidTable = modifiedFlagTable, keepDataUntil = (lastYear-4))
                # }
                
                nonMeatData[
                    measuredElement == nonMeatMeatFormulaParameters$areaHarvestedCode,
                    `:=`(
                        Value = NA_real_,
                        flagObservationStatus = "M",
                        flagMethod = "u"
                    )
                    ]
                
                ## Syncronize  slaughteredTransferedBackToAnimalData to the slaughtered element associated to the
                ## non-meat item
                slaughteredTransferToNonMeatChildData <-
                    transferParentToChild(
                        parentData                = slaughteredTransferedBackToAnimalData,
                        childData                 = nonMeatData,
                        transferMethodFlag        = "c",
                        imputationObservationFlag = "I",
                        mappingTable              = animalNonMeatMappingShare,
                        parentToChild             = TRUE
                    )
                
                nonMeatImputationParameters <-
                    with(currentNonMeatFormulaTable,
                         getImputationParameters(productionCode = output,
                                                 areaHarvestedCode = input,
                                                 yieldCode = productivity)
                    )
                
                ## Imputation without removing all the non protected figures for Production and carcass weight!
                
                
                ## Some checks are requested because we cannot remove all the non protected values.
                ## 1. SLAUGHTERED: synchronized
                ## 2. YIELD: to stabilize imputations I have to keep non-protected figures
                ## 3. Non-MEAT PRODUCTION: remove non-protected figures, computed as IDENTITY (where possible), IMPUTED
                
                ##slaughteredTransferToNonMeatChildDataPROD=slaughteredTransferToNonMeatChildData[measuredElement==nonMeatMeatFormulaParameters$productionCode]
                ##slaughteredTransferToNonMeatChildDataNoPROD=slaughteredTransferToNonMeatChildData[(measuredElement!=nonMeatMeatFormulaParameters$productionCode)]
                ##Remove non protected flags just for PRODUCTION
                ##slaughteredTransferToNonMeatChildDataPROD =  removeNonProtectedFlag(slaughteredTransferToNonMeatChildDataPROD)
                ##slaughteredTransferToNonMeatChildData=rbind(slaughteredTransferToNonMeatChildDataNoPROD,slaughteredTransferToNonMeatChildDataPROD)
                
                slaughteredTransferToNonMeatChildData <-
                    denormalise(
                        slaughteredTransferToNonMeatChildData,
                        denormalise = "measuredElement",
                        fillEmptyRecords = TRUE
                    )
                
                ## In addition, since the number of animal slaugheterd might have changed, we delete also  the
                ## the figures previously calculated ad identity (flagMethod="i") if also production is available
                
                ##remove those yields where both PRODUCTION and SLAUGHTERED are not NA:
                
                noNAProd <- slaughteredTransferToNonMeatChildData[,!is.na(get(nonMeatMeatFormulaParameters$productionValue))]
                
                noNASlaughterd <- slaughteredTransferToNonMeatChildData[,!is.na(get(nonMeatMeatFormulaParameters$areaHarvestedValue))]
                
                myfilter <- noNAProd & noNASlaughterd
                
                slaughteredTransferToNonMeatChildData[
                    myfilter,
                    ":="(
                        c(nonMeatMeatFormulaParameters$yieldValue,
                          nonMeatMeatFormulaParameters$yieldObservationFlag,
                          nonMeatMeatFormulaParameters$yieldMethodFlag),
                        list(NA_real_,"M","u"))
                    ]
                
                nonMeatImputed <-
                    imputeProductionTripletOriginal(
                        data                 = slaughteredTransferToNonMeatChildData,
                        processingParameters = processingParameters,
                        imputationParameters = nonMeatImputationParameters,
                        formulaParameters    = nonMeatMeatFormulaParameters
                    )
                
                nonMeatImputedList[[j]] <- normalise(nonMeatImputed)
                
                slaughteredTransferToNonMeatChildData <- rbindlist(nonMeatImputedList)
                
                slaughteredTransferToNonMeatChildData <- slaughteredTransferToNonMeatChildData[flagMethod!="u", ]
                
                
                slaughteredTransferToNonMeatChildData <- slaughteredTransferToNonMeatChildData[get(processingParameters$yearVar) %in% (lastYear - 0:(lastYear - imputationStartYear))]
                
                # if (imputationTimeWindow == "all") {
                #   slaughteredTransferToNonMeatChildData <- postProcessing(data = slaughteredTransferToNonMeatChildData)
                # } else if (imputationTimeWindow == "lastThree") {
                #   slaughteredTransferToNonMeatChildData <- slaughteredTransferToNonMeatChildData[get(processingParameters$yearVar) %in% (lastYear - 0:2)]
                # } else if (imputationTimeWindow == "lastFive") {
                #   slaughteredTransferToNonMeatChildData <- slaughteredTransferToNonMeatChildData[get(processingParameters$yearVar) %in% (lastYear - 0:4)]
                # }
                
                slaughteredTransferToNonMeatChildData <-
                    removeInvalidDates(data = slaughteredTransferToNonMeatChildData, context = sessionKey)
                
                slaughteredTransferToNonMeatChildData <-
                    postProcessing(data = slaughteredTransferToNonMeatChildData)
                
                #saving and cleaning data before any M- 
                
                if (eu_parameter == "no") {
                    
                    slaughteredTransferToNonMeatChildData=slaughteredTransferToNonMeatChildData[geographicAreaM49 %!in% eu_list, ]
                    
                    slaughtered_save <- copy(slaughteredTransferToNonMeatChildData)
                    
                    slaughtered_save[, M_first_year :=  max(as.numeric(timePointYears[flagObservationStatus %in% "M" & flagMethod %in% "-"])),
                                 by = c("geographicAreaM49","measuredItemCPC","measuredElement")]
                    
                    slaughtered_save[, to_remove := FALSE]
                    
                    slaughtered_save[as.numeric(timePointYears) < as.numeric(M_first_year) & !(flagObservationStatus %in% "M" & flagMethod %in% "-"), 
                                     to_remove := TRUE,
                                 by = c("geographicAreaM49","measuredItemCPC","measuredElement")]
                    
                    slaughtered_save <- slaughtered_save[to_remove == FALSE,]
                    
                    slaughtered_save[, to_remove := NULL]
                    
                    slaughtered_save[, M_first_year := NULL]
                    
                    SaveData(domain = sessionKey@domain,
                             dataset = sessionKey@dataset,
                             data =  slaughtered_save)
        
                    
                }else{
                    
                    slaughtered_save <- copy(slaughteredTransferToNonMeatChildData)
                    
                    slaughtered_save[, M_first_year :=  max(as.numeric(timePointYears[flagObservationStatus %in% "M" & flagMethod %in% "-"])),
                                     by = c("geographicAreaM49","measuredItemCPC","measuredElement")]
                    
                    slaughtered_save[, to_remove := FALSE]
                    
                    slaughtered_save[as.numeric(timePointYears) < as.numeric(M_first_year) & !(flagObservationStatus %in% "M" & flagMethod %in% "-"), 
                                     to_remove := TRUE,
                                     by = c("geographicAreaM49","measuredItemCPC","measuredElement")]
                    
                    slaughtered_save <- slaughtered_save[to_remove == FALSE,]
                    
                    slaughtered_save[, to_remove := NULL]
                    
                    slaughtered_save[, M_first_year := NULL]
                    
                    SaveData(domain = sessionKey@domain,
                             dataset = sessionKey@dataset,
                             data =  slaughtered_save)
                }
                #     SaveData(domain = sessionKey@domain,
                #              dataset = sessionKey@dataset,
                #              data = slaughteredTransferToNonMeatChildData)
            }
        }
        
        ## ---------------------------------------------------------------------
        
        message("\nSynchronisation and Imputation Completed for\n",
                "Animal Parent: ", currentAnimalItem, "\n",
                "Meat Child: ", currentMeatItem, "\n",
                "Non-meat Child: ", paste0(currentNonMeatItem, collapse = ", "), "\n",
                rep("-", 80), "\n")
    })
    
    ## Capture the items that failed
    if (inherits(imputationProcess, "try-error")) {
        imputationResult <-
            rbind(
                imputationResult,
                data.table(item = currentMeatItem, error = imputationProcess[iter])
            )
    }
}

if (exists("noBalanced") && nrow(noBalanced) > 0) {
    write.csv(noBalanced,tmp_file_Not_balanced_triplet)
}

if (exists("ProtectedOverwritten") &&nrow(ProtectedOverwritten) > 0) {
    write.csv(ProtectedOverwritten,tmp_file_ToBeChecked)
}
## Initiate email

body_message <- sprintf(
    "Livestock production module successfully ran.
    
    If some triplet were not balanced, please check Not_balanced_Triplet.csv file.
    
    If some protected figures have been overwritten, please check ToBeChecked.csv file")

if (!CheckDebug()) {
    send_mail(
        from <- "sws@fao.org",
        to <- swsContext.userEmail,
        subject <- "Livestock module",
        body = c(body_message,
                 tmp_file_Not_balanced_triplet,
                 tmp_file_ToBeChecked
        )
    )
}

##' ---
##' ## Return Message

if (nrow(imputationResult) > 0 & !CheckDebug()) {
    ## Initiate email
    from <- "sws@fao.org"
    to <- swsContext.userEmail
    subject <- "Imputation Result"
    body <- paste0("The following items failed, please inform the maintainer "
                   , "of the module")
    
    
    write.csv(imputationResult, tmp_file_no_ls)
    
    
    bodyWithAttachment <- tmp_file_no_ls
    send_mail(from = "no-reply@fao.org", 
              to = swsContext.userEmail,
              subject = "Imputation Result", 
              body = c(body,bodyWithAttachment))
    stop("Production imputation incomplete, check following email to see where ",
         " it failed")
}



# if (!CheckDebug()) {
# 
#   msg <- "Imputation Completed Successfully"
#   message(msg)
# 
#   ## Initiate email
#   from <- "sws@fao.org"
#   to <- swsContext.userEmail
#   subject <- "Crop-production imputation plugin has correctly run"
#   body <- paste0("Livestock production module successfully ran. You can browse results in the session: ", sessionKey@sessionId)
# 
#   send_mail(from = from, to = to, subject = subject, body = body)
#   
#   
# }

unlink(TMP_DIR, recursive = TRUE)

print("Imputation Completed Successfully")
