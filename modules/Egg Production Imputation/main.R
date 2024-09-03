#' EGGS IMPUTATION Sub-module
#' 
#' eggsItems=c("0231","0232") which correspond to their relative animalItem 
#' c("02151","02194"). The correspondence between the milk and animal item is
#' contained in the table animalMilkCorrespondence.RData file stored in the package. The module works with
#' items (both milk and animal) which share the same Elements.
#' Livestock= 5112 (heads)
#' Laying (heads) [input]
#' Eggs Prod= 5510 (tons)  [output]
#' Yiels= 5424    (kg/heads)  [productivity]
#' 
#' Intead of using the ensemble apporach 
#' 1) estimate the dairy animals when it is missed 
#'
#' 

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
    library(bit64)
    library(curl)
    library(lme4)
    library(reshape2)
    library(igraph)
    library(plyr)
    library(ggplot2)
    library(splines)
    library(sendmailR)
    
})

##' Get the shared path
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
    
    library(faoswsModules)
    SETTINGS = ReadSettings("modules/animal_stockFRANCESCA/sws.yml")
    
    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH = SETTINGS[["share"]]
    
    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])
    
    ## Get session information from SWS. Token must be obtained from web interface
    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])
    
}

##' Get data configuration and session
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)


startYear <- as.numeric(swsContext.computationParams$start_year)
#imputationStartYear <- 2000
##' Check the validity of the computational parameter
stopifnot(startYear >= 1991)
# imputationTimeWindow = swsContext.computationParams$imputation_timeWindow
# 
# if (!imputationTimeWindow %in% c("all", "lastThree", "lastFive")) {
#   stop("Incorrect imputation selection specified")
# }

lastYear=max(as.numeric(swsContext.computationParams$last_year))

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

##' Build processing parameters
processingParameters = productionProcessingParameters(datasetConfig = datasetConfig)

##' Obtain the complete imputation key

completeImputationKey = getCompleteImputationKey("production")

completeImputationKey@dimensions$timePointYears@keys <-
    as.character(min(completeImputationKey@dimensions$timePointYears@keys):lastYear)
#oldlastYear = max(as.numeric(completeImputationKey@dimensions$timePointYears@keys))

##' Here I need to take the data table directly from the SWS

animalEggsCorrespondence = ReadDatatable("animal_eggs_correspondence")

livestockItems = animalEggsCorrespondence[, animal_item_cpc]

livestockFormulaTable = getProductionFormula(itemCode = livestockItems[1])

livestockFormulaParameters <-
  with(
    livestockFormulaTable,
    productionFormulaParameters(
      datasetConfig = datasetConfig,
      productionCode = output,
      areaHarvestedCode = input,
      yieldCode = productivity,
      unitConversion = unitConversion
    )
  )

##Pull livestock numbers

completeImputationKey@dimensions$measuredItemCPC@keys = livestockItems
completeImputationKey@dimensions$measuredElement@keys = livestockFormulaParameters$productionCode

livestockData <- GetData(completeImputationKey)

## ---------------------------------------------------------------------
##Pull eggs animals

itemMap = GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredItemCPC")

eggsItems = itemMap[type == "EGGW", code]

eggsFormulaTable = getProductionFormula(itemCode = eggsItems[1])

eggsFormulaParameters <-
  with(
    eggsFormulaTable,
     productionFormulaParameters(
       datasetConfig = datasetConfig,
       productionCode = output,
       areaHarvestedCode = input,
       yieldCode = productivity,
       unitConversion = unitConversion
     )
  )

eggsAnimalKey = completeImputationKey
eggsAnimalKey@dimensions$measuredItemCPC@keys = eggsItems
eggsAnimalKey@dimensions$measuredElement@keys = eggsFormulaParameters$areaHarvestedCode

eggsAnimalData <-
  GetData(eggsAnimalKey) %>%
  preProcessing() %>%
  expandYear(newYears = lastYear)

## remove non protected flag combinations for eggs animals which is the variable that has to be imputed
## if the user has seleceted "last three years" this means that we have to keep all the data upto last three years

# if (imputationTimeWindow == "all") {
#   eggsAnimalData = removeNonProtectedFlag(eggsAnimalData)
# } else if (imputationTimeWindow == "lastThree") {
#   eggsAnimalData = removeNonProtectedFlag(eggsAnimalData, keepDataUntil = (lastYear-2))
# } else if (imputationTimeWindow == "lastFive") {
#   eggsAnimalData = removeNonProtectedFlag(eggsAnimalData, keepDataUntil = (lastYear-4))
# }

eggsAnimalData = removeNonProtectedFlag(eggsAnimalData, keepDataUntil = (lastYear - (lastYear - startYear)))
## Pull numbers
eggsNumbersKey = completeImputationKey
eggsNumbersKey@dimensions$measuredItemCPC@keys = eggsItems
eggsNumbersKey@dimensions$measuredElement@keys = "5513" # XXX parameterise

eggsNumbersData <-
  GetData(eggsNumbersKey) %>%
  preProcessing() %>%
  expandYear(newYears = lastYear)

# if (imputationTimeWindow == "all") {
#   eggsNumbersData = removeNonProtectedFlag(eggsNumbersData)
# } else if (imputationTimeWindow == "lastThree") {
#   eggsNumbersData = removeNonProtectedFlag(eggsNumbersData, keepDataUntil = (lastYear-2))
# } else if (imputationTimeWindow == "lastFive") {
#   eggsNumbersData = removeNonProtectedFlag(eggsNumbersData, keepDataUntil = (lastYear-4))
# }

eggsNumbersData = removeNonProtectedFlag(eggsNumbersData, keepDataUntil = (lastYear - (lastYear - startYear)))
##Impute eggs animals

eggsAnimalsDataToModel <-
  rbind(livestockData, eggsAnimalData) %>%
  preProcessing()

#'
#'  In this data I have 2 different Typologies of commodities:
#'  1) animals
#'  2) eggs
#'  I have to convert the animal commodities into their correspondet egg commodities
#'  02211         Chicken           0251    chicken
#'  02211         Other birds       0294    Other birds
#'

for (i in 1:length(eggsItems)) {
  eggsAnimalsDataToModel[
    measuredItemCPC == animalEggsCorrespondence[, animal_item_cpc][i],
    measuredItemCPC := animalEggsCorrespondence[, eggs_item_cpc][i]
  ]
}

eggsAnimalsDataToModel = denormalise(eggsAnimalsDataToModel,
                                     denormaliseKey = "measuredElement",
                                     fillEmptyRecords = TRUE)

# This is the dataset to build the module
eggsAnimalFinalDataToModel <-
  eggsAnimalsDataToModel[
    (!is.na(get(eggsFormulaParameters$areaHarvestedValue)) |
    !is.na(get(livestockFormulaParameters$productionValue))) &
    get(eggsFormulaParameters$areaHarvestedValue) != 0]


eggsAnimalModel = lmer(log(get(eggsFormulaParameters$areaHarvestedValue)) ~ 0 +
            timePointYears + (0 + log(get(livestockFormulaParameters$productionValue)) |
                                 measuredItemCPC / measuredItemCPC:geographicAreaM49),
        data = eggsAnimalFinalDataToModel)

## Here I am trying to use the info of geographical groups: since "other birds" may be pigeons - geese or duck and 
## it could depend on the geogrpahic area.
#   eggsAnimalsDataToModelTEST=merge(eggsAnimalsDataToModel,geoHierachy, by="geographicAreaM49")
#
#
#   eggsAnimalModel =lmer(  log(get(eggsFormulaParameters$areaHarvestedValue)) ~ 0 +
#                            timePointYears +(0 + log(get(livestockFormulaParameters$productionValue)) | measuredItemCPC:geographicAreaM49:geoParent),
#                        data = eggsAnimalsDataToModelTEST)
#

# I am excluding those items where no livestock number exists.
# XXX: note that there are (some) cases where:
# eggsAnimalFinalDataToModel[Value_measuredElement_5313 > Value_measuredElement_5112]
eggsAnimalsDataToModel = eggsAnimalsDataToModel[!is.na(get(livestockFormulaParameters$productionValue))]    

## ---------------------------------------------------------------------
#eggsAnimalsDataToModel=merge(eggsAnimalsDataToModel, geoHierachy, by="geographicAreaM49")
## ---------------------------------------------------------------------
eggsAnimalsDataToModel[, predicted := exp(predict(eggsAnimalModel,
                                                    newdata = eggsAnimalsDataToModel,
                                                    allow.new.levels = TRUE))]

toBeImputed <-
  eggsAnimalsDataToModel[, get(eggsFormulaParameters$areaHarvestedObservationFlag)] == processingParameters$missingValueObservationFlag &
  eggsAnimalsDataToModel[, get(eggsFormulaParameters$areaHarvestedMethodFlag)] == processingParameters$missingValueMethodFlag &
  !is.na(eggsAnimalsDataToModel[, predicted])

eggsAnimalsDataToModel[
  toBeImputed,
  `:=`(
    Value_measuredElement_5313 = predicted,
    flagObservationStatus_measuredElement_5313 = processingParameters$imputationObservationFlag,
    flagMethod_measuredElement_5313 = processingParameters$imputationMethodFlag
  )
]

# TODO: here we should do something else when:
# eggsAnimalsDataToModel[Value_measuredElement_5313 > Value_measuredElement_5112]

eggsAnimalsDataToModel[, predicted := NULL]

eggsAnimals = normalise(eggsAnimalsDataToModel)

eggsAnimals = eggsAnimals[measuredElement == eggsFormulaParameters$areaHarvestedCode]

## ---------------------------------------------------------------------
##Pull eggs production

completeImputationKey@dimensions$measuredElement@keys = eggsFormulaParameters$productionCode
completeImputationKey@dimensions$measuredItemCPC@keys = eggsItems

eggsProductionData <-
  GetData(completeImputationKey) %>%
  preProcessing() %>%
  expandYear(newYears = lastYear)

## Isolate those series for which egg items already exist:
## The linear mixed model creates milk production wherever livestock number exist.
## Here we want to be sure to not create a series from scratch
## Even if: in teory if we have livestock numbers whe should have milk production.
## ---------------------------------------------------------------------
existingSeries = unique(eggsProductionData[, .(geographicAreaM49, measuredItemCPC)])
## ---------------------------------------------------------------------

# if (imputationTimeWindow == "all") {
#   eggsProductionData = removeNonProtectedFlag(eggsProductionData)
# } else if (imputationTimeWindow == "lastThree") {
#   eggsProductionData = removeNonProtectedFlag(eggsProductionData, keepDataUntil = (lastYear-2))
# } else if (imputationTimeWindow == "lastFive") {
#   eggsProductionData = removeNonProtectedFlag(eggsProductionData, keepDataUntil = (lastYear-4))
# }

eggsProductionData = removeNonProtectedFlag(eggsProductionData, keepDataUntil = (lastYear - (lastYear - startYear)))
#eggsProductionData = removeNonProtectedFlag(eggsProductionData)

eggsProductionDataToModel <-
  rbind(eggsAnimals, eggsProductionData, eggsNumbersData) %>%
  preProcessing() %>%
  denormalise(denormaliseKey = "measuredElement", fillEmptyRecords = TRUE)

# We are going to use now the (protected) number of eggs, if reported,
# to calculate production based on a table with conversion factors


eggs_conversion_factors <- ReadDatatable("eggs_technical_conversion_factors")

stopifnot(nrow(eggs_conversion_factors) > 0)

eggs_conversion_factors <-
  eggs_conversion_factors[,
    .(geographicAreaM49 = m49, measuredItemCPC = item, avg_weight)
  ]

eggsProductionDataToModel <-
  eggs_conversion_factors[
    eggsProductionDataToModel,
    on = c('geographicAreaM49', 'measuredItemCPC')
  ][,
    # {(5513 * 1000) * [avg_weight / 1000]} / 1000
    # () = numbers, [] = kilos per number, {} = kilos
    imputed_prod := Value_measuredElement_5513 * avg_weight / 1000 # XXX parameterise 5513?
  ]

eggsProductionToBeImputed_with_avg <-
  eggsProductionDataToModel[, get(eggsFormulaParameters$productionObservationFlag)] == processingParameters$missingValueObservationFlag &
  eggsProductionDataToModel[, get(eggsFormulaParameters$productionMethodFlag)] == processingParameters$missingValueMethodFlag &
  !is.na(eggsProductionDataToModel[, imputed_prod])

eggsProductionDataToModel[
  eggsProductionToBeImputed_with_avg,
  `:=`(
    Value_measuredElement_5510 = imputed_prod,
    flagObservationStatus_measuredElement_5510 = "T",
    flagMethod_measuredElement_5510 = "i"
  )
][,
  imputed_prod := NULL
]

# NOTE: we use below the production imputed above also in the model
eggsProdFinalDataToModel <-
  eggsProductionDataToModel[
    (!is.na(get(eggsFormulaParameters$areaHarvestedValue)) |
    !is.na(get(eggsFormulaParameters$productionValue))) &
    get(eggsFormulaParameters$productionValue) != 0]

eggsLmeModel =
    lmer(log(get(eggsFormulaParameters$productionValue)) ~ 0 +
            timePointYears + (0 + log(get(eggsFormulaParameters$areaHarvestedValue))
                              | measuredItemCPC / measuredItemCPC:geographicAreaM49),
        data = eggsProdFinalDataToModel
    )

eggsProductionDataToModel = eggsProductionDataToModel[!is.na(get(eggsFormulaParameters$areaHarvestedValue))]

eggsProductionDataToModel[,
  predicted := exp(predict(eggsLmeModel, newdata = eggsProductionDataToModel, allow.new.levels = TRUE))
]

eggsProductionToBeImputed <-
  eggsProductionDataToModel[, get(eggsFormulaParameters$productionObservationFlag)] == processingParameters$missingValueObservationFlag &
  eggsProductionDataToModel[, get(eggsFormulaParameters$productionMethodFlag)] == processingParameters$missingValueMethodFlag &
  !is.na(eggsProductionDataToModel[, predicted])

eggsProductionDataToModel[
  eggsProductionToBeImputed,
  `:=`(
    Value_measuredElement_5510 = predicted,
    flagObservationStatus_measuredElement_5510 = processingParameters$imputationObservationFlag,
    flagMethod_measuredElement_5510 = processingParameters$imputationMethodFlag
  )
]

eggsProductionFinalImputedData = normalise(eggsProductionDataToModel)

## ---------------------------------------------------------------------
## Finalize the triplet computing the eggs-yield
completeImputationKey@dimensions$measuredElement@keys = eggsFormulaParameters$yieldCode
completeImputationKey@dimensions$measuredItemCPC@keys = eggsItems

eggsYieldProductionData = GetData(completeImputationKey)
# 
# if (imputationTimeWindow == "all") {
#   eggsYieldProductionData = removeNonProtectedFlag(eggsYieldProductionData)
# } else if (imputationTimeWindow == "lastThree") {
#   eggsYieldProductionData = removeNonProtectedFlag(eggsYieldProductionData, keepDataUntil = (lastYear-2))
# } else if (imputationTimeWindow == "lastFive") {
#   eggsYieldProductionData = removeNonProtectedFlag(eggsYieldProductionData, keepDataUntil = (lastYear-4))
# }

eggsYieldProductionData = removeNonProtectedFlag(eggsYieldProductionData, keepDataUntil = (lastYear - (lastYear - startYear)))
#eggsYieldProductionData = removeNonProtectedFlag(eggsYieldProductionData)

completeTriplet <-
  rbind(eggsProductionFinalImputedData, eggsYieldProductionData) %>%
  denormalise(denormaliseKey = "measuredElement") %>%
  fillRecord()

##compute yield where necessary:
eggsYieldToBeImputed = is.na(completeTriplet[, get(eggsFormulaParameters$yieldValue)])

completeTriplet[
  eggsYieldToBeImputed,
  Value_measuredElement_5424 := ( get(eggsFormulaParameters$productionValue) / get(eggsFormulaParameters$areaHarvestedValue)) * 1000
]

eggsYieldImputedFlags = eggsYieldToBeImputed & !is.na(completeTriplet[, get(eggsFormulaParameters$yieldValue)])

completeTriplet[
  eggsYieldImputedFlags,
  `:=`(
    flagObservationStatus_measuredElement_5424 =
      aggregateObservationFlag(
        get(eggsFormulaParameters$productionObservationFlag),
        get(eggsFormulaParameters$areaHarvestedObservationFlag)
      ),
    flagMethod_measuredElement_5424 = processingParameters$balanceMethodFlag
  )
]

#Manage (M,-)

## if animal milk numbers are (M,-)

MdashEggsAnimal <-
  completeTriplet[, get(eggsFormulaParameters$areaHarvestedObservationFlag)] == "M" &
  completeTriplet[, get(eggsFormulaParameters$areaHarvestedMethodFlag)] == "-"

completeTriplet[
  MdashEggsAnimal,
  `:=`(
    c(
      eggsFormulaParameters$yieldValue,
      eggsFormulaParameters$yieldObservationFlag,
      eggsFormulaParameters$yieldMethodFlag
    ),
    list(NA_real_, "M", "-")
  )
]

completeTriplet[
  MdashEggsAnimal & flagObservationStatus_measuredElement_5513 == "M" & flagMethod_measuredElement_5513 == "u",
  flagMethod_measuredElement_5513 := "-"
]


## if production of milk numbers are (M,-)
MdashEggsProd <-
  completeTriplet[, get(eggsFormulaParameters$productionObservationFlag)] == "M" &
  completeTriplet[, get(eggsFormulaParameters$productionMethodFlag)] == "-"

completeTriplet[
  MdashEggsProd,
  `:=`(
    c(
      eggsFormulaParameters$yieldValue,
      eggsFormulaParameters$yieldObservationFlag,
      eggsFormulaParameters$yieldMethodFlag),
    list(NA_real_, "M", "-")
  )
]

completeTriplet[
  MdashEggsProd & flagObservationStatus_measuredElement_5513 == "M" & flagMethod_measuredElement_5513 == "u",
  flagMethod_measuredElement_5513 := "-",
]


# Estimation of missing numbers
completeTriplet <-
  eggs_conversion_factors[
    completeTriplet,
    on = c('geographicAreaM49', 'measuredItemCPC')
  ]

numbersToBeEstimated <-
    completeTriplet$flagObservationStatus_measuredElement_5513 == "M" &
    completeTriplet$flagMethod_measuredElement_5513 == "u" &
    !is.na(completeTriplet$Value_measuredElement_5510) &
    !is.na(completeTriplet$avg_weight)

completeTriplet[
  numbersToBeEstimated,
  `:=`(
    Value_measuredElement_5513 = (Value_measuredElement_5510 * 1000) / avg_weight,
    flagObservationStatus_measuredElement_5513 = "I",
    # XXX method actually shouldn't be always "e", sometimes "i"
    flagMethod_measuredElement_5513 = "e"
  )
][,
 avg_weight := NULL
]


## ---------------------------------------------------------------------
## Push data back into the SWS

##Add some checks from the ensure package!

## ensureProtectedData
## ensureProductionOutput

# XXX: before normalisation, we need to handle cases where:
# Value_measuredElement_5510 / Value_measuredElement_5313 * 1000 != Value_measuredElement_5424
# (with some tolerance, say 1%)

completeTriplet <-
  normalise(completeTriplet) %>%
  removeInvalidDates(context = sessionKey) %>%
  postProcessing()

#ensureProtectedData(completeTriplet,domain = "agriculture", "aproduction",getInvalidData = TRUE)

ensureProductionOutputs(
    completeTriplet,
    processingParameters = processingParameters,
    formulaParameters =  eggsFormulaParameters,
    normalised = TRUE,
    testImputed = FALSE,
    testCalculated = TRUE,
    returnData = FALSE
)

completeTriplet <-
  completeTriplet[
    flagMethod == "i" | (flagObservationStatus == "I" & flagMethod == "e") | (flagObservationStatus == "M" & flagMethod == "-")
  ][
    existingSeries,
    on = c("geographicAreaM49", "measuredItemCPC")
  ][
    !is.na(measuredElement)
  ]

data_to_save <- copy(completeTriplet)

data_to_save=data_to_save[timePointYears %in% as.character(startYear:lastYear), ]
#%!in%

if (eu_parameter == "no") {

data_to_save=data_to_save[geographicAreaM49 %!in% eu_list, ]

}
# 
# if (imputationTimeWindow == "lastThree") {
#   completeTriplet = completeTriplet[timePointYears %in% (lastYear - 0:2)]
# } else if (imputationTimeWindow == "lastFive") {
#   completeTriplet = completeTriplet[timePointYears %in% (lastYear - 0:4)]
# }

SaveData(domain = sessionKey@domain, 
         dataset = sessionKey@dataset,
         data = data_to_save) 
 

if (!CheckDebug()) {
    ## Initiate email
    # from = "sws@fao.org"
    # to = swsContext.userEmail
    # subject = "Eggs production module"
    # body = paste0("Eggs production module successfully ran. You can browse results in the session: ", sessionKey@sessionId )
    # sendmail(from = from, to = to, subject = subject, msg = body)
}

message("Module finished successfully")
