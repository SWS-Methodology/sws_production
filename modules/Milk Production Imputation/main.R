#' MILK IMPUTATION Sub-module
#'
#' milkItems=c("02211","02212","02291","02292","02293") which correspond to their relative animalItem
#' c("02111","02112","02122","02123","02121.01"). The correspondence between the milk and animal item is
#' contained in the table animalMilkCorrespondence.RData file stored in the package. The module works with
#' items (both milk and animal) which share the same Elements.
#' Livestock= 5111 (heads)
#' Milking animals (heads) [input]
#' Milk Prod= 5510 (tons)  [output]
#' Yiels= 5417 (kg/heads)  [productivity]
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
# if(!imputationTimeWindow %in% c("all", "lastThree"))
#     stop("Incorrect imputation selection specified")


##' Build processing parameters
processingParameters =
    productionProcessingParameters(datasetConfig = datasetConfig)


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


##' Obtain the complete imputation key
completeImputationKey = getCompleteImputationKey("production")
completeImputationKey@domain <- sessionKey@domain
completeImputationKey@dataset <- sessionKey@dataset


completeImputationKey@dimensions$timePointYears@keys <-
    as.character(min(completeImputationKey@dimensions$timePointYears@keys):lastYear)

# lastYear=max(as.numeric(completeImputationKey@dimensions$timePointYears@keys))

animalMilkCorrespondence=ReadDatatable("animal_milk_correspondence")


livestockItems=animalMilkCorrespondence[,animal_item_cpc]


livestockFormulaTable =
    getProductionFormula(itemCode = livestockItems[1])



livestockFormulaParameters =
    with(livestockFormulaTable,
         productionFormulaParameters(datasetConfig = datasetConfig,
                                     productionCode = output,
                                     areaHarvestedCode = input,
                                     yieldCode = productivity,
                                     unitConversion = unitConversion)
    )


##Pull livestock numbers

completeImputationKey@dimensions$measuredItemCPC@keys= livestockItems
completeImputationKey@dimensions$measuredElement@keys= livestockFormulaParameters$productionCode
message("Get animal stock data")
livestockData=GetData(completeImputationKey)

## ---------------------------------------------------------------------
##Pull milking animals
##Get the milk triplet
itemMap = GetCodeList(domain = sessionKey@domain, dataset = sessionKey@dataset, "measuredItemCPC")
milkItems=itemMap[type=="MILK",code]

##All the milk items shares the same triplet
milkFormulaTable=
    getProductionFormula(itemCode = milkItems[1])

milkFormulaParameters =
    with(
        milkFormulaTable,
        productionFormulaParameters(
            datasetConfig = datasetConfig,
            productionCode = output,
            areaHarvestedCode = input,
            yieldCode = productivity,
            unitConversion = unitConversion
        )
    )

##Pull the elements of the milk items one by one


message("Get milking animal data")
milkKey=completeImputationKey
milkKey@dimensions$measuredItemCPC@keys= milkItems
milkKey@dimensions$measuredElement@keys= milkFormulaParameters$areaHarvestedCode

milkingAnimalData=GetData(milkKey)
milkingAnimalData=preProcessing(data = milkingAnimalData) 
milkingAnimalData=expandYear(milkingAnimalData, newYears=lastYear)
## remove non protected flag combinations for milk animals which is the variable that has to be imputed
## if the user has seleceted "last three years" this means that we have to keep all the data upto last three years

# 
# if(imputationTimeWindow=="all"){milkingAnimalData=removeNonProtectedFlag(milkingAnimalData)}else{
#     milkingAnimalData=removeNonProtectedFlag(milkingAnimalData, keepDataUntil = (lastYear-2))}

milkingAnimalData=removeNonProtectedFlag(milkingAnimalData, keepDataUntil = (lastYear - (lastYear - startYear)),
                                         flagValidTable = ReadDatatable("valid_flags_ocs2023"))

message("Impute milking animals")
milkingAnimalsDataToModel =rbind(livestockData, milkingAnimalData )
milkingAnimalsDataToModel=preProcessing(milkingAnimalsDataToModel)
#'
#'  In this dataset I have 2 different Typologies of commodities:
#'  1) animals
#'  2) milk
#'  I have to convert the animal commoties into their correspondet milk commodities
#'  02211         Raw milk of cattle           02111    Cattle
#'  02212         Raw milk of buffalo          02112    Buffalo
#'  02291         Raw milk of sheep            02122    Sheep
#'  02292         Raw milk of goats            02123    Goats
#'  02293         Raw milk of camel            02121.01 Camels
#'

## In this dataset we have both animal and milking animal, but they are stored on different codes:
## because the milking animals are stored as input of the the milk item and "it is recorder as milk".
## this loop transform the livestock commodity into the corresponding milk commodity because now we have to 
## use the livestock number to impute the number of milking animals:

for(i in seq(milkItems)){
    milkingAnimalsDataToModel[measuredItemCPC== animalMilkCorrespondence[,animal_item_cpc][i], measuredItemCPC:=animalMilkCorrespondence[,milk_item_cpc][i]]
}

## ---------------------------------------------------------------------


milkingAnimalsDataToModel=denormalise(milkingAnimalsDataToModel,denormaliseKey="measuredElement", fillEmptyRecords=TRUE)

milkingFinalDataToModel=milkingAnimalsDataToModel[!is.na(get(milkFormulaParameters$areaHarvestedValue))]
milkingFinalDataToModel=milkingFinalDataToModel[!is.na(get(livestockFormulaParameters$productionValue))]

## This condition might be retundant.. why should it be zero?
milkingFinalDataToModel=milkingAnimalsDataToModel[get(milkFormulaParameters$areaHarvestedValue)!=0]


milkingAnimalModel =
    lmer(log(get(milkFormulaParameters$areaHarvestedValue)) ~ 0+
             timePointYears +
             (0+log(get(livestockFormulaParameters$productionValue))|measuredItemCPC/measuredItemCPC:geographicAreaM49),
         data = milkingFinalDataToModel)
## I had to filter out those lines where livestock numbers are not available and consequently milking animals cannot be computed
## This issue arose when I had to go back to lme4 1.1-7 instead of 1.1-12

milkingAnimalsDataToModel=milkingAnimalsDataToModel[!is.na(get(livestockFormulaParameters$productionValue))]

milkingAnimalsDataToModel[, predicted:=exp(predict(milkingAnimalModel,
                                                   newdata = milkingAnimalsDataToModel,
                                                   allow.new.levels = TRUE))]


toBeImputed=milkingAnimalsDataToModel[,get(milkFormulaParameters$areaHarvestedObservationFlag) ]==processingParameters$missingValueObservationFlag &
    milkingAnimalsDataToModel[,get(milkFormulaParameters$areaHarvestedMethodFlag) ]==processingParameters$missingValueMethodFlag &
    !is.na(milkingAnimalsDataToModel[,predicted])

##milkingAnimalsDataToModel[toBeImputed, get(milkFormulaParameters$areaHarvestedValue):= predicted]
##milkingAnimalsDataToModel[toBeImputed, get(milkFormulaParameters$areaHarvestedObservationFlag) :="I"]
##milkingAnimalsDataToModel[toBeImputed, get(milkFormulaParameters$areaHarvestedMethodFlag) :="e"]

## I should generalize this part of code:
milkingAnimalsDataToModel[toBeImputed,":="(c("Value_measuredElement_5318",
                                             "flagObservationStatus_measuredElement_5318",
                                             "flagMethod_measuredElement_5318"),list(predicted,
                                                                                   processingParameters$imputationObservationFlag,
                                                                                   processingParameters$imputationMethodFlag))]

#milkingAnimalsDataToModel[toBeImputed, Value_measuredElement_5318:= predicted]
#milkingAnimalsDataToModel[toBeImputed, flagObservationStatus_measuredElement_5318 :=processingParameters$imputationObservationFlag]
#milkingAnimalsDataToModel[toBeImputed, flagMethod_measuredElement_5318 :=processingParameters$imputationMethodFlag]

##If the livestock number are 0 ot (M,-), also milking animals should be flagged as (M,-)

milkingAnimalsDataToModel[,predicted:=NULL]

milkingAnimals=normalise(milkingAnimalsDataToModel)

milkingAnimals=milkingAnimals[measuredElement==milkFormulaParameters$areaHarvestedCode ]

##Keep just the series that had milking animals 
##milkingAnimals=milkingAnimals[filterMilkingAnimalData,,on=c("geographicAreaM49" ,"measuredElement" ,"measuredItemCPC", "timePointYears")]

##Pull milk production

completeImputationKey@dimensions$measuredElement@keys= c(milkFormulaParameters$productionCode)
completeImputationKey@dimensions$measuredItemCPC@keys=milkItems
milkProductionData=GetData(completeImputationKey)
milkProductionData=preProcessing(data = milkProductionData) 
milkProductionData=expandYear(milkProductionData, newYears=lastYear)

## Isolate those series for which milk items already exist:
## The linear mixed model creates milk production wherever livestock number exist.
## Here we want to be sure to not create a series from scratch
## Even if: in teory if we have livestock numbers whe should have milk production.
## ---------------------------------------------------------------------
existingSeries = unique(milkProductionData[,.(geographicAreaM49 , measuredItemCPC )])
## ---------------------------------------------------------------------


# if(imputationTimeWindow=="all"){milkProductionData=removeNonProtectedFlag(milkProductionData)}else{
#     milkProductionData=removeNonProtectedFlag(milkProductionData, keepDataUntil = (lastYear-2))}

milkProductionData=removeNonProtectedFlag(milkProductionData, keepDataUntil = (lastYear - (lastYear - startYear)),
                                          flagValidTable = ReadDatatable("valid_flags_ocs2023"))

#milkProductionData= removeNonProtectedFlag(milkProductionData)


milkProductionDataToModel=rbind(milkingAnimals,
                                milkProductionData)


milkProductionDataToModel=preProcessing(milkProductionDataToModel)
milkProductionDataToModel=denormalise(milkProductionDataToModel,denormaliseKey="measuredElement", fillEmptyRecords = TRUE)




milkProdFinalDataToModel=milkProductionDataToModel[!is.na(get(milkFormulaParameters$areaHarvestedValue))]
milkProdFinalDataToModel=milkProdFinalDataToModel[!is.na(get(milkFormulaParameters$productionValue))]
milkProdFinalDataToModel=milkProdFinalDataToModel[get(milkFormulaParameters$productionValue)!=0]


milkLmeModel =
    lmer(log(get(milkFormulaParameters$productionValue)) ~ 0+
             timePointYears +
             (0+log(get(milkFormulaParameters$areaHarvestedValue))|measuredItemCPC/measuredItemCPC:geographicAreaM49),
         data = milkProdFinalDataToModel)


milkProductionDataToModel=milkProductionDataToModel[!is.na(get(milkFormulaParameters$areaHarvestedValue))]
milkProductionDataToModel[, predicted:=exp(predict(milkLmeModel,
                                                   newdata = milkProductionDataToModel,
                                                   allow.new.levels = TRUE))]


#Copy (M,-) in the series of production, if milking animals are flagged as (M,-)
areaHArvestedMDash=milkProductionDataToModel[,get(milkFormulaParameters$areaHarvestedObservationFlag)]=="M" &
                   milkProductionDataToModel[,get(milkFormulaParameters$areaHarvestedMethodFlag)]=="-" 
milkProductionToBeImputed=milkProductionDataToModel[areaHArvestedMDash,":="(c("Value_measuredElement_5510","flagObservationStatus_measuredElement_5510","flagMethod_measuredElement_5510"), list(NA_real_,"M","-") ) ]


milkProductionToBeImputed=milkProductionDataToModel[,get(milkFormulaParameters$productionObservationFlag)]==processingParameters$missingValueObservationFlag &
    milkProductionDataToModel[,get(milkFormulaParameters$productionMethodFlag)]==processingParameters$missingValueMethodFlag &
    !is.na(milkProductionDataToModel[,predicted])
## ---------------------------------------------------------------------

## I should generalize this part of code:
milkProductionDataToModel[milkProductionToBeImputed, Value_measuredElement_5510 := predicted]
milkProductionDataToModel[milkProductionToBeImputed, flagObservationStatus_measuredElement_5510 := processingParameters$imputationObservationFlag]
milkProductionDataToModel[milkProductionToBeImputed, flagMethod_measuredElement_5510 := processingParameters$imputationMethodFlag]


milkProductionFinalImputedData=normalise(milkProductionDataToModel)
## ---------------------------------------------------------------------
## Finalize the triplet computing the milk-yield
completeImputationKey@dimensions$measuredElement@keys= c(milkFormulaParameters$yieldCode)
#pull yield data
completeImputationKey@dimensions$measuredItemCPC@keys= milkItems
milkYieldProductionData=GetData(completeImputationKey)


#Remove non protected flags
# 
# if(imputationTimeWindow=="all"){milkYieldProductionData=removeNonProtectedFlag(milkYieldProductionData)}else{
#     milkYieldProductionData=removeNonProtectedFlag(milkYieldProductionData, keepDataUntil = (lastYear-2))}

milkYieldProductionData=removeNonProtectedFlag(milkYieldProductionData, keepDataUntil = (lastYear - (lastYear - startYear)),
                                               flagValidTable = ReadDatatable("valid_flags_ocs2023"))

#milkYieldProductionData= removeNonProtectedFlag(milkYieldProductionData)

milkYieldProductionData= remove0M(milkYieldProductionData, valueVars = "Value",flagVars = "flagObservationStatus")

completeTriplet=rbind(milkProductionFinalImputedData,
                      milkYieldProductionData)

completeTriplet=denormalise(completeTriplet, denormaliseKey = "measuredElement")

completeTriplet=fillRecord(completeTriplet)


##compute yield where necessary:
milkYieldToBeImputed= is.na(completeTriplet[,get(milkFormulaParameters$yieldValue)])

completeTriplet[milkYieldToBeImputed, Value_measuredElement_5417:=(get(milkFormulaParameters$productionValue)/get(milkFormulaParameters$areaHarvestedValue))*1000]

milkYieldImputedFlags = milkYieldToBeImputed & !is.na(completeTriplet[,get(milkFormulaParameters$yieldValue)])

completeTriplet[milkYieldImputedFlags, flagObservationStatus_measuredElement_5417:=aggregateObservationFlag(get(milkFormulaParameters$productionObservationFlag),
                                                                                                            get(milkFormulaParameters$areaHarvestedObservationFlag),
                                                                                                            flagTable = ReadDatatable("ocs2023_flagweight"))]

completeTriplet[milkYieldImputedFlags, flagMethod_measuredElement_5417:=processingParameters$balanceMethodFlag]



#Manage (M,-)

## if animal milk numbers are (M,-)

MdashMilkingAnimal=completeTriplet[,get(milkFormulaParameters$areaHarvestedObservationFlag)]=="M"&
    completeTriplet[,get(milkFormulaParameters$areaHarvestedMethodFlag)]=="-"


completeTriplet[MdashMilkingAnimal,":="(c(milkFormulaParameters$yieldValue,milkFormulaParameters$yieldObservationFlag,milkFormulaParameters$yieldMethodFlag),
                                        list(NA_real_,"M", "-"))]


## if production of milk numbers are (M,-)
MdashMilkProd=completeTriplet[,get(milkFormulaParameters$productionObservationFlag)]=="M"&
    completeTriplet[,get(milkFormulaParameters$productionMethodFlag)]=="-"

completeTriplet[MdashMilkProd,":="(c(milkFormulaParameters$yieldValue,milkFormulaParameters$yieldObservationFlag,milkFormulaParameters$yieldMethodFlag),
                                   list(NA_real_,"M", "-"))]


## ---------------------------------------------------------------------
## Push data back into the SWS


completeTriplet=normalise(completeTriplet)
completeTriplet=removeInvalidDates(data = completeTriplet, context = sessionKey)
completeTriplet=postProcessing (completeTriplet)

##Check if we are overwriting any protected


ensureProductionOutputs(completeTriplet,
                        processingParameters = processingParameters,
                        formulaParameters =  milkFormulaParameters,
                        normalised = TRUE,
                        testImputed = FALSE,
                        testCalculated =TRUE,
                        returnData  = FALSE)





completeTriplet=completeTriplet[flagMethod == "i" |(flagObservationStatus == "I" & flagMethod == "e")|
                                        (flagObservationStatus == "M" & flagMethod == "-"),]

completeTriplet=completeTriplet[existingSeries, , on=c("geographicAreaM49", "measuredItemCPC")]
completeTriplet=completeTriplet[!is.na(measuredElement)]

#protectedFigures=ensureProtectedData(completeTriplet,domain = "agriculture", "aproduction",getInvalidData = TRUE)
#protectedFigures=protectedFigures[flagCombination!="(M, -)"]
data_to_save <- copy(completeTriplet)

data_to_save=data_to_save[timePointYears %in% as.character(startYear:lastYear), ]


if (eu_parameter == "no") {
    
    data_to_save=data_to_save[geographicAreaM49 %!in% eu_list, ]
    
}

SaveData(domain = sessionKey@domain, 
         dataset = sessionKey@dataset,
         data = data_to_save) 


# if(imputationTimeWindow=="lastThree"){
#     completeTriplet=completeTriplet[timePointYears %in% c(lastYear, lastYear-1, lastYear-2), ]
#     SaveData(domain = sessionKey@domain, 
#              dataset = sessionKey@dataset,
#              data = completeTriplet) 
#     
# }else{
#     SaveData(domain = sessionKey@domain, 
#              dataset = sessionKey@dataset,
#              data = completeTriplet)
# }

if(!CheckDebug()){
    ## Initiate email
    from = "sws@fao.org"
    to = swsContext.userEmail
    subject = "Milk production module"
    body = paste0("Milk production module successfully ran. You can browse results in the session: ", sessionKey@sessionId )
    sendmail(from = from, to = to, subject = subject, msg = body)
}

message("Module finished successfully")
