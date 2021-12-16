message("plug-in starts to run")

# Import libraries
suppressMessages({
    library(data.table)
    library(faosws)
    library(faoswsFlag)
    library(faoswsUtil)
    library(faoswsImputation)
    library(faoswsProduction)
    library(faoswsProcessing)

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

`%!in%` = Negate(`%in%`)

startYear = as.numeric(swsContext.computationParams$start_year)
endYear = as.numeric(swsContext.computationParams$end_year)
# Get data configuration and session
sessionKey = swsContext.datasets[[1]]

originalData = GetData(sessionKey)



data <- copy(originalData)

data <- data[measuredItemCPC %in% "0231",]


data[, flagObs_5513 := flagObservationStatus[measuredElement %in% "5513"], 
     by = c("geographicAreaM49","measuredItemCPC", "timePointYears")]

data[, flagMeth_5513 := flagMethod[measuredElement %in% "5513"], 
     by = c("geographicAreaM49","measuredItemCPC", "timePointYears")]


data[, flagObs_5510 := flagObservationStatus[measuredElement %in% "5510"], 
     by = c("geographicAreaM49","measuredItemCPC", "timePointYears")]

data[, flagMeth_5510 := flagMethod[measuredElement %in% "5510"], 
     by = c("geographicAreaM49","measuredItemCPC", "timePointYears")]


data[flagObs_5513 %in% "E" & flagMeth_5513 %in% "f" & flagObs_5510 %in% "T" & flagMeth_5510 %in% "i" 
     & measuredElement %in% "5510", flagObservationStatus:= "E"]


data[flagObs_5513 %in% "T" & flagMeth_5513 %in% "i" & flagObs_5510 %in% "E" & flagMeth_5510 %in% "f" 
     & measuredElement %in% "5513", flagObservationStatus:= "E"]

data[flagObs_5510 %in% "" & flagObs_5513 %!in% c("","T","E") 
     & measuredElement %in% "5513", `:=` (flagObservationStatus = "T", flagMethod = "i")]

data[flagObs_5510 %in% c("T", "E") & flagObs_5513 %!in% c("","T","E") 
     & measuredElement %in% "5513", `:=` (flagObservationStatus = flagObs_5510, flagMethod = "i")]

data[, c("flagObs_5513","flagMeth_5513","flagObs_5510","flagMeth_5510"):=NULL]

#to be parametrized
data <- data[timePointYears %in% as.character(startYear:endYear),]

datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

SaveData(domain = datasetConfig$domain,
         dataset = datasetConfig$dataset,
         data = data, waitTimeout = 2000000)


print('Plug-in Completed')