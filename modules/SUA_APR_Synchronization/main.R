##' 
##'
##' **Author: Amsata Niang**
##' 
##' **Description:**
##'
##' This module is designed to synchronize production data from SUA balance to agriv=culture domain
##'

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
  library(faoswsEnsure)
  library(magrittr)
  library(dplyr)
  library(sendmailR)
  library(faoswsStandardization)
})

options(scipen = 999)
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
    
    library(faoswsModules)
    SETTINGS = ReadSettings("modules\\SUA_APR_Synchronization\\sws.yml")
    
    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH = SETTINGS[["share"]]
    
    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])
    
    ## Get session information from SWS. Token must be obtained from web interface
    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])
    
}

startYear = as.numeric(swsContext.computationParams$start_year)
endYear = as.numeric(swsContext.computationParams$end_year)
#geoM49 = swsContext.computationParams$geom49
stopifnot(startYear <= endYear)
yearVals = startYear:endYear

`%!in%`<-Negate(`%in%`)

Utilization_Table <- ReadDatatable("utilization_table_2018")
primary_items <- Utilization_Table[primary_item == 'X', cpc_code]

# Get production data from SUA domain

sessionKey = swsContext.datasets[[2]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

prod_sua <- GetData(sessionKey)
prod_sua<-prod_sua[flagObservationStatus %in% c("","E","T"),]
prod_sua<-prod_sua[flagMethod %!in% c("c","i","t","e"),]
prod_sua<-prod_sua[measuredItemCPC %in% primary_items,]

# Get production data from production domain

selectedGEOCode =
    getQueryKey("geographicAreaM49", sessionKey)

geoKeys = GetCodeList(domain = "agriculture", dataset = "aproduction",
                      dimension = "geographicAreaM49")[type == "country", code]

message("Pulling data from Agriculture Production")

## if the 

geoDim = Dimension(name = "geographicAreaM49", keys = selectedGEOCode)


eleKeys = GetCodeTree(domain = "agriculture", dataset = "aproduction",
                      dimension = "measuredElement")
## Get all children of old codes
# oldProductionCode = "51"
# eleKeys = strsplit(eleKeys[parent %in% c(oldProductionCode), children],
#                    split = ", ")
## Combine with single codes
eleDim = Dimension(name = "measuredElement", keys = "5510"
                                                      # ,industrialCode
                                                      )
# itemKeys = GetCodeList(domain = "agriculture", dataset = "aproduction",
#                        dimension = "measuredItemCPC")[, code]

itemKeys=primary_items

itemDim = Dimension(name = "measuredItemCPC", keys = itemKeys)
timeDim = Dimension(name = "timePointYears", keys = as.character(yearVals))
agKey = DatasetKey(domain = "agriculture", dataset = "aproduction",
                   dimensions = list(
                       geographicAreaM49 = geoDim,
                       measuredElement = eleDim,
                       measuredItemCPC = itemDim,
                       timePointYears = timeDim)
)
                          

prod_apr <- GetData(agKey)


#Harmonize the colunm name
names(prod_sua)<-names(prod_apr)

#Select production data for the selected year range
prod_apr<-prod_apr[timePointYears %in% yearVals & measuredElement=="5510",]
prod_sua<-prod_sua[timePointYears %in% yearVals & measuredElement=="5510",]





prod_apr[,Flag:=paste0(flagObservationStatus,",",flagMethod)]


synch_data<-merge(
    prod_apr[,list(geographicAreaM49,
                   measuredElement,
                   measuredItemCPC,
                   timePointYears,Value,
                   Flag)],
    prod_sua[,list(geographicAreaM49,
                   measuredElement,
                   measuredItemCPC,
                   timePointYears,
                   prod_sua=Value,
                   flagObservationStatus,
                   flagMethod)],
    by=c("geographicAreaM49",
         "measuredElement",
         "measuredItemCPC",
         "timePointYears")
)

synch_data[,check:=dplyr::near(round(Value),round(prod_sua))]

data_analyse=synch_data[check==FALSE,]

d=data_analyse[,list(Numc=.N),
               by=c("geographicAreaM49")]
dd=data_analyse[,list(Numc=.N),
               by=c("Flag")]


to_save<-synch_data[check==FALSE,][,list(geographicAreaM49,
                               measuredElement,
                               measuredItemCPC,
                               timePointYears,Value,
                               flagObservationStatus,
                               flagMethod)]



sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)


# SaveData(domain = datasetConfig$domain,
#          dataset = datasetConfig$dataset,
#          data = to_save, waitTimeout = 2000000)
