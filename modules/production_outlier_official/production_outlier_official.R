library(faosws)
library(faoswsUtil)
library(dplyr)
library(data.table)
library(tidyr)
library(openxlsx)
library(RcppRoll)



start_time <- Sys.time()

R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

if (CheckDebug()) {
  R_SWS_SHARE_PATH <- "//hqlprsws1.hq.un.fao.org/sws_r_share"
  
  mydir <- "modules/production_oulier_official"
  
  SETTINGS <- faoswsModules::ReadSettings(file.path(mydir, "sws.yml"))
  
  SetClientFiles(SETTINGS[["certdir"]])
  
  GetTestEnvironment(baseUrl = SETTINGS[["server"]], token = SETTINGS[["token"]])
}



COUNTRY <- as.character(swsContext.datasets[[1]]@dimensions$geographicAreaM49@keys)

COUNTRY_NAME <-
  nameData(
    "aproduction", "aproduction",
    data.table(geographicAreaM49 = COUNTRY))$geographicAreaM49_description

USER <- regmatches(
  swsContext.username,
  regexpr("(?<=/).+$", swsContext.username, perl = TRUE)
)

dbg_print <- function(x) {
  message(paste0("PROD (", COUNTRY, "): ", x))
}

dbg_print("start parameters")

startYear <- as.numeric(swsContext.computationParams$startYear)
#startYear = as.numeric(2014)

endYear <- as.numeric(swsContext.computationParams$endYear)
#endYear = as.numeric(2018)

stopifnot(startYear <= endYear)

window <- as.numeric(5)

#############################################
#                                           #
#   mancano intervallo e yearVal            #
#                                           #
#############################################

YEARS <- startYear:endYear#2013-2019 sono gli anni che mi devo scaricare
interval <- (startYear-window):(startYear-1)#2009-2013 sono gli anni per la media, da cambiare con Irina


#TMP file
TMP_DIR <- file.path(tempdir(), USER)
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)

tmp_file_outliers <- file.path(TMP_DIR, paste0("production_outlier_", COUNTRY_NAME, ".xlsx"))

dbg_print("end parameters")

dbg_print("start functions")
##########################################################SEND EMAIL FUNCTION#############################################
`%!in%` <- Negate(`%in%`)
#New version with unlink of the temporary folders integrated
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



# rollavg <- function(x, order = 5) {
#   # order should be > 2
#   stopifnot(order >= 5)
#   
#   non_missing <- sum(!is.na(x))
#   
#   # For cases that have just two non-missing observations
#   order <- ifelse(order > 2 & non_missing == 2, 2, order)
#   
#   if (non_missing == 1) {
#     x[is.na(x)] <- na.omit(x)[1]
#   } else if (non_missing >= order) {
#     n <- 1
#     while(any(is.na(x)) & n <= 10) { # 10 is max tries
#       movav <- suppressWarnings(RcppRoll::roll_mean(x, order, fill = 'extend', align = 'right'))
#       movav <- data.table::shift(movav)
#       x[is.na(x)] <- movav[is.na(x)]
#       n <- n + 1
#     }
#     
#     x <- zoo::na.fill(x, 'extend')
#   }
#   
#   return(x)
# }
############################################################END SEND EMAIL##########################################
dbg_print("end functions")
#COUNTRY already defined
#YEARS already defined

#ITEMS 
#-take the codelist item from the server 
#take them from the session
#take from the data table list
#Dimension("measuredItemCPC", GetCodeList("aproduction", "aproduction", "measuredItemCPC")$code)
#ITEMS <- swsContext.datasets[[1]]@dimensions$measuredItemCPC@keys

#change with read data table from sws
# cpc_cluster <- read.xlsx("C:/Users/lombardi/Documents/Livia/faoswsProduction/modules/outlierDetection/CPC_cluster.xlsx")
# cpc_cluster <- as.data.table(cpc_cluster)
dbg_print("Getting data")

cpc_cluster <- ReadDatatable("cpc_validation_production")



#ELEMENTS let get the triplets needed:
#o tutti da chiave
#measuredElement = Dimension("measuredElement", GetCodeList("aproduction", "aproduction", "measuredElement")$code)
#Da prendere poi unique(ELEMENTS) in the POST CALL
ELEMENTS <- c("5312","5510","5421",#crops: Area Harvested, Production, Yields 
              "5111","5315","5417",#Big animals: stocks, slaughtered, Yield/Carcass
              "5112","5316","5424",#Small animals: stocks, slaughtered, Yield/Carcass
              "5320","5510","5417",#Big meat: Slaughtered, Production, Yield carcass
              "5321","5510","5424",#Small meat: Slaughtered, Production, Yield/Carcass
              "5314","5319","5422", #keep only official/EF flag. Do not compute Yield
              "5318","5510","5417",#Milk: Milk animals, Production, Yields/Carcass
              "5313","5424","5510","5513")#EGGS: Laying, Yelds/Carcass, Production t, Production 1000. Yield: 5510/5313

#sessionKey = swsContext.datasets[[1]]
#Creating the key to get data
production_key = DatasetKey(domain = "aproduction", 
                             dataset = "aproduction", 
                             dimensions = list(measuredItemCPC = Dimension("measuredItemCPC", unique(cpc_cluster$cpc)), 
                                               measuredElement = Dimension("measuredElement", unique(ELEMENTS)),
                                               geographicAreaM49 = Dimension("geographicAreaM49", as.vector(COUNTRY)),
                                               timePointYears = Dimension("timePointYears", as.character(YEARS))))


#call data
data <- GetData(production_key)

data <- as.data.table(data)

#get fag table
flagValidTable <- ReadDatatable("valid_flags")
flagValidTable <- flagValidTable[is.na(flagObservationStatus), flagObservationStatus := ""]


#take a copy as backup
data_prod <- copy(data)

dbg_print("Got data")
                                  ######################################
                                  #                                    #
                                  #                                    #
                                  #           CROPS YIELD              #
                                  #                                    #
                                  #                                    #
                                  ######################################

dbg_print("Starting Yield calculation")

crops_data <- data_prod[measuredElement %in% c("5312","5510"),]

crops_data <- dcast(crops_data, measuredItemCPC + geographicAreaM49 + timePointYears ~ measuredElement, 
                  value.var = c("Value"))

if("5510" %in% colnames(crops_data) & "5312" %in% colnames(crops_data)){
  crops_data <- crops_data[, `5421` := `5510`/`5312`]
  
  crops_data <- crops_data[!(is.na(`5421`)) & !(is.nan(`5421`)) & !(is.infinite(`5421`)),]
  
  
  crops_data <- merge(crops_data,
                      data_prod[measuredElement %in% "5510", 
                                list(geographicAreaM49,measuredItemCPC, timePointYears, 
                                     flagO_P = flagObservationStatus,
                                     flagM_P = flagMethod)], 
                      by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"))
  
  crops_data <- merge(crops_data,
                      data_prod[measuredElement %in% "5312", 
                                list(geographicAreaM49,measuredItemCPC, timePointYears, 
                                     flagO_H = flagObservationStatus,
                                     flagM_H = flagMethod)], 
                      by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"))
  
  crops_data <- crops_data[, `:=` (flagObservationStatus = NA_character_, flagMethod = NA_character_)]
  
  #insert official flag to yield where both official
  crops_data <- crops_data[flagO_P %in% "" &
                           flagO_H %in% "", 
                           `:=` (flagObservationStatus = "", flagMethod = "i") ]
  
  #insert imputed flag to yield where both imputed
  crops_data <- crops_data[flagO_P %in% c("I","E") &
                           flagO_H %in% c("I","E"), 
                           `:=` (flagObservationStatus = "I", flagMethod = "i") ]
  
  #insert t flag to yield where are Tp
  crops_data <- crops_data[flagO_P %in% "T" &
                           flagO_H %in% "T", 
                           `:=` (flagObservationStatus = "T", flagMethod = "i") ]
  
  #insert flag to yeld manual estimated
  crops_data <- crops_data[flagO_P %in% "E" & flagM_P %in% "f" &
                           flagO_H %in% "E" & flagM_H %in% "f", 
                           `:=` (flagObservationStatus = "E", flagMethod = "i") ]
  
  #insert flag one at leats one of the 2 element is imputed
  crops_data <- crops_data[flagO_P %in% "" & flagO_H %in% "I" |
                           flagO_P %in% "I" & flagO_H %in% "", 
                           `:=` (flagObservationStatus = "I", flagMethod = "i") ]
  
  crops_data <- crops_data[flagO_P %in% "" & flagO_H %in% "T" |
                           flagO_P %in% "T" & flagO_H %in% "", 
                               `:=` (flagObservationStatus = "T", flagMethod = "i") ]
  
  crops_data <- crops_data[flagO_P %in% "" & flagO_H %in% "E" |
                                 flagO_P %in% "E" & flagO_H %in% "", 
                               `:=` (flagObservationStatus = "E", flagMethod = "i") ]
  #reorganize data in sws format
  crops_yield = crops_data[, list(geographicAreaM49, measuredElement = "5421", measuredItemCPC,
                                 timePointYears,Value = `5421` ,flagObservationStatus,flagMethod)]
  
  #bind with original dataset and removing the previous yield before attaching
  tot_crops <- rbind(data_prod[! crops_yield , on = c("measuredElement","geographicAreaM49",
                                                   "timePointYears", "measuredItemCPC")], crops_yield)

}else{
  tot_crops <- data_prod
}
                                      ######################################
                                      #                                    #
                                      #                                    #
                                      #         LIVESTOCK YIELD            #
                                      #                                    #
                                      #                                    #
                                      ######################################

#1) Saving Animal slaughtering under Meat slaughterings

copy_for_big_animals <- data_prod[measuredElement %in% "5315"]
copy_for_small_animals <- data_prod[measuredElement %in% "5316"]

copy_for_big_animals[, c("measuredElement"):=NULL]
copy_for_big_animals[, measuredElement := "5320"] 

copy_for_small_animals[, c("measuredElement"):=NULL]
copy_for_small_animals[, measuredElement := "5321"]

# copy_for_big_animals[, flagObservationStatus := ""]
# copy_for_big_animals[, flagMethod := "c"]
# 
# copy_for_small_animals[, flagObservationStatus := ""]
# copy_for_small_animals[, flagMethod := "c"]

copy_for_animals <- rbind(copy_for_big_animals,copy_for_small_animals)
#

mapping_for_animals <- data.table("meat_code" = c("21111.01","21115","21116","21113.01","21121","21122","21124","21112","21123","21118.01","21118.02","21118.03","21117.01","21114","21119.01","21170.01"),
                                  "animal_code"= c("02111","02122","02123","02140","02151","02154","02152","02112","02153","02131","02132","02133","02121.01","02191","02192.01","02194"))

copy_for_animals <- merge(copy_for_animals, mapping_for_animals, by.x = "measuredItemCPC",
                          by.y = "animal_code", all.x = T)

copy_for_animals[, c("measuredItemCPC"):=NULL]

setnames(copy_for_animals, "meat_code","measuredItemCPC")

copy_for_animals[, flagMethod := "c"]

data_prod_2 <- rbind(tot_crops[! copy_for_animals , on = c("measuredElement","geographicAreaM49",
                                                    "timePointYears", "measuredItemCPC")], copy_for_animals)


#2) Calculate Meat Yield for big animals

bigmeat_data <- data_prod_2[measuredElement %in% c("5320","5510"),]

bigmeat_data <- dcast(bigmeat_data, measuredItemCPC + geographicAreaM49 + timePointYears ~ measuredElement, 
                    value.var = c("Value"))

if("5510" %in% colnames(bigmeat_data) & "5320" %in% colnames(bigmeat_data)){
  
  bigmeat_data <- bigmeat_data[, `5417` := (`5510`)*1000/`5320`] 
  
  bigmeat_data <- bigmeat_data[!(is.na(`5417`)) & !(is.nan(`5417`)) & !(is.infinite(`5417`)),]
  
  
  bigmeat_data <- merge(bigmeat_data,
                        data_prod_2[measuredElement %in% "5510", 
                                list(geographicAreaM49,measuredItemCPC, timePointYears, 
                                     flagO_P = flagObservationStatus,
                                     flagM_P = flagMethod)], 
                      by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"))
  
  bigmeat_data <- merge(bigmeat_data,
                        data_prod_2[measuredElement %in% "5320", 
                                list(geographicAreaM49,measuredItemCPC, timePointYears, 
                                     flagO_S = flagObservationStatus,
                                     flagM_S = flagMethod)], 
                      by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"))
  
  bigmeat_data <- bigmeat_data[, `:=` (flagObservationStatus = NA_character_, flagMethod = NA_character_)]
  
  #insert official flag to yield where both official
  bigmeat_data <- bigmeat_data[flagO_P %in% "" &
                             flagO_S %in% "", 
                           `:=` (flagObservationStatus = "", flagMethod = "i") ]
  
  #insert imputed flag to yield where both imputed
  bigmeat_data <- bigmeat_data[flagO_P %in% c("I","E") &
                             flagO_S %in% c("I","E"), 
                           `:=` (flagObservationStatus = "I", flagMethod = "i") ]
  
  #insert t flag to yield where are Tp
  bigmeat_data <- bigmeat_data[flagO_P %in% "T" &
                             flagO_S %in% "T", 
                           `:=` (flagObservationStatus = "T", flagMethod = "i") ]
  
  #insert flag to yeld manual estimated
  bigmeat_data <- bigmeat_data[flagO_P %in% "E" & flagM_P %in% "f" &
                             flagO_S %in% "E" & flagM_S %in% "f", 
                           `:=` (flagObservationStatus = "E", flagMethod = "i") ]
  
  #insert flag one at leats one of the 2 element is imputed
  bigmeat_data <- bigmeat_data[flagO_P %in% "" & flagO_S %in% "I" |
                             flagO_P %in% "I" & flagO_S %in% "", 
                           `:=` (flagObservationStatus = "I", flagMethod = "i") ]
  
  bigmeat_data <- bigmeat_data[flagO_P %in% "" & flagO_S %in% "T" |
                                 flagO_P %in% "T" & flagO_S %in% "", 
                               `:=` (flagObservationStatus = "T", flagMethod = "i") ]
  
  bigmeat_data <- bigmeat_data[flagO_P %in% "" & flagO_S %in% "E" |
                                     flagO_P %in% "E" & flagO_S %in% "", 
                                   `:=` (flagObservationStatus = "E", flagMethod = "i") ]
  #reorganize data in sws format
  bigmeat_data_carcass = bigmeat_data[, list(geographicAreaM49, measuredElement = "5417", measuredItemCPC,
                                  timePointYears,Value = `5417` ,flagObservationStatus,flagMethod)]
  
  #copy yield values in 54170 with flag blank c
  bigmeat_data_carcass_indig = copy(bigmeat_data_carcass)
  
  bigmeat_data_carcass_indig[, measuredElement := "54170"]
  
  bigmeat_data_carcass_indig[, flagObservationStatus := ""]
  bigmeat_data_carcass_indig[, flagMethod := "c"]
  
  bigmeat_data_carcass_tot = rbind(bigmeat_data_carcass,bigmeat_data_carcass_indig)
  #bind with total dataset already containing crops yield and removing the previous yield before attaching
  data_prod_3 <- rbind(data_prod_2[! bigmeat_data_carcass_tot , on = c("measuredElement","geographicAreaM49",
                                                      "timePointYears", "measuredItemCPC")], bigmeat_data_carcass_tot)
}else{
  data_prod_3 <- data_prod_2
}
#3) Calculate Meat Yield for small animals


smallmeat_data <- data_prod_3[measuredElement %in% c("5321","5510"),]

smallmeat_data <- dcast(smallmeat_data, measuredItemCPC + geographicAreaM49 + timePointYears ~ measuredElement, 
                      value.var = c("Value"))

if("5510" %in% colnames(smallmeat_data) & "5321" %in% colnames(smallmeat_data)){
  
  smallmeat_data <- smallmeat_data[, `5424` := (`5510`)*1000/`5321`] 
  
  smallmeat_data <- smallmeat_data[!(is.na(`5424`)) & !(is.nan(`5424`)) & !(is.infinite(`5424`)),]
  
  
  smallmeat_data <- merge(smallmeat_data,
                        data_prod_3[measuredElement %in% "5510", 
                                    list(geographicAreaM49,measuredItemCPC, timePointYears, 
                                         flagO_P = flagObservationStatus,
                                         flagM_P = flagMethod)], 
                        by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"))
  
  smallmeat_data <- merge(smallmeat_data,
                        data_prod_3[measuredElement %in% "5321", 
                                    list(geographicAreaM49,measuredItemCPC, timePointYears, 
                                         flagO_S = flagObservationStatus,
                                         flagM_S = flagMethod)], 
                        by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"))
  
  smallmeat_data <- smallmeat_data[, `:=` (flagObservationStatus = NA_character_, flagMethod = NA_character_)]
  
  #insert official flag to yield where both official
  smallmeat_data <- smallmeat_data[flagO_P %in% "" &
                                 flagO_S %in% "", 
                               `:=` (flagObservationStatus = "", flagMethod = "i") ]
  
  #insert imputed flag to yield where both imputed
  smallmeat_data <- smallmeat_data[flagO_P %in% c("I","E") &
                                 flagO_S %in% c("I","E"), 
                               `:=` (flagObservationStatus = "I", flagMethod = "i") ]
  
  #insert t flag to yield where are Tp
  smallmeat_data <- smallmeat_data[flagO_P %in% "T" &
                                 flagO_S %in% "T", 
                               `:=` (flagObservationStatus = "T", flagMethod = "i") ]
  
  #insert flag to yeld manual estimated
  smallmeat_data <- smallmeat_data[flagO_P %in% "E" & flagM_P %in% "f" &
                                 flagO_S %in% "E" & flagM_S %in% "f", 
                               `:=` (flagObservationStatus = "E", flagMethod = "i") ]
  
  #insert flag one at leats one of the 2 element is imputed
  smallmeat_data <- smallmeat_data[flagO_P %in% "" & flagO_S %in% "I" |
                                 flagO_P %in% "I" & flagO_S %in% "", 
                               `:=` (flagObservationStatus = "I", flagMethod = "i") ]
  
  smallmeat_data <- smallmeat_data[flagO_P %in% "" & flagO_S %in% "T" |
                                 flagO_P %in% "T" & flagO_S %in% "", 
                               `:=` (flagObservationStatus = "T", flagMethod = "i") ]
  
  smallmeat_data <- smallmeat_data[flagO_P %in% "" & flagO_S %in% "E" |
                           flagO_P %in% "E" & flagO_S %in% "", 
                         `:=` (flagObservationStatus = "E", flagMethod = "i") ]
  #reorganize data in sws format
  smallmeat_data_carcass = smallmeat_data[, list(geographicAreaM49, measuredElement = "5424", measuredItemCPC,
                                             timePointYears,Value = `5424` ,flagObservationStatus,flagMethod)]
  
  #copy yield values in 54240 with flag blank c
  smallmeat_data_carcass_indig = copy(smallmeat_data_carcass)
  
  smallmeat_data_carcass_indig[, measuredElement := "54240"]
  
  smallmeat_data_carcass_indig[, flagObservationStatus := ""]
  smallmeat_data_carcass_indig[, flagMethod := "c"]
  
  smallmeat_data_carcass_tot = rbind(smallmeat_data_carcass,smallmeat_data_carcass_indig)
  #bind with total dataset already containing crops yield and removing the previous yield before attaching
  data_prod_4 <- rbind(data_prod_3[! smallmeat_data_carcass_tot, on = c("measuredElement","geographicAreaM49",
                                                                       "timePointYears", "measuredItemCPC")], 
                       smallmeat_data_carcass_tot)
}else{
  data_prod_4 <- data_prod_3
}

                                    ######################################
                                    #                                    #
                                    #                                    #
                                    #               MILK                 #
                                    #                                    #
                                    #                                    #
                                    ######################################

# "5318","5510","5417",#Milk: Milk animals, Production, Yields/Carcass
milk_data <- data_prod_4[measuredElement %in% c("5318","5510"),]

milk_data <- dcast(milk_data, measuredItemCPC + geographicAreaM49 + timePointYears ~ measuredElement, 
                    value.var = c("Value"))

if("5510" %in% colnames(milk_data) & "5318" %in% colnames(milk_data)){
  milk_data <- milk_data[, `5417` := (`5510`)*1000/`5318`]
  
  milk_data <- milk_data[!(is.na(`5417`)) & !(is.nan(`5417`)) & !(is.infinite(`5417`)),]
  
  
  milk_data <- merge(milk_data,
                      data_prod_4[measuredElement %in% "5510", 
                                list(geographicAreaM49,measuredItemCPC, timePointYears, 
                                     flagO_P = flagObservationStatus,
                                     flagM_P = flagMethod)], 
                      by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"))
  
  milk_data <- merge(milk_data,
                      data_prod_4[measuredElement %in% "5318", 
                                list(geographicAreaM49,measuredItemCPC, timePointYears, 
                                     flagO_H = flagObservationStatus,
                                     flagM_H = flagMethod)], 
                      by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"))
  
  milk_data <- milk_data[, `:=` (flagObservationStatus = NA_character_, flagMethod = NA_character_)]
  
  #insert official flag to yield where both official
  milk_data <- milk_data[flagO_P %in% "" &
                             flagO_H %in% "", 
                           `:=` (flagObservationStatus = "", flagMethod = "i") ]
  
  #insert imputed flag to yield where both imputed
  milk_data <- milk_data[flagO_P %in% c("I","E") &
                             flagO_H %in% c("I","E"), 
                           `:=` (flagObservationStatus = "I", flagMethod = "i") ]
  
  #insert t flag to yield where are Tp
  milk_data <- milk_data[flagO_P %in% "T" &
                             flagO_H %in% "T", 
                           `:=` (flagObservationStatus = "T", flagMethod = "i") ]
  
  #insert flag to yeld manual estimated
  milk_data <- milk_data[flagO_P %in% "E" & flagM_P %in% "f" &
                             flagO_H %in% "E" & flagM_H %in% "f", 
                           `:=` (flagObservationStatus = "E", flagMethod = "i") ]
  
  #insert flag one at leats one of the 2 element is imputed
  milk_data <- milk_data[flagO_P %in% "" & flagO_H %in% "I" |
                             flagO_P %in% "I" & flagO_H %in% "", 
                           `:=` (flagObservationStatus = "I", flagMethod = "i") ]
  
  milk_data <- milk_data[flagO_P %in% "" & flagO_H %in% "T" |
                             flagO_P %in% "T" & flagO_H %in% "", 
                           `:=` (flagObservationStatus = "T", flagMethod = "i") ]
  
  milk_data <- milk_data[flagO_P %in% "" & flagO_H %in% "E" |
                           flagO_P %in% "E" & flagO_H %in% "", 
                         `:=` (flagObservationStatus = "E", flagMethod = "i") ]
  
  #reorganize data in sws format
  milk_yield = milk_data[, list(geographicAreaM49, measuredElement = "5417", measuredItemCPC,
                                  timePointYears,Value = `5417` ,flagObservationStatus,flagMethod)]
  
  #bind with original dataset and removing the previous yield before attaching
  data_prod_5 <- rbind(data_prod_4[! milk_yield , on = c("measuredElement","geographicAreaM49",
                                                      "timePointYears", "measuredItemCPC")], milk_yield)
}else{
  data_prod_5 <- data_prod_4
}


                                        ######################################
                                        #                                    #
                                        #                                    #
                                        #               EGGS                 #
                                        #                                    #
                                        #                                    #
                                        ######################################

#"5313","5424","5510","5513")#EGGS: Laying, Yelds/Carcass, Production t, Production 1000. Yield: 5510/5313
eggs_data <- data_prod_5[measuredElement %in% c("5313","5510"),]

eggs_data <- dcast(eggs_data, measuredItemCPC + geographicAreaM49 + timePointYears ~ measuredElement, 
                   value.var = c("Value"))

if("5510" %in% colnames(eggs_data) & "5313" %in% colnames(eggs_data)){
  eggs_data <- eggs_data[, `5424` := (`5510`)*1000/`5313`]
  
  eggs_data <- eggs_data[!(is.na(`5424`)) & !(is.nan(`5424`)) & !(is.infinite(`5424`)),]
  
  
  eggs_data <- merge(eggs_data,
                     data_prod_5[measuredElement %in% "5510", 
                                 list(geographicAreaM49,measuredItemCPC, timePointYears, 
                                      flagO_P = flagObservationStatus,
                                      flagM_P = flagMethod)], 
                     by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"))
  
  eggs_data <- merge(eggs_data,
                     data_prod_5[measuredElement %in% "5313", 
                                 list(geographicAreaM49,measuredItemCPC, timePointYears, 
                                      flagO_H = flagObservationStatus,
                                      flagM_H = flagMethod)], 
                     by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"))
  
  eggs_data <- eggs_data[, `:=` (flagObservationStatus = NA_character_, flagMethod = NA_character_)]
  
  #insert official flag to yield where both official
  eggs_data <- eggs_data[flagO_P %in% "" &
                           flagO_H %in% "", 
                         `:=` (flagObservationStatus = "", flagMethod = "i") ]
  
  #insert imputed flag to yield where both imputed
  eggs_data <- eggs_data[flagO_P %in% c("I","E") &
                           flagO_H %in% c("I","E"), 
                         `:=` (flagObservationStatus = "I", flagMethod = "i") ]
  
  #insert t flag to yield where are Tp
  eggs_data <- eggs_data[flagO_P %in% "T" &
                           flagO_H %in% "T", 
                         `:=` (flagObservationStatus = "T", flagMethod = "i") ]
  
  #insert flag to yeld manual estimated
  eggs_data <- eggs_data[flagO_P %in% "E" & flagM_P %in% "f" &
                           flagO_H %in% "E" & flagM_H %in% "f", 
                         `:=` (flagObservationStatus = "E", flagMethod = "i") ]
  
  #insert flag one at leats one of the 2 element is imputed
  eggs_data <- eggs_data[flagO_P %in% "" & flagO_H %in% "I" |
                           flagO_P %in% "I" & flagO_H %in% "", 
                         `:=` (flagObservationStatus = "I", flagMethod = "i") ]
  
  eggs_data <- eggs_data[flagO_P %in% "" & flagO_H %in% "T" |
                           flagO_P %in% "T" & flagO_H %in% "", 
                         `:=` (flagObservationStatus = "T", flagMethod = "i") ]
  
  eggs_data <- eggs_data[flagO_P %in% "" & flagO_H %in% "E" |
                           flagO_P %in% "E" & flagO_H %in% "", 
                         `:=` (flagObservationStatus = "E", flagMethod = "i") ]
  #reorganize data in sws format
  eggs_yield = eggs_data[, list(geographicAreaM49, measuredElement = "5424", measuredItemCPC,
                                timePointYears,Value = `5424` ,flagObservationStatus,flagMethod)]
  
  #bind with original dataset and removing the previous yield before attaching
  data_prod_6 <- rbind(data_prod_5[! eggs_yield , on = c("measuredElement","geographicAreaM49",
                                                         "timePointYears", "measuredItemCPC")], eggs_yield)
}else{
  data_prod_6 <- data_prod_5
}



                                              ######################################
                                              #                                    #
                                              #                                    #
                                              #          Single series             #
                                              #           (e.g. Honey)             #
                                              #                                    #
                                              ######################################


data_single <- copy(data_prod_6)

data_single <- data_single[measuredElement %in% c("5314","5319"),]

data_single <- merge(data_single,flagValidTable, all.x = T, by = c("flagObservationStatus","flagMethod"))

data_single[, c("Valid"):=NULL]

data_single <- data_single[Protected %in% c("FALSE"),]

data_single[, c("Protected"):=NULL]

#Removing Not protected single series from the main dataset
data_final <- data_prod_6[! data_single , on = c("measuredElement","geographicAreaM49",
                                                      "timePointYears", "measuredItemCPC")]

dbg_print("End of Yield calculation")
                                                    ######################################
                                                    #                                    #
                                                    #                                    #
                                                    #          OUTLIER DETECTION         #
                                                    #           (e.g. Honey)             #
                                                    #                                    #
                                                    ######################################
dbg_print("Start outlier routine")
#outlier first round. On elements != then Yield
#3 outlier detection 
#CROPS -> Area Harvested, Production
#BIG animal <- stocks, slaughtered,
#Small animals: stocks, slaughtered
#Big meat: Slaughtered, Production
#Small meat: Slaughtered, Production
#5315 e 5319
#Milk animals, Production
#Laying, Production t, Production 1000
#Check single series at this point if we didn t have 5314 | 5319 official in 2014-2019 we deleted the element. 
#For 2014-2019 I take the last aanipulated dataset with the new Yields calculated
#For each element I calculate the moving average by item e elemen e geo. 

#out1_elements <- c("5510","5312","5111","5315","5112","5316","5320","5321","5314","5319","5318","5313")

out1 <- data_final[measuredElement %in% unique(ELEMENTS),]

old_year_key <- DatasetKey(domain = "aproduction", 
                           dataset = "aproduction", 
                           dimensions = list(measuredItemCPC = Dimension("measuredItemCPC", unique(cpc_cluster$cpc)), 
                                             measuredElement = Dimension("measuredElement", unique(ELEMENTS)),
                                             geographicAreaM49 = Dimension("geographicAreaM49", as.vector(COUNTRY)),
                                             timePointYears = Dimension("timePointYears", as.character(interval))))

old_year_data <- GetData(old_year_key)

out1_data <- rbind(out1,old_year_data)

#Calculate moving average for each year
dbg_print("moving average")

#out1_data <- out1_data[order(geographicAreaM49, measuredElement, measuredItemCPC),] [order( -timePointYears ),] 
#analisi sugli na e se dipendono da mancanza dei 5 anni. voglio vedere se fa automaticamente con 4-3-2
out1_data <- out1_data[order(geographicAreaM49, measuredElement, measuredItemCPC, timePointYears), 
                       avg := roll_meanr(Value, 3),
                       by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[order(geographicAreaM49,measuredItemCPC,measuredElement,timePointYears),prev_avg:=lag(avg),
          by= c("geographicAreaM49","measuredItemCPC","measuredElement")]

out1_data <- out1_data[order(geographicAreaM49, measuredElement, measuredItemCPC),] [order( -timePointYears ),] 

#if yields elements exceed 30% so I put Yield check True or False [by item and year]
#con la colonna avrò per item per anno se l'yeld ha superato true or not. Quindi potrei anche dividere i dataset e usare i diversi
#contraints sugli altri elementi!

#Keep star year-last year

out1_data <- out1_data[timePointYears %in% as.character(startYear:endYear),]

out1_data[,`:=`(yield_lower_th = NA_real_, yield_upper_th = NA_real_)]



out1_data[, `:=`(
  yield_lower_th = prev_avg[measuredElement %in% c("5421","5417","5424","5417","5422")] - 
    prev_avg[measuredElement %in% c("5421","5417","5424","5417","5422")]*0.3,
  yield_upper_th = prev_avg[measuredElement %in% c("5421","5417","5424","5417","5422")] + 
    prev_avg[measuredElement %in% c("5421","5417","5424","5417","5422")]*0.3
),
by = c("geographicAreaM49","measuredItemCPC","timePointYears")]


out1_data[,yieldCheck:=ifelse(Value[measuredElement %in% c("5421","5417","5424","5417","5422")] <yield_lower_th | 
                              Value[measuredElement %in% c("5421","5417","5424","5417","5422")] > yield_upper_th,TRUE,FALSE),
          by = c("geographicAreaM49","measuredItemCPC","timePointYears")]


out1_data[,`:=`(lower_th = NA_real_, upper_th = NA_real_)]


dbg_print("Outlier criteria")
                                              ######################################
                                              #                                    #
                                              #                                    #
                                              #          STRICT CRITERIA           #
                                              #                                    #
                                              #                                    #
                                              ######################################
out1_data[Value < 100 & yieldCheck == TRUE | Value < 100 & is.na(yieldCheck), `:=`(
  lower_th = prev_avg - prev_avg*5,
  upper_th = prev_avg + prev_avg*5
  ),
  by = c("geographicAreaM49","measuredElement","measuredItemCPC")]


out1_data[ Value >= 100 & Value < 1000 & yieldCheck == TRUE | Value >= 100 & Value < 1000 & is.na(yieldCheck), `:=`(
  lower_th = prev_avg - prev_avg*1,
  upper_th = prev_avg + prev_avg*1
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 1000 & Value < 10000 & yieldCheck == TRUE | Value >= 1000 & Value < 10000 & is.na(yieldCheck), `:=`(
  lower_th = prev_avg - prev_avg*0.8,
  upper_th = prev_avg + prev_avg*0.8
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 10000 & Value < 50000 & yieldCheck == TRUE | Value >= 10000 & Value < 50000 & is.na(yieldCheck), `:=`(
  lower_th = prev_avg - prev_avg*0.6,
  upper_th = prev_avg + prev_avg*0.6
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 50000 & Value < 100000 & yieldCheck == TRUE |  Value >= 50000 & Value < 100000 & is.na(yieldCheck), `:=`(
  lower_th = prev_avg - prev_avg*0.5,
  upper_th = prev_avg + prev_avg*0.5
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 100000 & Value < 500000 & yieldCheck == TRUE | Value >= 100000 & Value < 500000 & is.na(yieldCheck), `:=`(
  lower_th = prev_avg - prev_avg*0.4,
  upper_th = prev_avg + prev_avg*0.4
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 500000 & Value < 1000000 & yieldCheck == TRUE | Value >= 500000 & Value < 1000000 & is.na(yieldCheck), `:=`(
  lower_th = prev_avg - prev_avg*0.3,
  upper_th = prev_avg + prev_avg*0.3
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 1000000 & Value < 3000000 & yieldCheck == TRUE | Value >= 1000000 & Value < 3000000 & is.na(yieldCheck), `:=`(
  lower_th = prev_avg - prev_avg*0.15,
  upper_th = prev_avg + prev_avg*0.15
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 3000000 & Value < 50000000 & yieldCheck == TRUE |  Value >= 3000000 & Value < 50000000 & is.na(yieldCheck), `:=`(
  lower_th = prev_avg - prev_avg*0.1,
  upper_th = prev_avg + prev_avg*0.1
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value>=50000000 & yieldCheck == TRUE |  Value>=50000000 & is.na(yieldCheck), `:=`(
  lower_th = prev_avg - prev_avg*0.1,
  upper_th = prev_avg + prev_avg*0.1
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

dbg_print("End of strict criteria")
#
                                              
                                              ######################################
                                              #                                    #
                                              #                                    #
                                              #          SOFT CRITERIA             #
                                              #                                    #
                                              #                                    #
                                              ######################################
#dovrei aggiungere come filtro a ogni check: | Value >= 100 & Value < 1000 & measuredElement %in% "5111"

out1_data[Value < 100 & yieldCheck == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*9,
  upper_th = prev_avg + prev_avg*9
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]


out1_data[ Value >= 100 & Value < 1000 & yieldCheck == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*5,
  upper_th = prev_avg + prev_avg*5
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]


out1_data[ Value >= 1000 & Value < 10000 & yieldCheck == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*1,
  upper_th = prev_avg + prev_avg*1
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 10000 & Value < 50000 & yieldCheck == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.6,
  upper_th = prev_avg + prev_avg*0.6
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 50000 & Value < 100000 & yieldCheck == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.6,
  upper_th = prev_avg + prev_avg*0.6
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 100000 & Value < 500000 & yieldCheck == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.5,
  upper_th = prev_avg + prev_avg*0.5
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 500000 & Value < 1000000 & yieldCheck == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.4,
  upper_th = prev_avg + prev_avg*0.4
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 1000000 & Value < 3000000 & yieldCheck == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.4,
  upper_th = prev_avg + prev_avg*0.4
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 3000000 & Value < 20000000 & yieldCheck == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.3,
  upper_th = prev_avg + prev_avg*0.3
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value >= 20000000 & Value < 50000000 & yieldCheck == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.15,
  upper_th = prev_avg + prev_avg*0.15
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

out1_data[ Value>=50000000 & yieldCheck == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.1,
  upper_th = prev_avg + prev_avg*0.1
),
by = c("geographicAreaM49","measuredElement","measuredItemCPC")]

####################################################FINAL CHECK#####################################################
out1_data[,outCheck:=ifelse(Value <lower_th & flagObservationStatus %in% c("","T","E") |
                            Value > upper_th & flagObservationStatus %in% c("","T","E"),TRUE,FALSE)]

out1_data[(prev_avg/Value) > 9 & flagObservationStatus %in% c("","T","E"),outCheck:=TRUE]

out1_data[Value < 1000 & prev_avg < 1000 & yieldCheck == FALSE,outCheck:=FALSE]
#if prev_avg / Value > 9, outCheck := TRUE

#out1_data[,softCheck:=ifelse(Value <lower_soft_th | Value > upper_soft_th,TRUE,FALSE)]
#strict <- out1_data[outCheck == TRUE,]
#soft <- out1_data[softCheck == TRUE,]
dbg_print("Outliers final check ended")
#outlier_file <- out1_data[outCheck==TRUE,]
outlier_file <- copy(out1_data) 

setnames(outlier_file, "prev_avg", "moving_average_3_years")

#outlier_file[, c("avg","outCheck"):= NULL]


#outlier_file[, c("softCheck","outCheck"):= NULL]
outlier_file <- outlier_file[outCheck==TRUE,]

outlier_file[, c("avg","yield_lower_th","yield_upper_th","lower_th","upper_th","outCheck"):= NULL]

outlier_file <- nameData("aproduction", "aproduction", outlier_file, except = "timePointYears")

#######convert format separator of thousands
#outlier_file$Value= format(outlier_file$Value,big.mark=",",nsmall=2,scientific = F)
outlier_file$Value = format(round(outlier_file$Value, 0),nsmall=0 , big.mark=",",scientific=FALSE)

outlier_file$moving_average_3_years = format(round(outlier_file$moving_average_3_years, 0),nsmall=0 , big.mark=",",scientific=FALSE)
#c("5421","5417","5424","5417","5422")

outlier_file = outlier_file[measuredElement %!in% c("5421","5417","5424","5417","5422"),]


#order by item
outlier_file = outlier_file[order(measuredItemCPC),]

setnames(outlier_file, "yieldCheck","Comments")



                                              ######################################
                                              #                                    #
                                              #                                    #
                                              #          FLAG CONTROL              #
                                              #                                    #
                                              #                                    #
                                              ######################################
#5 if 2016 o 2017 o 18 average is positive and flag blank o T (off / semi off) e 2019 is.null  -> colore viola

flag_elements <- c("5510","5312","5111","5315","5112","5316","5320","5321","5314","5319","5318","5313")

flag_data <- data_final[measuredElement %in% flag_elements,]

#flag_data[, flagOld := NA_character_]
#
flag_data[,flagOld:=ifelse(timePointYears %in% c("2014","2015","2016") & flagObservationStatus %in% c("","T"),TRUE,FALSE),
          by = c("geographicAreaM49","measuredItemCPC","measuredElement")]



#flag_data[, offOld := NA_character_]

flag_data[, offOld:= ifelse(sum(flagOld) >=1 ,TRUE,FALSE),
          by = c("geographicAreaM49","measuredItemCPC","measuredElement")]


flag_data[,flagNew:=ifelse(timePointYears %in% c("2017","2018","2019") & flagObservationStatus %in% c("I","E"),TRUE,FALSE),
          by = c("geographicAreaM49","measuredItemCPC","measuredElement")]


flag_data[, offNew:= ifelse(sum(flagNew) >=1 ,TRUE,FALSE),
          by = c("geographicAreaM49","measuredItemCPC","measuredElement")]

flag_data[, flagCheck:= ifelse(offOld==TRUE & offNew ==TRUE ,TRUE,FALSE),
          by = c("geographicAreaM49","measuredItemCPC","measuredElement")]


missing_official_flag <- flag_data[flagCheck == TRUE,]

missing_official_flag$Value = format(round(missing_official_flag$Value, 0),nsmall=0 , big.mark=",",scientific=FALSE)

#togli i decimali e controllo di 2019 NA
missing_official_flag$Value<- paste(missing_official_flag$Value,missing_official_flag$flagObservationStatus,
                                    missing_official_flag$flagMethod)


missing_official_flag <-missing_official_flag[, c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears","Value"),
                                              with= FALSE]

if(nrow(missing_official_flag) != 0){
  
  missing_flag_cast <- dcast(missing_official_flag, geographicAreaM49 + measuredElement + measuredItemCPC  ~ timePointYears, 
                             value.var = c("Value"))
  
  
  
  missing_flag_cast <- nameData("aproduction", "aproduction", missing_flag_cast)
  
}else{
  
  missing_flag_cast <- data.table()
}


                                          
                                          ######################################
                                          #                                    #
                                          #                                    #
                                          #          2019 missing              #
                                          #                                    #
                                          #                                    #
                                          ######################################

miss_last_year <- data_final[measuredElement %in% flag_elements,]

miss_last_year[,
      `:=`(
        mean = mean(Value[timePointYears %in% c("2014","2015","2016","2017","2018")], na.rm = TRUE)
      ),
      by = c("geographicAreaM49", "measuredItemCPC", "measuredElement")
      ]

##prendo solo quelli che hanno media passata diversa da na quinid per cui ci sono i valori
miss_last_year = miss_last_year[!is.na(mean),]
no_data = miss_last_year[is.na(mean),]
#per ogni item c e il 2019 ?
#devi anche dire se è official flag
miss_last_year[, exists:= ifelse(timePointYears %in% c("2019") ,TRUE,FALSE),
          by = c("geographicAreaM49","measuredItemCPC","measuredElement")]

miss_last_year[, exists:= ifelse(sum(exists) ==1 ,TRUE,FALSE),
               by = c("geographicAreaM49","measuredItemCPC","measuredElement")]


miss_last_year = miss_last_year[exists==FALSE & flagObservationStatus %in% c("","T"),]

miss_last_year$Value = format(round(miss_last_year$Value, 0),nsmall=0 , big.mark=",",scientific=FALSE)

miss_last_year$Value<- paste(miss_last_year$Value,miss_last_year$flagObservationStatus,
                             miss_last_year$flagMethod)


miss_last_year <-miss_last_year[, c("geographicAreaM49","measuredElement","measuredItemCPC","timePointYears","Value"),
                                              with= FALSE]

if(nrow(miss_last_year) != 0){
  miss_last_year_cast <- dcast(miss_last_year, geographicAreaM49 + measuredElement + measuredItemCPC  ~ timePointYears, 
                               value.var = c("Value"))
  
  #setnames(miss_last_year_cast,tail(names(miss_last_year_cast), n=1), "")
  #tail(names(miss_last_year_cast), n=1)
  #last_year <- as.numeric(tail(names(miss_last_year_cast), n=1))
    if(tail(names(miss_last_year_cast), n=1) == "2018"){
      
      miss_last_year_cast <- miss_last_year_cast[!is.na(`2018`),]
      
      miss_last_year_cast <- nameData("aproduction", "aproduction", miss_last_year_cast)
    
    }else{
      miss_last_year_cast <- data.table()
    }
  }else{
    
    miss_last_year_cast <- data.table()
    
}
  




                                          ######################################
                                          #                                    #
                                          #                                    #
                                          #          Yield saving              #
                                          #                                    #
                                          #                                    #
                                          ######################################
dbg_print("Saving Yield figures")

faosws::SaveData(
  domain = "aproduction",
  dataset = "aproduction",
  data = data_prod_6,
  waitTimeout = 20000)

dbg_print("Yield figures saved")



                                          ######################################
                                          #                                    #
                                          #                                    #
                                          #          WORKBOOOK file            #
                                          #                                    #
                                          #                                    #
                                          ######################################



wb <- createWorkbook(USER)

if(nrow(outlier_file) != 0){
  
  addWorksheet(wb, "Outlier_Production")
  writeDataTable(wb, "Outlier_Production",outlier_file)
  first_fill <- createStyle(fgFill = "red")
  
  second_fill <- createStyle(fgFill = "yellow")
  
  
  
  for (i in c(8)) {
    addStyle(wb, "Outlier_Production", cols = i, 
             rows = 1 + c((1:nrow(outlier_file))[outlier_file[[i+4]] == TRUE | 
                                                   is.na(outlier_file[[i+4]])]), 
             style = first_fill, gridExpand = TRUE)
  }
  
  for (i in c(8)) {
    addStyle(wb, "Outlier_Production", cols = i, 
             rows = 1 + c((1:nrow(outlier_file))[outlier_file[[i+4]] == FALSE]), 
             style = second_fill, gridExpand = TRUE, stack = TRUE)
  }
  
  deleteData(wb, "Outlier_Production", cols = ncol(outlier_file), rows = 1:(nrow(outlier_file))+1, gridExpand = T)

}

if(nrow(missing_flag_cast) != 0){
  
  addWorksheet(wb, "Official_flag_Lost")
  writeDataTable(wb, "Official_flag_Lost",missing_flag_cast)
  
}


if(nrow(miss_last_year_cast) != 0){
  
  addWorksheet(wb, "Lost_in_2019")
  writeDataTable(wb, "Lost_in_2019",miss_last_year_cast)
  
}

#miss_last_year_cast


#removeColWidths(wb, "Outlier_Production", cols = 12:17)

# library(devtools)
# Sys.setenv(PATH = paste("C:/Rtools/bin", Sys.getenv("PATH"), sep=";"))
# Sys.setenv(BINPREF = "C:/Rtools/mingw_$(WIN)/bin/")
# saveWorkbook(wb, file = "Russia_outlier.xlsx", overwrite = TRUE)


saveWorkbook(wb, tmp_file_outliers, overwrite = TRUE)

body_message = paste("Plugin completed. Production outliers official figures.
                      ######### Colors description #########
                      Red figures: Outlier values with Yield/Carcass out of the 30% given the last 3 years;
                      Yellow figures: Outlier values with Yield/Carcass in range.
                      ",
                      sep='\n')

send_mail(from = "no-reply@fao.org", 
          to = swsContext.userEmail,
          subject = paste0("Production outliers in ", COUNTRY_NAME), 
          body = c(body_message, tmp_file_outliers))

unlink(TMP_DIR, recursive = TRUE)

print('Plug-in Completed')





