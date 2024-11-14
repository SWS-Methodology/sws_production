library(faosws)
library(faoswsUtil)
library(dplyr)
library(data.table)
library(tidyr)
library(openxlsx)
library(RcppRoll)
library(stringr)
library(faoswsProduction)



start_time <- Sys.time()

R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

if (CheckDebug()) {
  R_SWS_SHARE_PATH <- "//hqlprsws1.hq.un.fao.org/sws_r_share"
  
  mydir <- "modules/Derived Faostat check"
  
  SETTINGS <- faoswsModules::ReadSettings(file.path(mydir, "sws.yml"))
  
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
  message(paste0("Derived check in (", COUNTRY, "): ", x))
}

#years to identify the outlier in the 2013-2014 gap
start_year <- as.character(as.numeric(swsContext.computationParams$start_year)-1)

end_year <- as.character(swsContext.computationParams$end_year)

#need to extract data to have at least 3 years before to compute the mean moving avg
YEARS <- as.character((as.numeric(start_year)-3):as.numeric(end_year))
#years to work with. Serie to check anomalies
FOCUS_INTERVAL <- start_year:end_year

#startYear <- 2013
#endYear <- 2019

`%!in%` <- Negate(`%in%`)

#################################################### tmp file + email #################################

TMP_DIR <- file.path(tempdir(), USER)

if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)
#"IMBALANCE_", COUNTRY, ".xlsx"
tmp_file_derived <- file.path(TMP_DIR, paste0("Anomalies_derived_", COUNTRY,".xlsx"))


expandYear = function(data,
                      areaVar = "geographicAreaM49",
                      elementVar = "measuredElementSuaFbs",
                      itemVar = "measuredItemFbsSua",
                      yearVar = "timePointYears",
                      valueVar = "Value",
                      obsflagVar="flagObservationStatus",
                      methFlagVar="flagMethod",
                      newYears=NULL){
  key = c(elementVar, areaVar,  itemVar)
  keyDataFrame = data[, key, with = FALSE]
  
  keyDataFrame=keyDataFrame[with(keyDataFrame, order(get(key)))]
  keyDataFrame=keyDataFrame[!duplicated(keyDataFrame)]
  
  yearDataFrame = unique(data[,get(yearVar)])
  if(!is.null(newYears)){
    
    yearDataFrame=unique(c(yearDataFrame, newYears, newYears-1, newYears-2))
    
  }
  
  yearDataFrame=data.table(yearVar=yearDataFrame)
  colnames(yearDataFrame) = yearVar
  
  completeBasis =
    data.table(merge.data.frame(keyDataFrame, yearDataFrame))
  expandedData = merge(completeBasis, data, by = colnames(completeBasis), all.x = TRUE)
  expandedData = fillRecord(expandedData,areaVar=areaVar,itemVar=itemVar, yearVar=yearVar )
  
  ##------------------------------------------------------------------------------------------------------------------
  ## control closed series: if in the data pulled from the SWS, the last protected value is flagged as (M,-).
  ## In this situation we do not have to expand the session with (M, u), but with (M, -) in order to
  ## avoid that the series is imputed for the new year
  
  ## 1. add a column containing the last year for which it is available a PROTECTED value
  seriesToBlock=expandedData[(get(methFlagVar)!="u"),]
  #seriesToBlock[,lastYearAvailable:=max(timePointYears), by=c( "geographicAreaM49","measuredElement","measuredItemCPC")]
  seriesToBlock[,lastYearAvailable:=max(get(yearVar)), by=key]
  ## 2. build the portion of data that has to be overwritten
  
  seriesToBlock[,flagComb:=paste(get(obsflagVar),get(methFlagVar), sep = ";")]
  seriesToBlock=seriesToBlock[get(yearVar)==lastYearAvailable & flagComb=="M;-"]
  
  
  ##I have to expand the portion to include all the yers up to the last year
  if(nrow(seriesToBlock)>0){
    seriesToBlock=seriesToBlock[, {max_year = max(as.integer(.SD[,timePointYears]))
    data.table(timePointYears = seq.int(max_year + 1, newYears),
               Value = NA_real_,
               flagObservationStatus = "M",
               flagMethod = "-")[max_year < newYears]},  by = key]
    
    setDT(seriesToBlock)[, ("timePointYears") := lapply(.SD, as.character), .SDcols = "timePointYears"]
    ##I have to expand the portion to include all the yers up to the last year
    expandedData=
      merge(expandedData, seriesToBlock,
            by =  c(areaVar, elementVar, itemVar, yearVar),
            all.x=TRUE, suffixes = c("","_MDash"))
    
    expandedData[!is.na(flagMethod_MDash),flagMethod:=flagMethod_MDash]
    expandedData[!is.na(flagObservationStatus_MDash),flagObservationStatus:=flagObservationStatus_MDash]
    expandedData=expandedData[,colnames(data),with=FALSE]
  }
  
  
  expandedData
}
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
##############################################################################################################


derived_selected <- c("24310.01","01921.02","0143","21700.02","23540","2166","2168","2162","21691.12","21691.02",
                      "2167","2165","21691.14","21641.01","21631.02","21691.07","2161","21631.01","01491.02","2351f",
                      "24212.02","22249.01","22249.02","22242.01","22241.01","22254","22252","22253","22251.02",
                      "22251.01","22120","22241.02","22242.02","22230.04","22222.02","22110.02","22212","22221.02",
                      "22222.01","22211","22221.01","22130.03","22130.02","22230.01","26110","21523","21521","23913")


key <- swsContext.datasets[[1]]

key@dimensions$timePointYears@keys <- as.character(YEARS)
key@dimensions$measuredItemFbsSua@keys <- derived_selected
key@dimensions$measuredElementSuaFbs@keys <- "5510"
key@dimensions$geographicAreaM49@keys <- COUNTRY

data <- GetData(key)

if(nrow(data) == 0){
  
  
  wb <- createWorkbook(USER)
  
  
  saveWorkbook(wb, tmp_file_derived, overwrite = TRUE)
  
  body_message = paste("Plugin completed. No derived faostat data to check",
                       sep='\n')
  
  send_mail(from = "no-reply@fao.org", 
            to = swsContext.userEmail,
            subject = paste0("Derived outliers in ", COUNTRY_NAME), 
            body = c(body_message, tmp_file_derived))
  
  unlink(TMP_DIR, recursive = TRUE)
  
  
  print('No derived Faostat data to check')
  stop('No derived Faostat data to check')
  
}

processedData <-
  expandYear(
    data       = data,
    areaVar    = "geographicAreaM49",
    elementVar = "measuredElementSuaFbs",
    itemVar    = "measuredItemFbsSua",
    valueVar   = "Value",
    yearVar = "timePointYears",
    newYears=as.numeric(end_year)
  )

###############################################
########## OUTLIERS ROUTINE 13-19 #############
###############################################
out_data <- copy(processedData)
dbg_print("Data processed")
out_data[order(geographicAreaM49, measuredElementSuaFbs, measuredItemFbsSua, timePointYears), 
         avg := roll_meanr(Value, 3, na.rm = TRUE),
         by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[order(geographicAreaM49,measuredItemFbsSua,measuredElementSuaFbs,timePointYears),prev_avg:=lag(avg),
         by= c("geographicAreaM49","measuredItemFbsSua","measuredElementSuaFbs")]

out_data[, avg := NULL]

out_data <- out_data[order(geographicAreaM49, measuredElementSuaFbs, 
                           measuredItemFbsSua),] [order( -timePointYears ),]

out_data <- out_data[timePointYears %in% as.character(start_year:end_year),]

out_data[,flag_Check:=ifelse(flagObservationStatus %in% c("", "T", "A", "X"),TRUE,FALSE)]

out_data[,`:=`(lower_th = NA_real_, upper_th = NA_real_)]


######################################
#                                    #
#                                    #
#          STRICT CRITERIA           #
#                                    #
#                                    #
######################################
out_data[Value < 100 & flag_Check == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*5,
  upper_th = prev_avg + prev_avg*5
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

dbg_print("line 268")
out_data[ Value >= 100 & Value < 1000 & flag_Check == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*2,
  upper_th = prev_avg + prev_avg*2
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]


out_data[ Value >= 1000 & Value < 5000 & flag_Check == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*1.5,
  upper_th = prev_avg + prev_avg*1.5
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 5000 & Value < 10000 & flag_Check == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.7,
  upper_th = prev_avg + prev_avg*0.7
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 10000 & Value < 50000 & flag_Check == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.6,
  upper_th = prev_avg + prev_avg*0.6
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 50000 & Value < 100000 & flag_Check == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.5,
  upper_th = prev_avg + prev_avg*0.5
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 100000 & Value < 500000 & flag_Check == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.4,
  upper_th = prev_avg + prev_avg*0.4
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 500000 & Value < 1000000 & flag_Check == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.3,
  upper_th = prev_avg + prev_avg*0.3
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 1000000 & Value < 3000000 & flag_Check == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.15,
  upper_th = prev_avg + prev_avg*0.15
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 3000000 & Value < 50000000 & flag_Check == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.1,
  upper_th = prev_avg + prev_avg*0.1
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value>=50000000 & flag_Check == FALSE, `:=`(
  lower_th = prev_avg - prev_avg*0.1,
  upper_th = prev_avg + prev_avg*0.1
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

dbg_print("End of strict criteria")


######################################
#                                    #
#                                    #
#          SOFT CRITERIA             #
#                                    #
#                                    #
######################################
out_data[Value < 100 & flag_Check == TRUE, `:=`(
  lower_th = prev_avg - prev_avg*9,
  upper_th = prev_avg + prev_avg*9
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

dbg_print("line 346")
out_data[ Value >= 100 & Value < 1000 & flag_Check == TRUE, `:=`(
  lower_th = prev_avg - prev_avg*5,
  upper_th = prev_avg + prev_avg*5
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 1000 & Value < 10000 & flag_Check == TRUE, `:=`(
  lower_th = prev_avg - prev_avg*1,
  upper_th = prev_avg + prev_avg*1
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 10000 & Value < 50000 & flag_Check == TRUE, `:=`(
  lower_th = prev_avg - prev_avg*0.7,
  upper_th = prev_avg + prev_avg*0.7
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 50000 & Value < 100000 & flag_Check == TRUE, `:=`(
  lower_th = prev_avg - prev_avg*0.6,
  upper_th = prev_avg + prev_avg*0.6
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 100000 & Value < 500000 & flag_Check == TRUE, `:=`(
  lower_th = prev_avg - prev_avg*0.5,
  upper_th = prev_avg + prev_avg*0.5
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 500000 & Value < 1000000 & flag_Check == TRUE, `:=`(
  lower_th = prev_avg - prev_avg*0.4,
  upper_th = prev_avg + prev_avg*0.4
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 1000000 & Value < 3000000 & flag_Check == TRUE, `:=`(
  lower_th = prev_avg - prev_avg*0.4,
  upper_th = prev_avg + prev_avg*0.4
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 3000000 & Value < 20000000 & flag_Check == TRUE, `:=`(
  lower_th = prev_avg - prev_avg*0.3,
  upper_th = prev_avg + prev_avg*0.3
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value >= 20000000 & Value < 50000000 & flag_Check == TRUE, `:=`(
  lower_th = prev_avg - prev_avg*0.15,
  upper_th = prev_avg + prev_avg*0.15
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

out_data[ Value>=50000000 & flag_Check == TRUE, `:=`(
  lower_th = prev_avg - prev_avg*0.1,
  upper_th = prev_avg + prev_avg*0.1
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

dbg_print("End of soft criteria")


####################################################FINAL CHECK#####################################################
#1)outlier since Value out of range wrt previous average


out_data[,outCheck:=ifelse(Value <lower_th  |
                             Value > upper_th ,TRUE,FALSE)]

out_data[(prev_avg/Value) > 9 ,outCheck:=TRUE]

out_data[Value < 1000 & prev_avg < 1000, outCheck:=FALSE]


#2) outliers since previous average ha positiva value and the current is either 0 or na

out_data[,zeroCheck:=ifelse(prev_avg > 0 & Value == 0 ,TRUE,FALSE)]

out_data[,sparseCheck:=ifelse(prev_avg == 0 & is.na(Value) ,TRUE,FALSE)]

out_data[,naCheck:=ifelse(prev_avg > 0 & is.na(Value) ,TRUE,FALSE)]



###############################################
########### Growth ROUTINE 11-12 ##############
###############################################
# gr_data <- processedData[timePointYears %in% c("2010","2011","2012"),]
# 
# gr_data <- gr_data[order(geographicAreaM49, measuredItemFbsSua, measuredElementSuaFbs, timePointYears)]
# 
# gr_data[,
#         `:=`(
#           growth_rate = Value / shift(Value) - 1
#         ),
#         by = c("geographicAreaM49", "measuredItemFbsSua", "measuredElementSuaFbs")
#         ]
# 
# gr_data[,flag_Check:=ifelse(flagObservationStatus %in% "T",TRUE,FALSE)]
# 
# gr_data[,`:=`(grCheck = FALSE)]
# ######################################
# #                                    #
# #                                    #
# #          G RATE CRITERIA           #
# #                                    #
# #                                    #
# ######################################
# 
# #### STRICT ####
# 
# gr_data[Value < 100 & growth_rate > 5 & flag_Check == FALSE | Value < 100 & growth_rate < -5 & flag_Check == FALSE, grCheck := TRUE]
# 
# gr_data[ Value >= 100 & Value < 1000 & growth_rate > 2 & flag_Check == FALSE | Value >= 100 & Value < 1000 & growth_rate < -2 & flag_Check == FALSE, grCheck := TRUE]
# 
# gr_data[ Value >= 1000 & Value < 5000 & growth_rate > 1.5 & flag_Check == FALSE | Value >= 1000 & Value < 5000 & growth_rate < -1.5 & flag_Check == FALSE, grCheck := TRUE]
# 
# gr_data[ Value >= 5000 & Value < 10000 & growth_rate > 0.7 & flag_Check == FALSE | Value >= 5000 & Value < 10000 & growth_rate < -0.7 & flag_Check == FALSE, grCheck := TRUE]
# 
# gr_data[ Value >= 10000 & Value < 50000 & growth_rate > 0.6 & flag_Check == FALSE | Value >= 10000 & Value < 50000 & growth_rate < -0.6 & flag_Check == FALSE, grCheck := TRUE]
# 
# gr_data[ Value >= 50000 & Value < 100000 & growth_rate > 0.5 & flag_Check == FALSE | Value >= 50000 & Value < 100000 & growth_rate < -0.5 & flag_Check == FALSE, grCheck := TRUE]
# 
# gr_data[ Value >= 100000 & Value < 500000 & growth_rate > 0.4 & flag_Check == FALSE | Value >= 100000 & Value < 500000 & growth_rate < -0.4 & flag_Check == FALSE, grCheck := TRUE]
# 
# gr_data[ Value >= 500000 & Value < 1000000 & growth_rate > 0.3 & flag_Check == FALSE | Value >= 500000 & Value < 1000000 & growth_rate < -0.3 & flag_Check == FALSE, grCheck := TRUE]
# 
# gr_data[ Value >= 1000000 & Value < 3000000 & growth_rate > 0.15 & flag_Check == FALSE| Value >= 1000000 & Value < 3000000 & growth_rate < -0.15 & flag_Check == FALSE, grCheck := TRUE]
# 
# gr_data[ Value >= 3000000 & Value < 50000000 & growth_rate > 0.1 & flag_Check == FALSE | Value >= 3000000 & Value < 50000000 & growth_rate < -0.1 & flag_Check == FALSE, grCheck := TRUE]
# 
# gr_data[ Value>=50000000 & growth_rate > 0.1 & flag_Check == FALSE |  Value>=50000000 & growth_rate < -0.1 & flag_Check == FALSE, grCheck := TRUE]
# 
# #### SOFT ####
# 
# gr_data[Value < 100 & growth_rate > 9 & flag_Check == TRUE | Value < 100 & growth_rate < -9 & flag_Check == TRUE, grCheck := TRUE]
# 
# gr_data[ Value >= 100 & Value < 1000 & growth_rate > 5 & flag_Check == TRUE | Value >= 100 & Value < 1000 & growth_rate < -5 & flag_Check == TRUE, grCheck := TRUE]
# 
# gr_data[ Value >= 1000 & Value < 10000 & growth_rate > 1 & flag_Check == TRUE | Value >= 1000 & Value < 10000 & growth_rate < -1 & flag_Check == TRUE, grCheck := TRUE]
# 
# gr_data[ Value >= 10000 & Value < 50000 & growth_rate > 0.7 & flag_Check == TRUE | Value >= 10000 & Value < 50000 & growth_rate < -0.7 & flag_Check == TRUE, grCheck := TRUE]
# 
# gr_data[ Value >= 50000 & Value < 100000 & growth_rate > 0.6 & flag_Check == TRUE | Value >= 50000 & Value < 100000 & growth_rate < -0.6 & flag_Check == TRUE, grCheck := TRUE]
# 
# gr_data[ Value >= 100000 & Value < 500000 & growth_rate > 0.5 & flag_Check == TRUE | Value >= 100000 & Value < 500000 & growth_rate < -0.5 & flag_Check == TRUE, grCheck := TRUE]
# 
# gr_data[ Value >= 500000 & Value < 1000000 & growth_rate > 0.4 & flag_Check == TRUE | Value >= 500000 & Value < 1000000 & growth_rate < -0.4 & flag_Check == TRUE, grCheck := TRUE]
# 
# gr_data[ Value >= 1000000 & Value < 3000000 & growth_rate > 0.4 & flag_Check == TRUE | Value >= 1000000 & Value < 3000000 & growth_rate < -0.4 & flag_Check == TRUE, grCheck := TRUE]
# 
# gr_data[ Value >= 3000000 & Value < 20000000 & growth_rate > 0.3 & flag_Check == TRUE | Value >= 3000000 & Value < 20000000 & growth_rate < -0.3 & flag_Check == TRUE, grCheck := TRUE]
# 
# gr_data[ Value >= 20000000 & Value < 50000000 & growth_rate > 0.15 & flag_Check == TRUE | Value >= 20000000 & Value < 50000000 & growth_rate < -0.15 & flag_Check == TRUE, grCheck := TRUE]
# 
# gr_data[ Value>=50000000 & growth_rate > 0.1 & flag_Check == TRUE |  Value>=50000000 & growth_rate < -0.1 & flag_Check == TRUE, grCheck := TRUE]
# 
# 
# dbg_print("End of gr criteria")


############ filter outliers data ############

outliers1 <- out_data[outCheck==TRUE | zeroCheck ==TRUE | sparseCheck == TRUE |naCheck == TRUE,]

#outliers2 <- gr_data[grCheck == TRUE,]

# outlier_final <- rbind(outliers1[, c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua","timePointYears","Value","flagObservationStatus"), with= FALSE],
#                        outliers2[, c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua","timePointYears","Value","flagObservationStatus"), with = FALSE])

outlier_final <- outliers1[,c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua","timePointYears","Value","flagObservationStatus"), with= FALSE]

#trasforma i valori con la virgola qui

######################## EXPAND FORM #####################


data_dcast <- processedData[measuredItemFbsSua %in% unique(outlier_final$measuredItemFbsSua) & timePointYears %in% YEARS,]

data_dcast$Value = format(round(data_dcast$Value, 0),nsmall=0 , big.mark=",",scientific=FALSE)

shrink_flag <- unite(data_dcast, flag, c(flagObservationStatus,flagMethod), remove=TRUE, sep = "")

shrink_flag <- unite(shrink_flag, Value, c(Value,flag), remove=TRUE, sep = " ")

if (nrow(shrink_flag) > 0) {
  
  data_dcast <- dcast(shrink_flag, geographicAreaM49 + measuredElementSuaFbs + measuredItemFbsSua  ~ timePointYears, 
                      value.var = c("Value"))
  
  data_dcast <- nameData(key@domain, key@dataset, data_dcast)
  
}

######################################
#                                    #
#                                    #
#          2019 missing              # 
#                                    # 
#                                    # 
######################################

check_last_year <- copy(processedData)


check_last_year[order(geographicAreaM49, measuredElementSuaFbs, measuredItemFbsSua, timePointYears), 
                avg := roll_meanr(Value, 3, na.rm = TRUE),
                by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

check_last_year[order(geographicAreaM49,measuredItemFbsSua,measuredElementSuaFbs,timePointYears),prev_avg:=lag(avg),
                by= c("geographicAreaM49","measuredItemFbsSua","measuredElementSuaFbs")]

check_last_year[, avg := NULL]

check_last_year <- check_last_year[order(geographicAreaM49, measuredElementSuaFbs, 
                                         measuredItemFbsSua),] [order( -timePointYears ),]


check_last_year[,`:=`(missing = FALSE)]

#se il 19 manca anche solo imputation da segnalare. avendo fatto expand year ed essendoci processed data dovrebbe bastare questo check
#aggiungi & prev_avg => 0 (per vedere se gli anni precedenti esistono. ma chiedi a irina come gestiamo casi come 22241.02 in Bulgaria)
missing_last_year <- check_last_year[is.na(Value) & timePointYears %in% as.character(tail(YEARS,1)) & prev_avg >= 0, missing := TRUE]

#se il 19 c e e non è ufficiale mentre negli anni prima c e almeno un ufficiale

missing_last_year[,
                  `:=`(
                    mean = mean(Value[timePointYears %in% as.character((tail(FOCUS_INTERVAL,1)-2): (tail(FOCUS_INTERVAL,1)))], na.rm = TRUE)
                  ),
                  by = c("geographicAreaM49", "measuredItemFbsSua", "measuredElementSuaFbs")
                  ]

missing_last_year = missing_last_year[!is.nan(mean),]

###########################################
#### RIMUOVI -2 metti -1 IN LAST CHECK 2019 OFFICIAL
###########################################
missing_last_year[, exists:= ifelse(timePointYears %in% as.character((tail(FOCUS_INTERVAL,1)-2): (tail(FOCUS_INTERVAL,1)-1)) 
                                    & flagObservationStatus %in% c("", "T", "A", "X"),TRUE,FALSE),
                  by = c("geographicAreaM49","measuredItemFbsSua","measuredElementSuaFbs")]

missing_last_year[, exists:= ifelse(sum(exists) >=1 ,TRUE,FALSE),
                  by = c("geographicAreaM49","measuredItemFbsSua","measuredItemFbsSua")]


missing_last_year[,`:=`(miss_official = FALSE)]

missing_last_year[timePointYears %in% as.character(tail(FOCUS_INTERVAL,1))  & flagObservationStatus %!in% c("", "T", "A", "X") 
                  & exists == TRUE, miss_official := TRUE]

missing_last_year <- missing_last_year[miss_official == TRUE | missing == TRUE,]

last_check <- processedData[measuredItemFbsSua %in% unique(missing_last_year$measuredItemFbsSua),]

last_check$Value = format(round(last_check$Value, 0),nsmall=0 , big.mark=",",scientific=FALSE)
#miss_last_year = check_last_year[exists==TRUE & !is.na(Value) & flagObservationStatus %in% c("","T"),]

#miss_last_year = processedData[measuredItemFbsSua %in% unique(miss_last_year$measuredItemFbsSua),]

shrink_flag_last <- unite(last_check, flag, c(flagObservationStatus,flagMethod), remove=TRUE, sep = "")

shrink_flag_last <- unite(shrink_flag_last, Value, c(Value,flag), remove=TRUE, sep = " ")

if (nrow(shrink_flag_last) > 0) {
  
  data_last_dcast <- dcast(shrink_flag_last, geographicAreaM49 + measuredElementSuaFbs + measuredItemFbsSua  ~ timePointYears, 
                           value.var = c("Value"))
  
  data_last_dcast <- nameData(key@domain, key@dataset, data_last_dcast)
  
}



dbg_print("end of derived production check.. preparing excel file")
######################################
#                                    #
#                                    #
#          WORKBOOOK file            #
#                                    #
#                                    #
######################################



wb <- createWorkbook(USER)

if(nrow(shrink_flag) != 0){
  
  addWorksheet(wb, "Derived_outliers")
  writeDataTable(wb, "Derived_outliers",data_dcast)
  
  
}

if(nrow(shrink_flag_last) != 0){
  
  addWorksheet(wb, "Last_year_check")
  writeDataTable(wb, "Last_year_check",data_last_dcast)
  
}
# 
# 
# library(devtools)
# Sys.setenv(PATH = paste("C:/Rtools/bin", Sys.getenv("PATH"), sep=";"))
# Sys.setenv(BINPREF = "C:/Rtools/mingw_$(WIN)/bin/")
# 
# saveWorkbook(wb, file = "Gambia_CHECK.xlsx", overwrite = TRUE)


saveWorkbook(wb, tmp_file_derived, overwrite = TRUE)

body_message = paste("Plugin completed. Derived Items to check.
                     ######### Excel sheets description #########
                     Derived_outliers: Anomalies in series;
                     Last_year_check: Missing values or missing official figures identified in the last year of the analysis.
                     ",
                     sep='\n')

# send_mail(from = "no-reply@fao.org", 
#           to = swsContext.userEmail,
#           subject = paste0("Derived outliers in ", COUNTRY_NAME), 
#           body = c(body_message, tmp_file_derived))

unlink(TMP_DIR, recursive = TRUE)

print('Plug-in Completed, check email')

