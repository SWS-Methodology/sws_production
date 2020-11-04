library(faosws)
library(faoswsUtil)
library(dplyr)
library(data.table)
library(tidyr)
library(openxlsx)
library(RcppRoll)
library(stringr)



start_time <- Sys.time()

R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

if (CheckDebug()) {
  R_SWS_SHARE_PATH <- "//hqlprsws1.hq.un.fao.org/sws_r_share"
  
  mydir <- "modules/derivedFaostat"
  
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

dbg_print("getting data")

#years to identify the outlier in the 2013-2014 gap
YEARS <- 2011:2018
#years to work with. Serie to check anomalies
INTERVAL <- 2014:2018

`%!in%` <- Negate(`%in%`)
#################################################### tmp file + email #################################

TMP_DIR <- file.path(tempdir(), USER)
if (!file.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)

tmp_file_derived <- file.path(TMP_DIR, paste0("Anomalies_derived_FAOSTAT.xlsx"))

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
                      "22222.01","22211","22221.01","22130.03","22130.02","22230.01","26110","21523","21521")

#GetCodeList("suafbs", "sua_balanced", "measuredElementSuaFbs")$code
selected_key = DatasetKey(domain = "suafbs", 
                          dataset = "sua_balanced", 
                          dimensions = list(measuredItemFbsSua = Dimension("measuredItemFbsSua", derived_selected), 
                                            measuredElementSuaFbs = Dimension("measuredElementSuaFbs", "5510"),
                                            geographicAreaM49 = Dimension("geographicAreaM49", as.vector(COUNTRY)),
                                            timePointYears = Dimension("timePointYears", as.character(YEARS))))

data <- GetData(selected_key)

data <- as.data.table(data)

######################################
#                                    #
#                                    #
#           JUMPS 2013-14            #
#                                    #
#                                    #
######################################
data_jump <- copy(data)

data_jump <- data_jump[order(geographicAreaM49, measuredElementSuaFbs, measuredItemFbsSua, timePointYears), 
                       avg := roll_meanr(Value, 3),
                       by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

data_jump[, avg_2013 := avg[timePointYears %in% "2013"],
          by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]


data_jump <-data_jump[, c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua","timePointYears","Value",
                          "flagObservationStatus","flagMethod","avg_2013"),
                      with= FALSE]


data_jump <- unite(data_jump, flag, c(flagObservationStatus,flagMethod), remove=TRUE, sep = "")

data_jump <- unite(data_jump, Value, c(Value,flag), remove=TRUE, sep = "_")

# data_jump$Value<- paste(data_jump$Value,data_jump$flagObservationStatus,
#                         data_jump$flagMethod)

data_jump_cast <- dcast(data_jump, geographicAreaM49 + measuredElementSuaFbs + measuredItemFbsSua + avg_2013 ~ timePointYears, 
                        value.var = c("Value"))

outlier_dt <- copy(data_jump_cast)

outlier_dt[,`:=`(lower_th = NA_real_, upper_th = NA_real_)]



######################################
#          CRITERIA                  #
######################################
#as.numeric(str_extract(`2014`, "[^_]+"))

outlier_dt[as.numeric(str_extract(`2014`, "[^_]+")) < 100 , `:=`(
  lower_th = avg_2013 - avg_2013*5,
  upper_th = avg_2013 + avg_2013*5
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]


outlier_dt[ as.numeric(str_extract(`2014`, "[^_]+")) >= 100 & as.numeric(str_extract(`2014`, "[^_]+")) < 1000 , `:=`(
  lower_th = avg_2013 - avg_2013*1,
  upper_th = avg_2013 + avg_2013*1
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

outlier_dt[ as.numeric(str_extract(`2014`, "[^_]+")) >= 1000 & as.numeric(str_extract(`2014`, "[^_]+")) < 10000 , `:=`(
  lower_th = avg_2013 - avg_2013*0.8,
  upper_th = avg_2013 + avg_2013*0.8
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

outlier_dt[ as.numeric(str_extract(`2014`, "[^_]+")) >= 10000 & as.numeric(str_extract(`2014`, "[^_]+")) < 50000 , `:=`(
  lower_th = avg_2013 - avg_2013*0.6,
  upper_th = avg_2013 + avg_2013*0.6
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

outlier_dt[ as.numeric(str_extract(`2014`, "[^_]+")) >= 50000 & as.numeric(str_extract(`2014`, "[^_]+")) < 100000 , `:=`(
  lower_th = avg_2013 - avg_2013*0.5,
  upper_th = avg_2013 + avg_2013*0.5
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

outlier_dt[ as.numeric(str_extract(`2014`, "[^_]+")) >= 100000 & as.numeric(str_extract(`2014`, "[^_]+")) < 500000 , `:=`(
  lower_th = avg_2013 - avg_2013*0.4,
  upper_th = avg_2013 + avg_2013*0.4
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

outlier_dt[ as.numeric(str_extract(`2014`, "[^_]+")) >= 500000 & as.numeric(str_extract(`2014`, "[^_]+")) < 1000000 , `:=`(
  lower_th = avg_2013 - avg_2013*0.3,
  upper_th = avg_2013 + avg_2013*0.3
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

outlier_dt[ as.numeric(str_extract(`2014`, "[^_]+")) >= 1000000 & as.numeric(str_extract(`2014`, "[^_]+")) < 3000000 , `:=`(
  lower_th = avg_2013 - avg_2013*0.15,
  upper_th = avg_2013 + avg_2013*0.15
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

outlier_dt[ as.numeric(str_extract(`2014`, "[^_]+")) >= 3000000 & as.numeric(str_extract(`2014`, "[^_]+")) < 50000000 , `:=`(
  lower_th = avg_2013 - avg_2013*0.1,
  upper_th = avg_2013 + avg_2013*0.1
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

outlier_dt[ as.numeric(str_extract(`2014`, "[^_]+"))>=50000000 , `:=`(
  lower_th = avg_2013 - avg_2013*0.1,
  upper_th = avg_2013 + avg_2013*0.1
),
by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

#1 outlier since 2014 value out of range in general!

outlier_dt[,outCheck:=ifelse(as.numeric(str_extract(`2014`, "[^_]+")) <lower_th |
                             as.numeric(str_extract(`2014`, "[^_]+")) > upper_th ,TRUE,FALSE)]

outlier_dt <- outlier_dt[outCheck==TRUE,]

#2 outlier since avg before 2014 is positive value and 2014 start to be zero

zero_dt <- copy(data_jump_cast)

zero_dt[,zeroCheck:=ifelse(avg_2013 > 0 & as.numeric(str_extract(`2014`, "[^_]+")) == 0 ,TRUE,FALSE)]

zero_dt <- zero_dt[zeroCheck==TRUE,]

# outlier since avg before 2014 is positive value and 2014 start to be na

na_dt <- copy(data_jump_cast)

na_dt[,naCheck:=ifelse(avg_2013 >= 0 & is.na(`2014`) ,TRUE,FALSE)]

na_dt <- na_dt[naCheck==TRUE,]

##################################
#          BINDING               #
##################################
outlier_dt =outlier_dt[, c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua",
                           "2011","2012","2013","2014","2015","2016","2017","2018"),with= FALSE]

zero_dt = zero_dt[, c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua",
                      "2011","2012","2013","2014","2015","2016","2017","2018"),with= FALSE]

na_dt= na_dt[, c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua",
                 "2011","2012","2013","2014","2015","2016","2017","2018"),with= FALSE]

jump_13_14 <- rbind(outlier_dt[! zero_dt , on = c("measuredElementSuaFbs","geographicAreaM49",
                                                  "measuredItemFbsSua")], zero_dt)

jump_13_14 <- rbind(jump_13_14[! na_dt , on = c("measuredElementSuaFbs","geographicAreaM49",
                                                "measuredItemFbsSua")], na_dt)



jump_13_14[] <- lapply(jump_13_14, function (x) {gsub("_", " ", x)})

jump_13_14$`2011` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_13_14$`2011`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_13_14$`2011`))
jump_13_14$`2012` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_13_14$`2012`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_13_14$`2012`))
jump_13_14$`2013` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_13_14$`2013`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_13_14$`2013`))
jump_13_14$`2014` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_13_14$`2014`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_13_14$`2014`))
jump_13_14$`2015` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_13_14$`2015`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_13_14$`2015`))
jump_13_14$`2016` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_13_14$`2016`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_13_14$`2016`))
jump_13_14$`2017` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_13_14$`2017`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_13_14$`2017`))
jump_13_14$`2018` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_13_14$`2018`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_13_14$`2018`))
# prova <- copy(jump_13_14)
# 
# prova <- melt(prova, id.vars = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua"),
#                       measure.vars = c("2011","2012","2013","2014","2015","2016","2017","2018"))
#devo rimuovere gli NA?
#missing_official_flag$Value = format(round(missing_official_flag$Value, 0),nsmall=0 , big.mark=",",scientific=FALSE)

# prova$value = paste(format(round(gsub(" .*$", "", prova$value), 0),nsmall=0 , big.mark=",",scientific=FALSE),
#               sub(".*\\s", "", prova$value))

#as.numeric(str_extract(Value, " "))
#jump_13_14$`2011` <- gsub('_', ' ', jump_13_14$`2011`)

######################################
#                                    #
#                                    #
#           JUMPS in serie           #
#                                    #
#                                    #
######################################

#check na if at least one na in the years by element item and geographic -> posso faro con 
#check zeros if at least one zero in the years by element item and geographic
data_serie<- copy(data)

data_serie <- data_serie[timePointYears %in% as.character(INTERVAL)]


#devo capire come fare il check di na perche nrow di Value non è efficace
zero_data_serie <- copy(data_serie)

zero_data_serie <- zero_data_serie[,zeroCheck:=ifelse(Value == 0 ,TRUE,FALSE)]


#if sum di check at least one zero == 5 or interval range then serieCheck := FLASE come 2162 in Bulgaria
zero_data_serie[,zeroOut:= ifelse(sum(zeroCheck) >=1 & sum(zeroCheck) < length(INTERVAL) ,TRUE,FALSE),
                by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

#if sum di check at least one na == 5  __ NON SERVE SE SONO TUTTI NA NON COMPARE LA SERIE ( prendo solo dal 2014)
zero_data_serie <- zero_data_serie[zeroOut==TRUE,]

zero_data_serie <- unite(zero_data_serie, flag, c(flagObservationStatus,flagMethod), remove=TRUE, sep = "")
zero_data_serie <- unite(zero_data_serie, Value, c(Value,flag), remove=TRUE, sep = " ")

zero_data_serie <- dcast(zero_data_serie, geographicAreaM49 + measuredElementSuaFbs + measuredItemFbsSua ~ timePointYears, 
                         value.var = c("Value"))
#NA serie

na_data_serie<- copy(data_serie)

na_data_serie <- unite(na_data_serie, flag, c(flagObservationStatus,flagMethod), remove=TRUE, sep = "")
na_data_serie <- unite(na_data_serie, Value, c(Value,flag), remove=TRUE, sep = " ")
na_data_serie <- dcast(na_data_serie, geographicAreaM49 + measuredElementSuaFbs + measuredItemFbsSua ~ timePointYears, 
                       value.var = c("Value"))

na_data_serie$na_count <- apply(na_data_serie, 1, function(x) sum(is.na(x)))

na_data_serie <- na_data_serie[,naOut:=ifelse(na_count != 0 & na_count>1 | na_count != 0 & length(INTERVAL),
                                              TRUE,FALSE)]

na_data_serie <- na_data_serie[naOut==TRUE,]

na_data_serie[, c("na_count","naOut") := NULL]


#######################################
#                                     #
#       OUTLIERS in SERIE             #
#                                     #
#######################################

outlier_data_serie<- copy(data_serie)

outlier_data_serie[order(geographicAreaM49, measuredItemFbsSua, measuredElementSuaFbs, timePointYears),
      `:=`(
        growth_rate = Value / shift(Value) - 1
      ),
      by = c("geographicAreaM49", "measuredItemFbsSua", "measuredElementSuaFbs")
      ]

outlier_data_serie <- outlier_data_serie[!is.nan(growth_rate) & !is.infinite(growth_rate),]

#### check on magnitu
# prendi serie with avg positiva:
# if 5000 <value < 10000 threshold 3 solo rispetto al valore precedente partendo dal 2015
# if 10000 < value < 50000 thre *2
# if Value > 50000 * 0.5
# aggiungi sheet outlier altri e sheet outlier Top
#
outlier_data_serie[Value >= 5000 & Value < 10000 & growth_rate > 3 |
                   Value >= 5000 & Value < 10000 & growth_rate < -3, `:=`(
      out = TRUE
),
  by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

#
outlier_data_serie[Value >= 10000 & Value < 50000 & growth_rate > 2 |
                   Value >= 10000 & Value < 50000 & growth_rate < -2, `:=`(
                       out = TRUE
                     ),
  by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]

#
outlier_data_serie[Value >= 50000 & growth_rate > 0.5 |
                   Value >=50000 & growth_rate < -0.5, `:=`(
                       out = TRUE
                     ),
  by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua")]


outlier_final <- outlier_data_serie[out==TRUE,]

outlier_final[, out := NULL]

outlier_final_cast <- copy(data_serie)
outlier_final_cast <- unite(outlier_final_cast, flag, c(flagObservationStatus,flagMethod), remove=TRUE, sep = "")
outlier_final_cast <- unite(outlier_final_cast, Value, c(Value,flag), remove=TRUE, sep = " ")
outlier_final_cast <- dcast(outlier_final_cast, geographicAreaM49 + measuredElementSuaFbs + measuredItemFbsSua ~ timePointYears, 
                            value.var = c("Value"))


outliers <- outlier_final[, c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua",
                              "timePointYears","growth_rate"),with= FALSE]

outliers <- merge(outliers, outlier_final_cast, by = c("geographicAreaM49","measuredElementSuaFbs","measuredItemFbsSua"),
                  all.x = TRUE)

setnames(outliers, "timePointYears", "year_outlier")
##################################
#          BINDING               #
##################################

jump_interval <- rbind(zero_data_serie[! na_data_serie , on = c("measuredElementSuaFbs","geographicAreaM49",
                                                                "measuredItemFbsSua")], na_data_serie)

##################################
#          THous. conv           #
##################################


jump_interval$`2014` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_interval$`2014`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_interval$`2014`))
jump_interval$`2015` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_interval$`2015`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_interval$`2015`))
jump_interval$`2016` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_interval$`2016`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_interval$`2016`))
jump_interval$`2017` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_interval$`2017`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_interval$`2017`))
jump_interval$`2018` <- paste(format(round(as.numeric(gsub(" .*$", "", jump_interval$`2018`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                           sub(".*\\s", "", jump_interval$`2018`))
###########################################################################à
outliers$`2014` <- paste(format(round(as.numeric(gsub(" .*$", "", outliers$`2014`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                              sub(".*\\s", "", outliers$`2014`))
outliers$`2015` <- paste(format(round(as.numeric(gsub(" .*$", "", outliers$`2015`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                              sub(".*\\s", "", outliers$`2015`))
outliers$`2016` <- paste(format(round(as.numeric(gsub(" .*$", "", outliers$`2016`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                              sub(".*\\s", "", outliers$`2016`))
outliers$`2017` <- paste(format(round(as.numeric(gsub(" .*$", "", outliers$`2017`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                              sub(".*\\s", "", outliers$`2017`))
outliers$`2018` <- paste(format(round(as.numeric(gsub(" .*$", "", outliers$`2018`)), 0),nsmall=0 , big.mark=",",scientific=FALSE),
                              sub(".*\\s", "", outliers$`2018`))
##################################
#          NAMES data            #
##################################

jump_13_14 <- nameData("suafbs", "sua_balanced", jump_13_14)

jump_interval <- nameData("suafbs", "sua_balanced", jump_interval)

outliers <- nameData("suafbs", "sua_balanced", outliers)



top_62 <- c("4","12","24","50","68","854","116","120","140","148","1248","170","178","180","384","218","748","231","320","324","332",
            "340","356","360","364","368","404","408","418","430","450","454","466","484","508","104","524","562","566","586","598",
            "604","608","646","686","694","706","710","144","729","760","762","834","764","768","800","860","862","704","887","894",
            "716")

jump_13_14_ALL <- jump_13_14[geographicAreaM49 %!in% top_62]
jump_interval_ALL <- jump_interval[geographicAreaM49 %!in% top_62]
outlier_final_ALL <- outliers[geographicAreaM49 %!in% top_62]

jump_13_14_TOP <- jump_13_14[geographicAreaM49 %in% top_62]
jump_interval_TOP <- jump_interval[geographicAreaM49 %in% top_62]
outlier_final_TOP <- outliers[geographicAreaM49 %in% top_62]

#jump_13_14[] <- lapply(jump_13_14, function (x) {gsub("_", " ", x)})
#as.numeric(str_extract(`2014`, "[^_]+"))
#as.numeric(str_extract(jump_13_14$`2014`, "[^_]+"))
#jump_13_14$`2014` = format(round(as.numeric(str_extract(jump_13_14$`2014`, "[^_]+")), 0),nsmall=0 ,big.mark=",",scientific=FALSE)
##################################
#          Create file          #
##################################

wb <- createWorkbook(USER)

if(nrow(jump_13_14_ALL) != 0){
  
  addWorksheet(wb, "Jumps_2013_2014_ALL")
  writeDataTable(wb, "Jumps_2013_2014_ALL",jump_13_14_ALL)
  
}

if(nrow(jump_interval_ALL) != 0){
  
  addWorksheet(wb, "Jumps_2014_2018_ALL")
  writeDataTable(wb, "Jumps_2014_2018_ALL",jump_interval_ALL)
  
}

if(nrow(outlier_final_ALL) != 0){
  
  addWorksheet(wb, "Outlier_ALL")
  writeDataTable(wb, "Outlier_ALL",outlier_final_ALL)
  
}

if(nrow(jump_13_14_TOP) != 0){
  
  addWorksheet(wb, "Jumps_2013_2014_TOP62")
  writeDataTable(wb, "Jumps_2013_2014_TOP62",jump_13_14_TOP)
  
}

if(nrow(jump_interval_TOP) != 0){
  
  addWorksheet(wb, "Jumps_2014_2018_TOP62")
  writeDataTable(wb, "Jumps_2014_2018_TOP62",jump_interval_TOP)
  
}

if(nrow(outlier_final_TOP) != 0){
  
  addWorksheet(wb, "Outlier_TOP")
  writeDataTable(wb, "Outlier_TOP",outlier_final_TOP)
  
}


saveWorkbook(wb, tmp_file_derived, overwrite = TRUE)

body_message = paste("Plugin completed.
                     ######### Sheet description (Devided by top 62 countries and all the others) #########
                     Jumps_2013_2014: The sheet contains jumps between 2013-2014 (can be outliers, change of trend);
                     Jumps_2014_2018: The sheet contains the times series with anomalies in the last years;
                     Outlier: Contains values exceeding growth rates thresholds
                     ",
                     sep='\n')

send_mail(from = "no-reply@fao.org", 
          to = swsContext.userEmail,
          subject = paste0("Monitoring derived selectet Faostat dissemination "), 
          body = c(body_message, tmp_file_derived))

unlink(TMP_DIR, recursive = TRUE)

print('Plug-in Completed')
