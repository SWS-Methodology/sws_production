##' 
##'
##' **Author: Amsata Niang**
##' **Author: Aydan Selek**
##' 
##' **Description:**
##'
##' This module is designed to identify and automatically correct the outliers in the production module.
##'


# Import libraries
library(data.table)
library(faosws)
library(faoswsProduction)
library(faoswsFlag)


# Set-up the test environement and parameters
USER<-"amsa"

if(USER=="amsa"){
  
  CERTIFICATES_DIR <- "C:/Users/NIANGAM\\Documents\\certificates\\certificates\\production"
} 

if(USER=="aydan"){
  CERTIFICATES_DIR <- "C:/Users/Selek/Documents/certificates/production"
}

if(USER=="christian") {
  CERTIFICATES_DIR <- "C:/Users/mongeau.FAODOMAIN/Documents/certificates/production"
} 

COUNTRY <- 840

if (CheckDebug()) {
  SetClientFiles(CERTIFICATES_DIR)
  GetTestEnvironment(
    baseUrl = "https://hqlprswsas2.hq.un.fao.org:8181/sws",
    token = "6ccad2da-586c-4180-b153-0a3f2f92ae52"
  )
}


startYear = swsContext.computationParams$start_year
endYear = swsContext.computationParams$end_year
geoM49 = swsContext.computationParams$geom49

# Set-up the parameters
startYear <- 2014
endYear <- 2018
THRESHOLD_IMPUTED <- 0.1
window <- 3
interval <- (startYear-window):(startYear-1)
yearVals <- (startYear-window):endYear


# Read the data needed
flagValidTable <- ReadDatatable("valid_flags")

mapping <- ReadDatatable("animal_parent_child_mapping")


livestock_triplet_lst_1 <- list(input="5111", output="5315", productivity="9999") # delete after
livestock_triplet_lst_1bis <- list(input="5112", output="5316", productivity="9999") # delete after

livestock_triplet_lst_2 <- list(input="5320", output="5510", productivity="5417") # delete after

livestock_triplet_lst_2bis <- list(input="5321", output="5510", productivity="5424") # delete after

crop_triplet_lst<-list(input="5312",output="5510",productivity="5421")


#triplets <- ReadDatatable("item_type_yield_elements")
#triplets$element_41[19] = "9999"# delete after off-take rate saved
data <- readRDS(paste0("//hqlprsws1.hq.un.fao.org/sws_r_share/mongeau/tmp/production_outliers/data/production/", COUNTRY, ".rds"))



# itemMap <- GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredItemCPC")
# itemMap <- itemMap[!is.na(type), .(measuredItemCPC = code, item_type = type)]  
# 
# # Identify the triplets of: Input-Outpot-Productivity
# item_map_factor <-
#     triplets[
#         nchar(element_41) == 4
#         ][,
#           .(item_type, input = element_31, productivity = element_41, output = element_51, factor)
#           ]
# 
# item_map_factor <-
#     melt(
#         item_map_factor,
#         id.vars = c("item_type", "factor"),
#         value.name = "measuredElement"
#     )
# 
# map_items <-
#     merge(
#         item_map_factor[, .(item_type, measuredElement)],
#         itemMap,
#         by = "item_type",
#         all.x = TRUE,
#         allow.cartesian = TRUE
#     )
# 
# map_items <-
#     merge(
#         map_items, 
#         item_map_factor[, .(item_type, variable, measuredElement)], 
#         by = c("item_type", "measuredElement")
#     )
# 
# map_items[, item_type := NULL]
# 
# data <- merge(data, map_items, by = c("measuredElement", "measuredItemCPC"), all.x = TRUE)

## To be deleted after off-take inserted
pathtoofftake = paste0("S:\\Livestock\\validationFAODOMAIN_rosa\\",
                       list.files("S:\\Livestock\\validationFAODOMAIN_rosa\\"))

data_list <- lapply(pathtoofftake,read.csv)

for(i in seq_along(data_list)) {
  df<-data_list[[i]]
  df<-as.data.table(df)
  
  data_list[[i]] <- df[,list(geographicAreaM49,timePointYears,
                             measuredItemCPC=paste0("0",measuredItemCPC),
                             takeOffRate,TakeOffFlagObservationStatus,
                             TakeOffRateFlagMethod)]
  
}


data_offtake <- do.call("rbind",data_list)
data_offtake <- data_offtake[geographicAreaM49==840,]
setnames(data_offtake, "TakeOffFlagObservationStatus", "flagObservationStatus")
setnames(data_offtake, "TakeOffRateFlagMethod", "flagMethod")
data_offtake$geographicAreaM49 <- as.character(data_offtake$geographicAreaM49)
data_offtake$timePointYears <- as.character(data_offtake$timePointYears)
setnames(data_offtake,"takeOffRate","Value")

data_offtake[,measuredElement:=9999] #arbitrary
data_offtakecomplete <-
    CJ(
        geographicAreaM49 = unique(data_offtake$geographicAreaM49),
        measuredElement = 9999,
        timePointYears = unique(data$timePointYears),
        measuredItemCPC = unique(data_offtake$measuredItemCPC)
    )

data_offtake<-merge(
    data_offtakecomplete,
    data_offtake,
    by=c("geographicAreaM49","timePointYears","measuredItemCPC","measuredElement"),
    all=TRUE
)

data_offtake<-data_offtake[,names(data),with=FALSE] #arbitrary


data<-rbind(data,data_offtake)

data <- merge(
    data,
    flagValidTable,
    by=c("flagObservationStatus","flagMethod")
)
# data <- merge(data, data_offtake, by = c("geographicAreaM49", "timePointYears", "measuredItemCPC", "flagObservationStatus", "flagMethod"), all.x = TRUE)

#### FUNCTIONS #####

# rollavg() is a rolling average function that uses computed averages
# to generate new values if there are missing values (and FOCB/LOCF).
# I.e.:
# vec <- c(NA, 2, 3, 2.5, 4, 3, NA, NA, NA)
#
#> RcppRoll::roll_mean(myvec, 3, fill = 'extend', align = 'right')
#[1]       NA       NA       NA 2.500000 3.166667 3.166667       NA       NA       NA 
#
#> rollavg(myvec)
#[1] 2.000000 2.000000 3.000000 2.500000 4.000000 3.000000 3.166667 3.388889 3.185185

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



Label_outlier<-function(data=data, element, type){
  Value <- paste0("Value_", element)
  Protected <- paste0("Protected_", element)
  outlier <- paste0("isOutlier_", type)
  
  data[,
       Meanold := mean(get(Value)[timePointYears %in% interval], na.rm = TRUE),
       by = c('geographicAreaM49', 'measuredItemCPC')
       ]
  
  data[is.na(get(Value)), c(Value) := 0]
  
  data[, ratio := get(Value) / Meanold]
  
  data[,c(outlier):=ifelse(abs(ratio-1) > THRESHOLD_IMPUTED & get(Protected)==FALSE,TRUE,FALSE)]
  
  data[,`:=`(Meanold=NULL,ratio=NULL)]
  
  return(data)
}

# imput_with_average() function find the outliers  the imputation for given elements (in this case only for Productivity)
imput_with_average <- function(data,element){
  
  data_element<-data[measuredElement %in% element]
  
  data_element[,
               Meanold := mean(Value[timePointYears %in% interval], na.rm = TRUE),
               by = c('geographicAreaM49', 'measuredItemCPC', 'measuredElement')
               ]

  data_element[is.na(data$Value), Value := 0]

  data_element[, ratio := Value / Meanold]

  data_element[,is_outlier:=ifelse(abs(ratio-1) > THRESHOLD_IMPUTED & Protected==FALSE,TRUE,FALSE)]
  # 
  data_element[,value_new:=Value]
  data_element[timePointYears>2013,value_new:=NA]
  
  data_element <-
    data_element[
      order(geographicAreaM49, measuredElement, measuredItemCPC, timePointYears),
      value_avg := rollavg(value_new, order = 3),
      by = c("geographicAreaM49", "measuredElement", "measuredItemCPC")
      ]
  
  data_element[is_outlier==TRUE,`:=`(Value=value_avg,
                                                 flagObservationStatus="I",
                                                 flagMethod="e")]
  
  data_element<-data_element[,colnames(data),with=FALSE]
  
  data<-rbind(
    data_element,
    data[!data_element,on=c("measuredElement")]
  )
  return(data)
}



compute_movav<-function(data=data_crop,element="5510",type="output") {
  Value<-paste0("Value_",element)
  movag<-paste0("movav_",type)
  
  data[,value_new:=get(Value)]
  data[timePointYears>2013,value_new:=NA]
  
  data <-
    data[
      order(geographicAreaM49,measuredItemCPC, timePointYears),
      c(movag) := rollavg(value_new, order = 3),
      by = c("geographicAreaM49","measuredItemCPC")
      ]
  
  data[,value_new:=NULL]
  return(data)
}


#Correcting outliers
#Parametrize input var, output var and productivity var

correctInputOutput<-function(data=data,
                             triplet=livestock_triplet_lst_1
) {
  data<-imput_with_average(data,triplet$productivity)
  
  data <- data[measuredElement %in% triplet]
  inteminput<-data[measuredElement %in% triplet$input,get("measuredItemCPC")]
  
  data<-data[measuredItemCPC %in% inteminput]
  data<- dcast.data.table(data, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value', 'Protected'))
  
  
  Label_outlier(data=data,element=triplet$output,type="output")
  compute_movav(data=data,element=triplet$output,type="output")
  
  Label_outlier(data=data,element=triplet$input,type="input")
  compute_movav(data=data,element=triplet$input,type="input")
  
  
  input<-paste0("Value_",triplet$input)
  output<-paste0("Value_",triplet$output)
  productivity<-paste0("Value_",triplet$productivity)
  
  data[isOutlier_input==TRUE & isOutlier_output==TRUE,c(output):=movav_output]
  data[isOutlier_input==TRUE & isOutlier_output==TRUE,c(input):=movav_output/get(productivity)]
  
  data[isOutlier_input==TRUE & isOutlier_output==TRUE,
       `:=`(isOutlier_input=FALSE,isOutlier_output=FALSE)]
  
  data[isOutlier_input==TRUE & isOutlier_output==FALSE,c(input):=ifelse(get(productivity)!=0,get(output)/get(productivity),NA)]
  data[isOutlier_input==TRUE & isOutlier_output==FALSE,isOutlier_input:=FALSE]
  
  data[isOutlier_input==FALSE & isOutlier_output==TRUE,
       c(output):=get(input)*get(productivity)]
  
  data[isOutlier_input==FALSE & isOutlier_output==TRUE,isOutlier_output:=TRUE]
  
  #update the productivity
  data[,c(productivity):=ifelse(get(input)!=0,get(output)/get(input),NA)]
  
  # data<-correctInputOutput(data,triplet = crop_triplet_lst)
  
  id.vars=c("geographicAreaM49","timePointYears","measuredItemCPC")
  data<-data[,c(id.vars,grep("^Value",names(data),value = TRUE)),with=FALSE]
  
  data<-melt(data, id.vars=c("geographicAreaM49","timePointYears",
                                       "measuredItemCPC"), grep("^Value",names(data),value = TRUE),
                  variable.name = "measuredElement", value.name = "value_new")
  
  data[,measuredElement:=substr(measuredElement,start = 7,stop = 10)]
  # finalCrop[value_new==Inf,value_new:=NA_real_]
  
  
  return(data)
  
}

finalCrop<-correctInputOutput(data,triplet = crop_triplet_lst)

finalliv<-correctInputOutput(data,triplet = livestock_triplet_lst_1)

setnames(mapping,"measured_item_parent_cpc","measuredItemCPC")
setnames(mapping,"measured_element_parent","measuredElement")


datamerged<-merge(
    data,
    mapping,
    by=c("measuredElement", "measuredItemCPC")
)

datamerged<-datamerged[,list(geographicAreaM49,timePointYears,
                             measuredElement=measured_element_child,
                             measuredItemCPC=measured_item_child_cpc,
                             Value,flagObservationStatus,flagMethod,
                             Valid,Protected)
                       ]

datamerged<-datamerged[,names(data),with=FALSE]

data<-rbind(
    data,
    datamerged[!data, on=c('geographicAreaM49', 'timePointYears',
                           'measuredItemCPC', 'measuredElement')]
)


finalliv2<-correctInputOutput(data,triplet = livestock_triplet_lst_2)

# poultry<-correctInputOutput(data,triplet = livestock_triplet_lst_1bis)
#seconde triplet




finalData<-rbind(finalCrop,finalliv,finalliv2)

data<-merge(
  data,
  finalData,
  by=c("geographicAreaM49","timePointYears","measuredElement", "measuredItemCPC"),
  all.x = TRUE
)

data[,difference:=value_new-Value]

#datatosend via email

dataToSend<-data[Protected==FALSE & !is.na(value_new) & difference!=0 & timePointYears>2013]

data[Protected==FALSE & !is.na(value_new) &
         difference!=0 & timePointYears>2013,`:=`(Value=value_new,
                                                               flagObservationStatus="E",
                                                               flagMethod="e")]
data[,value_new:=NULL]





# View(data)
# 
# 
# ########## PLOTS #########
# 
# dataplot<-data[measuredElement=="5312" & measuredItemCPC=="01379.90" & timePointYears>2000]
# 
# library(ggplot2)
# 
# ggplot(dataplot)+
#   geom_line(aes(as.numeric(timePointYears),Value),
#             size=1, color="green")+
#   geom_line(aes(as.numeric(timePointYears),value_new),
#             size=1, color="red")


