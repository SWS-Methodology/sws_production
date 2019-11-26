
# Import libraries
library(data.table)
library(faosws)
library(faoswsProduction)
library(faoswsFlag)


USER<-"aydan"

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


imput_with_average<-function(data,element){
    
    #TO DO: quality check
    
    data_element<-data[measuredElement %in% element]
    
    data_element[,
                 Meanold := mean(Value[timePointYears %in% interval], na.rm = TRUE),
                 by = c('geographicAreaM49', 'measuredItemCPC', 'measuredElement')
                 ]
    
    data_element[is.na(data$Value), Value := 0]
    
    data_element[, ratio := Value / Meanold]
    
    data_element[,is_outlier:=ifelse(abs(ratio-1) > THRESHOLD_IMPUTED & Protected==FALSE,TRUE,FALSE)]
    
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








startYear<-2014
endYear<-2018
# interval<-2008:2013
THRESHOLD_IMPUTED<-0.1
window <- 3
interval <- (startYear-window):(startYear-1)
yearVals <- (startYear-window):endYear



# Read the data needed

flagValidTable <- ReadDatatable("valid_flags")
triplets <- ReadDatatable("item_type_yield_elements")

data <- readRDS(paste0("//hqlprsws1.hq.un.fao.org/sws_r_share/mongeau/tmp/production_outliers/data/production/", COUNTRY, ".rds"))

data<-merge(
    data,
    flagValidTable,
    by=c("flagObservationStatus","flagMethod")
)


# Define the triplets: INPUT, OUTPUT and PRODUCTIVITY
# TO DO: write a function for triplet selection

crop_triplet<-as.vector(triplets[item_type=="CRPR"])

crop_triplet_lst<-list(input="5312",productivity="5421",output="5510")
crop_triplet_vec<-list("5312","5421","5510")

data_crop<-data[measuredElement %in% crop_triplet_vec]



#### First step: automatic outlier check and correct for PRODUCTIVITY ####
# Select the productivity of your triplet
productivity_vars <- c("5421")

# Find and correct of outliers of productivity
data_crop <- imput_with_average(data=data_crop,"5421")

data_crop <- dcast.data.table(data_crop, geographicAreaM49 + measuredItemCPC + timePointYears ~ measuredElement, value.var = list('Value', 'Protected'))



Label_outlier<-function(data=data_crop,element="5510",type="output"){
    Value<-paste0("Value_",element)
    Protected<-paste0("Protected_",element)
    outlier<-paste0("isOutlier_",type)
    
    
    #TO DO: quality check
    
    # data_element<-data[measuredElement %in% element]
    
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

Label_outlier(data=data_crop,element="5510",type="output")
Label_outlier(data=data_crop,element="5421",type="input")


# Function to imput with the other elements
imput_with_elements <-function(data, type){
  
  #TO DO: quality check
  if (type == 'input'){
    data[Value_5421 > 0,
      `:=`(
        Value_5312 = round((Value_5510 / Value_5421), digits=6)
      )
      ]
    
  } else if(type == 'output'){
    data[Value_5421 > 0,
      `:=`(
        Value_5510 = round((Value_5421 * Value_5312), digits=6)
      )
      ]
  }

}


# Correcting outliers of Input and Output

correct_input_output <- function(data){
  # TO DO: Quality check
  if(data[,isOutlier_input == TRUE & isOutlier_output == FALSE]){
    data <- imput_with_elements(data, type = 'input')
    
  } else if (data[,isOutlier_input == TRUE & isOutlier_output == TRUE]){
    data <- imput_with_average(data, '5510')
    data <- imput_with_elements(data,type = 'input')
    
  } else if (data[,isOutlier_input == FALSE & isOutlier_output == TRUE]){
    data <- imput_with_elements(data, type = 'outlier')
    
  } else{}
  
}

