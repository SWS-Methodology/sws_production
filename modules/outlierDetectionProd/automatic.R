library(data.table)
library(faosws)
library(faoswsProduction)
library(faoswsFlag)

CERTIFICATES_DIR <- "C:/Users/NIANGAM\\Documents\\certificates\\certificates\\production"
COUNTRY <- 840



if (CheckDebug()) {
  SetClientFiles(CERTIFICATES_DIR)
  GetTestEnvironment(
    baseUrl = "https://hqlprswsas2.hq.un.fao.org:8181/sws",
    token = "6ccad2da-586c-4180-b153-0a3f2f92ae52"
  )
}

#functions

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




startYear<-2014
endYear<-2018
# interval<-2008:2013
THRESHOLD_IMPUTED<-0.3
window <- 3
interval <- (startYear-window):(startYear-1)
yearVals <- (startYear-window):endYear


flagValidTable <- ReadDatatable("valid_flags")

data <- readRDS(paste0("//hqlprsws1.hq.un.fao.org/sws_r_share/mongeau/tmp/production_outliers/data/production/", COUNTRY, ".rds"))

data<-merge(
    data,
    flagValidTable,
    by=c("flagObservationStatus","flagMethod")
)
#triplet
triplets <- ReadDatatable("item_type_yield_elements")

crop_triplet<-as.vector(triplets[item_type=="CRPR"])

crop_triplet_lst<-list(input="5312",productivity="5421",output="5510")
crop_triplet_vec<-list("5312","5421","5510")

data_crop<-data[measuredElement %in% crop_triplet_vec]

#first step
productivity_data<-data_crop[measuredElement=="5421"]

productivity_data[,
     Meanold := mean(Value[timePointYears %in% interval], na.rm = TRUE),
     by = c('geographicAreaM49', 'measuredItemCPC', 'measuredElement')
     ]

productivity_data[is.na(data$Value), Value := 0]

productivity_data[, ratio := Value / Meanold]

productivity_data[,is_outlier:=ifelse(abs(ratio-1) > THRESHOLD_IMPUTED & Protected==FALSE,TRUE,FALSE)]

productivity_data[,value_new:=Value]
productivity_data[timePointYears>2013,value_new:=NA]

productivity_data <-
    productivity_data[
        order(geographicAreaM49, measuredElement, measuredItemCPC, timePointYears),
        value_avg := rollavg(value_new, order = 3),
        by = c("geographicAreaM49", "measuredElement", "measuredItemCPC")
        ]

productivity_data[is_outlier==TRUE,Value:=value_avg]

productivity_data[,`:=`(value_avg=NULL,value_new=NULL,is_outlier=NULL)]




