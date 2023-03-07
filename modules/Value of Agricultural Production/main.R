##### CHINA CODE FIX ###############################################################
# Remove the following lines once the china code has been patched from 1248 to 156 #
# Remove lines also in data_production.R script
# OR set CHINA_FIX = F

CHINA_FIX = T

# If TRUE, pull production data with key 1248 and convert it to 156
# if TRUE, do not calculate region aggregate
####################################################################################
####################################################################################

message(paste('Your Value of Agricultural Production Plugin has started.'))

# Loading libraries -------------------------------------------------------

message(paste('Your Value of Agricultural Production Plugin is loading libraries.'))

.libPaths("/newhome/shared/Library/3.3.3/")

suppressMessages({
  library(faosws)
  library(faoswsUtil)
  library(faoswsFlag)
  library(data.table)
  library(stringr)
  library(dplyr)
  library(methods)
})


# Setting the environment -------------------------------------------------

if(CheckDebug()){
  
  library(faoswsModules)
  SETT <- ReadSettings("~/Agriculture Production/module/Production Indices/sws.yml")
  SetClientFiles(SETT[["certdir"]])
  GetTestEnvironment(baseUrl = SETT[["server"]], token = SETT[["token"]])
  #source("~/Agriculture Production/module/Value of Agricultural Production/R function/gap_filler.R")
  source("~/Agriculture Production/module/Production Indices/R Function/discard_year_list.R", print.eval = T)
  source("~/Agriculture Production/module/Production Indices/R Function/data_production.R", print.eval = T)
  source("~/Agriculture Production/module/Production Indices/R Function/data_prices.R", print.eval = T)
  #source("~/Agriculture Production/module/Production Indices/R Function/auth_check.R", print.eval = T)
  
}

# Dataset chosen  ---------------------------------------------------------

domain_ = swsContext.datasets[[1]]@domain
dataset_ = swsContext.datasets[[1]]@dataset

# Parameters --------------------------------------------------------------

message( paste0('Your Value of Agricultural Production Plugin is reading its parameters.'))

param_base_year = ReadDatatable("value_of_agricultural_production_baseyear_param")[['baseyear']]
param_item_aggr = swsContext.computationParams$item_aggr
param_country_aggr = swsContext.computationParams$country_aggr
param_source_prod = swsContext.computationParams$source_production
param_source_prices = swsContext.computationParams$source_prices

base_year_range <- c(
  as.character(as.numeric(param_base_year)-1),
  param_base_year,
  as.character(as.numeric(param_base_year)+1))

#selected_countries = swsContext.datasets[[1]]@dimensions[["geographicAreaM49"]]@keys
selected_element = swsContext.datasets[[1]]@dimensions[["measuredElement"]]@keys
selected_years = swsContext.datasets[[1]]@dimensions[["timePointYears"]]@keys

# Check rights on datasets -------------------------------------------------

tryCatch({
  
  if (param_source_prod == "production"){ GetDatasetConfig("agriculture", "aproduction")
    
  } else if (param_source_prod == "disseminated"){
    
    GetDatasetConfig("disseminated", "livestock_production")
    GetDatasetConfig("disseminated", "crops_production")
    
  }
  
  if (param_source_prices == "prices"){
    
    GetDatasetConfig("prod_prices","annual_producer_prices_validation")
    
  } else if (param_source_prices == "diss"){
    
    GetDatasetConfig("disseminated","annual_producer_prices_validation_diss")
  }
  
  if ("434" %in% selected_element ){
    
    GetDatasetConfig("disseminated", "population_disseminated")
    
  }
  
  message("User authorized for datasets")
  
},error=function(e) {
  #message('An Error Occurred')
  message(print(e))
  stop()
})


# Get support datatable data -------------------------------------------------------

message(paste('Your Value of Agricultural Production Plugin is reading datatables.'))

vop_item <- ReadDatatable("value_of_production_item", columns = c( "description","cpc_code"))
setnames(vop_item, c("cpc_code"), c("measuredItemCPC"))

vop_item_group <- ReadDatatable("value_of_production_item_group", columns = c("domain", "item_group_code", "cpc_code"))
vop_item_group[, item_group_code := paste0("F", item_group_code)] #from FAOSTAT CODE TO SWS CODE
setnames(vop_item_group, "cpc_code", "measuredItemCPC")

# vop_indigenous <- ReadDatatable("value_of_production_indigenous_meat", columns = c("description","cpc_code"))
# setnames(vop_indigenous, c("cpc_code"), c("measuredItemCPC"))

vop_country <- ReadDatatable("value_of_production_country", columns = c("m49_code","start_date", "end_date" ))
setnames(vop_country, "m49_code", "geographicAreaM49")

vop_country_group <- ReadDatatable("value_of_production_country_group", columns =  c("country_group_code","m49_code"))
setnames(vop_country_group, "m49_code", "geographicAreaM49")


message(paste('Your Value of Agricultural Production Plugin is pulling data.'))


## Get production data ---------------------------------------------------------


# Pull data from production/disseminated data

if (param_source_prod == "production"){ 
  
  data_production <- data_prod("agriculture","aproduction", selected_years = selected_years)
  
  } else if (param_source_prod == "disseminated"){
    
    data_production = rbind(data_prod("disseminated","crops_production", selected_years = selected_years),
                            data_prod("disseminated","livestock_production", selected_years = selected_years))
    
}


# pullind data with seeds and feeds for net production value
if ( "154" %in% selected_element ) {
  if (param_source_prod == "production"){ 
    
    data_net_prod <- data_prod("agriculture","aproduction",seed_feed = T, selected_years = selected_years)
    
  } else if (param_source_prod == "disseminated"){
    
    data_net_prod = rbind(data_prod("disseminated","crops_production",seed_feed = T, selected_years = selected_years),
                            data_prod("disseminated","livestock_production",seed_feed = T, selected_years = selected_years))
    
  }
}  
  


# Removing leading zeroes ------------------------------------------------------


vop_country[, geographicAreaM49 := sub( "^0+","", geographicAreaM49 )]
vop_country_group[, geographicAreaM49 := sub( "^0+","", geographicAreaM49 )]



### Get prices data ------------------------------------------------------------


if (param_source_prices == "prices"){ 
  
  domain_prices = "prod_prices"
  dataset_prices = "annual_producer_prices_validation"
  
} else if (param_source_prices == "diss"){
  
  domain_prices = "disseminated"      #need data on disseminated domain
  dataset_prices = "annual_producer_prices_validation_diss"
}


data_price = list()

if ("152" %in% selected_element | "154" %in% selected_element | "432" %in% selected_element | "434" %in% selected_element) {
  ## Read International dollar data table
  
  vop_international_dollar <- ReadDatatable("supplementary_international_dollar", columns = c("item_code", "year", "value"))
  setnames(vop_international_dollar, c("item_code", "year", "value"), c("measuredItemCPC", "timePointYears", "Prices"))
  
  vop_international_dollar[, measuredItemCPC := str_pad(measuredItemCPC,4, pad="0")] #pad for fcl2cpc function
  vop_international_dollar[, measuredItemCPC := fcl2cpc(measuredItemCPC)] #convert from fao code to cpc
  
  vop_international_dollar <- vop_international_dollar[timePointYears %in% param_base_year]
  
}

if ("152" %in% selected_element | "432" %in% selected_element | "434" %in% selected_element){
  
  International_dollar <- merge(data_production, vop_international_dollar, by = 'measuredItemCPC', all.x = TRUE)
  International_dollar[,timePointYears.y := NULL]
  setnames(International_dollar, "timePointYears.x" , "timePointYears" ) #change names
  International_dollar[, Prices := as.numeric(Prices)]
  

      
    #International Dollar
    if ("152" %in% selected_element) {
      #Gross production International dollar
      
      data_price[["152"]] <- copy(International_dollar[, measuredElement := "152"]) 
    } 
    
    if ("432" %in% selected_element ) {
      #Gross Production Index Number (2014-2016 = 100)

      data_price[["432"]] <- copy(International_dollar[, measuredElement := "432"])
    }

    if ("434" %in% selected_element ) {
      #Gross per Capita  Production Index Number (2014-2016 = 100)

      data_price[["434"]] <- copy(International_dollar[, measuredElement := "434"])
    }
  
rm(International_dollar)
  
}

#International Dollar - Net Prod
if ("154" %in% selected_element) {
  #Net production International dollar
  
  data_net_prod <- merge(data_net_prod, vop_international_dollar, by = 'measuredItemCPC', all.x = TRUE)
  data_net_prod[,timePointYears.y := NULL]
  setnames(data_net_prod, "timePointYears.x" , "timePointYears" ) #change names
  data_net_prod[, Prices := as.numeric(Prices)]
  
  data_net_prod[, measuredElement := "154"] 
  
  prod_minus_seed_feed <- copy(data_net_prod)
  prod_minus_seed_feed[, Production := Production - Seed - Feed]
  prod_minus_seed_feed[ , c('Seed', 'Feed'):= NULL ]
  
  data_price[["154"]] <- prod_minus_seed_feed #Temporary FAOSTAT Code
  rm(prod_minus_seed_feed) 
  
} 


#Standard Local Currency
if ("56" %in% selected_element) {
  
  data_SLC <- data_prices('5531', domain_prices, dataset_prices)
  data_SLC <- merge(data_production, data_SLC, by = c("geographicAreaM49", "measuredItemCPC", "timePointYears"), all.x = TRUE)
  data_SLC[, measuredElement := "56"] 
  
  data_price[["56"]] <- data_SLC 
  rm(data_SLC)
  
}

#Average Standard Local Currency
if ("55" %in% selected_element) {
  
  data_SLC.avg <- data_prices('5534', domain_prices,dataset_prices)
  data_SLC.avg[, timePointYears := NULL]
  data_SLC.avg <- merge(data_production, data_SLC.avg, by = c("geographicAreaM49", "measuredItemCPC"), all.x = TRUE)
  data_SLC.avg[, measuredElement := "55"] 
  
  data_price[["55"]] <- data_SLC.avg 
  rm(data_SLC.avg)
  
}

#US Dollar
if ("57" %in% selected_element) {
  
  data_USD <- data_prices("5532",domain_prices,dataset_prices)
  data_USD <- merge(data_production, data_USD, by = c("geographicAreaM49", "measuredItemCPC", "timePointYears"), all.x = TRUE)
  data_USD[, measuredElement := "57"]  #Temporary FAOSTAT Code
  
  data_price[["57"]] <- data_USD 
  rm(data_USD)
  
}

#Average US Dollar
if ("58" %in% selected_element) {
  
  data_SLC.avg <- data_prices('5535', domain_prices,dataset_prices)
  data_SLC.avg[, timePointYears := NULL]
  data_SLC.avg <- merge(data_production, data_SLC.avg, by = c("geographicAreaM49", "measuredItemCPC"), all.x = TRUE)
  data_SLC.avg[, measuredElement := "58"] 
  
  data_price[["58"]] <- data_SLC.avg 
  rm(data_SLC.avg)
  
}


## Calculation -----------------------------------------------------------------

gross_production_value <- do.call("rbind", data_price)
rm(data_price)

message(paste('Your Value of Agricultural Production Plugin is calculating Gross Production Value.'))
  
gross_production_value[ , Value := round(Production*Prices /1000, digits = 2)]
gross_production_value[,  c('Production', 'Prices') := NULL]





## Production Indices (PINs) --------------------------------------------------- 


if ("432" %in% selected_element | "434" %in% selected_element ) {

message(paste0('Your Value of Agricultural Production Plugin is calculating Gross Production Index Number.'))

gross_production_value <- split(gross_production_value, gross_production_value$measuredElement) 

##Average Gross Production around base year
# Check if all the three year range for the base year is in selected session's years
if ( sum( base_year_range %in% selected_years ) == 3 ){
  
  Gross_prod.avg <- merge(data_production[timePointYears %in% base_year_range ], vop_international_dollar, by = 'measuredItemCPC', all.x = TRUE)
  Gross_prod.avg[,timePointYears.y := NULL]
  setnames(Gross_prod.avg, "timePointYears.x" , "timePointYears" ) #change names
  Gross_prod.avg[, Prices := as.numeric(Prices)]  
  
  Gross_prod.avg[ , Value := round(Production*Prices /1000, digits = 2)] #Gross Production Value
  Gross_prod.avg[,  c('Production', 'Prices') := NULL]
  
} else {
  #compute the three year mean of gross production respect to the base year
  
  # Retrive Production data for base year period
  if (param_source_prod == "production"){Gross_prod.avg <- data_prod("agriculture","aproduction", selected_years = base_year_range)
  
  } else if (param_source_prod == "disseminated"){Gross_prod.avg = rbind(data_prod("disseminated","crops_production", selected_years = base_year_range),
                                                                         data_prod("disseminated","livestock_production", selected_years = base_year_range))}
  
  Gross_prod.avg <- merge(Gross_prod.avg, vop_international_dollar, by = 'measuredItemCPC', all.x = TRUE)
  Gross_prod.avg[,timePointYears.y := NULL]
  setnames(Gross_prod.avg, "timePointYears.x" , "timePointYears" ) #change names
  Gross_prod.avg[, Prices := as.numeric(Prices)]  
  
  Gross_prod.avg[ , Value := round(Production*Prices /1000, digits = 2)] #Gross Production Value
  Gross_prod.avg[,  c('Production', 'Prices') := NULL]
  
}

#### Gross Production Index Number '432' ####

if ("432" %in% selected_element) {
  
  Gross_prod.432 <- Gross_prod.avg[, Mean := mean(Value), by = .(geographicAreaM49,measuredItemCPC)]
  Gross_prod.432 <- Gross_prod.432[timePointYears == param_base_year,]
  Gross_prod.432 <- Gross_prod.432[, c("Value","timePointYears") := NULL]
  
  if (param_item_aggr != "item_single"){item_aggr.432.avg<-copy(Gross_prod.432)} # calculate aggregation average for later 
  if (param_country_aggr != "country_single"){country_aggr.432.avg<-copy(Gross_prod.432)} # calculate aggregation average for later 

    gross_production_value$`432` <- merge(gross_production_value$`432`,Gross_prod.432, 
                                        by = c("measuredItemCPC","geographicAreaM49"),
                                        all = TRUE)
  
  if (param_item_aggr != "item_single"){Gross_prod.432<-copy(gross_production_value$`432`)} else {rm(Gross_prod.432)}
  if (param_country_aggr != "country_single"){country_aggr.432<-copy(gross_production_value$`432`)}
    
  gross_production_value$`432`[, Value := round(Value/Mean*100, digits = 2) ]
  gross_production_value$`432`[, Mean := NULL ]
  
}



#### Gross per Capita Production Index Number '434 ####

  
# function to pull Population data (item code 3010, element code 511)  from "disseminated" Domain, "population_disseminated" Dataset

if ("434" %in% selected_element) {

data_pop <- function(domain,dataset,years){
  
pop <-  GetData(DatasetKey(
                domain = domain,
                dataset =  dataset,
                dimensions = list(
                  Dimension(name = "geographicAreaM49",
                            keys = vop_country$geographicAreaM49 ),
                  Dimension(name = "measuredElement",
                            keys = "511" ),
                  Dimension(name = "timePointYears",
                            keys = years ))))

setnames(pop, 'Value', 'Population')
pop[,c("measuredElement", "flagObservationStatus" ,"flagMethod"):= NULL]

return(pop) }    

  
if ( sum( base_year_range %in% selected_years ) == 3 ){
  
  population <- data_pop("disseminated" , "population_disseminated" , years = selected_years)
  
} else {
  
  population <- data_pop("disseminated" , "population_disseminated" , years =  unique(c(selected_years,base_year_range)) )
  
}
  
  #dividing element 152 by Total Population
  gross_production_value$`434`<- merge( gross_production_value$`434`, population[timePointYears %in% selected_years], by = c( "geographicAreaM49", "timePointYears"), all.x = T)
  gross_production_value$`434`[, Value := Value/Population*100 ]
  gross_production_value$`434`[, Population := NULL]
  
  #then dividing by the average of element 152 divided by Total Population over the 3-year base period.
  Gross_prod.434 <- Gross_prod.avg[, .(measuredItemCPC,geographicAreaM49,timePointYears,Value)]
  Gross_prod.434 <- merge(Gross_prod.434, population[timePointYears %in% base_year_range], by = c( "geographicAreaM49", "timePointYears"), all.x = T)
  Gross_prod.434[, Value := Value/Population*100 ]
  Gross_prod.434[, Population := NULL]
  Gross_prod.434 <- Gross_prod.434[, Mean := mean(Value), by = .(geographicAreaM49,measuredItemCPC)]
  Gross_prod.434 <- Gross_prod.434[timePointYears == param_base_year,]
  Gross_prod.434 <- Gross_prod.434[, c("Value","timePointYears") := NULL]
  
  if (param_item_aggr != "item_single"){item_aggr.434.avg<-copy(Gross_prod.434)} # calculate aggregation average for later 
  if (param_country_aggr != "country_single"){country_aggr.434.avg<-copy(Gross_prod.434)} # calculate aggregation average for later 
  
  
  gross_production_value$`434` <- merge(gross_production_value$`434`,Gross_prod.434,
                                        by = c("measuredItemCPC","geographicAreaM49"),
                                        all.x = TRUE)
  
  if (param_item_aggr != "item_single"){Gross_prod.434<-copy(gross_production_value$`434`)} else {rm(Gross_prod.434)}
  if (param_country_aggr != "country_single"){country_aggr.434<-copy(gross_production_value$`434`)}
  
  gross_production_value$`434`[, Value := round(Value/Mean*100, digits = 2) ]
  gross_production_value$`434`[, Mean := NULL ]
  
}


gross_production_value <- do.call("rbind", gross_production_value)
gross_production_value <- gross_production_value[is.finite(Value)]

#suppressWarnings( rm(list = c("Gross_prod.avg","population")) )

}#End PINs




#### Aggregate for items groups ------------------------------------------------


if (param_item_aggr != "item_single" ) {
  
message(paste('Your Value of Agricultural Production Plugin is calculating item-aggregated Gross Production Value.'))

  if ( "154" %in% selected_element ) { 
  # item aggregate for net Production
  
    
    gross_production_value_item_aggregate <- merge(gross_production_value[!measuredElement %in% c("432","434")], vop_item_group[domain == "QV"], by = "measuredItemCPC", allow.cartesian = TRUE)
    gross_production_value_item_aggregate <- gross_production_value_item_aggregate[ measuredElement != "154"] #aggregate for other element
    gross_production_value_item_aggregate <- gross_production_value_item_aggregate[, list(Value = sum(Value, na.rm = TRUE)),
                                                                                   by = c('measuredElement','geographicAreaM49', 'timePointYears', 'item_group_code')]
    
    
    data_net_prod <- merge(data_net_prod, vop_item_group[domain == "QV"], by = "measuredItemCPC", allow.cartesian = TRUE)
    
    # Group aggregates Cereal, Total (1717), Roots and Tubers, Total (1720), Oilcrops Primary (1730), Crops (PIN) (2041), Sugar Crops Primary (1723), Vegetables and Fruit Primary (1739), Fruit Primary (1738), Vegetables Primary (1735) only subtract seed. 
    
    data_net_prod[item_group_code %in% c('F1717',
                                         'F2041',
                                         'F1738',
                                         'F1730',
                                         'F1720',
                                         'F1723',
                                         'F1739',
                                         'F1735'), Value := (Production - Seed)*Prices/1000 ]
    
    #Meat indigenous, total (1770) and Milk, total (1780) only subtract feed. 
    
    data_net_prod[item_group_code %in% c('F1770',
                                         'F1780'), Value := (Production - Feed)*Prices/1000 ]
    
    #For the other group aggregates Livestock (PIN) (2044), Agriculture (PIN) (2051), Food (PIN) (2054) and Non Food (PIN) (2057), both seed and feed are subtracted from production quantity. 
    
    data_net_prod[item_group_code %in% c('F2051',
                                         'F1753',
                                         'F2054',
                                         'F2044',
                                         'F2057'), Value := (Production - Seed - Feed)*Prices/1000 ]
    
    data_net_prod[, c("Production","Seed","Feed","Prices"):= NULL]
                                         
    data_net_prod <- data_net_prod[, list(Value = sum(Value, na.rm = TRUE)),
                                   by = c('measuredElement','geographicAreaM49', 'timePointYears', 'item_group_code')]
    
   gross_production_value_item_aggregate <- rbind(gross_production_value_item_aggregate, data_net_prod) 
   
    
    
  } else { 
      
  gross_production_value_item_aggregate <- merge(gross_production_value[!measuredElement %in% c("432","434")], vop_item_group[domain == "QV"], by = "measuredItemCPC", allow.cartesian = TRUE)
  gross_production_value_item_aggregate <- gross_production_value_item_aggregate[, list(Value = sum(Value, na.rm = TRUE)),
                                                                                 by = c('measuredElement','geographicAreaM49', 'timePointYears', 'item_group_code')]
  }
  

  # Aggregate for PINs ---------------------------------------------------------

  
  if ( "432" %in% selected_element){
    
    #aggregate the average of the gross production around base year
    item_aggr.432.avg <- merge(item_aggr.432.avg, vop_item_group[domain == "QI"], by = "measuredItemCPC", allow.cartesian = TRUE )
    item_aggr.432.avg<-item_aggr.432.avg[, list(Mean = sum(Mean, na.rm = TRUE)),by = c('geographicAreaM49', 'item_group_code')]
    
    #aggregate the gross production, current year
    item_aggr.432 <- merge(Gross_prod.432, vop_item_group[domain == "QI"], by = "measuredItemCPC", allow.cartesian = TRUE )
    item_aggr.432 <- item_aggr.432[, list(Value = sum(Value, na.rm = TRUE)),by = c('measuredElement','geographicAreaM49', 'timePointYears', 'item_group_code') ]
    
    #calculate the Index number
    item_aggr.432.fin <- copy(merge(item_aggr.432,item_aggr.432.avg,by=c("geographicAreaM49","item_group_code"), all.x = T ))
    item_aggr.432.fin[, Value := round(Value/Mean * 100, digits = 2)]
    item_aggr.432.fin[, Mean := NULL]
    
    #merge aggregate with other values
    gross_production_value_item_aggregate <- rbind(gross_production_value_item_aggregate, item_aggr.432.fin)
    
  } #end 432 aggregate 
  
  if ( "434" %in% selected_element){
    
    #aggregate the average of the gross production around base year
    item_aggr.434.avg <- merge(item_aggr.434.avg, vop_item_group[domain == "QI"], by = "measuredItemCPC", allow.cartesian = TRUE )
    item_aggr.434.avg<-item_aggr.434.avg[, list(Mean = sum(Mean, na.rm = TRUE)),by = c('geographicAreaM49', 'item_group_code')]
    
    #aggregate the gross production, current year
    item_aggr.434 <- merge(Gross_prod.434, vop_item_group[domain == "QI"], by = "measuredItemCPC", allow.cartesian = TRUE )
    item_aggr.434 <- item_aggr.434[, list(Value = sum(Value, na.rm = TRUE)),by = c('measuredElement','geographicAreaM49', 'timePointYears', 'item_group_code') ]
    
    #calculate the Index number
    item_aggr.434.fin <- copy(merge(item_aggr.434, item_aggr.434.avg, by=c("geographicAreaM49","item_group_code"), all.x =  T ))
    item_aggr.434.fin[, Value := round(Value/Mean * 100, digits = 2)]
    item_aggr.434.fin[, Mean := NULL]
    
    #merge aggregate with other values
    gross_production_value_item_aggregate <- rbind(gross_production_value_item_aggregate, item_aggr.434.fin)
    
  } #end 434 aggregate 
  
}


#### Aggregate for Countries groups ------------------------------------------------------


#### Country Aggregate Fix ############################################################################
# Fixing country aggregate parameter to Single item since country aggregates cannot be saved in SWS ###
#######################################################################################################

if(CHINA_FIX == T){param_country_aggr <- "country_single"}  # Remove to calculate country aggregates

#######################################################################################################

if (param_country_aggr != "country_single" ) {

message(paste('Your Value of Agricultural Production Plugin is calculating country-aggregated Gross Production Value.'))

gross_production_value_country_aggregate <- merge(gross_production_value[!measuredElement %in% c("432","434")], vop_country_group, by = "geographicAreaM49", allow.cartesian = TRUE)

discard_countries_year <- date_to_discard_list(vop_country) #create a list with all the country-years to remove: before startdate, after enddate

gross_production_value_country_aggregate <- merge(gross_production_value_country_aggregate,
                                                         discard_countries_year,
                                                         by = c("geographicAreaM49", "timePointYears"), all.x = T)

gross_production_value_country_aggregate <- gross_production_value_country_aggregate[ is.na(remove)] # adjust the groups for each years
gross_production_value_country_aggregate[, remove := NULL]

gross_production_value_country_aggregate <- gross_production_value_country_aggregate[, list(Value = sum(Value, na.rm = TRUE)),
                                                                by = c("measuredElement", 'country_group_code', 'timePointYears', 'measuredItemCPC')]


if ( "432" %in% selected_element){

  country_aggr.432.avg <- merge(country_aggr.432.avg, vop_country_group, by = "geographicAreaM49", allow.cartesian = TRUE)
  country_aggr.432.avg <- merge(country_aggr.432.avg,
                                discard_countries_year[timePointYears == param_base_year],
                                by = c("geographicAreaM49"), all.x = T)
  country_aggr.432.avg <- country_aggr.432.avg[ is.na(remove)] # adjust the groups for each years
  country_aggr.432.avg[, c("remove","timePointYears") := NULL]
  country_aggr.432.avg <- country_aggr.432.avg[, list(Mean = sum(Mean, na.rm = TRUE)),by = c('country_group_code','measuredItemCPC')]
  
  country_aggr.432 <- merge(country_aggr.432, vop_country_group, by = "geographicAreaM49", allow.cartesian = TRUE)
  country_aggr.432 <- merge(country_aggr.432, discard_countries_year, by = c("geographicAreaM49", "timePointYears"), all.x = T)
  country_aggr.432 <- country_aggr.432[ is.na(remove)] # adjust the groups for each years
  country_aggr.432[, c("remove") := NULL]
  country_aggr.432 <- country_aggr.432[, list(Value = sum(Value, na.rm = TRUE)),by = c('measuredElement','country_group_code', 'timePointYears', 'measuredItemCPC')]
  
  # #calculate the Index number
  country_aggr.432 <- merge(country_aggr.432, country_aggr.432.avg, by = c("country_group_code","measuredItemCPC"), all.x = T )
  country_aggr.432[, Value := round(Value/Mean * 100, digits = 2)]
  country_aggr.432[, Mean := NULL]
  
  #merge aggregate with other values
  gross_production_value_country_aggregate <- rbind(gross_production_value_country_aggregate, country_aggr.432)
  
  }

if ( "434" %in% selected_element){

  country_aggr.434.avg <- merge(country_aggr.434.avg, vop_country_group, by = "geographicAreaM49", allow.cartesian = TRUE)
  country_aggr.434.avg <- merge(country_aggr.434.avg,
                                discard_countries_year[timePointYears == param_base_year],
                                by = c("geographicAreaM49"), all.x = T)
  country_aggr.434.avg <- country_aggr.434.avg[ is.na(remove)] # adjust the groups for each years
  country_aggr.434.avg[, c("remove","timePointYears") := NULL]
  country_aggr.434.avg <- country_aggr.434.avg[, list(Mean = sum(Mean, na.rm = TRUE)),by = c('country_group_code','measuredItemCPC')]

  country_aggr.434 <- merge(country_aggr.434, vop_country_group, by = "geographicAreaM49", allow.cartesian = TRUE)
  country_aggr.434 <- merge(country_aggr.434, discard_countries_year, by = c("geographicAreaM49", "timePointYears"), all.x = T)
  country_aggr.434 <- country_aggr.434[ is.na(remove)] # adjust the groups for each years
  country_aggr.434[, c("remove") := NULL]
  country_aggr.434 <- country_aggr.434[, list(Value = sum(Value, na.rm = TRUE)),by = c('measuredElement','country_group_code', 'timePointYears', 'measuredItemCPC')]

  #calculate the Index number
  country_aggr.434 <- merge(country_aggr.434, country_aggr.434.avg, by = c("country_group_code","measuredItemCPC"), all.x =  T )
  country_aggr.434[, Value := round(Value/Mean * 100, digits = 2)]
  country_aggr.434[, Mean := NULL]

  #merge aggregate with other values
  gross_production_value_country_aggregate <- rbind(gross_production_value_country_aggregate, country_aggr.434)

}



#### Aggregate for Item groups - Countries groups ------------------------------------------------------

gross_production_value_country_item_aggregate <- merge(gross_production_value_item_aggregate[! measuredElement %in% c("432","434")],
                                                       vop_country_group, by = "geographicAreaM49", allow.cartesian = TRUE)



gross_production_value_country_item_aggregate <- merge(gross_production_value_country_item_aggregate,
                                                  discard_countries_year,
                                                  by = c("geographicAreaM49", "timePointYears"), all.x = T)

gross_production_value_country_item_aggregate <- gross_production_value_country_item_aggregate[ is.na(remove)] # adjust the groups for each years
gross_production_value_country_item_aggregate[, remove := NULL]

gross_production_value_country_item_aggregate <- gross_production_value_country_item_aggregate[, list(Value = sum(Value, na.rm = TRUE)),
                                                                                     by = c("measuredElement", 'country_group_code', 'timePointYears', 'item_group_code')]
# Production Index Numbers aggregates

if ( "432" %in% selected_element){
  
  #calculate the region aggregation for the items aggregations of base year
  item_aggr.432.avg <- merge(item_aggr.432.avg, vop_country_group, by = "geographicAreaM49", allow.cartesian = TRUE)
  item_aggr.432.avg <- merge( item_aggr.432.avg, discard_countries_year[timePointYears == param_base_year ], by = "geographicAreaM49", all.x = T)
  item_aggr.432.avg <- item_aggr.432.avg[ is.na(remove)] 
  item_aggr.432.avg[, c('timePointYears','remove') := NULL]
  item_aggr.432.avg <- item_aggr.432.avg[, list(Mean = sum(Mean, na.rm = TRUE)),by = c('country_group_code','item_group_code')]
  
  item_aggr.432 <- merge(item_aggr.432, vop_country_group, by = "geographicAreaM49", allow.cartesian = TRUE)
  item_aggr.432 <- merge( item_aggr.432, discard_countries_year, by = c("geographicAreaM49","timePointYears") , all.x = T)
  item_aggr.432 <- item_aggr.432[ is.na(remove)] 
  item_aggr.432[, c('remove') := NULL]
  item_aggr.432 <- item_aggr.432[, list(Value = sum(Value, na.rm = TRUE)),by = c('measuredElement','timePointYears', 'item_group_code','country_group_code')]
  
  #calculate the Index Number
  item_aggr.432 <- merge(item_aggr.432, item_aggr.432.avg, by = c("country_group_code","item_group_code"), all.x = TRUE )
  item_aggr.432[, Value := round(Value/Mean * 100, digits = 2)]
  item_aggr.432[, Mean := NULL]
  
  gross_production_value_country_item_aggregate <- rbind(gross_production_value_country_item_aggregate, item_aggr.432)
  
}
  
if ( "434" %in% selected_element){
  
  #calculate the region aggregation for the items aggregations of base year
  item_aggr.434.avg <- merge(item_aggr.434.avg, vop_country_group, by = "geographicAreaM49", allow.cartesian = TRUE)
  item_aggr.434.avg <- merge( item_aggr.434.avg, discard_countries_year[timePointYears == param_base_year ], by = "geographicAreaM49", all.x = T)
  item_aggr.434.avg <- item_aggr.434.avg[ is.na(remove)] 
  item_aggr.434.avg[, c('timePointYears','remove') := NULL]
  item_aggr.434.avg <- item_aggr.434.avg[, list(Mean = sum(Mean, na.rm = TRUE)),by = c('country_group_code','item_group_code')]
  
  item_aggr.434 <- merge(item_aggr.434, vop_country_group, by = "geographicAreaM49", allow.cartesian = TRUE)
  item_aggr.434 <- merge( item_aggr.434, discard_countries_year, by = c("geographicAreaM49","timePointYears") , all.x = T)
  item_aggr.434 <- item_aggr.434[ is.na(remove)] 
  item_aggr.434[, c('remove') := NULL]
  item_aggr.434 <- item_aggr.434[, list(Value = sum(Value, na.rm = TRUE)),by = c('measuredElement','timePointYears', 'item_group_code','country_group_code')]
  
  #calculate the Index Number
  item_aggr.434 <- merge(item_aggr.434, item_aggr.434.avg, by = c("country_group_code","item_group_code"), all.x = TRUE )
  item_aggr.434[, Value := round(Value/Mean * 100, digits = 2)]
  item_aggr.434[, Mean := NULL]
  
  gross_production_value_country_item_aggregate <- rbind(gross_production_value_country_item_aggregate, item_aggr.434)
  
}



setnames(gross_production_value_country_aggregate, 'country_group_code', "geographicAreaM49")
setnames(gross_production_value_country_item_aggregate, c('item_group_code','country_group_code'), c("measuredItemCPC", "geographicAreaM49"))
}#end Country aggregate


### setting names for aggregates ------------------------------------------------------


if (param_item_aggr != "item_single" ) {
setnames(gross_production_value_item_aggregate, 'item_group_code', "measuredItemCPC")
}





## Saving data -----------------------------------------------------------------


message(paste0("Your Value of Agricultural Production Plugin is saving data."))


if ( param_item_aggr == "item_single" & param_country_aggr == "country_single") {
  save_data = gross_production_value[, c('flagObservationStatus','flagMethod') := .("E", "i")  ]

} else if ( param_item_aggr == "item_single" & param_country_aggr == "country_aggr") {
  save_data = gross_production_value_country_aggregate[, c('flagObservationStatus','flagMethod') := .("E", "i")  ]
  
} else if ( param_item_aggr == "item_single" & param_country_aggr == "country_all") {
  save_data = rbind(gross_production_value, gross_production_value_country_aggregate)
  save_data = save_data[, c('flagObservationStatus','flagMethod') := .("E", "i")  ]
   
} else if ( param_item_aggr == "item_aggr" & param_country_aggr == "country_single") {
  save_data = gross_production_value_item_aggregate[, c('flagObservationStatus','flagMethod') := .("E", "i")  ]
  
} else if ( param_item_aggr == "item_aggr" & param_country_aggr == "country_aggr") {
  save_data = gross_production_value_country_item_aggregate[, c('flagObservationStatus','flagMethod') := .("E", "i")  ]
  
} else if ( param_item_aggr == "item_aggr" & param_country_aggr == "country_all") {
  save_data = rbind(gross_production_value_item_aggregate, gross_production_value_country_item_aggregate)
  save_data = save_data[, c('flagObservationStatus','flagMethod') := .("E", "i")  ]
  
} else if ( param_item_aggr == "item_all" & param_country_aggr == "country_single") {
  save_data = rbind(gross_production_value, gross_production_value_item_aggregate)
  save_data = save_data[, c('flagObservationStatus','flagMethod') := .("E", "i")  ]
  
} else if ( param_item_aggr == "item_all" & param_country_aggr == "country_aggr") {
  save_data = rbind(gross_production_value_country_aggregate, gross_production_value_country_item_aggregate)
  save_data = save_data[, c('flagObservationStatus','flagMethod') := .("E", "i")  ]
  
} else if ( param_item_aggr == "item_all" & param_country_aggr == "country_all") {
  save_data = rbind(gross_production_value,gross_production_value_country_aggregate,gross_production_value_item_aggregate,gross_production_value_country_item_aggregate)
  save_data = save_data[, c('flagObservationStatus','flagMethod') := .("E", "i")  ]
  
}
  

save_data <- save_data[is.finite(Value) & Value > 0 ]

## adding base year as metadata and saving data

if (any(c('55','58','152','154', '432', '434') %in% selected_element)) {

  config <- GetDatasetConfig(swsContext.datasets[[1]]@domain, swsContext.datasets[[1]]@dataset)
  metadata <- save_data[, mget(config$dimensions)]
  
  metadata[measuredElement %in% c('55','58','152','154', '432', '434'), `:=`(Metadata = "GENERAL",
                                           Metadata_Element = "COMMENT",
                                           Metadata_Language = "en",
                                           Metadata_Value = paste0("Base year: ",param_base_year))]
                             
  
  save <- SaveData(domain = domain_, dataset = dataset_,
                   data = save_data, metadata = metadata, waitTimeout = 100000)
} else { 
  
  save <- SaveData(domain = domain_, dataset = dataset_,
                  data = save_data, waitTimeout = 100000)
}



# Update LOG table -------------------------------------------------------------


LOG_table <- "value_of_agricultural_production_log"

LOG = data.table( user_ = swsContext.userEmail,
                  exec_date = Sys.Date(),
                  param_base_year = param_base_year,
                  param_item_aggr = if(param_item_aggr == "item_single") "Single items" 
                                    else if (param_item_aggr ==  "item_aggr") "Items Aggregates"
                                    else "Both",
                  param_country_aggr = if(param_country_aggr == "country_single") "Single Countries" 
                                       else if (param_country_aggr ==  "country_aggr") "Regions"
                                       else "Both",
                    
                  source_production = if (param_source_prod == "disseminated") "Disseminated Datasets"
                                      else "Agriculture Production",
                  source_prices = if (param_source_prices == "diss") "Disseminated Datasets"
                                  else "Annual Producer Prices (Validated)",
                  line_writed = if (is.na(save$inserted) ) 0 else save$inserted, #save$inserted,
                  line_omitted = if (is.na(save$ignored) ) 0 else save$ignored, #save$ignored,
                  line_discarded = if (is.na(save$discarded) ) 0 else save$discarded
                  )

changeset <- Changeset(LOG_table) 
AddInsertions(changeset, LOG) 
Finalise(changeset)

paste0("Your Value of Agricultural Production Plugin is completed successfully! ",
       save$inserted, " observations written, ",
       save$ignored, " weren't updated, ",
       save$discarded, " had problems.")
  