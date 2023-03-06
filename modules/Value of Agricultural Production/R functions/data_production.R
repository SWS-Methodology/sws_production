data_prod <- function(domain,dataset, seed_feed = F , selected_years ){

################################################################################
################ CHINA FIX #####################################################
# remove those lines when china code will be patched from 1248 to 156 ##########
if ( CHINA_FIX == T ){
  
countries <- vop_country$geographicAreaM49
countries[countries == "156" ] = "1248"


}
  

######### Remove also lines at the end of the function #########################
################################################################################

indigenous_meat <- grep("i", vop_item$measuredItemCPC, value = T)

indigenous_no_i <- gsub("i", "", indigenous_meat)

domain_ind = domain
dataset_ind = dataset

keys_geo_ind = sub("^0+","", as.character(countries)) #select countries of "aproduction_country" datatable, remove all the 0 before the code
keys_item_ind = as.character(indigenous_no_i)
keys_year_ind = selected_years

if (seed_feed == T ) {keys_elem_ind = c("55100","5520","5525")} else { keys_elem_ind = "55100" } # ind. production [t]

# Get keys 

production_ind_keys <- DatasetKey(
  domain = domain_ind,
  dataset =  dataset_ind,
  dimensions = list(
    Dimension(name = grep('geo', GetDatasetConfig(domain_ind, dataset_ind)$dimensions, value = T, ignore.case = T),
              keys = keys_geo_ind ),
    
    Dimension(name = grep('elem', GetDatasetConfig(domain_ind, dataset_ind)$dimensions, value = T, ignore.case = T),
              keys = keys_elem_ind ),
    
    Dimension(name = grep('item', GetDatasetConfig(domain_ind, dataset_ind)$dimensions, value = T, ignore.case = T),
              keys = keys_item_ind ),
    
    Dimension(name = grep('year', GetDatasetConfig(domain_ind, dataset_ind)$dimensions, value = T, ignore.case = T),
              keys = keys_year_ind )
  )
  
)


# Pull indigenous data 

system.time(data_production_ind <- GetData(production_ind_keys))
#data_production_ind[,c("measuredElement", "flagObservationStatus" ,"flagMethod"):= NULL]
data_production_ind$measuredItemCPC = paste0(data_production_ind$measuredItemCPC, "i")

# Production data - no meat ----------------------------------------------------

domain_prod = domain
dataset_prod = dataset

item_no_meat = setdiff( vop_item$measuredItemCPC,  indigenous_meat) # list of item without meat items

# Pull element 21170.92 [Other meat excluding mammals] from element 21119.90 [Other meats of mammals]

item_no_meat[item_no_meat == "21170.92" ] = "21119.90"

keys_geo = sub("^0+","", as.character(countries)) #select countries of "aproduction_country" datatable, remove all the 0 before the code
keys_item = item_no_meat
keys_year = selected_years

if (seed_feed == T ) {keys_elem = c("5510","5520","5525")} else { keys_elem = "5510" } # production [t]

# Get keys

production_keys <- DatasetKey(
  domain = domain_prod,
  dataset =  dataset_prod,
  dimensions = list(
    Dimension(name = grep('geo', GetDatasetConfig(domain_prod, dataset_prod)$dimensions, value = T, ignore.case = T),
              keys = keys_geo ),
    
    Dimension(name = grep('elem', GetDatasetConfig(domain_prod, dataset_prod)$dimensions, value = T, ignore.case = T),
              keys = keys_elem ),
    
    Dimension(name = grep('item', GetDatasetConfig(domain_prod, dataset_prod)$dimensions, value = T, ignore.case = T),
              keys = keys_item ),
    
    Dimension(name = grep('year', GetDatasetConfig(domain_prod, dataset_prod)$dimensions, value = T, ignore.case = T),
              keys = keys_year )
  )
  
)


# Pull indigenous data ----------------------------------------------------------------

system.time(data_production <- GetData(production_keys))

# bind item + indigenous meat production

data_production <- rbind(data_production, data_production_ind)
data_production[,c( "flagObservationStatus" ,"flagMethod"):= NULL]
data_production[ measuredElement == '55100']$measuredElement = "5510"

if ( seed_feed == T ) {
  data_production <- dcast.data.table(data = data_production, 
                         geographicAreaM49 + measuredItemCPC + timePointYears  ~ measuredElement,
                         value.var = 'Value',
                         fill = 0)
  setnames(data_production, c("5510", "5520", "5525") , c("Production", "Seed", "Feed"))
  
  
} else { 
  setnames(data_production, "Value", "Production")
  data_production[,c( "measuredElement" ):= NULL]
  
  }

################################################################################
################ CHINA FIX #####################################################
# remove those lines when china code will be patched from 1248 to 156 ##########

if ( CHINA_FIX == T ){ data_production[geographicAreaM49 == "1248" ]$geographicAreaM49 = "156" }

################################################################################

data_production[measuredItemCPC == "21119.90" ]$measuredItemCPC = "21170.92"

return(data_production)
}
