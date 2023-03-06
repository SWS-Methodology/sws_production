data_prices = function(price,domain,dataset){
  
  if (price == "5531" ) {
    
    domain_slc = domain #'disseminated'
    dataset_slc = dataset #'annual_producer_prices_validation_diss'
    
    keys_geo = sub("^0+","", as.character(vop_country$geographicAreaM49))
    keys_elem = "5531" #SLC
    keys_item = as.character(vop_item$measuredItemCPC) #select items of "value_of_production_item" datatable
    keys_year = selected_years
    
    production_keys <- DatasetKey(
      domain = domain_slc,
      dataset =  dataset_slc,
      dimensions = list(
        Dimension(name = grep('geo',  GetDatasetConfig(domain_slc, dataset_slc)$dimensions, value = T, ignore.case = T),
                  keys = keys_geo ),
        
        Dimension(name = grep('elem', GetDatasetConfig(domain_slc, dataset_slc)$dimensions, value = T, ignore.case = T),
                  keys = keys_elem ),
        
        Dimension(name = grep('item', GetDatasetConfig(domain_slc, dataset_slc)$dimensions, value = T, ignore.case = T),
                  keys = keys_item ),
        
        Dimension(name = grep('year', GetDatasetConfig(domain_slc, dataset_slc)$dimensions, value = T, ignore.case = T),
                  keys = keys_year )
      )
      
    )
    
    # Get data ----------------------------------------------------------------
    
    system.time(data_SLC <- GetData(production_keys))
    setnames(data_SLC, 'Value', 'Prices')
    data_SLC[,c("measuredElement", "flagObservationStatus" ,"flagMethod"):= NULL]
    data_SLC[, measuredElement := price]
    
    
    return(data_SLC)
    
  }
  if (price == "5532" ) {
    domain_usd = domain #'disseminated'
    dataset_usd = dataset #'annual_producer_prices_validation_diss'
    
    keys_geo = sub("^0+","", as.character(vop_country$geographicAreaM49))
    keys_elem = price #USD
    keys_item = as.character(vop_item$measuredItemCPC) #select items of "value_of_production_item" datatable
    keys_year = selected_years
    
    production_keys <- DatasetKey(
      domain = domain_usd,
      dataset =  dataset_usd,
      dimensions = list(
        Dimension(name = grep('geo',  GetDatasetConfig(domain_usd, dataset_usd)$dimensions, value = T, ignore.case = T),
                  keys = keys_geo ),
        
        Dimension(name = grep('elem', GetDatasetConfig(domain_usd, dataset_usd)$dimensions, value = T, ignore.case = T),
                  keys = keys_elem ),
        
        Dimension(name = grep('item', GetDatasetConfig(domain_usd, dataset_usd)$dimensions, value = T, ignore.case = T),
                  keys = keys_item ),
        
        Dimension(name = grep('year', GetDatasetConfig(domain_usd, dataset_usd)$dimensions, value = T, ignore.case = T),
                  keys = keys_year )
      )
      
    )
    
    # Get data ----------------------------------------------------------------
    
    system.time(data_USD <- GetData(production_keys))
    setnames(data_USD, 'Value', 'Prices')
    data_USD[,c("measuredElement", "flagObservationStatus" ,"flagMethod"):= NULL]
    data_USD[, measuredElement := price]
    
    return(data_USD)
  }
  if (price == "5534" ) {
    # Average SLC
    domain_slc_avg = domain #'disseminated'
    dataset_slc_avg = dataset #'annual_producer_prices_validation_diss'
    
    keys_geo = sub("^0+","", as.character(vop_country$geographicAreaM49))
    keys_elem = "5531" #SLC
    keys_item = as.character(vop_item$measuredItemCPC) #select items of "value_of_production_item" datatable
    keys_year = base_year_range
    
    production_keys <- DatasetKey(
      domain = domain_slc_avg,
      dataset =  dataset_slc_avg,
      dimensions = list(
        Dimension(name = grep('geo',  GetDatasetConfig(domain_slc_avg, dataset_slc_avg)$dimensions, value = T, ignore.case = T),
                  keys = keys_geo ),
        
        Dimension(name = grep('elem', GetDatasetConfig(domain_slc_avg, dataset_slc_avg)$dimensions, value = T, ignore.case = T),
                  keys = keys_elem ),
        
        Dimension(name = grep('item', GetDatasetConfig(domain_slc_avg, dataset_slc_avg)$dimensions, value = T, ignore.case = T),
                  keys = keys_item ),
        
        Dimension(name = grep('year', GetDatasetConfig(domain_slc_avg, dataset_slc_avg)$dimensions, value = T, ignore.case = T),
                  keys = keys_year )
      )
      
    )
    
    # Get data ----------------------------------------------------------------
    
    system.time(data_SLC_avg <- GetData(production_keys))
    setnames(data_SLC_avg, 'Value', 'Prices')
    data_SLC_avg[,c("measuredElement", "flagObservationStatus" ,"flagMethod"):= NULL]
    
    # Calculate three year mean 
    
    data_SLC_avg[, Prices := mean(Prices), by = .(geographicAreaM49,measuredItemCPC)]
    data_SLC_avg <- data_SLC_avg[ timePointYears == param_base_year]
    data_SLC_avg[, measuredElement := price]
    
    return(data_SLC_avg)  
    
  }
  
  if (price == "5535" ) {
    # Average USD
    domain_usd_avg = domain #'disseminated'
    dataset_usd_avg = dataset #'annual_producer_prices_validation_diss'
    
    keys_geo = sub("^0+","", as.character(vop_country$geographicAreaM49))
    keys_elem = "5532" #USD
    keys_item = as.character(vop_item$measuredItemCPC) #select items of "value_of_production_item" datatable
    keys_year = base_year_range
    
    production_keys <- DatasetKey(
      domain = domain_usd_avg,
      dataset =  dataset_usd_avg,
      dimensions = list(
        Dimension(name = grep('geo',  GetDatasetConfig(domain_usd_avg, dataset_usd_avg)$dimensions, value = T, ignore.case = T),
                  keys = keys_geo ),
        
        Dimension(name = grep('elem', GetDatasetConfig(domain_usd_avg, dataset_usd_avg)$dimensions, value = T, ignore.case = T),
                  keys = keys_elem ),
        
        Dimension(name = grep('item', GetDatasetConfig(domain_usd_avg, dataset_usd_avg)$dimensions, value = T, ignore.case = T),
                  keys = keys_item ),
        
        Dimension(name = grep('year', GetDatasetConfig(domain_usd_avg, dataset_usd_avg)$dimensions, value = T, ignore.case = T),
                  keys = keys_year )
      )
      
    )
    
    # Get data ----------------------------------------------------------------
    
    system.time(data_USD_avg <- GetData(production_keys))
    setnames(data_USD_avg, 'Value', 'Prices')
    data_USD_avg[,c("measuredElement", "flagObservationStatus" ,"flagMethod"):= NULL]
    
    # Calculate three year mean 
    
    data_USD_avg[, Prices := mean(Prices), by = .(geographicAreaM49,measuredItemCPC)]
    data_USD_avg <- data_USD_avg[ timePointYears == param_base_year]
    data_USD_avg[, measuredElement := price]
    
    return(data_USD_avg)  
  
  }

  }