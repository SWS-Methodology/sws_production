##' This function pull the SUA table necessary to compute the availabilities
##' for computing the processed item productions.
##' 
##'
##' @export
##'


getSUADataRestricted=function(){

##-------------------------------------------------------------------------------------------------------------------------------------
importCode = "5610"
exportCode = "5910"
productionCode="5510"
seedCode="5525"

# warning("Stocks is change in stocks, not absolute! This needs to be changed")
stocksCode = "5071"
################################################
##### Harvest from Agricultural Production #####
################################################

message("Pulling data from Agriculture Production")

geoDim = Dimension(name = "geographicAreaM49", keys = currentGeo)
eleDim = Dimension(name = "measuredElement", keys = c(productionCode, seedCode))

itemKeys = primaryInvolvedDescendents
itemDim = Dimension(name = "measuredItemCPC", keys = itemKeys)
timeDim = Dimension(name = "timePointYears", keys = as.character(timeKeys))
agKey = DatasetKey(domain = "agriculture", dataset = "aproduction",
                   dimensions = list(
                       geographicAreaM49 = geoDim,
                       measuredElement = eleDim,
                       measuredItemCPC = itemDim,
                       timePointYears = timeDim)
)
agData = GetData(agKey)
setnames(agData, c("measuredElement", "measuredItemCPC"),
         c("measuredElementSuaFbs", "measuredItemSuaFbs"))



################################################
##### Harvest from stockdata               #####
################################################

message("Pulling data from Stock domain")
stockEleDim = Dimension(name = "measuredElement", keys = c(stocksCode))
stokKey = DatasetKey(domain = "Stock", dataset = "stocksdata",
                   dimensions = list(
                       geographicAreaM49 = geoDim,
                       measuredElement = stockEleDim,
                       measuredItemCPC = itemDim,
                       timePointYears = timeDim)
)
stockData = GetData(stokKey)
setnames(stockData, c("measuredElement", "measuredItemCPC"),
         c("measuredElementSuaFbs", "measuredItemSuaFbs"))




################################################
#####       Harvest from Trade Domain      #####
################################################
# TRADE HAS TO BE PULLED:
# - FROM OLD FAOSTAT UNTIL 2013
# - FROM NEW DATA STARTING FROM 2010
################################################

message("Pulling data from Trade UNTIL 2013 (old FAOSTAT)")

eleTradeDim = Dimension(name = "measuredElementTrade",
                        keys = c(importCode, exportCode))
tradeItems <- na.omit(sub("^0+", "", cpc2fcl(unique(itemKeys), returnFirst = TRUE, version = "latest")), waitTimeout = 2000000)

geoKeysTrade=m492fs(currentGeo)

geokeysTrade=geoKeysTrade[!is.na(geoKeysTrade)]

if(2013>=endYear){
    timeTradeDimUp13 = Dimension(name = "timePointYears", keys = as.character(timeKeys))
    
    ###### Trade UNTIL 2013 (old FAOSTAT)
    message("Trade UNTIL 2013 (old FAOSTAT)")
    tradeKeyUp13 = DatasetKey(
        domain = "faostat_one", dataset = "updated_sua",
        dimensions = list(
            #user input except curacao,  saint martin and former germany
            geographicAreaFS= Dimension(name = "geographicAreaFS", keys = setdiff(geokeysTrade, c("279", "534", "280","274","283"))),
            measuredItemFS=Dimension(name = "measuredItemFS", keys = tradeItems),
            measuredElementFS=Dimension(name = "measuredElementFS",
                                        keys = c( "61", "91")),
            timePointYears = timeTradeDimUp13 ),
        sessionId =  slot(swsContext.datasets[[1]], "sessionId")
    )
    
    
    tradeDataUp13 = GetData(tradeKeyUp13)
    
    
    tradeDataUp13[, `:=`(geographicAreaFS = fs2m49(geographicAreaFS),
                         measuredItemFS = fcl2cpc(sprintf("%04d", as.numeric(measuredItemFS)),
                                                  version = "latest"))]
    
    
    setnames(tradeDataUp13, c("geographicAreaFS","measuredItemFS","measuredElementFS","flagFaostat" ),
             c("geographicAreaM49", "measuredItemSuaFbs","measuredElementSuaFbs","flagObservationStatus"))
    
    tradeDataUp13[, flagMethod := "-"]
    
    tradeDataUp13[flagObservationStatus %in% c("P", "*", "F"), flagObservationStatus := "T"]
    
    tradeDataUp13[measuredElementSuaFbs=="91",measuredElementSuaFbs:="5910"]
    tradeDataUp13[measuredElementSuaFbs=="61",measuredElementSuaFbs:="5610"]
    
    tradeData=tradeDataUp13 
    
}else{
    ###### Trade FROM 2014 (new Data)
    message("Trade FROM 2014 (new Data)")
    
    timeTradeDimFrom14 = Dimension(name = "timePointYears", keys = as.character(2014:endYear))
    
    tradeKeyFrom14 = DatasetKey(
        domain = "trade", dataset = "total_trade_cpc_m49",
        dimensions = list(geographicAreaM49 = geoDim,
                          measuredElementTrade = eleTradeDim,
                          measuredItemCPC = itemDim,
                          timePointYears = timeTradeDimFrom14)
    )
    tradeDataFrom14 = GetData(tradeKeyFrom14)
    setnames(tradeDataFrom14, c("measuredElementTrade", "measuredItemCPC"),
             c("measuredElementSuaFbs", "measuredItemSuaFbs"))
    
    ###### Merging Trade Data
    message("Merging Data")
    if(2013<startYear){
        tradeData=tradeDataFrom14
    }else{
        timeTradeDimUp13 = Dimension(name = "timePointYears", keys = as.character(startYear:2013))
        message("Trade UNTIL 2013 (old FAOSTAT)")
        tradeKeyUp13 = DatasetKey(
            domain = "faostat_one", dataset = "updated_sua",
            dimensions = list(
                #user input except curacao,  saint martin and former germany
                geographicAreaFS= Dimension(name = "geographicAreaFS", keys = setdiff(geokeysTrade, c("279", "534", "280","274","283"))),
                measuredItemFS=Dimension(name = "measuredItemFS", keys = tradeItems),
                measuredElementFS=Dimension(name = "measuredElementFS",
                                            keys = c( "61", "91")),
                timePointYears = timeTradeDimUp13 ),
            sessionId =  slot(swsContext.datasets[[1]], "sessionId")
        )
        
        
        tradeDataUp13 = GetData(tradeKeyUp13)
        
        
        tradeDataUp13[, `:=`(geographicAreaFS = fs2m49(geographicAreaFS),
                             measuredItemFS = fcl2cpc(sprintf("%04d", as.numeric(measuredItemFS)),
                                                      version = "latest"))]
        
        
        setnames(tradeDataUp13, c("geographicAreaFS","measuredItemFS","measuredElementFS","flagFaostat" ),
                 c("geographicAreaM49", "measuredItemSuaFbs","measuredElementSuaFbs","flagObservationStatus"))
        
        tradeDataUp13[, flagMethod := "-"]
        
        tradeDataUp13[flagObservationStatus %in% c("P", "*", "F"), flagObservationStatus := "T"]
        
        tradeDataUp13[measuredElementSuaFbs=="91",measuredElementSuaFbs:="5910"]
        tradeDataUp13[measuredElementSuaFbs=="61",measuredElementSuaFbs:="5610"]
        
        tradeData=rbind(tradeDataUp13,tradeDataFrom14)  
        
    }
    
}

################################################
#####       Merging data files together    #####
################################################

message("Merging data to build SUA ")
out = do.call("rbind", list(agData, tradeData,stockData))
out <- out[!is.na(Value),]
setnames(out,"measuredItemSuaFbs","measuredItemFbsSua")

return(out)


}
