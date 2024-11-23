##' This function guarantees that for the years for which imputations are requested
##' there are empty cells flaged as (M, u) to be overwritten with the imputed fugures.
##'
##' @param data The set of data for which we want to expand the year 
##' @param params List
##' @param startYear ffkf
##' @param endYear rrrrrrr
##' @return test duddudu
##' 
##' @export
##' 


expandDatasetYears=function( data, params, startYear, endYear){
    
    
    startYear=as.numeric(startYear)
    endYear=as.numeric(endYear)
    timeWindow=c(startYear:endYear)
    
    test=copy(data)

    uniqueLevels = test[, .N, by = c(params$areaVar,params$elementVar)]
    uniqueLevels[, N := NULL]    
    
    
    toBeMerged=list()
    
    for( i in seq_len(nrow(uniqueLevels)) ){
        filter = uniqueLevels[i, ]
        dataSubset = test[filter, , on = c(params$areaVar, params$elementVar)]
        
        
        
        
        if(sum(!timeWindow %in% dataSubset[get(params$yearVar)>=startYear, get(params$yearVar)] )>0){
            
                yearMiss= timeWindow[!timeWindow %in% dataSubset[, get(params$yearVar)]]
          
                
                toBeAdded=list()
                
 
                
                for(j in  seq_along(yearMiss)){
                    
                    currentYear=yearMiss[j]
                    
                    toBeAdded[[j]]=unique(dataSubset[,":="(c(params$yearVar,params$valueVar,params$flagObservationVar,
                                                 params$flagMethodVar), list(as.character(currentYear), NA, "M", "u"))])
                                                 
                                                 
                                                 
          
                }

                
                toBeAdded =rbindlist(toBeAdded) 
                
                
                    }   
          
        toBeMerged[[i]]=toBeAdded
        
    }
    
    toBeMerged =rbindlist(toBeMerged)
    
    test=rbind(test,toBeMerged)
    return(test)
    
}