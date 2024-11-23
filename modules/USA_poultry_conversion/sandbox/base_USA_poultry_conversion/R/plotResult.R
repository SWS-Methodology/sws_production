##' This function has been created to plot PROCESSED data results
##' It is very Module-specific 
##' 
##'
##' @param data The data.table we want to plot
##' @param toPubblish The set of commodities that we have to flag as "FAOSTAT" which are those that 
##'                   have a priority in the validation process.
##' @param pathToSave The path to the folder where the plots have to be saved.
##' 
##'
##' @export
##'


plotResult=function(data, toPubblish, pathToSave){
    
    if(!file.exists(pathToSave)){
        dir.create(pathToSave, recursive = TRUE)
    }
    
    suppressMessages({
    library(faosws)
    library(faoswsUtil)
    library(data.table)
    library(igraph)
    library(faoswsBalancing)
    library(faoswsStandardization)
    library(dplyr)
    library(MASS) 
    library(lattice)
    library(reshape2)
    library(forecast)
    library(tidyr)
    })
    
    processedCPC=unique(data[,measuredItemChildCPC])
    processedCPC[processedCPC %in% c("","")]
    
  
    #processedCPC=processedCPC[!processedCPC %in% secondLoop]  #secondLoop=ReadDatatable("processed_item")[multiple_level==TRUE,measured_item_cpc]
    
    
    toPlot_melt=melt(data,id.vars = colnames(data[c(1:3,7,8,9,10)]),
                     measure.vars=colnames(data)[c(5,6)],
                     value.name = "VALUE")
    toPlot_melt[is.na(VALUE), VALUE:=NA_real_]
    
    toPlot_melt[PROTECTED==TRUE, maxProtected:=max(VALUE),by=c("geographicAreaM49", "measuredItemChildCPC")]
    
    
    toPlot_melt[, maxProtectedAll:=max(maxProtected, na.rm = TRUE),by=c("geographicAreaM49", "measuredItemChildCPC")]
    
    toPlot_melt[benchmark_Value!=0, diffNewOld:=abs(VALUE-benchmark_Value)]
    
    toPlot_melt[, maxDiffNewOld:=max(diffNewOld,na.rm = TRUE),by=c("geographicAreaM49", "measuredItemChildCPC")]
    
    
    toPlot_melt[, score:=maxDiffNewOld/maxProtectedAll]
    
    toPlot_melt[,score:=round(score, digits = 3)]
    
    
    for (j in 1:length( processedCPC)){
        
        ## if (j == 7)
        ##     browser()
        
        
        countryName=nameData("agriculture", "aproduction", toPlot_melt[,.(geographicAreaM49)])
        countryName=unique(countryName)
        
        
        syncedDataLabel=merge(toPlot_melt,countryName, by="geographicAreaM49",all.x=TRUE)
        
        currentProcessed=processedCPC[j]
        syncedDataLabel=syncedDataLabel[measuredItemChildCPC==currentProcessed]
        syncedDataLabel=syncedDataLabel[variable %in% c("Value","benchmark_Value"), ]
        syncedDataLabel[, timePointYears:=as.numeric(timePointYears)]
        
        syncedDataLabel[, Imputed:=FALSE]
        syncedDataLabel[flagObservationStatus=="I" & flagMethod=="e", Imputed:=TRUE]
        syncedDataLabel[,geographicAreaM49_description:=paste0(geographicAreaM49_description, " MaxDiff/MaxProtectedValue :", score)]
        
        fileName=nameData("suafbs", "sua", data.table(measuredItemSuaFbs=currentProcessed))
        
        fileName[measuredItemSuaFbs=="21399.02", measuredItemSuaFbs_description:="Tomatoes,PeeledVinegar"]
        fileName[measuredItemSuaFbs=="F0472", measuredItemSuaFbs_description:="vegetables preserved nesVinegar"]
        fileName[measuredItemSuaFbs=="24110", measuredItemSuaFbs_description:="Undenatured ethyl alcohol of an alcoholic strength by volume of 80 vol or higher"]
        fileName[measuredItemSuaFbs=="34550", measuredItemSuaFbs_description:="Animal or vegetable fats and oils and their fraction"]
        fileName[measuredItemSuaFbs=="2413", measuredItemSuaFbs_description:="Undenatured ethyl alcohol of an alcoholic strength by volume of 80 vol;spirits,liqueur and other sp"]
        fileName[measuredItemSuaFbs=="34120", measuredItemSuaFbs_description:="Industrial monocarboxylic fatty acids; acid oils from refining"]
        
        
        currentLabel=fileName[,measuredItemSuaFbs_description]
        currentCPC=fileName[,measuredItemSuaFbs]
        
        if(currentCPC %in%  toPubblish){
            pdf(file.path(pathToSave, paste0(currentLabel,"-",currentCPC,"_disseminatedFAOSTAT.pdf")),
                paper = "a4",width=9, height=15)  
        }else{
            
            pdf(file.path(pathToSave, paste0(currentLabel,"-",currentCPC,"_",".pdf")),
                paper = "a4",width=9, height=15)  
            
        }
        geo=unique(syncedDataLabel[measuredItemChildCPC==currentCPC, geographicAreaM49])
        pnp=list()
        
        
        geo=unique(toPlot_melt[measuredItemChildCPC==currentCPC, geographicAreaM49])
        pags = 12
        nump <- seq(1,length(geo),pags)
        if(length(geo)/pags==length(geo)%/%pags){
            lgp=length(geo)/pags}else{
                lgp=length(geo)%/%pags+1
            }
        
        
        syncedDataLabel[is.na(Value), benchmark_Value:=NA_real_]
        
        
        
        for(i in 1:lgp){
            if(!is.na(nump[i+1])){
                
                
                
                pnp[[i]] <-  ggplot(syncedDataLabel[geographicAreaM49 %in% geo[nump[i]:nump[i+1]-1]], aes(x=timePointYears, y=VALUE)) + 
                    geom_line(aes(linetype=variable,color=variable,size=variable)) + 
                    scale_x_continuous(breaks=2000:2016) +
                    scale_linetype_manual(values=c("solid","dotdash","dotdash","solid","solid")) +
                    scale_colour_manual(values = c("red","blue")) +
                    scale_size_manual(values=c(0.5,0.5)) +
                    geom_point( aes(shape = PROTECTED))+
                    theme(axis.title =element_text(size=5),
                          axis.text.y = element_text(size=5),
                          axis.text.x = element_text(size=4,angle = 50, hjust = 1),
                          legend.text = element_text(size=6),
                          strip.text.x = element_text(size = 7),
                          legend.position = "top",
                          panel.grid.major = element_line(colour = "grey80", linetype = "longdash",size= 0.2 ), 
                          panel.grid.minor = element_line(colour="white",size=0), 
                          panel.background = element_rect(fill="white")) +
                    facet_wrap(~geographicAreaM49_description, ncol = 3,scales = "free")
                print(pnp[[i]])
                
            }else{
                if(!is.na(nump[i])){
                    pnp[[i]] <-  ggplot(syncedDataLabel[geographicAreaM49 %in% geo[nump[i]:length(geo)]], aes(x=timePointYears, y=VALUE)) + 
                        geom_line(aes(linetype=variable,color=variable,size=variable)) + 
                        scale_x_continuous(breaks=2000:2015) +
                        scale_linetype_manual(values=c("solid","dotdash","dotdash","solid","solid")) +
                        scale_colour_manual(values = c("red","blue")) +
                        scale_size_manual(values=c(0.5,0.5)) +
                        geom_point( aes(shape = PROTECTED))+
                        theme(axis.title =element_text(size=5),
                              axis.text.y = element_text(size=5),
                              axis.text.x = element_text(size=4,angle = 50, hjust = 1),
                              legend.text = element_text(size=6),
                              strip.text.x = element_text(size = 7),
                              legend.position = "top",
                              panel.grid.major = element_line(colour = "grey80", linetype = "longdash",size= 0.2 ), 
                              panel.grid.minor = element_line(colour="white",size=0), 
                              panel.background = element_rect(fill="white")) +
                        facet_wrap(~geographicAreaM49_description, ncol = 3,scales = "free") 
                    print(pnp[[i]])
                    
                }}}
        
        
        
        dev.off()
        
    }
    
}