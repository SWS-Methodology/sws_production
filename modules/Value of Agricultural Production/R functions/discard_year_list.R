
date_to_discard_list = function(vop_country){

countries_to_discard = vop_country[start_date != "" | end_date != "" ]
countries_to_discard[, start_date := as.numeric(start_date) ]
countries_to_discard[, end_date := as.numeric(end_date) ]

countries_to_discard$start_date[which(!is.na(countries_to_discard$start_date))] = paste0("1945:", countries_to_discard$start_date[which(!is.na(countries_to_discard$start_date))] )

countries_to_discard$end_date[which(!is.na(countries_to_discard$end_date))] = paste(countries_to_discard$end_date[which(!is.na(countries_to_discard$end_date))] , 
                                                                                    format(Sys.Date(),"%Y") ,
                                                                                    sep= ":")

list = list()
X = NULL

start_list <- countries_to_discard[!is.na(start_date), ]
start_list[, end_date := NA]

for (i in 1:dim(start_list)[1]) {

X <- start_list[i,]
Y <- eval(parse( text = X$start_date))
X <- X[rep(seq_len(nrow(X)), each = length(Y)),]
X[, start_date := Y ]

list[[i]] <- X

}

merged_df <- do.call("rbind", list)

list = NULL
list = list()


end_list <- countries_to_discard[!is.na(end_date), ]
end_list[, start_date := NA]


for (i in 1:dim(end_list)[1]) {
  
  X <- end_list[i,]
  Y <- eval(parse( text = X$end_date))
  X <- X[rep(seq_len(nrow(X)), each = length(Y)),]
  X[, end_date := Y ]
  
  list[[i]] <- X
  
}

for ( i in 1:length(list) ){merged_df <- rbind(merged_df, list[[i]] )}

merged_df <- melt(merged_df, id.vars = c("geographicAreaM49"), measure.vars = c('start_date', 'end_date'))
merged_df <- merged_df[complete.cases(merged_df),]
merged_df[, variable := "Date_to_remove"]
setnames(merged_df, c( "variable", "value"), c( "remove", "timePointYears"))
setcolorder(merged_df,  c("geographicAreaM49","timePointYears", "remove"))
# Removing leading zeroes
merged_df[, geographicAreaM49 := sub( "^0+","", geographicAreaM49 )]
setkey(merged_df, geographicAreaM49, timePointYears)

return(merged_df)

}
