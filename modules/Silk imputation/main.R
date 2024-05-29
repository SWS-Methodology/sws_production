library(faosws)
library(faoswsUtil)
library(faoswsBalancing)
library(faoswsStandardization)
library(dplyr)
library(data.table)
library(tidyr)
library(openxlsx)

# The only parameter is the string to print
# COUNTRY is taken from environment (parameter)
dbg_print <- function(x) {
  message(paste0("NEWBAL (", COUNTRY, "): ", x))
}

start_time <- Sys.time()

R_SWS_SHARE_PATH <- Sys.getenv("R_SWS_SHARE_PATH")

if (CheckDebug()) {
  R_SWS_SHARE_PATH <- "//hqlprsws1.hq.un.fao.org/sws_r_share"
  
  mydir <- "modules/Silk imputation"
  
  SETTINGS <- faoswsModules::ReadSettings(file.path(mydir, "sws.yml"))
  
  SetClientFiles(SETTINGS[["certdir"]])
  
  GetTestEnvironment(baseUrl = SETTINGS[["server"]], token = SETTINGS[["token"]])
}

`%!in%` = Negate(`%in%`)

COUNTRY <- as.character(swsContext.datasets[[1]]@dimensions$geographicAreaM49@keys)

COUNTRY_NAME <-
  nameData(
    "suafbs", "sua_unbalanced",
    data.table(geographicAreaM49 = COUNTRY))$geographicAreaM49_description

#removed european: "100","300","380","642", 
tree_countries <- c("116","156","158","356","360","364","392","4","408","410","417","450",
                    "704","76","760","762","764","792","795","818","860")

silk_countries <- c("116","156","4","408","356","360","364","392","762","764","792","818","860","410","417","450","704","76","158")


start_year <- as.numeric(swsContext.computationParams$start_year)
end_year = as.numeric(swsContext.computationParams$end_year)

YEAR = as.character(start_year:end_year)
####################### TAKING AGRICULTURAL PRODUCTION DATA ###############################

apr_key = DatasetKey(domain = "aproduction", 
                     dataset = "aproduction", 
                     dimensions = list(Item = Dimension("measuredItemCPC", c("02944","26110")), 
                                       Element = Dimension("measuredElement", "5510"),
                                       Geographic = Dimension("geographicAreaM49", silk_countries),
                                       timePointYears = Dimension("timePointYears", YEAR)))
apr_data <- GetData(apr_key)

####################### TAKING TRADE DATA ###############################


trade_key = DatasetKey(domain = "trade", 
                       dataset = "total_trade_cpc_m49", 
                       dimensions = list(Item = Dimension("measuredItemCPC", c("02944","26110")), 
                                         Element = Dimension("measuredElementTrade", c("5610","5910")),
                                         Geographic = Dimension("geographicAreaM49", silk_countries),
                                         timePointYears = Dimension("timePointYears", YEAR)))
trade_data <- GetData(trade_key)


setnames(trade_data, "measuredElementTrade", "measuredElement")


########################### TAKING EX. RATES ################################################

tree_key = DatasetKey(domain = "suafbs", 
                      dataset = "ess_fbs_commodity_tree2", 
                      dimensions = list(ItemParent = Dimension("measuredItemParentCPC_tree", "02944"),
                                        ItemChild = Dimension("measuredItemChildCPC_tree", "26110"),
                                        Element = Dimension("measuredElementSuaFbs", "5423"),
                                        Geographic = Dimension("geographicAreaM49", silk_countries),
                                        timePointYears = Dimension("timePointYears", YEAR)))
tree_data <- GetData(tree_key)


################## bind #####################

data = rbind(apr_data, trade_data)



data_p <-
  merge(
    data,
    tree_data,
    by = c("geographicAreaM49", "timePointYears"),
    all.x = TRUE
  )


data_p[, c("measuredElementSuaFbs","measuredItemParentCPC_tree","measuredItemChildCPC_tree",
           "flagObservationStatus.y","flagMethod.y"):=NULL]

setnames(data_p, c("Value.x","flagObservationStatus.x","flagMethod.x","Value.y"),
         c("Value","flagObservationStatus","flagMethod","extraction"))

d <- copy(data_p)


d <- na.omit(d, cols="extraction")


d[, check_parent := ifelse(measuredItemCPC %in% "02944" & measuredElement %in% "5510" & flagObservationStatus %in% c("", "T"),
                           TRUE, FALSE),
  by = c("geographicAreaM49", "timePointYears")]

d[, check_child := ifelse(measuredItemCPC %in% "26110" & measuredElement %in% "5510" & flagObservationStatus %in% c("", "T"),
                           TRUE, FALSE),
  by = c("geographicAreaM49", "timePointYears")]

#if any true in respective column of country, year production then TRUE
d[, check_both := ifelse(sum(check_child) == 1 & sum(check_parent) == 1,
                          TRUE, FALSE),
  by = c("geographicAreaM49", "timePointYears","measuredElement")]

#remove countries with both parent and child protected
protected_countries <- unique(d[check_both==TRUE,]$geographicAreaM49)

d_cleaned <- d[geographicAreaM49 %!in% protected_countries,]

d_cleaned[, check_both:= NULL]

d_cleaned[, parent_value := Value[measuredItemCPC %in% "02944" & measuredElement %in% "5510" & flagObservationStatus %in% c("","T")],
     by = c("geographicAreaM49", "timePointYears")]

d_cleaned[, child_value := Value[measuredItemCPC %in% "26110" & measuredElement %in% "5510" & flagObservationStatus %in% c("","T")],
          by = c("geographicAreaM49", "timePointYears")]
# check is na for year in which coco is not present:
# if check is a value then num becomes the coco prod + coco import + coco export
# if check is NA then num becomes raw silk prod

# d_cleaned[, num := ifelse(!is.na(parent_value),
#                   sum(Value[get("measuredElement") %in% c("5510", "5610") & measuredItemCPC %in% "02944"],
#                       - Value[get("measuredElement") %in% "5910" & measuredItemCPC %in% "02944"],
#                       na.rm = TRUE
#                   ),
#                   Value[measuredItemCPC %in% "26110" & measuredElement %in% "5510"]),
#   by = c("geographicAreaM49", "timePointYears")]

#IF parent protected or no protected at all in silk items then impute silk as cocons prod + import - exp (later multiplied by er)
d_cleaned[, num := ifelse(!is.na(parent_value) | is.na(parent_value) & is.na(child_value),
                          sum(Value[get("measuredElement") %in% c("5510", "5610") & measuredItemCPC %in% "02944"],
                              - Value[get("measuredElement") %in% "5910" & measuredItemCPC %in% "02944"],
                              na.rm = TRUE
                          ),
                          NA_real_),
          by = c("geographicAreaM49", "timePointYears")]

#IF Silk protected impute cocons as silk (later divided bby er and added export-import of cocons)

d_cleaned[, den := ifelse(!is.na(child_value),
                          Value[measuredItemCPC %in% "26110" & measuredElement %in% "5510"],
                          NA_real_),
          by = c("geographicAreaM49", "timePointYears")]
#Manage the countries with num NA. It means no protected tarting point to impute child
#ORA: num has cocon + import - export ready to be multipied by extraction


d_cleaned[!is.na(parent_value) | is.na(parent_value) & is.na(child_value), ratio := num*extraction,
          by = c("geographicAreaM49", "timePointYears")]

#den has silk value ready to be divided by the extraction
d_cleaned[!is.na(child_value), ratio := den/extraction,
          by = c("geographicAreaM49", "timePointYears")]

#export - import need to be added to the production imputation of cocons
d_cleaned[, parent_bound := ifelse(!is.na(child_value),
                                   sum(Value[get("measuredElement") %in% "5910" & measuredItemCPC %in% "02944"],
                                       - Value[get("measuredElement") %in% "5610" & measuredItemCPC %in% "02944"],
                                       na.rm = TRUE), NA_real_),
          by = c("geographicAreaM49", "timePointYears")]

#take imputations

d_cleaned[!is.na(parent_value) | is.na(parent_value) & is.na(child_value), imputation := ratio]

d_cleaned[!is.na(child_value), imputation := ratio+parent_bound]


# d_cleaned[, imputation := ifelse(!is.na(parent_value),
#                     ratio,
#                     ratio+parent_bound),
#   by = c("geographicAreaM49", "timePointYears")]



new_prod <-
  unique(
    d_cleaned[,
          c("geographicAreaM49", "timePointYears", "parent_value","child_value",
          "imputation"),
          with = FALSE
          ],
    by = c("geographicAreaM49", "timePointYears")
  )



production_parent_imputed <- new_prod[!is.na(child_value),]

production_child_imputed <- new_prod[!is.na(parent_value) | is.na(parent_value) & is.na(child_value),]



production_parent_imputed[
    !is.na(child_value),
  `:=`(
    measuredItemCPC = "02944",
    measuredElement = "5510",
    Value = imputation,
    flagObservationStatus = "E",
    flagMethod = "e"
  )
  ]

production_parent_imputed[, c("parent_value", "child_value") := NULL]



production_child_imputed[
    !is.na(parent_value) | is.na(parent_value) & is.na(child_value),
    `:=`(
        measuredItemCPC = "26110",
        measuredElement = "5510",
        Value = imputation,
        flagObservationStatus = "E",
        flagMethod = "e"
    )
    ]

production_child_imputed[, c("parent_value", "child_value") := NULL]


if(nrow(production_parent_imputed)>0 & nrow(production_child_imputed)>0){
    data_to_save <- rbind(production_parent_imputed,production_child_imputed)
} else if (nrow(production_parent_imputed)>0 & nrow(production_child_imputed)==0){
    data_to_save <- production_parent_imputed
} else {
    data_to_save <- production_child_imputed
}



#china <- xxx[geographicAreaM49 == "1248" & timePointYears >= "2007",]
#grecia <- xxx[geographicAreaM49 == "300" & timePointYears >= "1993",]


data_to_save[, imputation := NULL]

#data_to_save <- na.omit(data_to_save, cols="Value")

faosws::SaveData(
  domain = "aproduction",
  dataset = "aproduction",
  data = data_to_save,
  waitTimeout = 20000
)

print('Plug-in Completed')