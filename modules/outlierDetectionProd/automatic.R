library(data.table)
library(faosws)
library(faoswsProduction)
library(faoswsFlag)

CERTIFICATES_DIR <- "C:/Users/mongeau.FAODOMAIN/Documents/certificates/production"
COUNTRY <- 840



if (CheckDebug()) {
  SetClientFiles(CERTIFICATES_DIR)
  GetTestEnvironment(
    baseUrl = "https://hqlprswsas2.hq.un.fao.org:8181/sws",
    token = "6ccad2da-586c-4180-b153-0a3f2f92ae52"
  )
}

flagValidTable <- ReadDatatable("valid_flags")

data <- readRDS(paste0("//hqlprsws1.hq.un.fao.org/sws_r_share/mongeau/tmp/production_outliers/data/production/", COUNTRY, ".rds"))


