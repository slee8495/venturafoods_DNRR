library(tidyverse)
library(magrittr)
library(openxlsx)
library(readxl)
library(writexl)
library(reshape2)
library(skimr)
library(janitor)
library(lubridate)


# Read po, receipt ----
po <- read_excel("C:/Users/lliang/OneDrive - Ventura Foods/R Studio/Source Data/wo receipt custord po - 12.07.22.xlsx",
                 sheet = "po",
                 col_names = FALSE)


receipt <- read_excel("C:/Users/lliang/OneDrive - Ventura Foods/R Studio/Source Data/wo receipt custord po - 12.07.22.xlsx",
                      sheet = "receipt",
                      col_names = FALSE)


# Read campus_ref 
campus <- read_excel("S:/Supply Chain Projects/RStudio/BoM/Master formats/RM_on_Hand/Campus_ref.xlsx")




####################### ETL ##########################

##### PO #####

po %>% 
  dplyr::rename(aa = "...1") %>% 
  tidyr::separate(aa, c("1", "2", "3", "4", "5", "6", "7", "8"), sep = "~") %>% 
  dplyr::select(-"3") %>% 
  dplyr::rename(aa = "1") %>% 
  tidyr::separate(aa, c("global", "rp", "item")) %>% 
  dplyr::select(-global, -rp, -"4",-"8") %>% 
  dplyr::rename(location = "2",
                qty = "5",
                po_no = "6",
                date = "7") %>% 
  dplyr::mutate(qty = as.double(qty),
                date = as.Date(date)) %>% 
  dplyr::mutate(location = sub("^0+", "",location)) %>% 
  dplyr::mutate(ref = paste0(location, "_", item)) %>% 
  dplyr::relocate(ref, item, location) -> po


#### receipt ####
receipt %>% 
  dplyr::rename(aa = "...1") %>% 
  tidyr::separate(aa, c("1", "2", "3", "4", "5", "6", "7", "8"), sep = "~") %>% 
  dplyr::select(-"3", -"8") %>% 
  dplyr::rename(aa = "1") %>% 
  tidyr::separate(aa, c("global", "rp", "item")) %>% 
  dplyr::select(-global, -rp, -"4") %>% 
  dplyr::rename(location = "2",
                qty = "5",
                receipt_no = "6",
                date = "7") %>% 
  dplyr::mutate(qty = as.double(qty),
                date = as.Date(date)) %>% 
  dplyr::mutate(location = sub("^0+", "", location)) %>% 
  dplyr::mutate(ref = paste0(location, "_", item)) %>% 
  dplyr::relocate(ref, item, location) -> receipt



#### Campus ####
campus %>% 
  janitor::clean_names() %>% 
  dplyr::rename(location = business_unit) %>% 
  dplyr::select(location, campus) -> campus




#### vlookup campus to PO and receipt ####
po %>% 
  dplyr::left_join(campus) %>% 
  dplyr::mutate(campus_ref = paste0(campus, "_", item)) %>% 
  dplyr::relocate(ref, campus_ref, campus, item, location, qty, date, po_no) -> po


receipt %>% 
  dplyr::left_join(campus) %>% 
  dplyr::mutate(campus_ref = paste0(campus, "_", item)) %>% 
  dplyr::relocate(ref, campus_ref, campus, item, location, qty, date, receipt_no) -> receipt



#### Combine two files ####
openxlsx::createWorkbook("example") -> example
openxlsx::addWorksheet(example, "RM PO Data")
openxlsx::addWorksheet(example, "RM Reciept Data")


openxlsx::writeDataTable(example, "RM PO Data", po)
openxlsx::writeDataTable(example, "RM Reciept Data", receipt)


openxlsx::saveWorkbook(example, file = "po, receipt_1208.xlsx")


