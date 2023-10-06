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
po <- read.csv("Z:/IMPORT_JDE_OPENPO.csv",
               header = FALSE)
receipt <- read.csv("Z:/IMPORT_RECEIPTS.csv",
                    header = FALSE)



# Read campus_ref 
campus <- read_excel("S:/Supply Chain Projects/RStudio/BoM/Master formats/RM_on_Hand/Campus_ref.xlsx")




####################### ETL ##########################

##### PO #####

po %>% 
  dplyr::rename(aa = V1) %>% 
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
  dplyr::rename(aa = V1) %>% 
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


writexl::write_xlsx(po, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/10.05.23/po.xlsx")
writexl::write_xlsx(receipt, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/10.05.23/receipt.xlsx")
