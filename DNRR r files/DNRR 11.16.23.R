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
po <- read.csv("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DSXIE/2023/11.14/po.csv",
               header = FALSE)
receipt <- read.csv("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DSXIE/2023/11.14/receipt.csv",
                    header = FALSE)



# Read campus_ref 
campus <- read_excel("S:/Supply Chain Projects/Linda Liang/reference files/Campus reference.xlsx")




####################### ETL ##########################

##### PO #####

po %>% 
  dplyr::select(-1) %>% 
  dplyr::slice(-1) %>% 
  dplyr::rename(aa = V2)%>% 
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
  dplyr::mutate(ref = paste0(location, "-", item)) %>% 
  dplyr::relocate(ref, item, location) -> po


#### receipt ####
receipt %>% 
  dplyr::select(-1) %>% 
  dplyr::slice(-1) %>% 
  dplyr::rename(aa = V2)%>% 
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
  dplyr::mutate(ref = paste0(location, "-", item)) %>% 
  dplyr::relocate(ref, item, location) -> receipt



#### Campus ####
campus %>% 
  janitor::clean_names() %>% 
  dplyr::select(location, campus) -> campus




#### vlookup campus to PO and receipt ####
po %>% 
  dplyr::left_join(campus) %>% 
  dplyr::mutate(campus_ref = paste0(campus, "-", item)) %>% 
  dplyr::relocate(ref, campus_ref, campus, item, location, qty, date, po_no) -> po


receipt %>% 
  dplyr::left_join(campus) %>% 
  dplyr::mutate(campus_ref = paste0(campus, "-", item)) %>% 
  dplyr::relocate(ref, campus_ref, campus, item, location, qty, date, receipt_no) -> receipt



#### Combine two files ####


writexl::write_xlsx(po, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/11.16.2023/po.xlsx")
writexl::write_xlsx(receipt, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/11.16.2023/receipt.xlsx")



###########################################################################################################################################
###########################################################################################################################################
###########################################################################################################################################
###########################################################################################################################################
###########################################################################################################################################

#                                                              Inventory Analysis                                                         #

inv_analysis <- read_xlsx("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/Safety Stock Compliance/Weekly Run Files/2023/11.14.23/FG_2.xlsx")
manufacturers <- read_xlsx("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/11.16.2023/Loc - Manufacturing Loc - SKU.xlsx")


## Inventory ETL

inv_analysis %>% 
  janitor::clean_names() %>% 
  dplyr::mutate(product_label_sku = gsub("-", "", product_label_sku),
                ref = paste0(location, "_", product_label_sku)) %>% 
  dplyr::rename(location_nm = x2,
                item = product_label_sku,
                description = x4,
                hold_status = inventory_hold_status) %>% 
  dplyr::relocate(ref) %>% 
  dplyr::select(-inventory_status_code) %>% 
  dplyr::mutate(current_inventory_balance = ifelse(is.na(current_inventory_balance), 0, current_inventory_balance)) -> inv_analysis_2


# manufacturers ETL

manufacturers %>% 
  janitor::clean_names() %>%
  dplyr::select(location_no, product_label_sku_code, product_manufacturing_location_code) %>% 
  dplyr::rename(item = product_label_sku_code,
                product_manufacturing_location = product_manufacturing_location_code) %>% 
  dplyr::mutate(item = gsub("-", "", item),
                ref = paste0(location_no, "_", item),
                mfg_ref = paste0(product_manufacturing_location, "_", item)) %>% 
  dplyr::select(ref, mfg_ref, product_manufacturing_location) -> manufacturers_2

inv_analysis_2 %>% 
  dplyr::left_join(manufacturers_2) %>% 
  dplyr::relocate(ref, mfg_ref, location, location_nm, product_manufacturing_location) -> inv_analysis_2

inv_analysis_2 %>% 
  dplyr::mutate(current_inventory_balance = as.double(current_inventory_balance)) %>% 
  dplyr::group_by(ref, mfg_ref, location, location_nm, product_manufacturing_location, item, description, hold_status) %>% 
  dplyr::summarise(mfg_ref = first(mfg_ref),
                   location = first(location),
                   location_nm = first(location_nm),
                   product_manufacturing_location = first(product_manufacturing_location),
                   item = first(item),
                   description = first(description),
                   hold_status = first(hold_status),
                   current_inventory_balance = sum(current_inventory_balance)) %>% 
  dplyr::mutate(ref = gsub("_", "-", ref),
                mfg_ref = gsub("_", "-", mfg_ref)) %>% 
  dplyr::filter(item != "'1") -> inv_analysis_2

writexl::write_xlsx(inv_analysis_2, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/11.16.2023/FG inv.xlsx")


###########################################################################################################################################
###########################################################################################################################################
###########################################################################################################################################
###########################################################################################################################################
###########################################################################################################################################

#                                                              DSX Forcasting     Lag 1                                                   #


dsx <- read_excel("S:/Global Shared Folders/Large Documents/S&OP/Demand Planning/Demand Planning Team/BI Forecast Backup/2023/DSX Forecast Backup - 2023.10.02.xlsx")

dsx %>% 
  janitor::clean_names() %>% 
  dplyr::slice(-1) -> dsx_2

colnames(dsx_2) <- dsx_2[1, ]

dsx_2 %>%
  slice(-1) %>%
  janitor::clean_names() %>%
  data.frame() %>%
  dplyr::mutate(adjusted_forecast_cases = as.double(adjusted_forecast_cases),
                product_label_sku_code = gsub("-", "", product_label_sku_code),
                mfg_ref = paste0(product_manufacturing_location_code, "_", product_label_sku_code)) %>%
  dplyr::select(mfg_ref, forecast_month_year_id, adjusted_forecast_cases) %>%
  dplyr::group_by(mfg_ref, forecast_month_year_id) %>%
  dplyr::summarise(adjusted_forecast_cases = sum(adjusted_forecast_cases)) %>%
  dplyr::mutate(adjusted_forecast_cases = ifelse(is.na(adjusted_forecast_cases), 0, adjusted_forecast_cases)) %>% 
  pivot_wider(names_from = forecast_month_year_id, values_from = adjusted_forecast_cases, values_fill = list(adjusted_forecast_cases = 0)) %>% 
  dplyr::mutate(mfg_ref = gsub("_", "-", mfg_ref)) -> dsx_2



writexl::write_xlsx(dsx_2, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/11.16.2023/dsx.xlsx")
