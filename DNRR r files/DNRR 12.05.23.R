library(tidyverse)
library(magrittr)
library(openxlsx)
library(readxl)
library(writexl)
library(reshape2)
library(skimr)
library(janitor)
library(lubridate)



# Ctr F -> mm.dd.yyyy
# Ctr F -> mm.dd




base::dir.create("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/12.05.2023")

# Read po, receipt ----
po <- read.csv("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DSXIE/2023/12.05/po.csv",
               header = FALSE)
receipt <- read.csv("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DSXIE/2023/12.05/receipt.csv",
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


writexl::write_xlsx(po, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/12.05.2023/po.xlsx")
writexl::write_xlsx(receipt, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/12.05.2023/receipt.xlsx")



###########################################################################################################################################
###########################################################################################################################################

#                                                              DSX Forcasting     Lag 1                                                   #


dsx <- read_excel("S:/Global Shared Folders/Large Documents/S&OP/Demand Planning/Demand Planning Team/BI Forecast Backup/2023/DSX Forecast Backup - 2023.11.01.xlsx")

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



writexl::write_xlsx(dsx_2, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/12.05.2023/dsx.xlsx")




###########################################################################################################################################
#################################################### Inventory RM & FG ####################################################################

### RM
# Pulling Inventory Data 


inv_bal <- read_excel("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/Safety Stock Compliance/Weekly Run Files/2023/12.05.23/inv_bal.xlsx")
inv_bal %>% 
  janitor::clean_names() %>% 
  dplyr::slice(-1:-2) -> inv_bal

colnames(inv_bal) <- inv_bal[1, ]

inv_bal %>% 
  janitor::clean_names() %>% 
  dplyr::slice(-1) %>% 
  dplyr::mutate(bp = as.double(bp)) %>% 
  dplyr::filter(bp != 226) %>% 
  dplyr::mutate(item = as.double(item)) %>% 
  dplyr::filter(!is.na(item)) %>% 
  dplyr::select(bp, item, description, inventory) %>% 
  readr::type_convert() %>% 
  dplyr::left_join(campus %>% select(location, campus) %>% rename(bp = location) %>% mutate(bp = as.double(bp))) %>% 
  dplyr::mutate(ref = paste0(bp, "_", item),
                campus_ref = paste0(campus, "_", item)) %>% 
  dplyr::relocate(ref, campus_ref, bp, campus, item, description) %>% 
  dplyr::rename(location = bp) -> rm_inv


# for 226

rm <- read_excel("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/Safety Stock Compliance/Weekly Run Files/2023/12.05.23/Inventory Report for all locations (RM).xlsx",
                 col_names = FALSE)


rm[-1:-2, ] -> rm
colnames(rm) <- rm[1, ]


rm %>% 
  janitor::clean_names() %>% 
  dplyr::slice(-1) %>% 
  dplyr::mutate(item = as.double(item)) %>% 
  dplyr::select(location, na_2, current_inventory_balance, campus, item) %>% 
  dplyr::mutate(ref = paste0(location, "_", item),
                campus_ref = paste0(campus, "_", item)) %>% 
  dplyr::rename(description = na_2,
                inventory = current_inventory_balance) %>% 
  dplyr::relocate(ref, campus_ref, location, campus, item, description, inventory) %>% 
  readr::type_convert() %>% 
  dplyr::filter(location == 226) -> rm


rbind(rm_inv, rm) -> rm_inv

rm_inv %>% 
  dplyr::mutate(ref = gsub("_", "-", ref),
                campus_ref = gsub("_", "-", campus_ref)) -> rm_inv

writexl::write_xlsx(rm_inv, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/12.05.2023/RM Inv.xlsx")


### FG

inv_bal %>% 
  janitor::clean_names() %>% 
  dplyr::slice(-1) %>% 
  dplyr::mutate(bp = as.double(bp)) %>% 
  dplyr::filter(bp != 226) %>% 
  dplyr::mutate(item_2 = item) %>% 
  dplyr::relocate(item, item_2) %>% 
  dplyr::mutate(item_2 = as.double(item_2)) %>% 
  dplyr::filter(is.na(item_2)) %>% 
  dplyr::select(-item_2) %>% 
  dplyr::select(bp, item, description, usable, soft_hold, hard_hold) %>% 
  dplyr::left_join(campus %>% select(location, campus) %>% rename(bp = location) %>% mutate(bp = as.double(bp))) %>% 
  dplyr::mutate(ref = paste0(bp, "_", item),
                campus_ref = paste0(campus, "_", item)) %>% 
  dplyr::rename(location = bp,
                Useable = usable,
                "Soft Hold" = soft_hold,
                "Hard Hold" = hard_hold) %>% 
  tidyr::pivot_longer(cols = c("Useable", "Soft Hold", "Hard Hold"),
                      names_to = "inventory_hold_status",
                      values_to = "inventory_qty_cases") %>% 
  dplyr::mutate(inventory_qty_cases = as.double(inventory_qty_cases),
                inventory_qty_cases = ifelse(is.na(inventory_qty_cases), 0, inventory_qty_cases)) %>% 
  dplyr::relocate(ref, campus_ref, location, campus, item, description, inventory_hold_status, inventory_qty_cases) -> fg_inv



# For Location 226
fg <- read_excel("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/Safety Stock Compliance/Weekly Run Files/2023/12.05.23/Inventory Report for all locations (FG).xlsx")


fg[-1,] -> fg
colnames(fg) <- fg[1, ]

fg %>% 
  janitor::clean_names() %>% 
  dplyr::slice(-1) %>% 
  dplyr::mutate(location = as.double(location)) %>% 
  dplyr::filter(location == 226) %>% 
  dplyr::rename(campus = product_manufacturing_location) %>% 
  dplyr::mutate(item = gsub("-", "", item)) %>% 
  dplyr::rename(inventory_hold_status = hold_status,
                inventory_qty_cases = current_inventory_balance) %>% 
  dplyr::mutate(ref = paste0(location, "_", item),
                campus_ref = paste0(campus, "_", item)) %>% 
  dplyr::select(ref, campus_ref, location, campus, item, na_2, inventory_hold_status, inventory_qty_cases) %>% 
  dplyr::rename(description = na_2) %>% 
  readr::type_convert() -> fg


rbind(fg_inv, fg) -> fg_inv

fg_inv %>% 
  dplyr::mutate(ref = gsub("_", "-", ref),
                campus_ref = gsub("_", "-", campus_ref)) -> fg_inv

writexl::write_xlsx(fg_inv, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/12.05.2023/FG Inv.xlsx")




# BoM
bom <- read_excel("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/BoM version 2/Weekly Run/12.05.2023/JDE BoM 12.05.23.xlsx",
                  sheet = "BoM")

bom[-1, ] -> bom
colnames(bom) <- bom[1, ]

bom %>% 
  dplyr::slice(-1) %>% 
  janitor::clean_names() %>% 
  dplyr::select(1:43) -> bom

writexl::write_xlsx(bom, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/12.05.2023/bom.xlsx")


# Exception Report
exception_report <- read_excel("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/Safety Stock Compliance/Weekly Run Files/2023/12.05.23/exception report.xlsx")

exception_report[-1:-2, ] -> exception_report

colnames(exception_report) <- exception_report[1, ]
exception_report[-1, ] -> exception_report


exception_report %>% 
  janitor::clean_names() -> exception_report
  
writexl::write_xlsx(exception_report, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/12.05.2023/exception_report.xlsx")



# download below
# https://edgeanalytics.venturafoods.com/MicroStrategyLibrary/app/DF007F1C11E9B3099BB30080EF7513D2/F5F6A922304E44E2C850A397EFDE78FB


# Lag 1 formula
# =ROUND(IFERROR(VLOOKUP($A8, '[dsx.xlsx]Sheet1'!$A:$Z, COLUMNS($AB$1:AE$1), 0), 0),0)



