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
po <- read.csv("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DSXIE/2023/11.07/po.csv",
               header = FALSE)
receipt <- read.csv("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DSXIE/2023/11.07/receipt.csv",
                    header = FALSE)



# Read campus_ref 
campus <- read_excel("S:/Supply Chain Projects/RStudio/BoM/Master formats/RM_on_Hand/Campus_ref.xlsx")




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
  dplyr::rename(location = business_unit) %>% 
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


writexl::write_xlsx(po, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/11.09.2023/po.xlsx")
writexl::write_xlsx(receipt, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/11.09.2023/receipt.xlsx")



###########################################################################################################################################
###########################################################################################################################################
###########################################################################################################################################
###########################################################################################################################################
###########################################################################################################################################

#                                                              Inventory Analysis                                                         #

inv_analysis <- read_xlsx("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/11.09.2023/Inventory Analysis.xlsx")
manufacturers <- read_xlsx("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/11.09.2023/Loc - Manufacturing Loc - SKU.xlsx")

# inv_analysis ETL
inv_analysis %>% 
  dplyr::slice(-1) -> inv_analysis

colnames(inv_analysis) <- inv_analysis[1, ]

inv_analysis %>% 
  janitor::clean_names() %>% 
  dplyr::slice(-1) %>% 
  data.frame() %>% 
  dplyr::select(location, na_3, product_label_sku, na_2, inventory_hold_status, inventory_qty_cases) %>% 
  dplyr::rename(location_nm = na_3,
                item = product_label_sku,
                description = na_2,
                hold_status = inventory_hold_status,
                current_inventory_balance = inventory_qty_cases) %>% 
  dplyr::mutate(item = gsub("-", "", item),
                ref = paste0(location, "_", item)) %>% 
  dplyr::relocate(ref) -> inv_analysis_2


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
                mfg_ref = gsub("_", "-", mfg_ref)) -> inv_analysis_2

writexl::write_xlsx(inv_analysis_2, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2023/11.09.2023/FG inv.xlsx")



