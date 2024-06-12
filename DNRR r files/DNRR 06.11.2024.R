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




base::dir.create("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2024/06.11.2024")




# Read campus_ref 
campus <- read_excel("S:/Supply Chain Projects/Data Source (SCE)/Campus reference.xlsx")



#### Campus ####
campus %>% 
  janitor::clean_names() %>% 
  dplyr::select(location, campus) -> campus


###########################################################################################################################################

# BoM
bom <- read_excel("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/BoM version 2/Weekly Run/2024/06.11.2024/JDE BoM 06.11.2024.xlsx",
                  sheet = "BoM")

bom[-1, ] -> bom
colnames(bom) <- bom[1, ]

bom %>% 
  dplyr::slice(-1) %>% 
  janitor::clean_names() %>% 
  dplyr::select(1:31) -> bom



################## Lag 1 DSX #####################

dsx <- read_excel("S:/Global Shared Folders/Large Documents/S&OP/Demand Planning/BI Forecast Backup/2024/DSX Forecast Backup - 2024.05.01.xlsx")

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
  dplyr::mutate(mfg_ref = gsub("_", "-", mfg_ref)) %>% 
  dplyr::select(c(1, 4:15)) %>% 
  dplyr::rename_with(~letters[seq_along(.)], .cols = 2:ncol(.)) -> dsx_2


bom %>% 
  dplyr::rename(mfg_ref = ref) %>% 
  dplyr::left_join(dsx_2) %>%
  dplyr::mutate_at(vars(a:l), ~replace(., is.na(.), 0)) %>% 
  dplyr::mutate(quantity_w_scrap = as.numeric(quantity_w_scrap),
                next_28_days_open_order = as.numeric(next_28_days_open_order)) %>% 
  dplyr::mutate(A = pmax(next_28_days_open_order, a) * quantity_w_scrap,
                B = b * quantity_w_scrap,
                C = c * quantity_w_scrap,
                D = d * quantity_w_scrap,
                E = e * quantity_w_scrap,
                F = f * quantity_w_scrap,
                G = g * quantity_w_scrap,
                H = h * quantity_w_scrap,
                I = i * quantity_w_scrap,
                J = j * quantity_w_scrap,
                K = k * quantity_w_scrap,
                L = l * quantity_w_scrap) -> bom







writexl::write_xlsx(bom, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2024/06.11.2024/bom.xlsx")




######################### Exception Report
exception_report <- read_excel("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/Safety Stock Compliance/Weekly Run Files/2024/06.11.2024/exception report.xlsx")

exception_report[-1:-2, ] -> exception_report

colnames(exception_report) <- exception_report[1, ]
exception_report[-1, ] -> exception_report


exception_report %>% 
  janitor::clean_names() %>% 
  dplyr::mutate(b_p = as.double(b_p),
                item_number = as.double(item_number)) %>%
  dplyr::mutate(ref = paste0(b_p, "_", item_number)) %>%
  dplyr::left_join(campus %>% select(location, campus) %>% rename(b_p = location) %>% mutate(b_p = as.double(b_p))) %>% 
  dplyr::mutate(campus_ref = paste0(campus, "_", item_number)) %>% 
  dplyr::relocate(ref, campus_ref, campus) %>% 
  dplyr::mutate(ref = gsub("_", "-", ref),
                campus_ref = gsub("_", "-", campus_ref)) -> exception_report

writexl::write_xlsx(exception_report, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2024/06.11.2024/exception_report.xlsx")



###########################################################################################################################################
#################################################### Inventory RM & FG ####################################################################


inventory_rm <- read_excel("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/Safety Stock Compliance/Weekly Run Files/2024/06.11.2024/Inventory.xlsx",
                           sheet = "RM")

inventory_rm[-1, ] -> inventory_rm
colnames(inventory_rm) <- inventory_rm[1, ]
inventory_rm[-1, ] -> inventory_rm

inventory_rm %>% 
  janitor::clean_names() %>% 
  dplyr::mutate(item = gsub("-", "", item)) %>% 
  dplyr::mutate(ref = paste0(location, "_", item),
                campus_ref = paste0(campus_no, "_", item)) %>% 
  dplyr::select(ref, campus_ref, location, campus_no, item, description, current_inventory_balance) %>% 
  dplyr::mutate(current_inventory_balance = as.double(current_inventory_balance)) %>%
  dplyr::group_by(ref, campus_ref, location, campus_no, item, description) %>% 
  dplyr::summarise(inventory = sum(current_inventory_balance)) %>% 
  dplyr::rename(campus = campus_no) %>%
  dplyr::filter(grepl("^[0-9]+$", item)) %>% 
  dplyr::mutate(item = as.numeric(item)) %>% 
  dplyr::select(-ref, -campus_ref) %>% 
  dplyr::mutate(ref = paste0(location, "_", item),
                campus_ref = paste0(campus, "_", item)) %>% 
  dplyr::mutate(ref = gsub("_", "-", ref),
                campus_ref = gsub("_", "-", campus_ref)) %>% 
  filter(!str_starts(description, "PWS ") & 
           !str_starts(description, "SUB ") & 
           !str_starts(description, "THW ") & 
           !str_starts(description, "PALLET")) -> rm_inv




## 25, 55 label inventory add ##

jde_inv_for_25_55_label <- read_excel("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/Safety Stock Compliance/Weekly Run Files/2024/06.11.2024/JDE 25,55.xlsx")

jde_inv_for_25_55_label[-1:-5, ] -> jde_inv_for_25_55_label
colnames(jde_inv_for_25_55_label) <- jde_inv_for_25_55_label[1, ]
jde_inv_for_25_55_label[-1, ] -> jde_inv_for_25_55_label

jde_inv_for_25_55_label %>% 
  janitor::clean_names() %>% 
  dplyr::select(bp, item_number, description, on_hand) %>% 
  dplyr::mutate(on_hand = as.double(on_hand),
                campus = bp,
                ref = paste0(bp, "_", item_number),
                campus_ref = paste0(campus, "_", item_number)) %>%
  dplyr::rename(location = bp,
                item = item_number,
                inventory = on_hand) %>% 
  dplyr::relocate(ref, campus_ref, location, campus, item, description, inventory) %>% 
  dplyr::filter(grepl("^[0-9]+$", item)) %>% 
  dplyr::mutate(item = as.double(item)) %>% 
  dplyr::left_join(exception_report %>% 
                     janitor::clean_names() %>% 
                     dplyr::select(item_number, mpf_or_line) %>% 
                     dplyr::rename(item = item_number,
                                   label = mpf_or_line) %>% 
                     dplyr::mutate(item = as.double(item)) %>% 
                     dplyr::filter(label == "LBL") %>% 
                     dplyr::distinct(item, label), by = "item") %>% 
  dplyr::filter(!is.na(label)) %>% 
  dplyr::select(-label) %>% 
  dplyr::mutate(ref = gsub("_", "-", ref),
                campus_ref = gsub("_", "-", campus_ref)) -> label_inv_25_55


rbind(rm_inv, label_inv_25_55) -> rm_inv

writexl::write_xlsx(rm_inv, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2024/06.11.2024/RM Inv.xlsx")




### FG

inventory_fg <- read_excel("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/Safety Stock Compliance/Weekly Run Files/2024/06.11.2024/Inventory.xlsx",
                           sheet = "FG")

inventory_fg[-1, ] -> inventory_fg
colnames(inventory_fg) <- inventory_fg[1, ]
inventory_fg[-1, ] -> inventory_fg

inventory_fg %>% 
  janitor::clean_names() %>% 
  dplyr::mutate(item = gsub("-", "", item)) %>% 
  dplyr::mutate(ref = paste0(location, "_", item),
                campus_ref = paste0(campus_no, "_", item)) %>% 
  dplyr::select(ref, campus_ref, location, campus_no, item, description, inventory_hold_status, current_inventory_balance) %>% 
  dplyr::mutate(current_inventory_balance = as.double(current_inventory_balance)) %>%
  dplyr::group_by(ref, campus_ref, location, campus_no, item, description, inventory_hold_status) %>% 
  dplyr::summarise(inventory_qty_cases = sum(current_inventory_balance)) %>% 
  dplyr::rename(campus = campus_no) %>% 
  dplyr::filter(grepl("[A-Za-z]{3}$", item)) %>% 
  dplyr::mutate(ref = gsub("_", "-", ref),
                campus_ref = gsub("_", "-", campus_ref)) %>% 
  filter(!str_starts(description, "PWS ") & 
           !str_starts(description, "SUB ") & 
           !str_starts(description, "THW ") & 
           !str_starts(description, "PALLET")) -> fg_inv




writexl::write_xlsx(fg_inv, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2024/06.11.2024/FG Inv.xlsx")









jde_inv_for_25_55_label <- read_excel("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/Safety Stock Compliance/Weekly Run Files/2024/06.11.2024/JDE 25,55.xlsx")

jde_inv_for_25_55_label[-1:-5, ] -> jde_inv_for_25_55_label
colnames(jde_inv_for_25_55_label) <- jde_inv_for_25_55_label[1, ]
jde_inv_for_25_55_label[-1, ] -> jde_inv_for_25_55_label

jde_inv_for_25_55_label %>% 
  janitor::clean_names() %>% 
  dplyr::select(bp, item_number, description, on_hand) %>% 
  dplyr::mutate(on_hand = as.double(on_hand),
                campus = bp,
                ref = paste0(bp, "_", item_number),
                campus_ref = paste0(campus, "_", item_number)) %>%
  dplyr::rename(location = bp,
                item = item_number,
                inventory = on_hand) %>% 
  dplyr::relocate(ref, campus_ref, location, campus, item, description, inventory) %>% 
  dplyr::filter(grepl("^[0-9]+$", item)) %>% 
  dplyr::mutate(item = as.double(item)) %>% 
  dplyr::left_join(exception_report %>% 
                     janitor::clean_names() %>% 
                     dplyr::select(item_number, mpf_or_line) %>% 
                     dplyr::rename(item = item_number,
                                   label = mpf_or_line) %>% 
                     dplyr::mutate(item = as.double(item)) %>% 
                     dplyr::filter(label == "LBL") %>% 
                     dplyr::distinct(item, label), by = "item") %>% 
  dplyr::filter(!is.na(label)) %>% 
  dplyr::select(-label) %>% 
  dplyr::mutate(ref = gsub("_", "-", ref),
                campus_ref = gsub("_", "-", campus_ref)) -> label_inv_25_55


rbind(rm_inv, label_inv_25_55) -> rm_inv

writexl::write_xlsx(rm_inv, "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2024/06.11.2024/RM Inv.xlsx")




# download below
# https://edgeanalytics.venturafoods.com/MicroStrategyLibrary/app/DF007F1C11E9B3099BB30080EF7513D2/F5F6A922304E44E2C850A397EFDE78FB




file.copy("C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2024/06.04.2024/DNRR Tool ver.3 - 06.04.2024.xlsx",
          "C:/Users/slee/OneDrive - Ventura Foods/Ventura Work/SCE/Project/FY 23/DNRR Automation/DNRR Weekly Report/2024/06.11.2024/DNRR Tool ver.3 - 06.11.2024.xlsx")
